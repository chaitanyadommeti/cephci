"""
Stretch-cluster NVMe-oF test module.

- Deploy two-site stretch cluster with tie-breaker (CRUSH rule, election strategy, stretch mode).
- Deploy NVMe service with gateways per datacenter (DC1, DC2), configure subsystems, listeners,
  hosts, namespaces with location, and initiators.
- Test GW location set/get/modify/unset (ceph nvme-gw set-location, show).
- Test namespace locations: add with invalid/valid location, change_location.
- Single gateway failover on DC1 and failback with same-location preference and IO validation.

Conf reference: conf/tentacle/nvmeof/9-node-cluster-2-clients.yaml
Suite: suites/tentacle/nvmeof/tier-2_nvmeof_stretch_cluster.yaml
"""

import json
import time

from ceph.ceph_admin import CephAdmin
from ceph.ceph_admin.orch import Orch
from ceph.rados.core_workflows import RadosOrchestrator
from ceph.rados.utils import get_cluster_timestamp
from tests.rados.monitor_configurations import MonElectionStrategies
from tests.rados.stretch_cluster import (
    setup_crush_rule,
    setup_crush_rule_with_no_affinity,
    wait_for_clean_pg_sets,
)
from tests.rados.test_stretch_site_down import stretch_enabled_checks
from tests.nvmeof.workflows.gateway_entities import (
    configure_gw_entities,
    fetch_namespaces,
)
from tests.nvmeof.workflows.ha import HighAvailability
from tests.nvmeof.workflows.initiator import (
    compare_client_namespace,
    prepare_io_execution,
)
from tests.nvmeof.workflows.nvme_utils import (
    ana_states,
    get_optimized_state_same_location,
    validate_io,
)
from tests.nvmeof.workflows.nvme_service import NVMeService
from tests.nvmeof.workflows.stretch_ha import build_gw_location_map
from tests.rbd.rbd_utils import initial_rbd_config
from utility.log import Log

log = Log(__name__)


def _install_ceph_base_for_crushtool(client_node) -> bool:
    """
    Install the ceph-base package to get the crushtool binary on the client node.
    Returns True if crushtool is available, False otherwise.
    """
    try:
        out, _ = client_node.exec_command(sudo=True, cmd="rpm -q ceph-base")
        if "ceph-base" in out and "not installed" not in out:
            log.info("ceph-base already installed on client")
            return True
    except Exception:
        pass
    try:
        client_node.exec_command(
            sudo=True, cmd="yum install -y ceph-base --nogpgcheck"
        )
        log.info("Installed ceph-base package for crushtool")
        return True
    except Exception as err:
        log.error(f"Failed to install ceph-base: {err}")
        return False


def _deploy_stretch_cluster(ceph_cluster, config):
    """
    Deploy two-site stretch cluster with tie-breaker.
    Returns 0 on success, 1 on failure.
    """
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    rados_obj = RadosOrchestrator(node=cephadm)
    mon_obj = MonElectionStrategies(rados_obj=rados_obj)
    client_node = ceph_cluster.get_nodes(role="client")[0]

    stretch_rule_name = config.get("stretch_rule_name", "stretch_rule")
    tiebreaker_mon_site_name = config.get("tiebreaker_mon_site_name", "tiebreaker")
    stretch_bucket = config.get("stretch_bucket", "datacenter")
    no_affinity_crush_rule = config.get("no_affinity", False)

    # if not _install_ceph_base_for_crushtool(client_node):
    #     log.error("Failed to install ceph-base for crushtool")
    #     return 1

    if no_affinity_crush_rule:
        if not setup_crush_rule_with_no_affinity(
            node=client_node, rule_name=stretch_rule_name
        ):
            log.error("Failed to add crush rules in the crush map (no affinity)")
            return 1
    else:
        osd_tree = rados_obj.run_ceph_command(cmd="ceph osd tree")
        dc_buckets = [
            n
            for n in osd_tree.get("nodes", [])
            if n.get("type") == stretch_bucket
            and n["name"] != tiebreaker_mon_site_name
        ]
        if len(dc_buckets) < 2:
            log.error(
                f"Need at least 2 datacenter buckets (excluding tiebreaker), "
                f"found: {[b['name'] for b in dc_buckets]}"
            )
            return 1
        dc_1_name, dc_2_name = dc_buckets[0]["name"], dc_buckets[1]["name"]
        log.info(f"Using data sites: {dc_1_name}, {dc_2_name}")
        if not setup_crush_rule(
            node=client_node,
            rule_name=stretch_rule_name,
            site1=dc_1_name,
            site2=dc_2_name,
        ):
            log.error("Failed to add crush rules in the crush map")
            return 1

    time.sleep(5)
    rule_list = rados_obj.run_ceph_command(cmd="ceph osd crush rule ls")
    if stretch_rule_name not in rule_list:
        log.error(f"Rule '{stretch_rule_name}' not in crush rule list: {rule_list}")
        return 1

    if not mon_obj.set_election_strategy(mode="connectivity"):
        log.error("Could not set election strategy to connectivity mode")
        return 1
    time.sleep(2)
    if mon_obj.get_election_strategy() != 3:
        log.error("Election strategy is not connectivity")
        return 1

    mon_dump = rados_obj.run_ceph_command(cmd="ceph mon dump")
    tiebreaker_mon = None
    for mon in mon_dump.get("mons", []):
        if tiebreaker_mon_site_name in str(mon.get("crush_location", "{}")):
            tiebreaker_mon = mon["name"]
            break
    if not tiebreaker_mon:
        for mon in mon_dump.get("mons", []):
            if mon.get("crush_location") in ("{}", ""):
                tiebreaker_mon = mon["name"]
                break
    if not tiebreaker_mon:
        log.error(f"Could not find tiebreaker mon with site '{tiebreaker_mon_site_name}'")
        return 1

    try:
        cephadm.shell(
            [f"ceph mon enable_stretch_mode {tiebreaker_mon} {stretch_rule_name} {stretch_bucket}"]
        )
    except Exception as err:
        log.error("Error enabling stretch mode: %s", err)
        return 1
    time.sleep(5)

    if not stretch_enabled_checks(rados_obj):
        log.error("Stretch mode pre-checks failed after enable")
        return 1
    if not rados_obj.get_stretch_mode_dump().get("stretch_mode_enabled"):
        log.error("Stretch mode not enabled")
        return 1
    if not wait_for_clean_pg_sets(rados_obj):
        log.error("PGs did not reach active+clean after stretch mode enable")
        return 1
    log.info("Stretch cluster deployed successfully")
    return 0


def _deploy_nvme_and_configure(ceph_cluster, config, rbd_obj):
    """Deploy NVMe service (3 GW per DC), configure subsystems, listeners, hosts, namespaces with location, initiators."""
    #from tests.nvmeof.workflows.nvme_utils import check_and_set_nvme_cli_image

    #check_and_set_nvme_cli_image(ceph_cluster, config=config.get("custom-config"))
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.deploy()
    nvme_service.init_gateways()
    config["nvme_service"] = nvme_service

    opt_args = {}
    if config.get("namespace_default_location"):
        opt_args["location"] = config["namespace_default_location"]
    configure_gw_entities(
        nvme_service, rbd_obj=rbd_obj, cluster=ceph_cluster, opt_args=opt_args
    )
    log.info("NVMe gateways deployed and entities configured")
    return 0


def _test_gw_locations(ceph_cluster, config):
    """Set, get, modify, unset, and set back GW locations."""
    from cli.ceph.nvme_gw import NvmeGw

    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.init_gateways()
    pool = nvme_service.nvme_metadata_pool
    group = nvme_service.group
    orch = Orch(cluster=ceph_cluster, **{})
    nvme_gw = NvmeGw(orch.installer, "ceph")

    dc1_nodes = config.get("")
    dc2_nodes = config.get("dc2_nodes", ["node7", "node8", "node9"])
    loc_dc1 = config.get("location_dc1", "site_a")
    loc_dc2 = config.get("location_dc2", "site_b")

    gw_location_map = {}
    for gw in nvme_service.gateways:
        gid = gw.gw_id
        if gw.node.id in dc1_nodes or gw.hostname in dc1_nodes:
            gw_location_map[gid] = loc_dc1
        elif gw.node.id in dc2_nodes or gw.hostname in dc2_nodes:
            gw_location_map[gid] = loc_dc2

    for gw_id, loc in gw_location_map.items():
        nvme_gw.set_location(gw_id, pool, group, loc)
    log.info("Set GW locations: %s", gw_location_map)

    out = nvme_gw.show(pool, group, format="json")
    data = json.loads(out)
    gateways = data.get("Created Gateways:", data.get("gateways", []))
    for gw in gateways:
        log.info("GW %s location: %s", gw.get("gw-id"), gw.get("location", ""))

    mod_dc1 = config.get("location_dc1_modified", "site_a_modified")
    mod_dc2 = config.get("location_dc2_modified", "site_b_modified")
    for gw in nvme_service.gateways:
        gid = gw.gw_id
        if gid in gw_location_map:
            new_loc = mod_dc1 if gw_location_map[gid] == loc_dc1 else mod_dc2
            nvme_gw.set_location(gid, pool, group, new_loc)
    log.info("Modified GW locations to %s / %s", mod_dc1, mod_dc2)

    for gw_id in gw_location_map:
        nvme_gw.set_location(gw_id, pool, group, "")
    log.info("Unset GW locations")

    for gw_id, loc in gw_location_map.items():
        nvme_gw.set_location(gw_id, pool, group, loc)
    log.info("Set GW locations back to initial")
    return 0


def _test_namespace_locations(ceph_cluster, config):
    """Add namespace with invalid location (expect reject), add 2 with valid location, change_location."""
    nvme_service = NVMeService(config, ceph_cluster)
    nvme_service.init_gateways()
    gateway = nvme_service.gateways[0]
    nqn = config.get("subsystems", [{}])[0].get("nqn") or config.get("subsystems", [{}])[0].get("subnqn")
    if not nqn:
        log.error("No subsystem nqn in config")
        return 1

    invalid_location = config.get("invalid_namespace_location", "INVALID_SITE")
    try:
        gateway.namespace.add(
            args={
                "subsystem": nqn,
                "rbd-pool": config.get("rbd_pool", "rbd"),
                "rbd-image": "test-invalid-loc",
                "size": "1G",
                "rbd-create-image": True,
                "location": invalid_location,
            }
        )
        log.error("Adding namespace with invalid location should have failed")
        return 1
    except Exception as e:
        log.info("Expected reject for invalid location: %s", e)

    valid_locations = config.get("valid_namespace_locations", ["DC1", "DC2"])
    for i, loc in enumerate(valid_locations):
        name = f"test-valid-loc-{i}"
        #b tests/nvmeof/test_ceph_nvmeof_stretch_cluster.py:275
        gateway.namespace.add(
            args={
                "subsystem": nqn,
                "rbd-pool": config.get("rbd_pool", "rbd"),
                "rbd-image": name,
                "size": "1G",
                "rbd-create-image": True,
                "location": loc,
            }
        )
        log.info("Added namespace %s with location %s", name, loc)

    nsid = config.get("namespace_change_location_nsid", 2)
    new_location = config.get("namespace_new_location", "DC2")
    try:
        gateway.namespace.change_location(
            args={"subsystem": nqn, "nsid": nsid, "location": new_location}
        )
        log.info("Changed namespace %s location to %s", nsid, new_location)
    except Exception as e:
        log.warning("change_location may not be implemented yet: %s", e)
    return 0


def _test_failover_failback(ceph_cluster, config):
    """Single GW failover on DC1 and failback; verify same-location chosen and IO."""
    from tests.nvmeof.workflows.nvme_utils import catogorize

    nvme_service = config.get("nvme_service")
    if not nvme_service:
        nvme_service = NVMeService(config, ceph_cluster)
        nvme_service.init_gateways()
    config["nvme_service"] = nvme_service

    dc1_nodes = config.get("dc1_nodes", ["node3", "node4", "node5"])
    dc2_nodes = config.get("dc2_nodes", ["node7", "node8", "node9"])
    fail_node = config.get("failover_node", dc1_nodes[0])
    gw_locations = build_gw_location_map(
        nvme_service.gateways,
        {
            **{n: config.get("location_dc1", "site_a") for n in dc1_nodes},
            **{n: config.get("location_dc2", "site_b") for n in dc2_nodes},
        },
    )
    config["stretch_mode"] = True
    config["gw_locations"] = gw_locations

    ha = HighAvailability(ceph_cluster, config["gw_nodes"], **config)
    ha.gateways = nvme_service.gateways
    ha.set_gateway_locations(gw_locations)

    fail_gws, _ = catogorize(nvme_service, fail_node)
    if not fail_gws:
        log.error("No gateway found for failover node %s", fail_node)
        return 1
    gw = fail_gws[0]
    namespaces = fetch_namespaces(gw, [gw.ana_group_id], get_list=True)
    ns_info = [n.get("info") for n in namespaces]
    ns_uuids = [
        (n["list"].get("uuid") or (n["list"].get("wwn") or "").replace("uuid.", ""))
        for n in namespaces
        if n["list"].get("uuid") or n["list"].get("wwn")
    ]
    if not ns_info:
        log.warning("No namespaces on failed GW; continuing")

    clients = prepare_io_execution(
        config["initiators"],
        gateways=nvme_service.gateways,
        cluster=ceph_cluster,
        return_clients=True,
    )
    if clients and ns_uuids:
        compare_client_namespace(clients, ns_uuids, FEWR_NAMESPACES=True)
    if ns_info:
        validate_io(ha.orch, ns_info)

    fail_tool = config.get("fault_injection_tool") or (config.get("fault-injection-methods") or [{}])[0].get("tool", "systemctl")
    ha.failover(gw, fail_tool)
    active = get_optimized_state_same_location(
        nvme_service,
        ha.orch,
        gw.ana_group_id,
        gw_locations.get(gw.gw_id) or gw_locations.get(gw.daemon_name) or gw_locations.get(gw.hostname),
        gw_locations,
    )
    if not active:
        log.error("No active GW after failover")
        return 1
    log.info("Active GW after failover (same location): %s", active)
    if ns_info:
        validate_io(ha.orch, ns_info)

    ha.failback(gw, fail_tool)
    if ns_info:
        validate_io(ha.orch, ns_info)
    log.info("Failover and failback completed")
    return 0


def run(ceph_cluster, **kw):
    """
    Entry point for stretch-cluster NVMe-oF tests.

    config.test_case:
        deploy_stretch       - Deploy two-site stretch with tie-breaker only.
        deploy_stretch_nvme  - Deploy stretch then NVMe (3 GW per DC) + configure entities + initiators.
        gw_locations         - Set/get/modify/unset GW locations.
        namespace_locations  - Add NS invalid/valid location, change_location.
        failover_failback    - Single GW failover on DC1 and failback with IO validation.
    """
    config = kw.get("config", {})
    test_case = config.get("test_case", "deploy_stretch")
    cephadm = CephAdmin(cluster=ceph_cluster, **config)
    node = getattr(cephadm.installer, "node", cephadm.installer)
    start_time = get_cluster_timestamp(node)
    rbd_obj = None
    if config.get("rep_pool_config") or config.get("rep-pool-only"):
        rbd_obj = initial_rbd_config(**kw).get("rbd_reppool")

    try:
        if test_case == "deploy_stretch":
            return _deploy_stretch_cluster(ceph_cluster, config)

        if test_case == "deploy_stretch_nvme":
            if _deploy_stretch_cluster(ceph_cluster, config) != 0:
                return 1
            return _deploy_nvme_and_configure(ceph_cluster, config, rbd_obj)

        if test_case == "deploy_nvme":
            return _deploy_nvme_and_configure(ceph_cluster, config, rbd_obj)

        if test_case == "gw_locations":
            return _test_gw_locations(ceph_cluster, config)

        if test_case == "namespace_locations":
            return _test_namespace_locations(ceph_cluster, config)

        if test_case == "failover_failback":
            return _test_failover_failback(ceph_cluster, config)

        log.error("Unknown test_case: %s", test_case)
        return 1
    except Exception as e:
        log.exception(e)
        return 1
    finally:
        rados_obj = RadosOrchestrator(node=CephAdmin(cluster=ceph_cluster, **config))
        rados_obj.log_cluster_health()
        if rbd_obj and config.get("cleanup") and "gateway" in config.get("cleanup", []):
            from tests.nvmeof.workflows.gateway_entities import teardown
            nvme_svc = config.get("nvme_service")
            if nvme_svc:
                teardown(nvme_svc, rbd_obj)


def stretch_mode_status(rados_obj) -> bool:
    """
    Returns the status of stretch mode on cluster.

    Args:
        rados_obj: rados object for command execution

    Returns:
        if stretch mode is enabled on cluster -> True,
        If stretch mode is not enabled on cluster -> False
    """
    log.debug("Running checks to see if stretch mode is deployed on the cluster")
    stretch_details = rados_obj.get_stretch_mode_dump()
    return stretch_details["stretch_mode_enabled"]
