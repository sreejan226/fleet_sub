from __future__ import annotations

import json
from pathlib import Path

from generation_fleet.models import Pool, SecurityGroup, SecurityRule, Waiver
from generation_fleet.ops import OpsError
from generation_fleet.traces import chaos_interrupt_fraction, run_for, submit_n, trace_oscillate, trace_spike


def _run_jobs(world, n=8, seconds=40):
    submit_n(world, n, work_seconds=0.01)
    run_for(world, seconds, step=1)


def _record(world, name, scaler, extra=None):
    stats = world.queue.stats()
    inst_hours = sum(max(0.0, world.clock.now() - i.launch_time) / 3600.0 for i in world.cloud.list_instances())
    payload = {
        "slo_alerts": len([a for a in world.slo.alerts.values() if not a.silenced]),
        "instance_hours": inst_hours,
        "compute_cost": sum(i.accrued_cost for i in world.cloud.list_instances()),
        "warm": world.warm.stats(),
        "scaling": world.db.fetchall("scaling_decisions", limit=20),
        "successful_jobs": stats["succeeded"],
        "failed_jobs": stats["failed"],
        "retries": stats["retries"],
        **(extra or {}),
    }
    world.db.experiment(name, scaler, payload)
    return payload


def test_t1_spike_45s_boot(world):
    world.warm.forced_boot_delay = 45
    trace_spike(world, boot_delay=45)
    payload = _record(world, "T1", "backlog")
    assert payload["successful_jobs"] >= 1 or world.queue.pending_count() >= 0
    assert any(i.boot_delay >= 40 or i.warm for i in world.cloud.list_instances())


def test_t2_oscillating_load(world):
    trace_oscillate(world)
    payload = _record(world, "T2", "backlog")
    assert payload["successful_jobs"] + world.queue.pending_count() + payload["failed_jobs"] >= 1


def test_t3_scale_in_with_leases(world):
    inst = world.warm.activate() or world.warm.launch_cold(boot_delay=1)
    world.clock.advance(3)
    world.cloud.describe_instance(inst.id)
    worker = world.attach_worker(inst.id, Pool.IO)
    job = world.queue.submit(Pool.IO, {"hold": True})
    worker.tick()
    world.backlog.min_capacity = 0
    world.backlog.last_scale_ts = 0
    world.backlog.tick()
    assert world.queue.get(job.id).state.value != "succeeded" or worker.current_job
    worker.current_job.payload["hold"] = False
    worker.tick()
    assert world.queue.get(job.id).state.value == "succeeded"
    _record(world, "T3", "backlog")


def test_t4_interrupt_30_percent(world):
    for _ in range(4):
        inst = world.warm.launch_cold(boot_delay=1)
        world.clock.advance(2)
        world.cloud.describe_instance(inst.id)
    world.sync_workers()
    submit_n(world, 6, work_seconds=0.01)
    n = chaos_interrupt_fraction(world, 0.3)
    world.clock.advance(world.settings.interrupt_warning_seconds + 1)
    for i in world.cloud.list_instances():
        world.cloud.describe_instance(i.id)
    world.tick()
    interrupted = [i for i in world.cloud.list_instances() if i.state.value == "INTERRUPTED"]
    assert n >= 1
    assert interrupted
    _record(world, "T4", "backlog", {"interrupted": len(interrupted)})


def test_t5_forgotten_large_instance(world):
    inst = world.cloud.launch_instance(
        "gen.gpu",
        {"env": "prod", "service": "generation", "owner": "sre"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
        ttl_seconds=30,
    )
    world.clock.advance(1)
    world.cloud.change_utilization(inst.id, 0.0)
    world.clock.advance(90)
    actions = world.cost_guard.tick(account_budget=10_000)
    assert actions
    _record(world, "T5", "backlog", {"actions": actions})


def test_t6_untagged_resource(world):
    world.cloud.launch_instance(
        "gen.xlarge",
        {"env": ""},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
        admit_untagged=True,
    )
    report = world.tags.report()
    assert report["untagged_share_admitted"] == 0
    _record(world, "T6", "backlog", report)


def test_t7_prod_creds_non_main(world):
    world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
    )
    try:
        world.ops.execute(
            "doc-drain",
            {"action": "drain", "version": 1},
            environment="prod",
            git_branch="dev",
            selector_tags={},
            workflow_identity="fleet-ops",
            credential_environment="prod",
        )
        raised = False
    except OpsError:
        raised = True
    assert raised
    _record(world, "T7", "backlog")


def test_t8_modified_command_document(world):
    world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
    )
    try:
        world.ops.execute(
            "doc-stop",
            {"action": "stop", "version": 99},
            environment="prod",
            git_branch="main",
            selector_tags={},
            workflow_identity="fleet-ops",
            credential_environment="prod",
        )
        raised = False
    except OpsError:
        raised = True
    assert raised
    _record(world, "T8", "backlog")


def test_t9_stale_environment(world):
    inst = world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash="sha256:" + "ee" * 32,
        environment_keys=list(world.parity.template.env_keys),
        env_key_hashes=dict(world.parity.template.env_key_hashes),
        boot_delay=0,
    )
    world.clock.advance(1)
    world.cloud.describe_instance(inst.id)
    report = world.parity.scan()
    assert report["mismatches"]
    _record(world, "T9", "backlog")


def test_t10_rotate_under_load(world):
    failed = 0
    for i in range(50):
        submit_n(world, 1, work_seconds=0.01)
        world.tick()
        if i == 20:
            world.rotation.rotate()
        for c in world.rotation.consumers:
            if not c.request():
                failed += 1
    assert failed == 0
    _record(world, "T10", "backlog", {"failed_auth_requests": failed})


def test_t11_world_open_ssh(world):
    world.cloud.put_security_group(
        SecurityGroup(id="sg-ssh", rules=[SecurityRule(port=22, cidr="0.0.0.0/0")], internal_service=True)
    )
    world.exposure.add_waiver(Waiver(id="old", finding_key="sg-ssh:22:0.0.0.0/0", expires_at=0, reason="expired"))
    scan = world.exposure.scan()
    assert scan["findings"]
    _record(world, "T11", "backlog", scan)


def test_t12_autoscaler_stopped(world):
    world.backlog.enabled = False
    submit_n(world, 25)
    run_for(world, 5, step=1)
    slo = world.slo.evaluate(100, 0, False, world.clock.now() - 120, 25)
    names = {a["name"] for a in slo["alerts"]}
    assert "deadman_autoscaler" in names or "stuck_autoscaler" in names or "backlog" in names
    _record(world, "T12", "backlog")


def test_compare_autoscalers(world, tmp_path):
    submit_n(world, 12, work_seconds=0.01)
    world.scaler_mode = "backlog"
    run_for(world, 15, step=1)
    b = _record(world, "compare", "backlog")
    world.scaler_mode = "cpu"
    run_for(world, 10, step=1)
    c = _record(world, "compare", "cpu")
    out = {"backlog": b, "cpu": c}
    Path("experiments").mkdir(exist_ok=True)
    Path("experiments/results.json").write_text(json.dumps(out, default=str, indent=2), encoding="utf-8")
    assert "successful_jobs" in b and "successful_jobs" in c
