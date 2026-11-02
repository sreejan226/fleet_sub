from __future__ import annotations

from generation_fleet.models import Pool


def _ready_worker(world, boot=2.0):
    inst = world.warm.activate() or world.warm.launch_cold(boot_delay=boot)
    world.clock.advance(boot + 0.1)
    inst = world.cloud.describe_instance(inst.id)
    return world.attach_worker(inst.id, inst.pool)


def test_drain_returns_unfinished_lease(world):
    worker = _ready_worker(world)
    job = world.queue.submit(Pool.IO, {"hold": True})
    worker.tick()
    assert worker.current_job is not None
    world.cloud.drain_instance(worker.instance_id)
    worker.graceful_shutdown()
    pending = world.queue.claim(Pool.IO, "other")
    assert pending.id == job.id
    world.queue.complete(pending)
    assert world.queue.complete(pending) is False


def test_warm_faster_than_cold(world):
    world.warm.replenish()
    cold = world.warm.launch_cold(boot_delay=45)
    warm = world.warm.activate()
    assert warm is not None
    assert warm.boot_delay < cold.boot_delay
    stats = world.warm.stats()
    assert stats["avg_cold_ready_s"] >= stats["avg_warm_ready_s"]


def test_backlog_scaler_scales_out(world):
    for _ in range(20):
        world.queue.submit(Pool.IO, {}, work_seconds=0.01)
    d = world.backlog.tick()
    assert d.action in {"scale_out", "cooldown", "noop"}
    assert d.desired_capacity >= 1
    booting = [i for i in world.cloud.list_instances() if i.state.value in {"PENDING", "BOOTING", "RUNNING"}]
    assert len(booting) >= 1


def test_scale_in_blocked_by_leases(world):
    worker = _ready_worker(world)
    world.queue.submit(Pool.IO, {"hold": True})
    worker.tick()
    world.backlog.min_capacity = 0
    world.backlog.last_scale_ts = 0
    d = world.backlog.tick()
    assert "lease" in d.reason.lower() or d.action in {"noop", "scale_in_drain", "hysteresis", "cooldown"}
    assert world.queue.get(worker.current_job.id).state.value == "leased"


def test_cost_guard_stops_forgotten_instance_under_budget(world):
    inst = world.cloud.launch_instance(
        "gen.gpu",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
        ttl_seconds=60,
        keep_until=False,
    )
    world.clock.advance(1)
    world.cloud.describe_instance(inst.id)
    world.cloud.change_utilization(inst.id, 0.01)
    world.clock.advance(120)
    actions = world.cost_guard.tick(account_budget=1_000_000)
    assert actions
    assert world.cloud.describe_instance(inst.id).state.value == "STOPPED"
    assert world.cloud.get(inst.id) is not None


def test_keep_until_not_stopped(world):
    inst = world.cloud.launch_instance(
        "gen.gpu",
        {"env": "prod", "service": "generation", "owner": "platform", "keep-until": "true"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
        ttl_seconds=10,
        keep_until=True,
    )
    world.clock.advance(50)
    world.cloud.describe_instance(inst.id)
    world.cost_guard.tick()
    assert world.cloud.describe_instance(inst.id).state.value in {"RUNNING", "BOOTING", "PENDING"}


def test_untagged_quarantine_zero_admitted_share(world):
    world.cloud.launch_instance(
        "gen.large",
        {},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
        admit_untagged=True,
    )
    tagged = world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
    )
    report = world.tags.report()
    assert report["untagged_share_admitted"] == 0.0
    assert report["quarantined"] >= 1
    assert tagged.id in {i.id for i in world.cloud.list_instances() if not i.quarantined}
