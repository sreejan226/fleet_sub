from __future__ import annotations

import pytest

from generation_fleet.cloud import AuthorizationError, TagPolicyError
from generation_fleet.models import AUTHORIZED_TERMINATE, InstanceState, Pool


def test_launch_describe_boot(world):
    inst = world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=25,
    )
    assert inst.state == InstanceState.PENDING
    world.clock.advance(10)
    d = world.cloud.describe_instance(inst.id)
    assert d.state == InstanceState.BOOTING
    world.clock.advance(20)
    d = world.cloud.describe_instance(inst.id)
    assert d.state == InstanceState.RUNNING
    assert 20 <= d.boot_time <= 60


def test_terminate_requires_authorization(world):
    inst = world.cloud.launch_instance(
        "gen.small",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=1,
    )
    with pytest.raises(AuthorizationError):
        world.cloud.terminate_instance(inst.id, "nope")
    world.cloud.terminate_instance(inst.id, AUTHORIZED_TERMINATE)
    assert world.cloud.get(inst.id) is None


def test_missing_tags_rejected(world):
    with pytest.raises(TagPolicyError):
        world.cloud.launch_instance(
            "gen.large",
            {"env": "prod"},
            image_digest=world.parity.template.image_digest,
            environment_hash=world.parity.template.environment_hash,
        )


def test_queue_claim_complete_idempotent(world):
    job = world.queue.submit(Pool.IO, {})
    claimed = world.queue.claim(Pool.IO, "w1")
    assert claimed.id == job.id
    assert world.queue.complete(claimed) is True
    assert world.queue.complete(claimed) is False
    assert world.queue.get(job.id).state.value == "succeeded"


def test_lease_expiry_requeues(world):
    job = world.queue.submit(Pool.IO, {})
    world.queue.claim(Pool.IO, "w1")
    world.clock.advance(31)
    recovered = world.queue.requeue_expired_leases()
    assert job.id in recovered
    again = world.queue.claim(Pool.IO, "w2")
    assert again.id == job.id
    assert again.attempts == 2


def test_retry_then_fail(world):
    job = world.queue.submit(Pool.IO, {"force_fail": True}, max_attempts=2)
    w = world.warm.activate() or world.warm.launch_cold(boot_delay=0)
    world.clock.advance(3)
    world.cloud.describe_instance(w.id)
    worker = world.attach_worker(w.id, Pool.IO)
    worker.tick()
    worker.tick()
    worker.tick()
    worker.tick()
    final = world.queue.get(job.id)
    assert final.state.value == "failed"
    assert world.queue.stats()["retries"] >= 1
