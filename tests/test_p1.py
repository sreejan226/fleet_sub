from __future__ import annotations

import pytest

from generation_fleet.models import SecurityGroup, SecurityRule, Waiver
from generation_fleet.ops import OpsError, content_hash


def test_ops_rejects_non_main_prod_creds(world):
    inst = world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
    )
    world.clock.advance(1)
    world.cloud.describe_instance(inst.id)
    with pytest.raises(OpsError, match="main branch"):
        world.ops.execute(
            "doc-health_check",
            {"action": "health_check", "version": 1},
            environment="prod",
            git_branch="feature/x",
            selector_tags={"service": "generation"},
            workflow_identity="fleet-ops",
            credential_environment="prod",
        )


def test_ops_rejects_modified_document(world):
    inst = world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
    )
    world.clock.advance(1)
    world.cloud.describe_instance(inst.id)
    with pytest.raises(OpsError, match="hash"):
        world.ops.execute(
            "doc-health_check",
            {"action": "health_check", "version": 2, "extra": "tamper"},
            environment="prod",
            git_branch="main",
            selector_tags={"service": "generation"},
            workflow_identity="fleet-ops",
            credential_environment="prod",
        )
    assert content_hash({"action": "health_check", "version": 1}) == world.registry.get("doc-health_check").content_hash


def test_ops_selects_by_tags_not_ids(world):
    world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        boot_delay=0,
    )
    world.clock.advance(1)
    result = world.ops.execute(
        "doc-health_check",
        {"action": "health_check", "version": 1},
        environment="prod",
        git_branch="main",
        selector_tags={"owner": "platform"},
        workflow_identity="fleet-ops",
        credential_environment="prod",
    )
    assert result["ok"]
    audit = world.db.fetchall("audit_events")
    blob = str(audit)
    assert "secret" not in blob.lower() or "[REDACTED]" in blob


def test_stale_env_quarantine(world):
    inst = world.cloud.launch_instance(
        "gen.large",
        {"env": "prod", "service": "generation", "owner": "platform"},
        image_digest=world.parity.template.image_digest,
        environment_hash=world.parity.template.environment_hash,
        environment_keys=list(world.parity.template.env_keys),
        env_key_hashes={"LOG_LEVEL": "sha256:" + "ff" * 32, "MODEL_ID": world.parity.template.env_key_hashes["MODEL_ID"]},
        boot_delay=0,
    )
    world.clock.advance(1)
    world.cloud.describe_instance(inst.id)
    report = world.parity.scan()
    assert report["mismatches"]
    issue = report["mismatches"][0]["issues"]
    assert any(i.get("key") == "LOG_LEVEL" for i in issue)
    assert world.cloud.describe_instance(inst.id).quarantined
    dumped = str(report)
    assert "password" not in dumped
    assert "secret" not in dumped


def test_unpinned_image_rejected():
    from generation_fleet.parity import DesiredTemplate

    with pytest.raises(ValueError):
        DesiredTemplate("myimage:latest", ["A"], {}, "sha256:x")


def test_rotation_zero_failed_requests(world):
    failed = 0
    total = 0
    for i in range(80):
        if i == 30:
            out = world.rotation.rotate()
            assert out["ok"]
        for c in world.rotation.consumers:
            total += 1
            if not c.request():
                failed += 1
    assert failed == 0
    assert total == 80 * 3
    journal = world.db.fetchall("rotation_journal")
    assert journal
    assert "secret" not in str(journal).lower()


def test_rotation_rollback_on_interrupt(world):
    old = world.rotation.current_id
    world.rotation.rotate(interrupt_after="dual_accept")
    assert world.rotation.current_id == old
    assert all(c.request() for c in world.rotation.consumers)


def test_exposure_ssh_and_expired_waiver(world):
    world.cloud.put_security_group(
        SecurityGroup(
            id="sg-open",
            rules=[SecurityRule(port=22, cidr="0.0.0.0/0", description="ssh")],
            internal_service=True,
        )
    )
    world.exposure.add_waiver(Waiver(id="w-exp", finding_key="sg-open:22:0.0.0.0/0", expires_at=world.clock.now() - 10, reason="temp"))
    first = world.exposure.scan()
    assert any(f["type"] in {"world_open_ssh", "expired_waiver", "public_internal_service"} for f in first["findings"])
    second = world.exposure.scan()
    assert second["new"] == []
