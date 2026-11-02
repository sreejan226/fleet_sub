from __future__ import annotations

import json
import random
import uuid
from typing import Any

from redis import Redis

from generation_fleet.clock import Clock
from generation_fleet.models import (
    AUTHORIZED_TERMINATE,
    INSTANCE_PRICES,
    MANDATORY_TAGS,
    Instance,
    InstanceState,
    Pool,
    SecurityGroup,
    SecurityRule,
    SpotStatus,
)
from generation_fleet.secrets import digest

ADMIN_PORTS = {22, 3389, 2375, 2376, 6443}


class CloudError(Exception):
    pass


class TagPolicyError(CloudError):
    pass


class AuthorizationError(CloudError):
    pass


class CloudSimulator:
    def __init__(self, redis: Redis, clock: Clock, *, boot_min: float = 20.0, boot_max: float = 60.0, rng: random.Random | None = None) -> None:
        self.r = redis
        self.clock = clock
        self.boot_min = boot_min
        self.boot_max = boot_max
        self.rng = rng or random.Random(42)
        self._ensure_default_sg()

    def _ikey(self, instance_id: str) -> str:
        return f"gf:instance:{instance_id}"

    def _save(self, inst: Instance) -> None:
        self.r.set(self._ikey(inst.id), inst.model_dump_json())
        self.r.sadd("gf:instances", inst.id)

    def get(self, instance_id: str) -> Instance | None:
        raw = self.r.get(self._ikey(instance_id))
        if not raw:
            return None
        return Instance.model_validate_json(raw)

    def list_instances(self) -> list[Instance]:
        ids = list(self.r.smembers("gf:instances") or [])
        out = []
        for i in ids:
            inst = self.get(i)
            if inst:
                out.append(self.refresh(inst))
        return out

    def refresh(self, inst: Instance) -> Instance:
        now = self.clock.now()
        changed = False
        if inst.state in (InstanceState.PENDING, InstanceState.BOOTING) and now >= inst.ready_at:
            inst.state = InstanceState.RUNNING
            inst.boot_time = inst.ready_at - inst.launch_time
            changed = True
        elif inst.state == InstanceState.PENDING and now < inst.ready_at:
            inst.state = InstanceState.BOOTING
            changed = True
        if inst.interrupt_warn_at and now >= inst.interrupt_warn_at and inst.state == InstanceState.RUNNING:
            inst.state = InstanceState.DRAINING
            inst.spot_status = SpotStatus.WARNING
            changed = True
        if inst.interrupt_at and now >= inst.interrupt_at and inst.state in (InstanceState.RUNNING, InstanceState.DRAINING):
            inst.state = InstanceState.INTERRUPTED
            inst.spot_status = SpotStatus.INTERRUPTED
            changed = True
        elapsed = max(0.0, now - inst.last_billing_time)
        if inst.state in (InstanceState.RUNNING, InstanceState.BOOTING, InstanceState.DRAINING, InstanceState.PENDING) and elapsed > 0:
            inst.accrued_cost += inst.hourly_price * (elapsed / 3600.0)
            inst.last_billing_time = now
            changed = True
        if changed:
            self._save(inst)
        return inst

    def launch_instance(
        self,
        instance_type: str,
        tags: dict[str, str],
        *,
        image_digest: str,
        environment_hash: str,
        environment_keys: list[str] | None = None,
        env_key_hashes: dict[str, str] | None = None,
        boot_delay: float | None = None,
        warm: bool = False,
        image_cached: bool = False,
        ttl_seconds: float = 3600.0,
        keep_until: bool = False,
        pool: Pool = Pool.IO,
        spot: bool = False,
        fail: bool = False,
        security_group_id: str = "sg-default",
        admit_untagged: bool = False,
    ) -> Instance:
        missing = [t for t in MANDATORY_TAGS if t not in tags or not tags[t]]
        quarantined = False
        reason = None
        if missing and not admit_untagged:
            raise TagPolicyError(f"missing mandatory tags: {missing}")
        if missing:
            quarantined = True
            reason = f"missing mandatory tags: {missing}"
        delay = boot_delay if boot_delay is not None else self.rng.uniform(self.boot_min, self.boot_max)
        if warm or image_cached:
            delay = min(delay, 3.0 if warm else 8.0)
        now = self.clock.now()
        inst_id = "i-" + uuid.uuid4().hex[:12]
        price = INSTANCE_PRICES.get(instance_type, 0.25)
        inst = Instance(
            id=inst_id,
            instance_type=instance_type,
            state=InstanceState.FAILED if fail else InstanceState.PENDING,
            launch_time=now,
            boot_delay=delay,
            ready_at=now if fail else now + delay,
            tags=dict(tags),
            image_digest=image_digest,
            environment_hash=environment_hash,
            environment_keys=list(environment_keys or []),
            env_key_hashes=dict(env_key_hashes or {}),
            hourly_price=price,
            ttl_seconds=ttl_seconds,
            keep_until=keep_until or tags.get("keep-until", "").lower() in {"1", "true", "yes"},
            warm=warm,
            image_cached=image_cached,
            last_heartbeat=now,
            last_billing_time=now,
            pool=pool,
            security_group_id=security_group_id,
            quarantined=quarantined,
            quarantine_reason=reason,
            spot_status=SpotStatus.MARKED if spot else SpotStatus.NONE,
        )
        if fail:
            inst.state = InstanceState.FAILED
        self._save(inst)
        self.r.incr("gf:billing:launches")
        return inst

    def describe_instance(self, instance_id: str) -> Instance:
        inst = self.get(instance_id)
        if not inst:
            raise CloudError(f"unknown instance {instance_id}")
        return self.refresh(inst)

    def stop_instance(self, instance_id: str) -> Instance:
        inst = self.describe_instance(instance_id)
        inst.state = InstanceState.STOPPED
        inst.utilization = 0.0
        self._save(inst)
        return inst

    def terminate_instance(self, instance_id: str, authorization: str) -> Instance:
        if authorization != AUTHORIZED_TERMINATE:
            raise AuthorizationError("terminate requires an authorized operation")
        inst = self.describe_instance(instance_id)
        self.r.delete(self._ikey(instance_id))
        self.r.srem("gf:instances", instance_id)
        inst.state = InstanceState.STOPPED
        return inst

    def tag_instance(self, instance_id: str, tags: dict[str, str]) -> Instance:
        inst = self.describe_instance(instance_id)
        inst.tags.update(tags)
        missing = [t for t in MANDATORY_TAGS if t not in inst.tags or not inst.tags[t]]
        if not missing:
            if inst.quarantined and inst.quarantine_reason and inst.quarantine_reason.startswith("missing"):
                inst.quarantined = False
                inst.quarantine_reason = None
        self._save(inst)
        return inst

    def change_utilization(self, instance_id: str, utilization: float) -> Instance:
        inst = self.describe_instance(instance_id)
        inst.utilization = max(0.0, min(1.0, utilization))
        inst.last_heartbeat = self.clock.now()
        self._save(inst)
        return inst

    def heartbeat(self, instance_id: str, worker_id: str | None = None) -> Instance:
        inst = self.describe_instance(instance_id)
        inst.last_heartbeat = self.clock.now()
        if worker_id:
            inst.worker_id = worker_id
        self._save(inst)
        return inst

    def drain_instance(self, instance_id: str) -> Instance:
        inst = self.describe_instance(instance_id)
        inst.state = InstanceState.DRAINING
        self._save(inst)
        return inst

    def schedule_spot_interruption(self, instance_id: str, warning_seconds: float = 120.0) -> Instance:
        inst = self.describe_instance(instance_id)
        now = self.clock.now()
        inst.spot_status = SpotStatus.MARKED
        inst.interrupt_warn_at = now
        inst.interrupt_at = now + warning_seconds
        inst.state = InstanceState.DRAINING
        inst.spot_status = SpotStatus.WARNING
        self._save(inst)
        return inst

    def update_security_groups(self, instance_id: str, sg_id: str) -> Instance:
        inst = self.describe_instance(instance_id)
        inst.security_group_id = sg_id
        self._save(inst)
        return inst

    def put_security_group(self, sg: SecurityGroup) -> None:
        self.r.set(f"gf:sg:{sg.id}", sg.model_dump_json())
        self.r.sadd("gf:sgs", sg.id)

    def get_security_group(self, sg_id: str) -> SecurityGroup:
        raw = self.r.get(f"gf:sg:{sg_id}")
        if not raw:
            raise CloudError(f"unknown sg {sg_id}")
        return SecurityGroup.model_validate_json(raw)

    def list_security_groups(self) -> list[SecurityGroup]:
        return [self.get_security_group(i) for i in self.r.smembers("gf:sgs") or []]

    def generate_billing_records(self) -> list[dict[str, Any]]:
        records = []
        for inst in self.list_instances():
            records.append(
                {
                    "instance_id": inst.id,
                    "hourly_price": inst.hourly_price,
                    "accrued_cost": round(inst.accrued_cost, 6),
                    "instance_type": inst.instance_type,
                    "env": inst.tags.get("env"),
                    "service": inst.tags.get("service"),
                }
            )
        return records

    def issue_credentials(self, scope: str, ttl_seconds: float, environment: str) -> dict[str, str]:
        secret = uuid.uuid4().hex + uuid.uuid4().hex
        cred_id = "cred-" + uuid.uuid4().hex[:10]
        rec = {
            "id": cred_id,
            "scope": scope,
            "environment": environment,
            "hash": digest(secret),
            "expires_at": self.clock.now() + ttl_seconds,
            "revoked": False,
        }
        self.r.set(f"gf:cred:{cred_id}", json.dumps(rec))
        self.r.sadd("gf:creds", cred_id)
        return {"id": cred_id, "secret": secret, "expires_at": str(rec["expires_at"])}

    def validate_credentials(self, cred_id: str, secret: str) -> bool:
        raw = self.r.get(f"gf:cred:{cred_id}")
        if not raw:
            return False
        rec = json.loads(raw)
        if rec.get("revoked"):
            return False
        if rec["expires_at"] < self.clock.now():
            return False
        return rec["hash"] == digest(secret)

    def revoke_credentials(self, cred_id: str) -> None:
        raw = self.r.get(f"gf:cred:{cred_id}")
        if not raw:
            return
        rec = json.loads(raw)
        rec["revoked"] = True
        rec.pop("secret", None)
        self.r.set(f"gf:cred:{cred_id}", json.dumps(rec))

    def inject_failure(self, instance_id: str) -> Instance:
        inst = self.describe_instance(instance_id)
        inst.state = InstanceState.FAILED
        self._save(inst)
        return inst

    def _ensure_default_sg(self) -> None:
        if self.r.get("gf:sg:sg-default"):
            return
        self.put_security_group(
            SecurityGroup(
                id="sg-default",
                rules=[SecurityRule(port=443, cidr="10.0.0.0/8", description="https internal")],
                internal_service=False,
            )
        )
