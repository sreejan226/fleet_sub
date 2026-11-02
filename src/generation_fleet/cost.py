from __future__ import annotations

from collections import defaultdict

from generation_fleet.clock import Clock
from generation_fleet.cloud import CloudSimulator
from generation_fleet.db import Database
from generation_fleet.models import InstanceState, MANDATORY_TAGS
from generation_fleet.queue import JobQueue


class CostGuard:
    def __init__(self, cloud: CloudSimulator, clock: Clock, db: Database, util_threshold: float = 0.05) -> None:
        self.cloud = cloud
        self.clock = clock
        self.db = db
        self.util_threshold = util_threshold

    def tick(self, account_budget: float = 1e9) -> list[dict]:
        """Stop TTL-expired low-util instances. Never terminate. Budget is ignored on purpose."""
        actions = []
        now = self.clock.now()
        spend = sum(i.accrued_cost for i in self.cloud.list_instances())
        for inst in self.cloud.list_instances():
            if inst.state not in (InstanceState.RUNNING, InstanceState.BOOTING, InstanceState.PENDING):
                continue
            age = now - inst.launch_time
            if inst.keep_until or inst.tags.get("keep-until"):
                continue
            if age >= inst.ttl_seconds and inst.utilization <= self.util_threshold:
                self.cloud.stop_instance(inst.id)
                reason = (
                    f"ttl exceeded ({age:.0f}s >= {inst.ttl_seconds:.0f}s) with utilization "
                    f"{inst.utilization:.3f}; account_spend={spend:.4f} budget={account_budget:.4f} (not used)"
                )
                self.db.cost_guard(now, inst.id, reason, "stop", inst.utilization, inst.hourly_price, age)
                actions.append({"instance_id": inst.id, "action": "stop", "reason": reason})
        return actions


class TagReporter:
    def __init__(self, cloud: CloudSimulator, clock: Clock) -> None:
        self.cloud = cloud
        self.clock = clock

    def report(self) -> dict:
        hours_env: dict[str, float] = defaultdict(float)
        hours_svc: dict[str, float] = defaultdict(float)
        spend_env: dict[str, float] = defaultdict(float)
        spend_svc: dict[str, float] = defaultdict(float)
        admitted = 0
        untagged_admitted = 0
        quarantined = 0
        now = self.clock.now()
        for inst in self.cloud.list_instances():
            missing = [t for t in MANDATORY_TAGS if t not in inst.tags or not inst.tags[t]]
            if inst.quarantined or missing:
                quarantined += 1
                continue
            admitted += 1
            hours = max(0.0, now - inst.launch_time) / 3600.0
            env = inst.tags["env"]
            svc = inst.tags["service"]
            hours_env[env] += hours
            hours_svc[svc] += hours
            spend_env[env] += inst.accrued_cost
            spend_svc[svc] += inst.accrued_cost
        share = (untagged_admitted / admitted) if admitted else 0.0
        return {
            "instance_hours_by_env": dict(hours_env),
            "instance_hours_by_service": dict(hours_svc),
            "spend_by_env": dict(spend_env),
            "spend_by_service": dict(spend_svc),
            "admitted": admitted,
            "quarantined": quarantined,
            "untagged_share_admitted": share,
        }


class UnitEconomics:
    def __init__(self, cloud: CloudSimulator, queue: JobQueue, credit_prices: dict[str, float]) -> None:
        self.cloud = cloud
        self.queue = queue
        self.credit_prices = credit_prices

    def snapshot(self) -> dict:
        stats = self.queue.stats()
        compute = sum(i.accrued_cost for i in self.cloud.list_instances())
        provider = compute  # simulated provider bill equals instance-hours * price
        success = stats["succeeded"]
        failed = stats["failed"]
        retried = stats["retries"]
        cost_per_success = (compute / success) if success else None
        by_tier = {}
        raw_ok = self.queue.r.hgetall("gf:econ:success_by_tier") or {}
        raw_fail = self.queue.r.hgetall("gf:econ:fail_by_tier") or {}
        keys = set(raw_ok) | set(raw_fail)
        alerts = []
        for key in keys:
            ok = int(raw_ok.get(key, 0))
            fl = int(raw_fail.get(key, 0))
            model, tier = str(key).split(":", 1)
            credit = self.credit_prices.get(tier, self.credit_prices.get("standard", 0.05))
            revenue = ok * credit
            # attribute compute proportionally by successes
            share = (ok / success) if success else 0.0
            cost = compute * share
            margin = revenue - cost
            by_tier[str(key)] = {
                "model": model,
                "resolution_tier": tier,
                "successful": ok,
                "failed": fl,
                "revenue": revenue,
                "cost": cost,
                "margin": margin,
                "credit_price": credit,
            }
            if margin < 0:
                alerts.append({"name": "negative_margin", "key": str(key), "margin": margin})
        return {
            "compute_cost": compute,
            "provider_cost": provider,
            "successful_generations": success,
            "failed_generations": failed,
            "retried_generations": retried,
            "cost_per_successful_generation": cost_per_success,
            "by_tier": by_tier,
            "alerts": alerts,
        }
