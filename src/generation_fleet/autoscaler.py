from __future__ import annotations

import math

from generation_fleet.clock import Clock
from generation_fleet.cloud import CloudError, CloudSimulator
from generation_fleet.db import Database
from generation_fleet.models import InstanceState, Pool, ScalingDecision
from generation_fleet.queue import JobQueue
from generation_fleet.warmpool import WarmPool


class BacklogAutoscaler:
    def __init__(
        self,
        cloud: CloudSimulator,
        queue: JobQueue,
        warm: WarmPool,
        clock: Clock,
        db: Database,
        *,
        target_backlog: float = 15.0,
        min_capacity: int = 1,
        max_capacity: int = 20,
        cooldown: float = 20.0,
        hysteresis: int = 1,
        default_jps_per_worker: float = 1.0,
    ) -> None:
        self.cloud = cloud
        self.queue = queue
        self.warm = warm
        self.clock = clock
        self.db = db
        self.target_backlog = target_backlog
        self.min_capacity = min_capacity
        self.max_capacity = max_capacity
        self.cooldown = cooldown
        self.hysteresis = hysteresis
        self.default_jps = default_jps_per_worker
        self.last_scale_ts = 0.0
        self.last_decision: ScalingDecision | None = None
        self.uncertain = False
        self.enabled = True
        self.last_tick_ts = 0.0

    def capacities(self) -> dict[str, int]:
        running = booting = draining = warm = busy = 0
        for i in self.cloud.list_instances():
            if i.quarantined:
                continue
            if i.state == InstanceState.RUNNING:
                running += 1
            elif i.state in (InstanceState.PENDING, InstanceState.BOOTING):
                booting += 1
            elif i.state == InstanceState.DRAINING:
                draining += 1
            if i.warm and i.state == InstanceState.STOPPED:
                warm += 1
        leased = self.queue.processing_ids()
        busy = len(leased)
        return {"running": running, "booting": booting, "draining": draining, "warm": warm, "busy": busy}

    def tick(self) -> ScalingDecision:
        now = self.clock.now()
        self.last_tick_ts = now
        caps = self.capacities()
        pending = self.queue.pending_count()
        jps = self.queue.measured_jobs_per_second()
        self.uncertain = jps <= 0 and pending > 0 and caps["running"] == 0
        effective_jps = jps if jps > 0 else max(self.default_jps * max(caps["running"], 1), 0.05)
        backlog = pending / effective_jps if effective_jps else 0.0
        per_worker = effective_jps / max(caps["running"], 1)
        raw = pending / max(self.target_backlog * per_worker, 0.01)
        desired = int(math.ceil(raw)) if pending else self.min_capacity
        desired = max(self.min_capacity, min(self.max_capacity, desired))
        desired = max(desired, caps["busy"])  # never scale in below leased jobs
        current = caps["running"] + caps["booting"]
        action = "noop"
        reason = "at target"

        if not self.enabled:
            reason = "autoscaler disabled (fail static)"
            desired = current
            action = "fail_static"
        elif now - self.last_scale_ts < self.cooldown and desired != current:
            reason = "cooldown"
            desired = current
            action = "cooldown"
        elif abs(desired - current) <= self.hysteresis and desired < current:
            reason = "hysteresis holds capacity"
            desired = current
            action = "hysteresis"
        elif self.uncertain and desired < current:
            reason = "uncertain throughput; fail static"
            desired = current
            action = "fail_static"
        elif desired > current:
            need = desired - current
            launched = 0
            failed = 0
            for _ in range(need):
                try:
                    if self.warm.activate():
                        launched += 1
                    else:
                        self.warm.launch_cold()
                        launched += 1
                except CloudError:
                    failed += 1
            self.last_scale_ts = now
            action = "scale_out"
            reason = f"backlog_time={backlog:.2f}s need={need} launched={launched} provider_fail={failed}"
            if failed and launched == 0:
                reason += "; provider failure, not scaling in"
        elif desired < current:
            idle_targets = [
                i
                for i in self.cloud.list_instances()
                if i.state == InstanceState.RUNNING and not i.quarantined
            ]
            # Do not drain instances that currently hold leases
            busy_instances = {w.instance_id for w in self.queue.list_workers() if w.current_job_id}
            idle_targets = [i for i in idle_targets if i.id not in busy_instances]
            to_drain = min(current - desired, len(idle_targets))
            for inst in idle_targets[:to_drain]:
                self.cloud.drain_instance(inst.id)
            self.last_scale_ts = now
            action = "scale_in_drain"
            reason = f"drain {to_drain} idle workers; leased jobs protected"
            if to_drain == 0:
                action = "noop"
                reason = "scale-in blocked: all running workers hold leases"
        self.warm.replenish()
        decision = ScalingDecision(
            ts=now,
            scaler="backlog",
            pending_jobs=pending,
            jobs_per_second=jps,
            backlog_time=backlog,
            current_capacity=current,
            desired_capacity=desired,
            booting=caps["booting"],
            draining=caps["draining"],
            warm=caps["warm"],
            reason=reason,
            action=action,
        )
        self.last_decision = decision
        self.db.scaling(now, "backlog", pending, jps, backlog, current, desired, reason, action)
        return decision


class CpuAutoscaler:
    def __init__(
        self,
        cloud: CloudSimulator,
        queue: JobQueue,
        warm: WarmPool,
        clock: Clock,
        db: Database,
        *,
        high: float = 0.7,
        low: float = 0.3,
        min_capacity: int = 1,
        max_capacity: int = 20,
        cooldown: float = 20.0,
    ) -> None:
        self.cloud = cloud
        self.queue = queue
        self.warm = warm
        self.clock = clock
        self.db = db
        self.high = high
        self.low = low
        self.min_capacity = min_capacity
        self.max_capacity = max_capacity
        self.cooldown = cooldown
        self.last_scale_ts = 0.0
        self.last_decision: ScalingDecision | None = None

    def tick(self) -> ScalingDecision:
        now = self.clock.now()
        insts = [i for i in self.cloud.list_instances() if i.state.value in {"RUNNING", "BOOTING", "PENDING"}]
        utils = [i.utilization for i in insts if i.state.value == "RUNNING"]
        avg = sum(utils) / len(utils) if utils else 0.0
        current = len(insts)
        pending = self.queue.pending_count()
        desired = current
        action = "noop"
        reason = f"cpu_avg={avg:.2f}"
        if now - self.last_scale_ts >= self.cooldown:
            if avg > self.high and current < self.max_capacity:
                desired = min(self.max_capacity, current + 1)
                if not self.warm.activate():
                    self.warm.launch_cold()
                action = "scale_out"
                self.last_scale_ts = now
            elif avg < self.low and current > self.min_capacity:
                busy = {w.instance_id for w in self.queue.list_workers() if w.current_job_id}
                idle = [i for i in insts if i.state.value == "RUNNING" and i.id not in busy]
                if idle:
                    self.cloud.drain_instance(idle[0].id)
                    desired = current - 1
                    action = "scale_in_drain"
                    self.last_scale_ts = now
                else:
                    reason += "; blocked by leases"
        self.warm.replenish()
        decision = ScalingDecision(
            ts=now,
            scaler="cpu",
            pending_jobs=pending,
            jobs_per_second=0.0,
            backlog_time=0.0,
            current_capacity=current,
            desired_capacity=desired,
            booting=0,
            draining=0,
            warm=0,
            reason=reason,
            action=action,
        )
        self.last_decision = decision
        self.db.scaling(now, "cpu", pending, 0.0, 0.0, current, desired, reason, action)
        return decision
