from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml
from redis import Redis

from generation_fleet.autoscaler import BacklogAutoscaler, CpuAutoscaler
from generation_fleet.clock import Clock, WallClock
from generation_fleet.cloud import CloudSimulator
from generation_fleet.cost import CostGuard, TagReporter, UnitEconomics
from generation_fleet.db import Database
from generation_fleet.models import Pool
from generation_fleet.ops import CommandDocument, CommandRegistry, OperationsRunner, content_hash
from generation_fleet.parity import DesiredTemplate, ParityChecker
from generation_fleet.queue import JobQueue
from generation_fleet.reconcile import Reconciler
from generation_fleet.rotation import Consumer, RotationService, SimulatedDatabase
from generation_fleet.security import ExposureAuditor
from generation_fleet.settings import Settings
from generation_fleet.slo import SLOMonitor
from generation_fleet.warmpool import WarmPool
from generation_fleet.workers import Worker


@dataclass
class World:
    settings: Settings
    clock: Clock
    redis: Redis
    db: Database
    cloud: CloudSimulator
    queue: JobQueue
    warm: WarmPool
    backlog: BacklogAutoscaler
    cpu_scaler: CpuAutoscaler
    cost_guard: CostGuard
    tags: TagReporter
    economics: UnitEconomics
    registry: CommandRegistry
    ops: OperationsRunner
    parity: ParityChecker
    rotation: RotationService
    exposure: ExposureAuditor
    slo: SLOMonitor
    reconciler: Reconciler
    workers: list[Worker] = field(default_factory=list)
    desired: dict = field(default_factory=dict)
    scaler_mode: str = "backlog"

    def attach_worker(self, instance_id: str, pool: Pool) -> Worker:
        w = Worker(pool, self.queue, self.cloud, self.clock, instance_id)
        self.workers.append(w)
        return w

    def sync_workers(self) -> None:
        known = {w.instance_id for w in self.workers}
        for inst in self.cloud.list_instances():
            if inst.state.value == "RUNNING" and not inst.quarantined and inst.id not in known:
                self.attach_worker(inst.id, inst.pool)
        alive = []
        for w in self.workers:
            inst = self.cloud.get(w.instance_id)
            if not inst or inst.state.value in {"STOPPED", "FAILED", "INTERRUPTED"}:
                w.graceful_shutdown()
                continue
            if inst.state.value == "DRAINING":
                w.begin_drain()
            alive.append(w)
        self.workers = [w for w in alive if not w.shutdown]

    def tick(self, execute_workers: bool = True) -> dict:
        self.queue.requeue_expired_leases()
        if execute_workers:
            self.sync_workers()
            for w in list(self.workers):
                w.tick()
        if self.scaler_mode == "cpu":
            decision = self.cpu_scaler.tick()
            last = self.cpu_scaler.last_decision.ts if self.cpu_scaler.last_decision else self.clock.now()
            enabled = True
        else:
            decision = self.backlog.tick()
            last = self.backlog.last_tick_ts
            enabled = self.backlog.enabled
        guard = self.cost_guard.tick()
        parity = self.parity.scan()
        running = len([i for i in self.cloud.list_instances() if i.state.value == "RUNNING" and not i.quarantined])
        slo = self.slo.evaluate(decision.backlog_time, running, enabled, last, self.queue.pending_count())
        self._metrics(decision, slo, running)
        return {
            "decision": decision.model_dump(),
            "guard": guard,
            "parity": parity,
            "slo": slo,
            "stats": self.queue.stats(),
        }

    def _metrics(self, decision, slo, running) -> None:
        from generation_fleet import metrics as m

        m.PENDING.set(self.queue.pending_count())
        m.BACKLOG.set(decision.backlog_time)
        m.WORKER_AVAIL.set(running)
        m.AUTOSCALER_DESIRED.set(decision.desired_capacity)
        m.SUCCESS_RATE.set(slo["success_rate"])
        if slo["ttfg"] is not None:
            m.TTFG.set(slo["ttfg"])
        m.ALERTS.set(len(slo["alerts"]))
        m.WARM.set(self.warm.stats()["warm_available"])
        m.SPEND.set(sum(i.accrued_cost for i in self.cloud.list_instances()))
        econ = self.economics.snapshot()
        if econ["cost_per_successful_generation"] is not None:
            m.COST_PER_SUCCESS.set(econ["cost_per_successful_generation"])
        from collections import Counter

        counts = Counter(i.state.value for i in self.cloud.list_instances())
        for state, n in counts.items():
            m.INSTANCES.labels(state=state).set(n)

    def snapshot(self) -> dict:
        insts = [i.model_dump() for i in self.cloud.list_instances()]
        return {
            "instances": insts,
            "queue": self.queue.stats(),
            "warm": self.warm.stats(),
            "economics": self.economics.snapshot(),
            "tags": self.tags.report(),
            "rotation": self.rotation.status(),
            "slo": {
                "alerts": [a.model_dump() for a in self.slo.alerts.values() if not a.silenced],
                "ttfg": self.slo.first_success_at,
            },
            "parity": self.parity.scan(),
            "autoscaler": self.backlog.last_decision.model_dump() if self.backlog.last_decision else None,
            "audit": self.db.fetchall("audit_events"),
            "scaling": self.db.fetchall("scaling_decisions"),
            "cost_guard": self.db.fetchall("cost_guard_actions"),
            "exposure": self.exposure.scan(),
        }


def load_fleet_yaml(path: str | Path) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def build_world(settings: Settings, redis: Redis, clock: Clock | None = None, sqlite_path: str | None = None, fleet_path: str | None = None, initialize: bool = True) -> World:
    clock = clock or WallClock()
    db = Database(sqlite_path or settings.sqlite_path)
    fleet_path = fleet_path or settings.fleet_yaml
    desired = {}
    if Path(fleet_path).exists():
        desired = load_fleet_yaml(fleet_path)
    spec = desired.get("fleet", {})
    tags = spec.get("tags", {"env": "prod", "service": "generation", "owner": "platform"})
    image = spec.get("image", {}).get("digest", settings.default_image_digest)
    env = spec.get("env", {})
    keys = env.get("keys", ["LOG_LEVEL", "MODEL_ID"])
    hashes = env.get("value_hashes", {k: "sha256:" + "ab" * 32 for k in keys})
    env_hash = env.get("hash", settings.default_env_hash)
    itype = spec.get("instance_type", "gen.large")
    cap = spec.get("capacity", {})
    cloud = CloudSimulator(redis, clock, boot_min=settings.boot_delay_min, boot_max=settings.boot_delay_max)
    queue = JobQueue(redis, clock, lease_ttl=settings.lease_ttl_seconds)
    warm = WarmPool(
        cloud,
        clock,
        size=cap.get("warm", settings.warm_capacity),
        instance_type=itype,
        tags=tags,
        image_digest=image,
        environment_hash=env_hash,
        environment_keys=keys,
        env_key_hashes=hashes,
        pool=Pool.IO,
    )
    if initialize:
        warm.replenish()
    backlog = BacklogAutoscaler(
        cloud,
        queue,
        warm,
        clock,
        db,
        target_backlog=spec.get("autoscaler", {}).get("target_backlog_seconds", settings.target_backlog_seconds),
        min_capacity=cap.get("min", settings.min_capacity),
        max_capacity=cap.get("max", settings.max_capacity),
        cooldown=spec.get("autoscaler", {}).get("cooldown_seconds", settings.cooldown_seconds),
        hysteresis=spec.get("autoscaler", {}).get("hysteresis", settings.hysteresis),
    )
    cpu_scaler = CpuAutoscaler(cloud, queue, warm, clock, db, min_capacity=backlog.min_capacity, max_capacity=backlog.max_capacity)
    prices = spec.get("credit_prices", {"standard": settings.credit_price_standard, "high": settings.credit_price_high})
    simdb = SimulatedDatabase(redis)
    consumers = [Consumer("worker-a", simdb), Consumer("worker-b", simdb), Consumer("api", simdb)]
    rotation = RotationService(redis, clock, db, consumers, simdb)
    if initialize:
        rotation.bootstrap()
    template = DesiredTemplate(image, keys, hashes, env_hash)
    registry = CommandRegistry(redis)
    for action in ("drain", "stop", "terminate", "health_check"):
        body = {"action": action, "version": 1}
        registry.register(
            CommandDocument(
                document_id=f"doc-{action}",
                content_hash=content_hash(body),
                permitted_action=action,
                allowed_environment="prod",
                required_tags={"service": tags.get("service", "generation")},
                workflow_identity="fleet-ops",
                body=body,
            )
        )
    world = World(
        settings=settings,
        clock=clock,
        redis=redis,
        db=db,
        cloud=cloud,
        queue=queue,
        warm=warm,
        backlog=backlog,
        cpu_scaler=cpu_scaler,
        cost_guard=CostGuard(cloud, clock, db, settings.cost_util_threshold),
        tags=TagReporter(cloud, clock),
        economics=UnitEconomics(cloud, queue, prices),
        registry=registry,
        ops=OperationsRunner(cloud, registry, clock, db),
        parity=ParityChecker(cloud, template),
        rotation=rotation,
        exposure=ExposureAuditor(cloud, clock, db),
        slo=SLOMonitor(queue, clock, settings.slo_success_rate),
        reconciler=Reconciler(redis, cloud, warm),
        desired=desired,
    )
    return world
