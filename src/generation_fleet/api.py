from __future__ import annotations

import threading
import time
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from pydantic import BaseModel, Field
from redis import Redis

from generation_fleet.cloud import TagPolicyError
from generation_fleet.metrics import render
from generation_fleet.models import Pool, SecurityGroup, SecurityRule, Waiver
from generation_fleet.ops import OpsError
from generation_fleet.reconcile import StateLockError
from generation_fleet.secrets import configure_logging
from generation_fleet.settings import Settings
from generation_fleet.world import World, build_world

settings = Settings()
configure_logging(settings.log_level)
_world: World | None = None


def get_world() -> World:
    global _world
    if _world is None:
        r = Redis.from_url(settings.redis_url, decode_responses=True)
        _world = build_world(settings, r)
    return _world


app = FastAPI(title="Generation Fleet Control Plane", version="0.1.0")


@app.on_event("startup")
def _startup() -> None:
    if os.environ.get("GF_TESTING") == "1":
        return
    if os.environ.get("GF_ROLE", "control-plane") != "control-plane":
        return
    get_world()

    def loop() -> None:
        while True:
            try:
                get_world().tick(execute_workers=os.environ.get("GF_INLINE_WORKERS", "0") == "1")
            except Exception:
                pass
            time.sleep(float(os.environ.get("GF_TICK_SECONDS", "2")))

    threading.Thread(target=loop, daemon=True).start()


class JobIn(BaseModel):
    pool: Pool = Pool.IO
    work_seconds: float = 0.2
    model: str = "gen-base"
    resolution_tier: str = "standard"
    payload: dict = Field(default_factory=dict)


class OpsIn(BaseModel):
    document_id: str
    body: dict
    environment: str = "prod"
    git_branch: str = "main"
    selector_tags: dict[str, str] = Field(default_factory=dict)
    workflow_identity: str = "fleet-ops"
    credential_environment: str = "prod"


class LaunchIn(BaseModel):
    instance_type: str = "gen.large"
    tags: dict[str, str]
    boot_delay: float | None = None
    ttl_seconds: float = 3600
    keep_until: bool = False
    admit_untagged: bool = False
    utilization: float = 0.0


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/metrics")
def metrics() -> Response:
    return Response(render(), media_type="text/plain; version=0.0.4")


@app.get("/api/state")
def state() -> dict:
    w = get_world()
    w.sync_workers()
    return w.snapshot()


@app.post("/api/jobs")
def submit_job(body: JobIn) -> dict:
    w = get_world()
    job = w.queue.submit(
        body.pool,
        body.payload,
        work_seconds=body.work_seconds,
        model=body.model,
        resolution_tier=body.resolution_tier,
    )
    from generation_fleet.metrics import JOBS_SUBMITTED

    JOBS_SUBMITTED.inc()
    return job.model_dump()


@app.post("/api/tick")
def tick(n: int = 1) -> dict:
    w = get_world()
    last = {}
    for _ in range(max(1, n)):
        last = w.tick()
        if hasattr(w.clock, "advance"):
            w.clock.advance(1)
    return last


@app.post("/api/traces/{name}")
def run_trace(name: str) -> dict:
    from generation_fleet import traces

    w = get_world()
    fn = {
        "steady": traces.trace_steady,
        "spike": traces.trace_spike,
        "oscillate": traces.trace_oscillate,
        "ramp": traces.trace_ramp,
    }.get(name)
    if not fn:
        raise HTTPException(404, "unknown trace")
    fn(w)
    return {"ok": True, "stats": w.queue.stats()}


@app.post("/api/chaos/{name}")
def chaos(name: str) -> dict:
    from generation_fleet import traces

    w = get_world()
    if name == "interrupt":
        n = traces.chaos_interrupt_fraction(w, 0.3)
        return {"interrupted": n}
    if name == "disable_autoscaler":
        w.backlog.enabled = False
        return {"enabled": False}
    if name == "enable_autoscaler":
        w.backlog.enabled = True
        return {"enabled": True}
    if name == "open_ssh":
        w.cloud.put_security_group(
            SecurityGroup(
                id="sg-bad",
                rules=[SecurityRule(port=22, cidr="0.0.0.0/0", description="ssh")],
                internal_service=True,
            )
        )
        w.exposure.add_waiver(Waiver(id="w1", finding_key="sg-bad:22:0.0.0.0/0", expires_at=w.clock.now() - 1, reason="expired"))
        return w.exposure.scan()
    raise HTTPException(404, "unknown chaos")


@app.post("/api/instances")
def launch(body: LaunchIn) -> dict:
    w = get_world()
    try:
        inst = w.cloud.launch_instance(
            body.instance_type,
            body.tags,
            image_digest=w.parity.template.image_digest,
            environment_hash=w.parity.template.environment_hash,
            environment_keys=w.parity.template.env_keys,
            env_key_hashes=w.parity.template.env_key_hashes,
            boot_delay=body.boot_delay,
            ttl_seconds=body.ttl_seconds,
            keep_until=body.keep_until,
            admit_untagged=body.admit_untagged,
        )
    except TagPolicyError as e:
        raise HTTPException(400, str(e)) from e
    if body.utilization:
        w.cloud.change_utilization(inst.id, body.utilization)
    return inst.model_dump()


@app.post("/api/ops")
def ops(body: OpsIn) -> dict:
    w = get_world()
    try:
        return w.ops.execute(
            body.document_id,
            body.body,
            environment=body.environment,
            git_branch=body.git_branch,
            selector_tags=body.selector_tags,
            workflow_identity=body.workflow_identity,
            credential_environment=body.credential_environment,
        )
    except OpsError as e:
        raise HTTPException(403, str(e)) from e


@app.post("/api/rotation")
def rotation(interrupt_after: str | None = None) -> dict:
    return get_world().rotation.rotate(interrupt_after=interrupt_after)


@app.get("/api/fleet/plan")
def plan() -> dict:
    w = get_world()
    if not w.desired:
        raise HTTPException(400, "no fleet.yaml loaded")
    return w.reconciler.plan(w.desired)


@app.post("/api/fleet/apply")
def apply(confirm_destructive: bool = False) -> dict:
    w = get_world()
    try:
        return w.reconciler.apply(w.desired, confirm_destructive=confirm_destructive)
    except StateLockError as e:
        raise HTTPException(409, str(e)) from e


@app.get("/api/economics")
def economics() -> dict:
    return get_world().economics.snapshot()


@app.get("/api/tags")
def tags() -> dict:
    return get_world().tags.report()


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")


def worker_main() -> None:
    configure_logging()
    s = Settings()
    r = Redis.from_url(s.redis_url, decode_responses=True)
    w = build_world(s, r, initialize=False)
    pool = Pool(s.worker_pool)
    conc = s.worker_io_concurrency if pool == Pool.IO else s.worker_cpu_concurrency
    attached: list = []
    while True:
        w.queue.requeue_expired_leases()
        for inst in w.cloud.list_instances():
            if len(attached) >= conc:
                break
            if inst.state.value != "RUNNING" or inst.quarantined:
                continue
            if inst.pool != pool:
                continue
            lock = r.set(f"gf:worker-lock:{inst.id}", "1", nx=True, ex=s.lease_ttl_seconds * 6)
            if not lock:
                continue
            attached.append(w.attach_worker(inst.id, pool))
        for worker in list(attached):
            worker.tick()
            if worker.shutdown:
                r.delete(f"gf:worker-lock:{worker.instance_id}")
                attached.remove(worker)
        w.clock.sleep(0.2)


def control_loop_main() -> None:
    configure_logging()
    s = Settings()
    r = Redis.from_url(s.redis_url, decode_responses=True)
    w = build_world(s, r)
    while True:
        w.tick()
        w.clock.sleep(2)
