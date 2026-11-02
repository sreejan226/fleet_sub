from __future__ import annotations

import hashlib
import logging
import uuid

from generation_fleet.clock import Clock
from generation_fleet.cloud import CloudSimulator
from generation_fleet.models import InstanceState, Job, Pool, WorkerRecord
from generation_fleet.queue import JobQueue

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        pool: Pool,
        queue: JobQueue,
        cloud: CloudSimulator,
        clock: Clock,
        instance_id: str,
        *,
        worker_id: str | None = None,
        fail_rate: float = 0.0,
    ) -> None:
        self.pool = pool
        self.queue = queue
        self.cloud = cloud
        self.clock = clock
        self.instance_id = instance_id
        self.id = worker_id or "w-" + uuid.uuid4().hex[:10]
        self.claiming = True
        self.draining = False
        self.shutdown = False
        self.current_job: Job | None = None
        self.jobs_completed = 0
        self.fail_rate = fail_rate
        self._persist()

    def _persist(self) -> None:
        rec = WorkerRecord(
            id=self.id,
            pool=self.pool,
            instance_id=self.instance_id,
            claiming=self.claiming and not self.draining,
            draining=self.draining,
            last_heartbeat=self.clock.now(),
            current_job_id=self.current_job.id if self.current_job else None,
            jobs_completed=self.jobs_completed,
        )
        self.queue.save_worker(rec)

    def observe_instance(self) -> None:
        inst = self.cloud.describe_instance(self.instance_id)
        if inst.state in (InstanceState.DRAINING, InstanceState.INTERRUPTED, InstanceState.FAILED, InstanceState.STOPPED):
            self.begin_drain()
        if inst.quarantined:
            self.begin_drain()

    def begin_drain(self) -> None:
        self.draining = True
        self.claiming = False
        self._persist()

    def graceful_shutdown(self) -> None:
        self.begin_drain()
        if self.current_job:
            self.queue.return_lease(self.current_job.id)
            self.current_job = None
        self.shutdown = True
        self._persist()

    def execute(self, job: Job) -> None:
        if self.pool == Pool.CPU:
            blob = (job.id + str(job.payload)).encode()
            acc = blob
            for _ in range(2000):
                acc = hashlib.sha256(acc).digest()
        else:
            self.clock.sleep(job.work_seconds)
            try:
                self.cloud.change_utilization(self.instance_id, 0.7)
            except Exception:
                pass
        if job.payload.get("force_fail") or (self.fail_rate and job.id.endswith("f")):
            raise RuntimeError("injected job failure")

    def tick(self) -> bool:
        """One scheduling slice. Returns True if work happened."""
        if self.shutdown:
            return False
        self.observe_instance()
        self._persist()
        if self.current_job:
            self.queue.heartbeat_lease(self.current_job.id, self.id)
            if self.current_job.payload.get("hold"):
                self._persist()
                return True
            try:
                self.execute(self.current_job)
                ok = self.queue.complete(self.current_job)
                if ok:
                    self.jobs_completed += 1
            except Exception as exc:
                self.queue.fail(self.current_job, str(exc))
            self.current_job = None
            self._persist()
            return True
        if not self.claiming or self.draining:
            if self.draining:
                self.shutdown = True
            return False
        job = self.queue.claim(self.pool, self.id)
        if not job:
            return False
        self.current_job = job
        self._persist()
        return True

    def run_forever(self, idle_sleep: float = 0.2) -> None:
        log.info("worker %s pool=%s instance=%s starting", self.id, self.pool.value, self.instance_id)
        while not self.shutdown:
            worked = self.tick()
            if not worked:
                self.clock.sleep(idle_sleep)


class WorkerPool:
    def __init__(self, workers: list[Worker]) -> None:
        self.workers = workers

    def tick_all(self) -> int:
        return sum(1 for w in self.workers if w.tick())
