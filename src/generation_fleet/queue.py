from __future__ import annotations

import json
import uuid

from redis import Redis
from redis.exceptions import ResponseError

from generation_fleet.clock import Clock
from generation_fleet.models import Job, JobState, Pool, WorkerRecord
from generation_fleet.redis_lua import COMPLETE_LUA


class JobQueue:
    def __init__(self, redis: Redis, clock: Clock, lease_ttl: int = 30) -> None:
        self.r = redis
        self.clock = clock
        self.lease_ttl = lease_ttl
        self._complete = self.r.register_script(COMPLETE_LUA)

    def _job_key(self, job_id: str) -> str:
        return f"gf:job:{job_id}"

    def _lease_key(self, job_id: str) -> str:
        return f"gf:lease:{job_id}"

    def _pending(self, pool: Pool) -> str:
        return f"gf:queue:{pool.value}:pending"

    def submit(self, pool: Pool, payload: dict, **kwargs) -> Job:
        job = Job(
            id="job-" + uuid.uuid4().hex[:16],
            pool=pool,
            payload=payload,
            created_at=self.clock.now(),
            **kwargs,
        )
        self.r.set(self._job_key(job.id), job.model_dump_json())
        self.r.rpush(self._pending(pool), job.id)
        self.r.incr("gf:metrics:submitted")
        return job

    def get(self, job_id: str) -> Job | None:
        raw = self.r.get(self._job_key(job_id))
        return Job.model_validate_json(raw) if raw else None

    def pending_count(self, pool: Pool | None = None) -> int:
        if pool:
            return int(self.r.llen(self._pending(pool)))
        return sum(int(self.r.llen(self._pending(p))) for p in Pool)

    def processing_ids(self) -> set[str]:
        return set(self.r.smembers("gf:processing") or [])

    def claim(self, pool: Pool, worker_id: str) -> Job | None:
        script = self.r.register_script(
            """
            local pending = KEYS[1]
            local processing = KEYS[2]
            local job_id = redis.call('LPOP', pending)
            if not job_id then
              return false
            end
            redis.call('SADD', processing, job_id)
            redis.call('SET', 'gf:lease:' .. job_id, ARGV[1])
            return job_id
            """
        )
        token = json.dumps({"worker_id": worker_id, "expires_at": self.clock.now() + self.lease_ttl})
        try:
            job_id = script(keys=[self._pending(pool), "gf:processing"], args=[token])
        except ResponseError as exc:
            if "unknown command" not in str(exc).lower():
                raise
            # fakeredis installations without Lua support use the equivalent
            # operations below. Production Redis keeps the atomic script path.
            job_id = self.r.lpop(self._pending(pool))
            if job_id:
                self.r.sadd("gf:processing", job_id)
                self.r.set(self._lease_key(job_id), token)
        if not job_id:
            return None
        job = self.get(job_id)
        if not job:
            self.r.srem("gf:processing", job_id)
            return None
        if job.state == JobState.SUCCEEDED:
            self.r.srem("gf:processing", job_id)
            return None
        job.state = JobState.LEASED
        job.attempts += 1
        job.leased_at = self.clock.now()
        job.worker_id = worker_id
        self.r.set(self._job_key(job.id), job.model_dump_json())
        return job

    def _lease_owner(self, job_id: str) -> dict | None:
        raw = self.r.get(self._lease_key(job_id))
        if not raw:
            return None
        data = json.loads(raw)
        if data["expires_at"] < self.clock.now():
            return None
        return data

    def heartbeat_lease(self, job_id: str, worker_id: str) -> None:
        data = self._lease_owner(job_id)
        if data and data["worker_id"] == worker_id:
            self.r.set(
                self._lease_key(job_id),
                json.dumps({"worker_id": worker_id, "expires_at": self.clock.now() + self.lease_ttl}),
            )

    def complete(self, job: Job, result: str = "ok") -> bool:
        marker = f"gf:complete:{job.id}"
        try:
            first = self._complete(
                keys=[marker, "gf:processing"],
                args=["1", job.id, self._lease_key(job.id)],
            )
        except ResponseError as exc:
            if "unknown command" not in str(exc).lower():
                raise
            # Match SET NX + cleanup semantics for Lua-less test doubles.
            first = self.r.set(marker, "1", nx=True)
            if first:
                self.r.srem("gf:processing", job.id)
                self.r.delete(self._lease_key(job.id))
        if not first:
            return False
        job.state = JobState.SUCCEEDED
        job.completed_at = self.clock.now()
        job.result = result
        self.r.set(self._job_key(job.id), job.model_dump_json())
        self.r.incr("gf:metrics:succeeded")
        self.r.rpush("gf:metrics:success_ts", str(self.clock.now()))
        self.r.ltrim("gf:metrics:success_ts", -500, -1)
        if job.attempts > 1:
            self.r.incr("gf:metrics:retried_success")
        self.r.hincrby("gf:econ:success_by_tier", f"{job.model}:{job.resolution_tier}", 1)
        return True

    def fail(self, job: Job, error: str, requeue: bool = True) -> None:
        self.r.srem("gf:processing", job.id)
        self.r.delete(self._lease_key(job.id))
        if requeue and job.attempts < job.max_attempts:
            job.state = JobState.RETRYING
            job.error = error
            self.r.set(self._job_key(job.id), job.model_dump_json())
            self.r.lpush(self._pending(job.pool), job.id)
            self.r.incr("gf:metrics:retries")
            return
        job.state = JobState.FAILED
        job.error = error
        job.completed_at = self.clock.now()
        self.r.set(self._job_key(job.id), job.model_dump_json())
        self.r.incr("gf:metrics:failed")
        self.r.hincrby("gf:econ:fail_by_tier", f"{job.model}:{job.resolution_tier}", 1)

    def return_lease(self, job_id: str) -> None:
        job = self.get(job_id)
        if not job or job.state == JobState.SUCCEEDED:
            self.r.srem("gf:processing", job_id)
            self.r.delete(self._lease_key(job_id))
            return
        self.r.srem("gf:processing", job_id)
        self.r.delete(self._lease_key(job_id))
        job.state = JobState.PENDING
        job.worker_id = None
        self.r.set(self._job_key(job.id), job.model_dump_json())
        self.r.lpush(self._pending(job.pool), job.id)

    def requeue_expired_leases(self) -> list[str]:
        recovered = []
        for job_id in list(self.processing_ids()):
            job = self.get(job_id)
            if job and job.state == JobState.SUCCEEDED:
                self.r.srem("gf:processing", job_id)
                continue
            if self._lease_owner(job_id) is None:
                self.return_lease(job_id)
                recovered.append(job_id)
        return recovered

    def save_worker(self, worker: WorkerRecord) -> None:
        self.r.set(f"gf:worker:{worker.id}", worker.model_dump_json(), ex=self.lease_ttl * 3)

    def list_workers(self) -> list[WorkerRecord]:
        keys = list(self.r.scan_iter("gf:worker:*"))
        out = []
        for k in keys:
            raw = self.r.get(k)
            if raw:
                out.append(WorkerRecord.model_validate_json(raw))
        return out

    def measured_jobs_per_second(self, window: float = 60.0) -> float:
        now = self.clock.now()
        ts = [float(x) for x in self.r.lrange("gf:metrics:success_ts", 0, -1)]
        recent = [t for t in ts if now - t <= window]
        if not recent:
            return 0.0
        span = max(now - min(recent), 1.0)
        return len(recent) / span

    def stats(self) -> dict:
        return {
            "submitted": int(self.r.get("gf:metrics:submitted") or 0),
            "succeeded": int(self.r.get("gf:metrics:succeeded") or 0),
            "failed": int(self.r.get("gf:metrics:failed") or 0),
            "retries": int(self.r.get("gf:metrics:retries") or 0),
            "retried_success": int(self.r.get("gf:metrics:retried_success") or 0),
            "pending": self.pending_count(),
            "processing": len(self.processing_ids()),
            "jps": self.measured_jobs_per_second(),
        }
