from __future__ import annotations

import uuid

from redis import Redis

from generation_fleet.clock import Clock
from generation_fleet.db import Database
from generation_fleet.secrets import digest


class SimulatedDatabase:
    def __init__(self, redis: Redis) -> None:
        self.r = redis

    def accept(self, cred_id: str) -> None:
        self.r.sadd("gf:db:accepted", cred_id)

    def reject(self, cred_id: str) -> None:
        self.r.srem("gf:db:accepted", cred_id)

    def query(self, cred_id: str) -> bool:
        return bool(self.r.sismember("gf:db:accepted", cred_id))


class Consumer:
    def __init__(self, name: str, db: SimulatedDatabase) -> None:
        self.name = name
        self.db = db
        self.cred_id: str | None = None

    def set_cred(self, cred_id: str) -> None:
        self.cred_id = cred_id

    def request(self) -> bool:
        if not self.cred_id:
            return False
        return self.db.query(self.cred_id)


class RotationService:
    def __init__(self, redis: Redis, clock: Clock, sqlite: Database, consumers: list[Consumer], db: SimulatedDatabase) -> None:
        self.r = redis
        self.clock = clock
        self.sqlite = sqlite
        self.consumers = consumers
        self.db = db
        self.current_id: str | None = None
        self.previous_id: str | None = None
        self.dual = False
        self.step = "idle"

    def _journal(self, step: str, status: str, details: dict) -> None:
        self.sqlite.rotation(self.clock.now(), step, status, details)
        self.step = step

    def _issue(self) -> str:
        cred_id = "dbcred-" + uuid.uuid4().hex[:12]
        secret = uuid.uuid4().hex
        self.r.set(f"gf:dbcred:{cred_id}", digest(secret))
        return cred_id

    def bootstrap(self) -> str:
        cred = self._issue()
        self.db.accept(cred)
        self.current_id = cred
        for c in self.consumers:
            c.set_cred(cred)
        self._journal("bootstrap", "ok", {"cred_id": cred})
        return cred

    def rotate(self, interrupt_after: str | None = None) -> dict:
        """Zero-downtime rotation. interrupt_after can be a step name to simulate crash then rollback."""
        try:
            new_id = self._issue()
            self._journal("create_new", "ok", {"cred_id": new_id})
            if interrupt_after == "create_new":
                raise RuntimeError("interrupted")
            self.db.accept(new_id)
            self.dual = True
            self.previous_id = self.current_id
            self._journal("dual_accept", "ok", {"old": self.previous_id, "new": new_id})
            if interrupt_after == "dual_accept":
                raise RuntimeError("interrupted")
            for c in self.consumers:
                c.set_cred(new_id)
            self._journal("update_consumers", "ok", {"consumers": [c.name for c in self.consumers]})
            if interrupt_after == "update_consumers":
                raise RuntimeError("interrupted")
            failed = [c.name for c in self.consumers if not c.request()]
            if failed:
                raise RuntimeError("verify failed")
            self._journal("verify", "ok", {"checked": len(self.consumers)})
            if interrupt_after == "verify":
                raise RuntimeError("interrupted")
            if self.previous_id:
                self.db.reject(self.previous_id)
            self.dual = False
            self.current_id = new_id
            self._journal("revoke_old", "ok", {"revoked": self.previous_id})
            return {"ok": True, "current": self.current_id}
        except Exception as exc:
            self._journal("interrupt", "rollback", {"error": str(exc)})
            return self.rollback()

    def rollback(self) -> dict:
        stable = self.previous_id or self.current_id
        if stable:
            self.db.accept(stable)
            for c in self.consumers:
                c.set_cred(stable)
            self.current_id = stable
        self.dual = False
        self._journal("rollback", "ok", {"current": self.current_id})
        return {"ok": True, "rolled_back": True, "current": self.current_id}

    def status(self) -> dict:
        return {"step": self.step, "current": self.current_id, "dual": self.dual}
