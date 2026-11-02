from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from generation_fleet.cloud import CloudSimulator, TagPolicyError
from generation_fleet.models import InstanceState
from generation_fleet.warmpool import WarmPool


class StateLockError(Exception):
    pass


class Reconciler:
    def __init__(self, redis, cloud: CloudSimulator, warm: WarmPool) -> None:
        self.r = redis
        self.cloud = cloud
        self.warm = warm

    def load(self, path: str | Path) -> dict[str, Any]:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        image = data["fleet"]["image"]["digest"]
        if not str(image).startswith("sha256:"):
            raise ValueError("unpinned image rejected")
        return data

    def acquire(self) -> None:
        ok = self.r.set("gf:lock:reconcile", "1", nx=True, ex=30)
        if not ok:
            raise StateLockError("reconcile already running")

    def release(self) -> None:
        self.r.delete("gf:lock:reconcile")

    def plan(self, desired: dict[str, Any]) -> dict[str, Any]:
        spec = desired["fleet"]
        live = [i for i in self.cloud.list_instances() if i.state in (InstanceState.RUNNING, InstanceState.BOOTING, InstanceState.PENDING)]
        min_c = spec["capacity"]["min"]
        max_c = spec["capacity"]["max"]
        add = max(0, min_c - len(live))
        destructive = len(live) > max_c
        drift = []
        digest_desired = spec["image"]["digest"]
        env_hash = spec["env"]["hash"]
        for i in live:
            if i.image_digest != digest_desired:
                drift.append({"id": i.id, "field": "image_digest"})
            if i.environment_hash != env_hash:
                drift.append({"id": i.id, "field": "environment_hash"})
        return {
            "current": len(live),
            "min": min_c,
            "max": max_c,
            "to_add": add,
            "destructive_scale_in": destructive,
            "drift": drift,
        }

    def apply(self, desired: dict[str, Any], confirm_destructive: bool = False) -> dict[str, Any]:
        self.acquire()
        try:
            plan = self.plan(desired)
            spec = desired["fleet"]
            if plan["destructive_scale_in"] and not confirm_destructive:
                return {"applied": False, "needs_confirmation": True, "plan": plan}
            tags = spec["tags"]
            env_keys = list(spec["env"]["keys"])
            hashes = {k: spec["env"]["value_hashes"][k] for k in env_keys}
            for _ in range(plan["to_add"]):
                try:
                    if not self.warm.activate():
                        self.warm.launch_cold()
                except TagPolicyError:
                    raise
            if plan["destructive_scale_in"]:
                live = [i for i in self.cloud.list_instances() if i.state == InstanceState.RUNNING]
                extra = len(live) - spec["capacity"]["max"]
                for inst in live[: max(0, extra)]:
                    self.cloud.drain_instance(inst.id)
            self.warm.size = spec["capacity"]["warm"]
            self.warm.replenish()
            return {"applied": True, "plan": plan, "tags": tags, "env_keys": env_keys, "hashes_present": list(hashes)}
        finally:
            self.release()
