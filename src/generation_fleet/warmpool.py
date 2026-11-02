from __future__ import annotations

from generation_fleet.clock import Clock
from generation_fleet.cloud import CloudSimulator
from generation_fleet.models import Instance, InstanceState, Pool


class ImageCache:
    def __init__(self) -> None:
        self.digests: set[str] = set()
        self.pulls = 0
        self.hits = 0

    def ensure(self, digest: str) -> bool:
        if digest in self.digests:
            self.hits += 1
            return True
        self.pulls += 1
        self.digests.add(digest)
        return False


class WarmPool:
    def __init__(
        self,
        cloud: CloudSimulator,
        clock: Clock,
        *,
        size: int,
        instance_type: str,
        tags: dict[str, str],
        image_digest: str,
        environment_hash: str,
        environment_keys: list[str],
        env_key_hashes: dict[str, str],
        pool: Pool = Pool.IO,
    ) -> None:
        self.cloud = cloud
        self.clock = clock
        self.size = size
        self.instance_type = instance_type
        self.tags = tags
        self.image_digest = image_digest
        self.environment_hash = environment_hash
        self.environment_keys = environment_keys
        self.env_key_hashes = env_key_hashes
        self.pool = pool
        self.cache = ImageCache()
        self.cache.ensure(image_digest)
        self.cold_ready_samples: list[float] = []
        self.warm_ready_samples: list[float] = []
        self.forced_boot_delay: float | None = None

    def replenish(self) -> None:
        stopped_warm = [
            i
            for i in self.cloud.list_instances()
            if i.warm and i.state == InstanceState.STOPPED and not i.quarantined
        ]
        need = self.size - len(stopped_warm)
        for _ in range(max(0, need)):
            inst = self.cloud.launch_instance(
                self.instance_type,
                self.tags,
                image_digest=self.image_digest,
                environment_hash=self.environment_hash,
                environment_keys=self.environment_keys,
                env_key_hashes=self.env_key_hashes,
                boot_delay=2.0,
                warm=True,
                image_cached=True,
                pool=self.pool,
            )
            # pre-initialize then stop
            inst.state = InstanceState.STOPPED
            inst.ready_at = inst.launch_time
            inst.boot_time = 2.0
            self.cloud._save(inst)

    def activate(self) -> Instance | None:
        for inst in self.cloud.list_instances():
            if inst.warm and inst.state == InstanceState.STOPPED and not inst.quarantined:
                start = self.clock.now()
                inst.state = InstanceState.BOOTING
                inst.ready_at = start + 2.0
                inst.boot_delay = 2.0
                inst.last_billing_time = start
                self.cloud._save(inst)
                self.warm_ready_samples.append(2.0)
                return inst
        return None

    def launch_cold(self, boot_delay: float | None = None) -> Instance:
        cached = self.image_digest in self.cache.digests
        self.cache.ensure(self.image_digest)
        delay = boot_delay if boot_delay is not None else self.forced_boot_delay
        inst = self.cloud.launch_instance(
            self.instance_type,
            self.tags,
            image_digest=self.image_digest,
            environment_hash=self.environment_hash,
            environment_keys=self.environment_keys,
            env_key_hashes=self.env_key_hashes,
            boot_delay=delay,
            warm=False,
            image_cached=cached,
            pool=self.pool,
        )
        self.cold_ready_samples.append(inst.boot_delay)
        return inst

    def stats(self) -> dict:
        warm_avail = len([i for i in self.cloud.list_instances() if i.warm and i.state == InstanceState.STOPPED])
        warm_cost = sum(i.accrued_cost for i in self.cloud.list_instances() if i.warm)
        avg_cold = sum(self.cold_ready_samples) / len(self.cold_ready_samples) if self.cold_ready_samples else 0.0
        avg_warm = sum(self.warm_ready_samples) / len(self.warm_ready_samples) if self.warm_ready_samples else 2.0
        return {
            "warm_available": warm_avail,
            "warm_operating_cost": round(warm_cost, 6),
            "avg_cold_ready_s": avg_cold,
            "avg_warm_ready_s": avg_warm,
            "time_saved_s": max(0.0, avg_cold - avg_warm),
            "image_cache_hits": self.cache.hits,
            "image_cache_pulls": self.cache.pulls,
        }
