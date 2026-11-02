from __future__ import annotations

from generation_fleet.cloud import CloudSimulator
from generation_fleet.models import InstanceState


class DesiredTemplate:
    def __init__(self, image_digest: str, env_keys: list[str], env_key_hashes: dict[str, str], environment_hash: str) -> None:
        if not image_digest.startswith("sha256:") or "latest" in image_digest:
            raise ValueError("unpinned image rejected")
        self.image_digest = image_digest
        self.env_keys = sorted(env_keys)
        self.env_key_hashes = env_key_hashes
        self.environment_hash = environment_hash


class ParityChecker:
    def __init__(self, cloud: CloudSimulator, template: DesiredTemplate) -> None:
        self.cloud = cloud
        self.template = template

    def scan(self) -> dict:
        mismatches = []
        for inst in self.cloud.list_instances():
            if inst.state in (InstanceState.STOPPED, InstanceState.FAILED, InstanceState.INTERRUPTED):
                continue
            issues = []
            if inst.image_digest != self.template.image_digest:
                issues.append({"field": "image_digest", "live": inst.image_digest, "desired": self.template.image_digest})
            live_keys = sorted(inst.environment_keys)
            if live_keys != self.template.env_keys:
                missing = sorted(set(self.template.env_keys) - set(live_keys))
                extra = sorted(set(live_keys) - set(self.template.env_keys))
                if missing:
                    issues.append({"field": "env_keys_missing", "keys": missing})
                if extra:
                    issues.append({"field": "env_keys_extra", "keys": extra})
            for key, desired_hash in self.template.env_key_hashes.items():
                live_hash = inst.env_key_hashes.get(key)
                if live_hash != desired_hash:
                    issues.append({"field": "env_value_hash", "key": key})
            if inst.environment_hash != self.template.environment_hash:
                issues.append({"field": "environment_hash"})
            if issues:
                inst.quarantined = True
                inst.quarantine_reason = "config drift: " + ",".join(i["field"] for i in issues)
                self.cloud._save(inst)
                mismatches.append({"instance_id": inst.id, "issues": issues})
        return {"mismatches": mismatches, "template_image": self.template.image_digest}
