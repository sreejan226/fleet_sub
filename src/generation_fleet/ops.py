from __future__ import annotations

import hashlib
import json

from redis import Redis

from generation_fleet.clock import Clock
from generation_fleet.cloud import AuthorizationError, CloudSimulator
from generation_fleet.db import Database
from generation_fleet.models import AUTHORIZED_TERMINATE, CommandDocument


class OpsError(Exception):
    pass


class CommandRegistry:
    def __init__(self, redis: Redis) -> None:
        self.r = redis

    def register(self, doc: CommandDocument) -> None:
        self.r.set(f"gf:cmd:{doc.document_id}", doc.model_dump_json())

    def get(self, document_id: str) -> CommandDocument | None:
        raw = self.r.get(f"gf:cmd:{document_id}")
        return CommandDocument.model_validate_json(raw) if raw else None


def content_hash(body: dict) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


class OperationsRunner:
    def __init__(self, cloud: CloudSimulator, registry: CommandRegistry, clock: Clock, db: Database) -> None:
        self.cloud = cloud
        self.registry = registry
        self.clock = clock
        self.db = db

    def execute(
        self,
        document_id: str,
        presented_body: dict,
        *,
        environment: str,
        git_branch: str,
        selector_tags: dict[str, str],
        workflow_identity: str,
        credential_environment: str,
    ) -> dict:
        doc = self.registry.get(document_id)
        if not doc:
            raise OpsError("unregistered command document")
        presented = content_hash(presented_body)
        if presented != doc.content_hash:
            self.db.audit(self.clock.now(), workflow_identity, "ops.reject.hash", document_id, {"reason": "hash mismatch"})
            raise OpsError("document hash mismatch")
        if environment != doc.allowed_environment:
            raise OpsError("environment not allowed")
        if credential_environment == "prod" and git_branch != "main":
            self.db.audit(
                self.clock.now(),
                workflow_identity,
                "ops.reject.branch",
                document_id,
                {"branch": git_branch, "credential_environment": credential_environment},
            )
            raise OpsError("production credentials require main branch")
        if workflow_identity != doc.workflow_identity:
            raise OpsError("workflow identity mismatch")
        targets = []
        for inst in self.cloud.list_instances():
            if all(inst.tags.get(k) == v for k, v in {**doc.required_tags, **selector_tags}.items()):
                targets.append(inst)
        if not targets:
            raise OpsError("no instances match tag selector")
        creds = self.cloud.issue_credentials(doc.permitted_action, ttl_seconds=60, environment=credential_environment)
        cred_id = creds["id"]
        # drop secret immediately from local dict after validating we issued
        secret = creds.pop("secret")
        if not self.cloud.validate_credentials(cred_id, secret):
            raise OpsError("short-lived credential invalid")
        secret = "REDACTED"
        results = []
        for inst in targets:
            action = doc.permitted_action
            if action == "drain":
                self.cloud.drain_instance(inst.id)
                results.append({"instance_id": inst.id, "action": "drain"})
            elif action == "stop":
                self.cloud.stop_instance(inst.id)
                results.append({"instance_id": inst.id, "action": "stop"})
            elif action == "terminate":
                self.cloud.terminate_instance(inst.id, AUTHORIZED_TERMINATE)
                results.append({"instance_id": inst.id, "action": "terminate"})
            elif action == "health_check":
                d = self.cloud.describe_instance(inst.id)
                results.append({"instance_id": inst.id, "action": "health_check", "state": d.state.value})
            else:
                raise OpsError("action is not an approved document operation")
        self.cloud.revoke_credentials(cred_id)
        self.db.audit(
            self.clock.now(),
            workflow_identity,
            "ops.execute",
            document_id,
            {"action": doc.permitted_action, "targets": len(results), "cred_id": cred_id, "branch": git_branch},
        )
        return {"ok": True, "results": results}
