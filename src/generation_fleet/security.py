from __future__ import annotations

from generation_fleet.clock import Clock
from generation_fleet.cloud import ADMIN_PORTS, CloudSimulator
from generation_fleet.db import Database
from generation_fleet.models import Waiver

WORLD = "0.0.0.0/0"


class ExposureAuditor:
    def __init__(self, cloud: CloudSimulator, clock: Clock, db: Database) -> None:
        self.cloud = cloud
        self.clock = clock
        self.db = db
        self.waivers: dict[str, Waiver] = {}

    def add_waiver(self, waiver: Waiver) -> None:
        self.waivers[waiver.finding_key] = waiver

    def scan(self) -> dict:
        findings = []
        now = self.clock.now()
        for sg in self.cloud.list_security_groups():
            for rule in sg.rules:
                key = f"{sg.id}:{rule.port}:{rule.cidr}"
                world = rule.cidr in {WORLD, "::/0"}
                if world and rule.port == 22:
                    findings.append({"key": key, "type": "world_open_ssh", "sg": sg.id, "port": 22})
                elif world and rule.port in ADMIN_PORTS:
                    findings.append({"key": key, "type": "world_open_admin", "sg": sg.id, "port": rule.port})
                elif world and sg.internal_service:
                    findings.append({"key": key, "type": "public_internal_service", "sg": sg.id, "port": rule.port})
        active = []
        expired_waivers = []
        for f in findings:
            w = self.waivers.get(f["key"])
            if w and w.expires_at < now:
                expired_waivers.append({"key": f["key"], "type": "expired_waiver", "waiver_id": w.id})
                active.append(f)
            elif w and w.expires_at >= now:
                continue
            else:
                active.append(f)
        active.extend(expired_waivers)
        previous = self.db.last_exposure()
        prev_keys = {p.get("key") for p in previous}
        new = [f for f in active if f.get("key") not in prev_keys]
        self.db.exposure(now, active)
        alerts = [{"name": "new_exposure", "finding": f} for f in new]
        return {"findings": active, "new": new, "alerts": alerts}
