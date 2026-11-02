from __future__ import annotations

import uuid

from generation_fleet.clock import Clock
from generation_fleet.models import Alert, AlertSeverity
from generation_fleet.queue import JobQueue


class SLOMonitor:
    def __init__(self, queue: JobQueue, clock: Clock, success_slo: float = 0.99) -> None:
        self.queue = queue
        self.clock = clock
        self.success_slo = success_slo
        self.alerts: dict[str, Alert] = {}
        self.silences: dict[str, float] = {}
        self.last_heartbeat = clock.now()
        self.first_success_at: float | None = None
        self.started_at = clock.now()

    def heartbeat(self) -> None:
        self.last_heartbeat = self.clock.now()

    def _raise(self, name: str, group: str, message: str, severity: AlertSeverity, runbook: str) -> None:
        until = self.silences.get(name)
        if until and until > self.clock.now():
            return
        existing = [a for a in self.alerts.values() if a.name == name and not a.silenced]
        if existing:
            return
        alert = Alert(
            id="al-" + uuid.uuid4().hex[:8],
            name=name,
            severity=severity,
            group=group,
            message=message,
            runbook=runbook,
            ts=self.clock.now(),
        )
        self.alerts[alert.id] = alert

    def silence(self, name: str, seconds: float) -> None:
        self.silences[name] = self.clock.now() + seconds
        for a in self.alerts.values():
            if a.name == name:
                a.silenced = True

    def evaluate(self, backlog_time: float, worker_available: int, scaler_enabled: bool, scaler_last_tick: float, pending: int) -> dict:
        self.heartbeat()
        stats = self.queue.stats()
        total = stats["succeeded"] + stats["failed"]
        success_rate = (stats["succeeded"] / total) if total else 1.0
        if stats["succeeded"] and self.first_success_at is None:
            self.first_success_at = self.clock.now()
        ttfg = (self.first_success_at - self.started_at) if self.first_success_at else None
        # multi-window burn: short window approximated by recent fail/success counters
        error_rate = 1.0 - success_rate
        budget = 1.0 - self.success_slo
        burn = error_rate / budget if budget else 0.0
        if burn > 14 and total > 10:
            self._raise("burn_fast", "slo", f"fast burn rate {burn:.1f}", AlertSeverity.CRITICAL, "/runbooks/slo-burn")
        if burn > 6 and total > 20:
            self._raise("burn_slow", "slo", f"slow burn rate {burn:.1f}", AlertSeverity.WARNING, "/runbooks/slo-burn")
        if ttfg and ttfg > 60:
            self._raise("ttfg", "latency", f"time to first generation {ttfg:.1f}s", AlertSeverity.WARNING, "/runbooks/ttfg")
        if backlog_time > 45:
            self._raise("backlog", "queue", f"backlog_time={backlog_time:.1f}s", AlertSeverity.CRITICAL, "/runbooks/backlog")
        if worker_available <= 0 and pending > 0:
            self._raise("workers", "availability", "no available workers", AlertSeverity.CRITICAL, "/runbooks/workers")
        if not scaler_enabled and pending > 0:
            self._raise("deadman_autoscaler", "control", "autoscaler disabled while backlog grows", AlertSeverity.CRITICAL, "/runbooks/stuck-autoscaler")
        if self.clock.now() - scaler_last_tick > 60 and pending > 0:
            self._raise("stuck_autoscaler", "control", "autoscaler heartbeat stale", AlertSeverity.CRITICAL, "/runbooks/stuck-autoscaler")
        if self.clock.now() - self.last_heartbeat > 30:
            self._raise("deadman", "control", "control plane dead-man switch", AlertSeverity.CRITICAL, "/runbooks/deadman")
        return {
            "success_rate": success_rate,
            "ttfg": ttfg,
            "burn_rate": burn,
            "backlog_time": backlog_time,
            "alerts": [a.model_dump() for a in self.alerts.values() if not a.silenced],
        }
