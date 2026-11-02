from __future__ import annotations

import json
import sqlite3
from typing import Any


SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource TEXT,
    details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS scaling_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    scaler TEXT NOT NULL,
    pending_jobs INTEGER,
    jobs_per_second REAL,
    backlog_time REAL,
    current_capacity INTEGER,
    desired_capacity INTEGER,
    reason TEXT,
    action TEXT
);
CREATE TABLE IF NOT EXISTS cost_guard_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    instance_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    action TEXT NOT NULL,
    utilization REAL,
    hourly_price REAL,
    age_seconds REAL
);
CREATE TABLE IF NOT EXISTS experiment_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    scaler TEXT NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rotation_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    step TEXT NOT NULL,
    status TEXT NOT NULL,
    details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS exposure_scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    findings TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def audit(self, ts: float, actor: str, action: str, resource: str | None, details: dict[str, Any]) -> None:
        payload = json.dumps(details, sort_keys=True)
        if any(k in payload.lower() for k in ("password", "secret", "token=")):
            payload = json.dumps({"error": "refusing to persist secret-like details"})
        self._conn.execute(
            "INSERT INTO audit_events(ts, actor, action, resource, details) VALUES (?,?,?,?,?)",
            (ts, actor, action, resource, payload),
        )
        self._conn.commit()

    def scaling(self, ts: float, scaler: str, pending: int, jps: float, backlog: float, current: int, desired: int, reason: str, action: str) -> None:
        self._conn.execute(
            """INSERT INTO scaling_decisions(ts, scaler, pending_jobs, jobs_per_second, backlog_time,
               current_capacity, desired_capacity, reason, action) VALUES (?,?,?,?,?,?,?,?,?)""",
            (ts, scaler, pending, jps, backlog, current, desired, reason, action),
        )
        self._conn.commit()

    def cost_guard(self, ts: float, instance_id: str, reason: str, action: str, utilization: float, hourly_price: float, age: float) -> None:
        self._conn.execute(
            """INSERT INTO cost_guard_actions(ts, instance_id, reason, action, utilization, hourly_price, age_seconds)
               VALUES (?,?,?,?,?,?,?)""",
            (ts, instance_id, reason, action, utilization, hourly_price, age),
        )
        self._conn.commit()

    def experiment(self, name: str, scaler: str, payload: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO experiment_results(name, scaler, payload) VALUES (?,?,?)",
            (name, scaler, json.dumps(payload, sort_keys=True)),
        )
        self._conn.commit()

    def rotation(self, ts: float, step: str, status: str, details: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO rotation_journal(ts, step, status, details) VALUES (?,?,?,?)",
            (ts, step, status, json.dumps(details, sort_keys=True)),
        )
        self._conn.commit()

    def exposure(self, ts: float, findings: list[dict[str, Any]]) -> None:
        self._conn.execute(
            "INSERT INTO exposure_scans(ts, findings) VALUES (?,?)",
            (ts, json.dumps(findings, sort_keys=True)),
        )
        self._conn.commit()

    def fetchall(self, table: str, limit: int = 50) -> list[dict[str, Any]]:
        allowed = {
            "audit_events",
            "scaling_decisions",
            "cost_guard_actions",
            "experiment_results",
            "rotation_journal",
            "exposure_scans",
        }
        if table not in allowed:
            raise ValueError("unknown table")
        rows = self._conn.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def last_exposure(self) -> list[dict[str, Any]]:
        row = self._conn.execute("SELECT findings FROM exposure_scans ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            return []
        return json.loads(row["findings"])
