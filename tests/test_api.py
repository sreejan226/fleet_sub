from __future__ import annotations

from fastapi.testclient import TestClient

from generation_fleet import api


def test_health_and_dashboard_and_job(world, monkeypatch):
    monkeypatch.setattr(api, "_world", world)
    client = TestClient(api.app)
    assert client.get("/health").json()["ok"] is True
    dash = client.get("/")
    assert dash.status_code == 200
    assert "Queue depth" in dash.text
    job = client.post("/api/jobs", json={"work_seconds": 0.01})
    assert job.status_code == 200
    state = client.get("/api/state").json()
    assert "instances" in state
    metrics = client.get("/metrics")
    assert metrics.status_code == 200
    assert b"gf_queue_pending" in metrics.content
    plan = client.get("/api/fleet/plan")
    assert plan.status_code == 200
