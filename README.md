# Generation Fleet Control Plane

Local, production-style simulation of a generation fleet: FastAPI control plane, Redis jobs/leases, simulated cloud, backlog-time autoscaling, cost guard, least-privilege ops, credential rotation, and a monitoring dashboard.

No AWS/GCP/Azure accounts, no Kubernetes, no paid APIs.

## Install and start

Prerequisites: Python 3.11+, Docker, Docker Compose.

```bash
python -m pip install -r requirements.txt
python -m pytest
docker compose up --build
```

Dashboard: http://localhost:8000  
Prometheus: http://localhost:9090  
API health: http://localhost:8000/health  
Metrics: http://localhost:8000/metrics

Makefile targets (GNU make): `sim`, `test`, `replay`, `chaos`, `dashboard`, `clean`.

On Windows without make:

```powershell
$env:GF_TESTING=1
python -m pytest
docker compose up --build
```

## API usage

- `POST /api/jobs` — enqueue a generation job
- `POST /api/tick?n=5` — advance control loop (useful with a fake clock in tests)
- `GET /api/state` — fleet snapshot
- `POST /api/traces/{steady,spike,oscillate,ramp}`
- `POST /api/chaos/{interrupt,open_ssh,disable_autoscaler,enable_autoscaler}`
- `GET /api/fleet/plan` · `POST /api/fleet/apply`
- `POST /api/ops` — execute a registered command document by tag selector
- `POST /api/rotation` — rotate simulated DB credentials

## Trace replay and chaos

Automated scenarios T1–T12 live in `tests/test_scenarios.py` and use a deterministic simulation clock (no 45-second wall waits).

```bash
python -m pytest tests/test_scenarios.py
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md). Desired state: [fleet.yaml](fleet.yaml). Status: [BUILD_STATUS.md](BUILD_STATUS.md). Experiment notes: [EXPERIMENTS.md](EXPERIMENTS.md).

## Troubleshooting

- **Tests cannot import `generation_fleet`**: run pytest from the repo root (`pyproject.toml` sets `pythonpath = src`).
- **Compose API not up**: wait for Redis healthcheck, then `docker compose logs control-plane`.
- **Jobs stuck pending**: workers only claim `RUNNING` instances; first boot is 20–60s on wall clock in Docker (2s from warm pool).
- **Port 8000/6379/9090 busy**: stop local processes or change published ports.
- **SQLite locked**: one volume `fleet-data` is shared; do not run two control-plane replicas.

## Security notes

Credentials and env values are never printed. Audit records store hashes and IDs only. Terminate is not a public cloud call; it requires a registered operation document.
