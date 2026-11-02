from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry()

JOBS_SUBMITTED = Counter("gf_jobs_submitted_total", "Jobs submitted", registry=REGISTRY)
JOBS_SUCCEEDED = Counter("gf_jobs_succeeded_total", "Jobs succeeded", registry=REGISTRY)
JOBS_FAILED = Counter("gf_jobs_failed_total", "Jobs failed", registry=REGISTRY)
PENDING = Gauge("gf_queue_pending", "Pending jobs", registry=REGISTRY)
BACKLOG = Gauge("gf_backlog_seconds", "Backlog time", registry=REGISTRY)
INSTANCES = Gauge("gf_instances", "Instances by state", ["state"], registry=REGISTRY)
SPEND = Gauge("gf_spend_usd", "Simulated spend", registry=REGISTRY)
COST_PER_SUCCESS = Gauge("gf_cost_per_success", "Unit cost", registry=REGISTRY)
WORKER_AVAIL = Gauge("gf_worker_available", "Available workers", registry=REGISTRY)
TTFG = Gauge("gf_ttfg_seconds", "Time to first generation", registry=REGISTRY)
SUCCESS_RATE = Gauge("gf_job_success_rate", "Success rate", registry=REGISTRY)
AUTOSCALER_DESIRED = Gauge("gf_autoscaler_desired", "Desired capacity", registry=REGISTRY)
ALERTS = Gauge("gf_alerts_active", "Active alerts", registry=REGISTRY)
WARM = Gauge("gf_warm_available", "Warm pool size", registry=REGISTRY)
SCALING = Counter("gf_scaling_actions_total", "Scaling actions", ["action"], registry=REGISTRY)
TTFG_HIST = Histogram("gf_generation_seconds", "Job work time", registry=REGISTRY)


def render() -> bytes:
    return generate_latest(REGISTRY)
