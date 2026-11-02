# Architecture — Generation Fleet Control Plane

Local, production-style simulation of a generation fleet. No real cloud accounts.

## Control loop

1. Desired state is declared in `fleet.yaml`.
2. Clients submit generation/merge jobs to the FastAPI control plane.
3. Jobs land on Redis lists partitioned by pool (`io`, `cpu`).
4. Workers claim jobs with leases and heartbeats.
5. The backlog-time autoscaler measures pending jobs and throughput, then launches, activates warm capacity, or drains idle workers.
6. The cloud simulator advances instance state on the simulation clock (PENDING → BOOTING → RUNNING).
7. Cost guard, tag policy, parity, exposure scan, and SLO evaluation run on each control tick.
8. Every privileged action appends to SQLite audit tables. Secrets are never written.

If the control plane cannot compute a reliable throughput or lease picture, it **fails static**: it may scale out conservatively but will not scale in.

## Data flow

```
YAML ──► reconciler ──► Redis instance records
API  ──► job queue   ──► workers ──► job completion (SET NX)
              ▲              │
              └── requeue on lease expiry / drain / interrupt
Autoscaler ◄── queue depth, jps, instance states
Cost guard ◄── instance TTL, utilization, tags
Prometheus ◄── /metrics
Dashboard  ◄── JSON API
SQLite     ◄── audit, scaling decisions, experiments, rotation journal
```

## Redis key contract

| Key | Purpose |
| --- | --- |
| `gf:instance:{id}` | Instance JSON |
| `gf:instances` | Set of instance IDs |
| `gf:job:{id}` | Job JSON |
| `gf:queue:{pool}:pending` | Pending job IDs |
| `gf:processing` | Claimed job IDs |
| `gf:lease:{job}` | Worker ID, TTL |
| `gf:complete:{job}` | Idempotent success marker |
| `gf:worker:{id}` | Heartbeat JSON |
| `gf:lock:reconcile` | Desired-state lock |
| `gf:cmd:{id}` | Registered command documents |
| `gf:cred:{id}` | Credential metadata (hash only) |
| `gf:sg:{id}` | Security group |
| `gf:rotation` | Rotation state (IDs, never secrets) |

Lua scripts make claim and complete atomic relative to the lease.

## Scaling formula

Queue-driven desired capacity:

```
backlog_time = pending_jobs / max(measured_jobs_per_second, ε)
desired = pending_jobs / (target_backlog_seconds * per_worker_jps)
```

Then:

```
desired = clamp(desired, min_capacity, max_capacity)
desired = max(desired, busy_workers_with_leases)
account for booting + warm activating (never double-launch)
apply hysteresis band and cooldown
on provider failure: retry with backoff, do not scale in
if jps unknown: fail static (no scale-in)
```

CPU scaler (comparison only): scale out when mean utilization > high threshold, in when < low threshold.

## Drain protocol

On DRAINING or interruption warning (120s):

1. Worker stops claiming.
2. Current job may finish if lease remains.
3. Otherwise the lease is released and the job returns to pending.
4. Completion uses `SET complete:{job} NX` so a retry cannot double-count success.
5. Worker shuts down; autoscaler replaces capacity if backlog requires it.

Terminate is **not** a generic API. It requires an authorized operation document.

## Cost guard

Each instance has launch time, TTL, utilization, hourly price, and `env`/`service`/`owner` tags. A forgotten expensive instance with low utilization after TTL is **stopped** (never deleted) even if account spend is under budget. `keep-until` suppresses the stop. Actions are recorded in SQLite.

## Tag policy

Missing `env`, `service`, or `owner` → reject at admit or quarantine if discovered later. Attribution reports cover admitted resources only; untagged share of admitted resources is zero.

## Security model

- Command documents are registered with SHA-256, permitted action, environment, required tags, workflow identity.
- Runner verifies hash, environment, branch (`main` required for production credentials), and tag selectors. No shell.
- Credentials are short-lived IDs plus hashes. Plaintext is held only in memory for the issuing call and never logged.
- Rotation: issue new → dual-accept → update consumers → verify → revoke old → journal. Interrupt triggers rollback to the last verified set.

## Warm pool

Stopped pre-initialized instances with cached image digests. Activation skips full boot (short attach delay). Cold launch pays 20–60s boot. Image cache records pull-through hits.

## Design trade-offs

- **Redis as system of record for live state** so API, workers, and autoscaler share one picture without Kubernetes.
- **SQLite for audit** so experiment results survive Redis flushes.
- **Simulation clock** so pytest is deterministic; Docker uses wall clock.
- **fakeredis in unit tests** so `make test` works without Compose; Compose is the production-style runtime.
- **Fail static** over aggressive scale-in: lost jobs are worse than extra instance-hours.

## 20-minute walkthrough map

1. `fleet.yaml` and models
2. Cloud simulator states and authorized terminate
3. Queue Lua claim/complete
4. Drain + idempotency
5. Backlog autoscaler vs CPU scaler
6. Cost guard vs account budget
7. Ops runner + rotation + exposure
8. Trace replay T1–T12 and dashboard
