# Implementation checklist

Order is mandatory. Do not skip tests.

## P0

- [x] Shared models, simulation clock, SQLite audit schema
- [x] Redis-backed cloud simulator (launch/describe/stop/terminate/tag/util/SG/billing/creds)
- [x] Redis job queue: submit, claim, lease, heartbeat, retry, complete, fail, requeue
- [x] I/O and CPU worker pools with configurable concurrency and graceful shutdown
- [x] Idempotent completion (no double-success)
- [x] Backlog-time autoscaler (cooldown, hysteresis, warm/booting, fail-static)
- [x] Naive CPU-threshold autoscaler for comparison
- [x] Warm pool + image cache + cold vs warm timing
- [x] Drain / spot interruption protocol
- [x] Per-resource cost guard (TTL + low util, never auto-delete)
- [x] Mandatory tag enforcement and cost attribution

## P1

- [x] Unit economics (cost per success, margin, negative-margin alerts)
- [x] Command document registry and least-privilege runner
- [x] Configuration parity checker (no secret values in reports)
- [x] Dual-credential rotation with journal and rollback
- [x] Security-group exposure auditor and waivers

## P2

- [x] Prometheus metrics and SLO / burn-rate alerts
- [x] fleet.yaml plan/apply/lock/drift
- [x] Load traces and chaos (T1–T12)
- [x] Dashboard
- [x] Docker Compose, Makefile, README, ARCHITECTURE, EXPERIMENTS

## Module dependencies

```
clock, models, settings, db
        ↓
   cloud simulator  ←→  redis
        ↓
   job queue / leases
        ↓
   workers (io, cpu)
        ↓
   warm pool, autoscalers, drain
        ↓
   cost guard, tags, economics
        ↓
   ops runner, parity, rotation, exposure
        ↓
   metrics/SLO, reconciler, traces, API, dashboard
```
