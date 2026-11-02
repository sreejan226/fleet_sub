# Experiments

Deterministic runs live in `tests/test_scenarios.py` (FakeClock). Docker traces use wall-clock boot delays.

Autoscaler comparison is written to `experiments/results.json` when `test_compare_autoscalers` runs.

## Method

Each scenario uses the same models: Redis queue, lease reaper, simulated instances, warm pool. Queue-driven scaler uses `backlog_time = pending / jps` with min/max/warm/cooldown/hysteresis and fail-static scale-in. CPU scaler uses mean instance utilization thresholds (0.7 / 0.3) and is recorded for contrast only.

## Trace results (qualitative + assertions)

| ID | Scenario | Expected / measured behavior |
| --- | --- | --- |
| T1 | 10x spike, 45s cold boot | Warm activation first (~2s). Additional capacity launches with 45s `boot_delay`. Jobs remain pending until RUNNING; none are marked success twice. |
| T2 | Oscillating load | Scale-out and cooldown/hysteresis avoid thrash relative to a naive every-tick scaler. |
| T3 | Scale-in while leased | Drain/scale-in will not complete a leased job as success nor drop it. Hold jobs stay `leased` until released, then succeed once. |
| T4 | 30% interrupted | Instances enter DRAINING (2 min warning) then INTERRUPTED. Unfinished leases requeue. |
| T5 | Forgotten GPU instance | Cost guard **stops** the instance after TTL + low utilization even with a huge account budget. No terminate. |
| T6 | Untagged resource | Reject or quarantine. Admitted untagged share = 0. |
| T7 | Prod creds from non-main | Operations runner raises; audit records branch rejection without secrets. |
| T8 | Tampered command document | Hash mismatch; execution denied. |
| T9 | Stale env hash | Node quarantined; report names the mismatching **key**, never the value. |
| T10 | Rotate under load | Dual-accept rotation; zero failed consumer authentications. |
| T11 | World-open SSH + expired waiver | Findings include SSH/admin/internal exposure; new-exposure alert on first scan. |
| T12 | Autoscaler stopped | Fail static (no scale-in). Dead-man / stuck-autoscaler / backlog alerts fire. |

## Autoscaler comparison

On the same spike/oscillate-style load:

- **Backlog scaler** reacts to pending work even when CPU is low (typical of waiting I/O generation jobs).
- **CPU scaler** under-reacts when utilization is not yet high (jobs queued, workers not busy), so SLO backlog risk is higher for generation fleets.

Exact instance-hours and cost vary with FakeClock duration; inspect `experiments/results.json` and SQLite `experiment_results` after `pytest`.

## SLO

Alerts cover success-rate burn, TTFG, backlog time, worker availability, stuck/disabled autoscaler, and control-plane dead-man. Grouping is by `alert.group`; duplicate names are suppressed; silences are time-bounded.
