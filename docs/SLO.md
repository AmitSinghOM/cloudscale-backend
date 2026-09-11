# CloudScale Backend — Service Level Objectives

SLOs are derived from the gates the service has actually been measured
against (`scripts/http_gate_run.py`, evidence under `evidence/<sha>/`), with
headroom. Every SLI below is computable from metrics the service already
exports; every alert rule below is valid PromQL over those exact names.
Where an SLO cannot yet be claimed, this document says so.

## Service level indicators

| SLI | Source | Definition |
|---|---|---|
| Command latency | HTTP | `cloudscale_http_request_seconds_bucket{route="/v1/accounts/{account_id}/commands"}` |
| Query latency | HTTP | `cloudscale_http_request_seconds_bucket{route="/v1/accounts/{account_id}/balance"}` |
| Availability | HTTP | share of requests **not** `5xx` (`cloudscale_http_requests_total`) |
| Projection freshness | consumer | `cloudscale_consumer_lag_events`, `cloudscale_consumer_last_drain_timestamp_seconds` |
| Consumer leadership | consumer | `cloudscale_consumer_is_leader` |
| Poison rate | consumer | `cloudscale_consumer_events_dead_lettered_total` |

## Objectives

| Objective | Target (30-day window) | Basis |
|---|---|---|
| Command p99 latency | ≤ 300 ms | Gate; observed 36–73 ms under load on both tiers |
| Query p99 latency | ≤ 100 ms | Gate; observed 9–25 ms |
| Availability (non-5xx) | ≥ 99.9 % | **Target, not yet demonstrated** — requires 30 days of operation; zero 5xx in all gate runs |
| Projection lag | ≤ 1 s for 99 % of minutes | Gate; observed ≤ 0.2 s |
| Leader present | a leader exists ≥ 99.9 % of minutes | Session-lock failover verified in ~1 s |
| Poison events | 0 sustained | Every dead letter is a producer or infra defect to investigate |

Honesty note: the four latency/lag objectives have passing evidence from
10-second real-process runs, not 30 days. The availability objective is a
commitment to *measure*, recorded as `not_evaluated` in every evidence file
until a 30-day window exists.

## Error budget

At 99.9 % availability the 30-day budget is **43 minutes** of 5xx-serving
time. Burn-rate alerts below page at rates that would exhaust it in 2 days
(fast) or 10 days (slow).

## Alert rules (Prometheus)

```yaml
groups:
- name: cloudscale
  rules:
  # --- availability: multi-window burn rate --------------------------------
  - alert: CloudScaleHighErrorBurnFast
    expr: |
      ( sum(rate(cloudscale_http_requests_total{status=~"5.."}[5m]))
        / sum(rate(cloudscale_http_requests_total[5m])) ) > (14.4 * 0.001)
      and
      ( sum(rate(cloudscale_http_requests_total{status=~"5.."}[1h]))
        / sum(rate(cloudscale_http_requests_total[1h])) ) > (14.4 * 0.001)
    for: 2m
    labels: {severity: page}
    annotations: {runbook: "docs/RUNBOOK.md#r4"}

  - alert: CloudScaleHighErrorBurnSlow
    expr: |
      ( sum(rate(cloudscale_http_requests_total{status=~"5.."}[6h]))
        / sum(rate(cloudscale_http_requests_total[6h])) ) > (3 * 0.001)
    for: 15m
    labels: {severity: ticket}

  # --- latency ----------------------------------------------------------------
  - alert: CloudScaleCommandLatencyP99
    expr: |
      histogram_quantile(0.99, sum by (le) (
        rate(cloudscale_http_request_seconds_bucket{route="/v1/accounts/{account_id}/commands"}[5m])
      )) > 0.3
    for: 10m
    labels: {severity: page}

  - alert: CloudScaleQueryLatencyP99
    expr: |
      histogram_quantile(0.99, sum by (le) (
        rate(cloudscale_http_request_seconds_bucket{route="/v1/accounts/{account_id}/balance"}[5m])
      )) > 0.1
    for: 10m
    labels: {severity: page}

  # --- consumer: leadership, freshness, poison --------------------------------
  - alert: CloudScaleNoConsumerLeader
    expr: sum by (consumer) (cloudscale_consumer_is_leader) < 1
    for: 1m
    labels: {severity: page}
    annotations: {runbook: "docs/RUNBOOK.md#r2"}

  - alert: CloudScaleProjectionLagging
    expr: max by (consumer) (cloudscale_consumer_lag_events) > 1000
    for: 5m
    labels: {severity: page}
    annotations: {runbook: "docs/RUNBOOK.md#r2"}

  - alert: CloudScaleConsumerStalled
    expr: time() - max by (consumer) (cloudscale_consumer_last_drain_timestamp_seconds) > 60
    for: 2m
    labels: {severity: page}
    annotations: {runbook: "docs/RUNBOOK.md#r2"}

  - alert: CloudScaleConsumerCircuitOpen
    expr: rate(cloudscale_consumer_halts_total[5m]) > 0
    for: 5m
    labels: {severity: ticket}

  - alert: CloudScaleDeadLetters
    expr: increase(cloudscale_consumer_events_dead_lettered_total[15m]) > 0
    labels: {severity: ticket}
    annotations: {runbook: "docs/RUNBOOK.md#r3"}

  # --- abuse ------------------------------------------------------------------
  - alert: CloudScaleRateLimitingBroadly
    expr: |
      sum(rate(cloudscale_http_requests_total{status="429"}[10m]))
        / sum(rate(cloudscale_http_requests_total[10m])) > 0.05
    for: 10m
    labels: {severity: ticket}
    annotations: {runbook: "docs/RUNBOOK.md#r5"}
```

## Review cadence

Re-derive targets from a fresh dual-tier gate run at each tagged release; if
observed p99 drifts to within 2× of a target, tighten the target or
investigate. When 30 days of production metrics exist, replace the
availability "target" row with the measured value and its evidence path.
