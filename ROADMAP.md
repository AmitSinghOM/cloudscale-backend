# cloudscale-backend — Build Roadmap

Built in weekend-sized phases. Each phase ends in a tagged release, a short demo, and (where it fits) a blog post. **Do not start until AgentOS Phase 1 is shipped.**

> Rule: ship small, iterate visibly. Earn scope by finishing.

## Phase 0 — Command/query split  ·  ~2 wknds
**Goal:** Write path appends events; read path serves projections

- [ ] (break into tasks when you start this phase)

## Phase 1 — Kafka event log  ·  ~2 wknds
**Goal:** Events to Kafka; consumer builds read models; idempotent consume

- [ ] (break into tasks when you start this phase)

## Phase 2 — Resilience  ·  ~1 wknd
**Goal:** Circuit breakers, retries, DLQ for poison messages

- [ ] (break into tasks when you start this phase)

## Phase 3 — Load + observe  ·  ~1 wknd
**Goal:** Load test, capture p99 / throughput, trace the hot path, write up bottleneck+fix

- [ ] (break into tasks when you start this phase)

## Definition of done (every phase)
1. Tests pass, CI green.
2. README status updated.
3. Tagged release.
4. Blog post drafted if it maps to one.
