# cloudscale-backend

**A production-shaped distributed backend demonstrating CQRS, event sourcing, async processing, and resilience patterns at scale.**

> **Status: planned.** Part of the AI Platform Engineer blueprint. Not started — this is the planning scaffold. Build order: **AgentOS first**, then earn the right to start the next one by finishing the last.

## Why this exists

A reference backend that does the unglamorous things right: separates reads from writes, sources state from events, isolates failures with circuit breakers, and never silently drops work. The point is to *operate* it under load and show the numbers.

## Architecture (planned)

```
API (FastAPI) → Command side (writes → event log) → Kafka → Projections (read models) ; Query side reads projections
```

## Components

- CQRS: separate command and query paths
- Event sourcing with Kafka as the log
- Read-model projections in PostgreSQL
- Circuit breakers around downstream calls
- Dead-letter queue for poison messages
- Idempotent consumers + OpenTelemetry tracing

## Tech stack

Python · FastAPI · Kafka · PostgreSQL · Redis · OpenTelemetry · Prometheus/Grafana · k6 (load)

See [ROADMAP.md](./ROADMAP.md) for the phased build plan.
