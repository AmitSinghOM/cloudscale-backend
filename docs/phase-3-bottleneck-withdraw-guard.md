# Phase 3 bottleneck analysis — the withdraw guard's O(n) replay

**Status:** fixed and verified — 29.6× hot-path throughput, latency flat in depth
**Baseline evidence:** `evidence/74710a859d6cce56d70b912f6fe560bfaafa9978/phase-3-load/report.json`
**Harness:** `scripts/load_and_observe.py` (single process/thread, file-backed
SQLite, no HTTP — local baseline, not a service benchmark)

## Symptom

Baseline run (200 accounts × 50 commands + 2,000 withdraws against one hot
account; 12,001 events total):

| Segment                | Throughput   | p50      | p95      | p99      |
|------------------------|-------------:|---------:|---------:|---------:|
| mixed_commands         |    6,979.7/s | 0.111 ms | 0.263 ms | 0.491 ms |
| hot_account_withdraws  |    **312.7/s** | 3.153 ms | 5.749 ms | 7.767 ms |
| consumer_catchup       |   10,690.7/s |        — |        — |        — |
| queries                |  199,866.9/s | 0.004 ms | 0.005 ms | 0.009 ms |

Withdraw latency against the same account is **linear in stream depth**:

| Stream depth | Withdraw latency |
|-------------:|-----------------:|
|            1 |        0.084 ms  |
|        1,001 |        2.962 ms  |
|        2,000 |        5.649 ms  |

A 22× throughput collapse (6,980/s → 313/s) purely from stream depth, on an
account with 2,001 events. Real ledgers hold orders of magnitude more.

## Root cause

`CommandHandler._handle_withdraw` (`cqrs/commands.py`) enforces the
no-overdraft rule by replaying the **entire stream on every withdraw**:

```python
current = BalanceProjection().rebuild(self._store.read(self._stream(account_id)))
```

Two multiplied costs:

1. `SqliteEventStore.read(stream)` fetches every row of the stream —
   O(n) I/O per command.
2. `BalanceProjection.rebuild` folds every event — O(n) CPU per command.

Total work for m withdraws on one account: O(m·n) — quadratic in practice,
since each accepted withdraw also deepens the stream.

## Fix options considered

1. **Memoized fold + incremental catch-up read (chosen).** The handler keeps
   an in-memory `(last_seq, folded_state)` per stream. Before deciding, it
   reads only events with `seq > last_seq` (a new `read_after` on both
   stores), folds just those, then appends and advances the memo. The balance
   used for the decision is byte-for-byte the same fold the replay produced —
   memoization, not approximation. Per-command cost drops from O(n) to
   O(delta); O(1) amortized in the hot loop.
2. **Durable snapshot table in the event store.** Same effect, survives
   restarts, but adds write-path schema and a snapshot-invalidations story —
   more machinery than the measured problem needs. A cold start costs one
   full replay per stream, which the memo already bounds to once per process.
3. **Push the fold into SQL (`SELECT SUM(...)`).** Cheaper constants but
   still O(n) per command, couples the business rule to storage, and diverges
   from `BalanceProjection` semantics (unknown event types). Rejected.
4. **Read the consumer's projection.** Reading the eventually-consistent read
   model for a write-side decision changes correctness (projection lag would
   admit overdrafts). Rejected outright.

## Chosen design: memoized fold + incremental catch-up

- `EventStore.read_after(stream, after_seq)` and
  `SqliteEventStore.read_after(stream, after_seq)` — per-stream reads of
  events with `seq > after_seq`, in order. Additive API; `read(stream)` is
  unchanged (`read_after(stream, 0)` is equivalent).
- `CommandHandler` folds only unseen events through the *same*
  `BalanceProjection` before every decision, and advances the memo with the
  event it appends.

### Correctness invariants

1. **Equivalence:** for any command sequence, decisions and appended events
   are identical to the full-replay implementation (cache is a fold memo;
   catch-up before every decision folds exactly the suffix the memo lacks).
2. **External writers:** another handler on the same store is picked up by
   the catch-up read, because the decision always reads past the memo first.
   The pre-existing read-then-append race window is unchanged — not widened,
   not narrowed. (The typed `cloudscale` command path owns real concurrency
   control; property 04 covers it.)
3. **Crash safety:** the memo is process-local and rebuilt from the log on
   first touch; no new durable state exists to corrupt.

## Result (after-run)

**After evidence:** `evidence/d82039b4cb1b5d74ed48e1060726be59a76d37a2/phase-3-load/report.json`
(identical workload, same host, fix commit `d82039b`)

| Segment / sample        | Before (74710a8) | After (d82039b) | Change |
|-------------------------|-----------------:|----------------:|-------:|
| hot withdraws throughput|          312.7/s |       9,246.1/s | **29.6×** |
| hot withdraws p99       |         7.767 ms |        0.268 ms | **29×** |
| withdraw @depth 2,000   |         5.649 ms |        0.080 ms | **70×** |
| mixed commands p99      |         0.491 ms |        0.380 ms | 1.3× |

Withdraw latency is now flat in stream depth (0.094 ms @1, 0.104 ms @1,001,
0.080 ms @2,000) and the hot account sustains the same throughput as shallow
streams (9,246/s vs 9,051/s mixed) — the O(n) term is gone, exactly as the
design predicted. Mixed commands also improved ~30% because every third
command in that segment is a withdraw.

Correctness was held by construction and by test: decision-identity against
the original replay implementation across randomized command sequences,
external-writer catch-up, and the exact overdraft boundary
(`tests/unit/test_withdraw_guard_fold.py`; suite 159 green).
