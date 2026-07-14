# Flagship Project Spec v2 — "TickStream": Market-Data Pipeline Built as a System-Design Artifact

**Budget: 5-6 hrs/day → ~40 hrs of build time across 14 days.**

**Show-off thesis:** interviewers are not impressed by boxes in a diagram — they're impressed by (1) measured numbers, (2) reproducible failure drills, (3) written design decisions with rejected alternatives. v2 is built so the REPO ITSELF answers interview questions before you open your mouth.

**Repo:** `tickstream` (public, `trudransh`)

---

## Architecture v2

```
Binance/Coinbase public WebSocket (live, free, genuinely high-volume)
        │  reconnect w/ backoff+jitter, heartbeat watchdog
        ▼
[ingest-svc]  Python asyncio ws client + FastAPI admin API
  - normalize → event schema {event_uuid, symbol, ts_event, ts_ingest, price, qty}
  - Redis sliding-window rate limiter middleware (Lua, atomic)
  - graceful shutdown: SIGTERM → drain → flush producer
        │  confluent-kafka, acks=all, enable.idempotence, linger/batch tuned
        ▼
[Kafka · KRaft]  trades.raw (6 partitions, keyed by symbol)
  - trades.retry (bounded attempts) + trades.dlq
  - lag exported to Prometheus
        │
        ▼
[worker pool]  ★ THE CENTERPIECE: 3 switchable delivery modes (--mode flag)
  1. at-most-once   (commit before process)   → demo: crash = data loss
  2. at-least-once  (commit after process)    → demo: crash = duplicates
  3. effectively-once (at-least-once + UUID idempotent upsert) → demo: crash = correct
  - per-key ordered parallelism (workers hash-partitioned by symbol)
  - bounded internal queue → measurable backpressure
  - 1-min OHLCV windowed aggregation w/ late-event policy (event-time vs processing-time, watermark = 5s)
        │  batched INSERT ... ON DUPLICATE KEY UPDATE
        ▼
[MySQL 8 · primary + read REPLICA]
  - trades (RANGE partitioned by day), candles_1m, outbox, dedupe design
  - replication running in compose → demo real lag + read-your-writes fix
        ▼
[query-api]  FastAPI on the REPLICA
  - top-N movers (index-backed ORDER BY LIMIT), keyset pagination
  - Redis cache-aside w/ singleflight stampede guard + TTL jitter
  - read-your-writes demo endpoint (pin-to-primary after write)
        +
[obs]  JSON logs w/ correlation IDs end-to-end (HTTP → Kafka header → worker → DB row)
       Prometheus (throughput, lag, p50/p95/p99, error rate) + Grafana dashboard (committed as JSON)
        +
[chaos/]  ★ make kill-broker · make kill-consumer-midbatch · make poison
          make lag-storm · make replica-lag · make stampede
          each script prints WHAT TO OBSERVE; results logged in RUNBOOK.md
        +
[docs/adr/]  ★ 6 one-page ADRs (decision + rejected alternative + why):
  001 partition count & key choice        004 outbox vs dual-write
  002 delivery semantics per mode         005 batch size vs latency tradeoff (with measured curve)
  003 retry topic vs blocking (ordering)  006 replica reads + staleness handling
```

Everything: one `docker-compose up`. **Stretch (only if Day 12 is on schedule):** k8s/ dir with minikube manifests + probes + resource limits — deploy once, screenshot, done. Do NOT gold-plate this.

## Why this project shows off correctly

| Show-off asset | Interview effect |
|---|---|
| 3-mode delivery-semantics flag | Turns THE hardest Kafka question into a live demo you built. Nobody else in the pipeline has this |
| chaos/ directory | "Have you handled failures?" → "Here are my seven reproducible ones, run them" |
| ADRs with rejected alternatives | Senior signal: tradeoff thinking in writing (doc rubrics all score "discusses tradeoffs") |
| Measured k6 curve (batch size vs p99) | Numbers over adjectives; feeds resume bullets |
| Real replica + lag demo | microsystemDesign #6 answered from experience |
| Correlation ID traced HTTP→Kafka→MySQL | The "3 prod debugging improvements" answer, implemented |
| testcontainers integration tests | "I test against real brokers" — instant credibility |

## Milestones (5-6 h/day; ~3h/day build share)

| M | Days | Deliverable |
|---|------|-------------|
| M1 | 3 | E2E flow: ws → Kafka → single consumer → MySQL, compose up |
| M2 | 4–6 | 3-mode delivery flag + crash demos · per-key ordered worker pool + bounded queue · Redis Lua rate limiter |
| M3 | 7–9 | chaos/ scripts + RUNBOOK · retry topic + DLQ · outbox · MySQL replica + read-your-writes · cache + singleflight |
| M4 | 10–12 | k6 load curve + batch tuning · Grafana dashboard · testcontainers tests · ADRs · README w/ diagram · (stretch: minikube) |

## Resume bullets (fill AFTER measuring — brackets stay empty until k6 says otherwise)

- Built a real-time market-data pipeline (Python/asyncio, Kafka, MySQL 8 primary-replica, Redis) sustaining [X] events/sec from live exchange feeds, with switchable at-most-once / at-least-once / effectively-once delivery modes demonstrated under fault injection
- Designed transactional outbox, bounded-retry DLQ, per-key ordered parallel consumers, and Lua-based sliding-window rate limiting; verified zero event loss across 7 scripted chaos scenarios (broker kill, mid-batch consumer crash, poison pill, replica lag)
- Reduced p99 ingest-to-queryable latency [A]→[B] ms and raised throughput [C]→[D] events/sec via EXPLAIN-driven composite indexes, keyset pagination, and measured batch-size tuning over [N]M rows; instrumented end-to-end with correlation IDs, Prometheus, Grafana

## Guardrails (tighter now — more hours = more scope-creep risk, not less)

- The 3-mode flag, chaos/, and ADRs are the show-off core. If anything slips, cut the stretch K8s and the Grafana polish FIRST
- No auth, no frontend, no schema registry, no multi-broker cluster, one exchange feed
- Every chaos drill = 5 lines in RUNBOOK.md same day. Undocumented drill = didn't happen
- Numbers measured, never estimated. An empty [X] on the resume is a task, not a blank to improvise