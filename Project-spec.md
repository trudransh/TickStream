# Flagship Project Spec — "TickStream": Market-Data Ingestion Pipeline

**Why this project:** YipitData's business is turning raw feeds into analytics. You build exactly that, using data you already understand (crypto market data), with the exact stack the friend named: **Python + Kafka + MySQL**. Every scenario tab in the interview doc maps to a component you built and broke.

**Repo name:** `tickstream` (public, on `trudransh`)

---

## Architecture

```
Binance/Coinbase public WebSocket (real, free, high-volume)
        │
        ▼
[ingest-svc]  FastAPI + websocket client, Python
  - normalizes trade events → JSON schema with event UUID
  - Redis sliding-window rate limiter on admin/query endpoints
  - transactional OUTBOX variant for API-originated writes
        │  confluent-kafka producer, acks=all, idempotent producer
        ▼
[Kafka]  1 broker KRaft, topic trades.raw (6 partitions, keyed by symbol)
  - retry topic + dead-letter topic (trades.dlq)
        │
        ▼
[worker pool]  Python consumers, manual offset commit
  - idempotent upsert (event UUID unique key)
  - aggregates 1-min OHLCV candles + rolling volume
  - bounded internal queue → backpressure
        │
        ▼
[MySQL 8]  trades (partitioned by day), candles_1m, outbox
  - composite indexes designed via EXPLAIN, batch inserts
        │
        ▼
[query-api]  FastAPI: top-N movers, per-symbol history
  - Redis read-through cache (stampede-protected)
        +
[observability]  JSON logs + correlation IDs, Prometheus, Grafana panel
```

All via one `docker-compose up`. No K8s — Docker Compose is defensible and honest.

## Milestones (match PREP_PLAN days)

| M | Day | Deliverable |
|---|-----|-------------|
| M1 | 3 | End-to-end flow: ws → Kafka → consumer → MySQL |
| M2 | 4–6 | Idempotent consumer, worker pool + backpressure, rate limiter middleware |
| M3 | 7–9 | DLQ + retry topic, failure RUNBOOK, outbox, Redis cache |
| M4 | 10–11 | Load test numbers, metrics, testcontainers integration tests, README + diagram |

## Interview story map (doc tab → what you did)

| Doc scenario | Your lived answer |
|---|---|
| Kafka duplicates/out-of-order | Killed consumer mid-batch Day 4; fixed with UUID upsert; per-key ordering via symbol partitioning |
| Consumer lag explosion | Manufactured 100k backlog Day 7; measured recovery; partitions ceiling on parallelism |
| Poison pill → DLQ | Built retry-with-limit + trades.dlq Day 7 |
| DB→Kafka reliability (outbox) | Built it Day 9, killed relay mid-flight, zero loss |
| Rate limiter coding Q | Sliding-log limiter is IN the repo as middleware |
| Mongo/MySQL slow at peak | EXPLAIN-driven index fixes + batch inserts, before/after numbers Day 10 |
| Cache stampede | Singleflight lock on query-api cache Day 9 |
| Timeout/retry storm | Backoff+jitter+budget in ws reconnect and API client Day 10 |
| Worker backlog (Python concurrency) | Bounded queue + pool sizing Day 5 |
| Prod debugging | Correlation IDs traced one event end-to-end through logs |

## Resume bullets (fill numbers after Day 10 — do NOT invent them)

- Engineered a real-time market-data pipeline (Python, Kafka, MySQL 8, Redis) ingesting live exchange WebSocket feeds at [X] events/sec sustained, with idempotent exactly-once processing via UUID-keyed upserts and manual offset management
- Implemented transactional outbox, dead-letter topic with bounded retries, and a Redis sliding-window rate limiter; verified zero event loss under broker kill and consumer-crash fault injection
- Cut p99 ingest-to-queryable latency from [A] to [B] ms by replacing row-at-a-time inserts with batched writes and EXPLAIN-driven composite indexes over [N]M rows

## Guardrails

- Scope creep is the enemy: no auth, no frontend, no multi-broker cluster, one exchange feed is enough
- If a day runs over, cut the Redis cache (Day 9 optional part) first, never the failure drills — drills are the interview value
- Numbers must be measured, not estimated. Empty [X] brackets stay empty until k6 says otherwise