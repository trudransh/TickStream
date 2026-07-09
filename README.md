# TickStream — Real-Time Market-Data Ingestion Pipeline

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Kafka KRaft](https://img.shields.io/badge/kafka-KRaft-orange.svg)](https://kafka.apache.org/)
[![MySQL 8](https://img.shields.io/badge/mysql-8.0-blue.svg)](https://www.mysql.com/)
[![Docker Compose](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://docs.docker.com/compose/)

A production-grade, real-time cryptocurrency market data pipeline that ingests live trade events from exchange WebSocket feeds, processes them through Kafka, and stores normalized trades and aggregated OHLCV candles in MySQL — all observable end-to-end via correlation IDs, Prometheus metrics, and Grafana dashboards.

---

## Architecture

```
Binance / Coinbase public WebSocket (real, free, high-volume)
        │
        ▼
┌──────────────────────────────────────────────────────┐
│  ingest-svc  (FastAPI + websocket client)             │
│  • Normalizes trade events → JSON schema + event UUID │
│  • Redis sliding-window rate limiter on endpoints      │
│  • Transactional OUTBOX for API-originated writes      │
└──────────────┬───────────────────────────────────────┘
               │  confluent-kafka producer
               │  acks=all, idempotent
               ▼
┌──────────────────────────────────────────────────────┐
│  Kafka  (1 broker, KRaft mode)                        │
│  topic: trades.raw  (6 partitions, keyed by symbol)   │
│  + trades.retry  +  trades.dlq                        │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│  worker pool  (Python consumers, manual offset commit)│
│  • Idempotent upsert via event UUID unique key         │
│  • 1-min OHLCV candle aggregation + rolling volume     │
│  • Bounded internal queue → backpressure               │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│  MySQL 8                                              │
│  tables: trades (partitioned by day), candles_1m,     │
│          outbox                                       │
│  • Composite indexes (EXPLAIN-driven)                  │
│  • Batch inserts                                       │
└──────────────┬───────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────────┐
│  query-api  (FastAPI)                                 │
│  • Top-N movers, per-symbol history                    │
│  • Redis read-through cache (stampede-protected)       │
└──────────────────────────────────────────────────────┘
               +
┌──────────────────────────────────────────────────────┐
│  observability                                        │
│  • JSON structured logs + correlation IDs              │
│  • Prometheus metrics                                  │
│  • Grafana dashboard                                   │
└──────────────────────────────────────────────────────┘
```

**All services via one `docker-compose up`.** No Kubernetes — Docker Compose is honest and sufficient.

---

## Quick Start

### Prerequisites

- Docker & Docker Compose v2+
- Python 3.11+ (for local development)
- `make` (optional, for convenience targets)

### Run Everything

```bash
git clone https://github.com/trudransh/tickstream.git
cd tickstream
cp .env.example .env          # review and edit as needed
docker-compose up --build -d
```

### Verify

```bash
# Check all services are healthy
docker-compose ps

# Tail ingest logs (watch trades flow in)
docker-compose logs -f ingest-svc

# Query the API
curl http://localhost:8000/api/v1/trades/BTCUSDT?limit=10
curl http://localhost:8000/api/v1/movers?top=5
```

### Local Development

```bash
python -m venv .venv
source .venv/bin/activate      # or .venv\Scripts\activate on Windows
pip install -e ".[dev]"
pre-commit install
```

---

## Project Structure

```
tickstream/
├── docker-compose.yml          # Full stack orchestration
├── .env.example                # Environment variable template
├── pyproject.toml              # Python project metadata + deps
├── Makefile                    # Convenience targets
│
├── src/
│   ├── ingest/                 # Ingest service (FastAPI + WS client)
│   │   ├── __init__.py
│   │   ├── main.py             # FastAPI app entrypoint
│   │   ├── ws_client.py        # WebSocket connection manager
│   │   ├── normalizer.py       # Exchange-agnostic trade normalization
│   │   ├── producer.py         # Kafka producer (idempotent, acks=all)
│   │   ├── rate_limiter.py     # Redis sliding-window rate limiter
│   │   └── outbox.py           # Transactional outbox writer
│   │
│   ├── worker/                 # Consumer worker pool
│   │   ├── __init__.py
│   │   ├── main.py             # Worker pool entrypoint
│   │   ├── consumer.py         # Kafka consumer (manual commit)
│   │   ├── processor.py        # Trade processing + candle aggregation
│   │   ├── writer.py           # Batched MySQL writer
│   │   └── backpressure.py     # Bounded queue + flow control
│   │
│   ├── query/                  # Query API service
│   │   ├── __init__.py
│   │   ├── main.py             # FastAPI app entrypoint
│   │   ├── routes.py           # API endpoints
│   │   └── cache.py            # Redis read-through + stampede protection
│   │
│   ├── shared/                 # Shared modules across services
│   │   ├── __init__.py
│   │   ├── config.py           # Pydantic settings
│   │   ├── models.py           # Pydantic schemas (Trade, Candle, etc.)
│   │   ├── db.py               # MySQL connection pool + helpers
│   │   ├── logging.py          # Structured JSON logging + correlation IDs
│   │   └── metrics.py          # Prometheus metric definitions
│   │
│   └── outbox_relay/           # Outbox relay (polls outbox → Kafka)
│       ├── __init__.py
│       └── relay.py            # Outbox poller + publisher
│
├── migrations/                 # Database migrations
│   └── 001_init.sql            # Initial schema: trades, candles_1m, outbox
│
├── kafka/                      # Kafka configuration
│   └── create-topics.sh        # Topic creation script (trades.raw, retry, dlq)
│
├── grafana/                    # Grafana provisioning
│   ├── dashboards/
│   │   └── tickstream.json     # Main dashboard definition
│   └── datasources/
│       └── prometheus.yml      # Prometheus datasource config
│
├── prometheus/                 # Prometheus configuration
│   └── prometheus.yml          # Scrape targets
│
├── tests/                      # Test suite
│   ├── __init__.py
│   ├── conftest.py             # Shared fixtures (testcontainers)
│   ├── unit/                   # Unit tests
│   │   ├── __init__.py
│   │   ├── test_normalizer.py
│   │   ├── test_rate_limiter.py
│   │   ├── test_processor.py
│   │   └── test_backpressure.py
│   └── integration/            # Integration tests (testcontainers)
│       ├── __init__.py
│       ├── test_ingest_to_kafka.py
│       ├── test_consumer_idempotent.py
│       └── test_end_to_end.py
│
├── scripts/                    # Operational scripts
│   ├── load_test.js            # k6 load test script
│   └── fault_inject.sh         # Fault injection helpers
│
├── docs/                       # Documentation
│   ├── RUNBOOK.md              # Failure runbook
│   ├── ARCHITECTURE.md         # Detailed architecture decisions
│   └── LOAD_TEST_RESULTS.md    # Measured performance numbers
│
└── .github/
    └── workflows/
        └── ci.yml              # GitHub Actions CI pipeline
```

---

## Milestones

| M | Days | Deliverable | Status |
|---|------|-------------|--------|
| **M1** | 3 | End-to-end flow: WS → Kafka → consumer → MySQL | ⬜ |
| **M2** | 4–6 | Idempotent consumer, worker pool + backpressure, rate limiter | ⬜ |
| **M3** | 7–9 | DLQ + retry topic, failure runbook, outbox, Redis cache | ⬜ |
| **M4** | 10–11 | Load test numbers, metrics, testcontainers tests, README + diagram | ⬜ |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **UUID-keyed upserts** | Exactly-once semantics without Kafka transactions — simpler, verifiable |
| **Symbol-based partition key** | Guarantees per-symbol ordering; parallelism via partition count |
| **Manual offset commit** | Commit only after successful DB write — at-least-once with idempotent sink |
| **Bounded internal queue** | Backpressure prevents OOM under consumer lag; configurable depth |
| **Transactional outbox** | Guarantees DB ↔ Kafka consistency for API-originated writes |
| **Sliding-window rate limiter** | More accurate than fixed-window; implemented as middleware |
| **Read-through cache + singleflight** | Prevents cache stampede on popular queries |
| **KRaft mode (no ZooKeeper)** | Modern Kafka deployment; simpler single-broker setup |

---

## Performance (fill after M4 load tests)

> **Guardrail:** Numbers must be measured, not estimated. Empty brackets stay empty until k6 says otherwise.

| Metric | Value |
|--------|-------|
| Sustained ingest rate | [X] events/sec |
| p50 ingest-to-queryable latency | [A] ms |
| p99 ingest-to-queryable latency | [B] ms |
| Total rows processed (load test) | [N]M |
| Consumer recovery from 100k backlog | [T] sec |

---

## License

MIT
