# City Health Observatory

A real-time **Smart City Infrastructure Health Leaderboard** built for Luddy Hackathon Case 2.

The system ingests synthetic sensor telemetry (traffic, air quality, power, water, noise) from across the Indianapolis metro, computes a per-district **Urban Health Score (UHS)** in tumbling windows, ranks districts on a live leaderboard, and propagates **cascading-failure** alerts to neighboring districts when a catastrophic drop is detected.

It ships in two flavors:

1. **Local hack mode** — single Python FastAPI process, SQLite history, no streaming dependencies. Fast to demo.
2. **Full streaming stack** — Kafka (Redpanda) + Apache Flink + Redis + PostgreSQL, all wired up via Docker Compose.

Both modes serve the **same REST API** and the **same dashboard**.

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Repository Layout](#repository-layout)
- [Quick Start](#quick-start)
  - [Option A — Full streaming stack (Docker)](#option-a--full-streaming-stack-docker)
  - [Option B — Local Python only](#option-b--local-python-only)
- [The Urban Health Score (UHS)](#the-urban-health-score-uhs)
- [Cascading Failure Model](#cascading-failure-model)
- [REST API](#rest-api)
- [Configuration](#configuration)
- [Frontend Dashboard](#frontend-dashboard)
- [Development Notes](#development-notes)

---

## Architecture

```
                                    ┌───────────────────┐
                                    │  Synthetic        │
                                    │  Sensor Simulator │
                                    │  (services/       │
                                    │   simulator.py)   │
                                    └─────────┬─────────┘
                                              │ JSON events
                                              ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │  Kafka (Redpanda) topics:                                        │
   │    sensor.traffic   sensor.aqi   sensor.power                    │
   │    sensor.water     sensor.noise   city.cascade                  │
   └──────────────────────────────────────────────────────────────────┘
                                              │
                       ┌──────────────────────┴────────────────────────┐
                       ▼                                               ▼
        ┌──────────────────────────┐                    ┌──────────────────────────┐
        │  Apache Flink job        │                    │  Plain-Python worker     │
        │  (services/flink_job.py) │   ── OR fallback ──│  (services/worker.py)    │
        │  5s tumbling windows,    │                    │  same UHS math, no JVM   │
        │  weighted UHS, cascade   │                    │                          │
        └────────────┬─────────────┘                    └──────────┬───────────────┘
                     │                                             │
                     └──────────────────┬──────────────────────────┘
                                        ▼
                ┌──────────────────────────────────────┐
                │  Redis (live leaderboard + state)    │
                │  - sorted set of district UHS        │
                │  - per-district component snapshot   │
                │  - manual overrides / suppress flags │
                └──────────────────┬───────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────┐         ┌─────────────────┐
                    │  FastAPI service         │◄────────│  Postgres       │
                    │  (backend/app/main.py)   │ history │  (Docker mode)  │
                    │  REST + static UI        │         │  / SQLite       │
                    └────────────┬─────────────┘         │  (local mode)   │
                                 │                       └─────────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │  Browser dashboard       │
                    │  (client/index.html)     │
                    │  live polling, charts    │
                    └──────────────────────────┘
```

## Features

- **Tumbling-window stream aggregation** of five sensor families into a single 0–100 Urban Health Score per district (5-second windows, configurable weights).
- **Live leaderboard** ranked by UHS, top-N configurable, served as JSON, HTML, or via the polling dashboard.
- **Cascading-failure detection** — a catastrophic drop in one district applies a penalty to its geographic neighbors for the next N windows, simulating real-world infrastructure ripple effects.
- **Manual overrides** via `POST /add` for demo control: pin a district's score (with optional `catastrophic` flag to trigger ripples), or remove a district from the board.
- **Two execution modes**, identical API:
  - Streaming pipeline (Kafka → Flink/worker → Redis + Postgres).
  - Standalone in-memory + SQLite mode for laptop demos.
- **Distribution analytics** — `/info` returns mean, median, MAD, IQR, entropy and other robust stats over current live scores.
- **API latency telemetry** — every response carries an `X-Response-Time-Ms` header, and `/performance` exposes a rolling-average view plus pipeline lag.
- **Indy-metro neighborhood graph** seeded with ~60 districts (Downtown, Mass Ave, Broad Ripple, Carmel, Fishers, …) and a hand-curated adjacency list for cascade ripples.

## Tech Stack

| Layer | Tech |
|---|---|
| API | FastAPI · Uvicorn · Pydantic v2 |
| Streaming | Apache Flink (PyFlink) · Kafka (Redpanda) |
| Stream worker (fallback) | `kafka-python-ng` consumer/producer |
| Live state | Redis 7 (sorted sets + JSON blobs) |
| History | PostgreSQL 16 (Docker) or SQLite (local) |
| Frontend | Vanilla HTML/CSS/JS, Google Fonts |
| Orchestration | Docker Compose v2.24+ |
| Language | Python 3.12 |

## Repository Layout

```
.
├── backend/
│   ├── app/                # FastAPI service
│   │   ├── main.py         # Routes: /add, /remove, /leaderboard, /info, /history, /state, /performance
│   │   ├── config.py       # YAML + env loader
│   │   ├── db.py / pg.py   # SQLite + Postgres history adapters
│   │   ├── redis_store.py  # Sorted-set leaderboard + override/suppress helpers
│   │   └── stats.py        # Robust distribution metrics for /info
│   ├── services/
│   │   ├── simulator.py    # Synthetic sensor publisher → Kafka
│   │   ├── worker.py       # Pure-Python stream processor (Flink fallback)
│   │   └── flink_job.py    # PyFlink job: 5s tumbling windows + cascade alerts
│   ├── config.yaml         # All tunables (weights, districts, adjacency, …)
│   ├── openapi.yaml        # REST contract
│   └── requirements.txt
├── client/                 # Static dashboard served by FastAPI at /ui
│   ├── index.html
│   ├── app.js
│   └── styles.css
├── infra/
│   └── init.sql            # Postgres schema bootstrap
├── Dockerfile              # API + simulator image
├── Dockerfile.flink        # Flink image with PyFlink job baked in
├── docker-compose.yml      # Full streaming stack
└── docker-compose.override.yml
```

## Quick Start

### Option A — Full streaming stack (Docker)

Requires Docker Desktop / Docker Engine with Compose **v2.24+** (for `service_completed_successfully`).

```bash
docker compose up --build
```

Once everything is healthy:

| What | URL |
|---|---|
| Dashboard | <http://localhost:8000/ui/> |
| REST API root | <http://localhost:8000> |
| OpenAPI spec | <http://localhost:8000/openapi.yaml> |
| Flink Web UI | <http://localhost:8081> |
| Postgres | `postgres://app:app@localhost:5432/citylb` |
| Redis | `redis://localhost:6379/0` |
| Redpanda (Kafka) | `localhost:19092` |

The `simulator` container fires ~75 sensor events every 200ms across all seeded Indy-metro districts; the Flink job aggregates them into UHS scores every 5 seconds; the dashboard polls the API and re-renders the leaderboard live.

To use the **plain-Python worker instead of Flink** (e.g. when the Flink JobManager isn't healthy on your machine), comment out the `flink-*` services in `docker-compose.yml` and add a `worker` service that runs `python -m services.worker` against the same Redis / Postgres / Kafka.

### Option B — Local Python only

No Docker, no Kafka, no Flink. SQLite history, in-memory leaderboard. Great for UI work or quick demos.

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Then open <http://localhost:8000/ui/> and POST to `/add` to drive the leaderboard manually:

```bash
curl -X POST http://localhost:8000/add \
  -H 'content-type: application/json' \
  -d '{"district":"Downtown","score":78.5}'
```

## The Urban Health Score (UHS)

Each district's UHS is a weighted blend of five normalized sensor signals (each on a 0–100 "healthier-is-higher" scale):

```
UHS = 0.25·traffic + 0.30·aqi + 0.20·power + 0.10·water + 0.15·noise
```

Weights live in `backend/config.yaml` under `urban_health.weights` and must sum to 1.0. Each tumbling window's UHS is also blended with the previously published score (`previous_blend: 0.15`) to provide a small amount of temporal smoothing and avoid jitter.

## Cascading Failure Model

When a district's score drops by more than `cascade.drop_threshold` (default **20** points) in a single window **and** is flagged catastrophic, the worker:

1. Publishes a `city.cascade` event with the originating district and ripple metadata.
2. Subtracts `cascade.neighbor_penalty` (default **12** points) from each adjacent district's score for the next `cascade.penalty_windows` windows (default **2**).

Adjacency is an undirected graph defined under `districts.adjacency` in `config.yaml` — every seed district has 2–4 neighbors so a single failure propagates outward instead of stranding isolated nodes.

## REST API

Full contract is in [`backend/openapi.yaml`](backend/openapi.yaml). Highlights:

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/add` | Add or override a district's UHS (`{district, score, catastrophic?}`) |
| `POST` | `/remove` | Remove a district from the leaderboard |
| `GET` | `/leaderboard` | Top-N HTML table |
| `GET` | `/state` | JSON snapshot of every district + score components (used by dashboard) |
| `GET` | `/info` | Distribution stats (mean, median, MAD, IQR, entropy, …) |
| `GET` | `/history` | Score history from Postgres / SQLite, filterable by district + time range |
| `GET` | `/performance` | Rolling API latency + pipeline lag |

All responses include `X-Response-Time-Ms`.

## Configuration

Everything tunable lives in `backend/config.yaml`:

- `urban_health.weights` — per-sensor blend weights.
- `urban_health.aggregation_window_seconds` — Flink/worker tumbling-window size.
- `urban_health.cascade.*` — drop threshold, neighbor penalty, ripple length.
- `districts.ids` + `districts.adjacency` — seed neighborhoods and ripple graph.
- `simulator.events_per_tick`, `simulator.tick_interval_ms` — load knobs for the synthetic sensor feed.
- `leaderboard.top_n` — default leaderboard depth.
- `cors.allow_origins` — CORS allowlist for the frontend.


## Frontend Dashboard

The dashboard at `/ui/` is a single-page vanilla JS app:

- **Live leaderboard** — animated reorder, color-coded health bands, cascade markers.
- **Analytics** — distribution stats (mean / median / MAD / entropy) sourced from `/info`.
- **Activity feed** — newest UHS updates and cascade events.
- **Interaction panel** — POST `/add` and `/remove` against any seeded or custom district, with a `catastrophic` toggle to demo cascading failures.
- **Theme toggle** — light / dark, persisted to `localStorage`, with a no-flash inline boot script.


---

Built for **Luddy Hackathon — Case 2: Smart City Infrastructure Health Leaderboard**.
