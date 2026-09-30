# Ride-Hailing Fleet Operations — End-to-End Big Data Platform

> **EC 8203: Applied Big Data Engineering — Mini Project Assessment**  
> **Architecture Pattern:** Lambda Architecture (Speed Layer + Batch Layer + Serving Layer)  
> **Use Case:** Ride-Hailing Fleet Operations & Profitability Reconciliation  

[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.12-blue?logo=python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://www.docker.com/)
[![Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)](https://www.postgresql.org/)
[![SQLite](https://img.shields.io/badge/SQLite-Embedded-lightgrey?logo=sqlite)](https://www.sqlite.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-Serving-green?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit)](https://streamlit.io/)

---

## 📋 Overview

This platform implements an end-to-end **Lambda Architecture** data pipeline that addresses two fundamental operational requirements in ride-hailing fleet management:
1. **Real-Time Fleet Utilization (Speed Layer):** Ingests continuous GPS and trip telemetry every 2 seconds (via **Apache Kafka** in Docker mode, or high-throughput in-memory event stream in Standalone mode), performing windowed aggregations of zone utilization, trip counts, earnings, and automated threshold alerts when vehicles idle excessively ($\ge 60\text{s}$).
2. **Historical Profitability Reconciliation (Batch Layer):** Periodically reconciles daily vehicle expenses (fuel and garage maintenance) with streaming fare earnings to compute per-vehicle net profit, detect unprofitable vehicles, and generate automated fleet maintenance recommendations.
3. **Serving Layer & Interactive Dashboard:** Unified serving layer (**PostgreSQL** or **SQLite**) exposed through a **FastAPI** REST interface and an interactive **Streamlit** dashboard.

---

## 🏛️ System Architecture

```
       [ Telemetry Stream (GPS, Fares, Status) ]       [ Daily Batch Expense Files (CSV/JSON) ]
                         │                                                 │
                         ▼                                                 │
              ┌─────────────────────┐                                      │
              │ Ingestion Stream    │                                      │
              │ (Kafka / Stream Bus)│                                      │
              └──────────┬──────────┘                                      │
                         │                                                 │
         ┌───────────────┴───────────────┐                                 │
         ▼                               ▼                                 ▼
┌─────────────────┐             ┌─────────────────┐             ┌────────────────────┐
│   SPEED LAYER   │             │   BATCH LAYER   │             │    BATCH LAYER     │
│(Kafka Consumer) │             │ (Raw Data Lake) │             │  (Airflow / Engine)│
│  Window Aggs,   │             │ Parquet Storage │             │ Daily Cost & Fare  │
│  Live Earnings, │             │ (Historical Log)│             │  Reconciliation    │
│   Idle Alerts   │             └────────┬────────┘             └─────────┬──────────┘
└────────┬────────┘                      │                                │
         │                               └────────────────┬───────────────┘
         ▼                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│                  SERVING LAYER (PostgreSQL / Embedded SQLite)                      │
│   • speed_fleet_metrics (Real-time utilization, active count, zone earnings)       │
│   • speed_vehicle_alerts (Threshold-based idle alerts)                             │
│   • batch_daily_profitability (Vehicle daily net profit, ROI, unprofitable flag)   │
│   • v_unified_vehicle_summary (Unified serving SQL view)                           │
└─────────────────────────────────────────┬──────────────────────────────────────────┘
                                          │
                         ┌────────────────┴────────────────┐
                         ▼                                 ▼
              ┌─────────────────────┐           ┌─────────────────────┐
              │  FastAPI REST API   │           │ Streamlit Dashboard │
              │  Metrics & Alerts   │           │ Real-Time Map, Kpis │
              │  Serving Endpoints  │           │ Profitability Plots │
              │     (Port 8000)     │           │     (Port 8501)     │
              └─────────────────────┘           └─────────────────────┘
```

---

## ⏱️ Simulated Time Clock
To allow full end-to-end evaluation within a standard live demo:
- **1 Simulated Day = 5 Minutes (300 seconds)** *(or 60s in local test mode)*
- **Telemetry Frequency:** Emitted every 2 seconds
- **Batch Drop Cadence:** Drops garage expense CSV/JSON files every simulated day

---

## 🚀 How to Run the Platform

You can run the project in **two ways**:

### Option A: Standalone Mode (Zero Docker — Pure Python + SQLite)
*Recommended for quick local testing and development.*

1. **Activate your virtual environment:**
   ```powershell
   # Windows PowerShell
   .\.venv\Scripts\Activate.ps1

   # Or in Command Prompt:
   .\.venv\Scripts\activate.bat

   # Or in Linux / macOS / WSL:
   source .venv/bin/activate
   ```

2. **Install dependencies (if not already installed):**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start the pipeline and Serving API (Terminal 1):**
   ```bash
   python run_standalone.py
   ```
   *This starts the telemetry producer, speed window aggregations, daily batch reconciler, Parquet writer, and FastAPI server on `http://127.0.0.1:8000`.*

   Useful overrides (all optional): `SIMULATED_DAY_SEC=300` (match the documented 5-minute simulated day; the default is a faster 60s demo day), `WINDOW_SIZE_SEC=10`, `IDLE_ALERT_THRESHOLD_SEC=60`, `API_PORT=8000`, `SQLITE_DB_PATH=...`, `LAUNCH_DASHBOARD=1` (start Streamlit too), `LOG_LEVEL=INFO`.

4. **Launch the Live Dashboard (Terminal 2):**
   ```bash
   streamlit run serving/dashboard.py
   ```
   *Open **[http://localhost:8501](http://localhost:8501)** in your browser.*

---

### Option B: Docker Compose Mode (Full Production Stack)
*Recommended for final evaluation, viva demo, and submission.*

This mode runs the entire stack inside containers: **Apache Kafka (KRaft)**, **PostgreSQL 15**, the **Kafka stream processor** (Python stateful window engine), **Apache Airflow** (opt-in profile), **FastAPI**, and **Streamlit**.

#### If using Ubuntu WSL:
```bash
# Inside WSL terminal:
cd /mnt/e/Github/big-data-mini-project
docker compose up --build -d
```

#### Or directly from Windows PowerShell:
```powershell
wsl docker compose up --build -d
```

#### View container logs:
```powershell
wsl docker compose logs -f
```

#### Stop all containers:
```powershell
wsl docker compose down
```

#### Optional: run the orchestration layer (Apache Airflow)
The Airflow service is opt-in (compose profile `airflow`) so the default stack stays light. It runs the same reconciliation engine as `batch-layer`:
```bash
docker compose --profile airflow up -d
# Airflow UI: http://localhost:8080  (the generated admin password is printed in the container logs)
docker compose logs -f airflow | grep -i password
```
> Run **either** the `batch-layer` scheduled runner **or** Airflow. Both are idempotent (already-reconciled simulated dates are skipped), but they duplicate work if both are active.

#### Ports
Compose publishes the demo services on all host interfaces: dashboard `8501`, API `8000`, PostgreSQL `5432`, Kafka `9092` and `9094`, and optional Airflow `8080`. Use `http://<VPS-IP>:8501` for the dashboard and the matching ports for the other services. Allow these ports through the VPS firewall. The demo uses known sample database credentials, and Kafka is unauthenticated, so run it on a network where this exposure is intended. For remote Kafka clients, set `KAFKA_EXTERNAL_HOST=<VPS-IP>` in the environment before starting Compose so Kafka advertises the VPS address on port `9094`.

#### VPS quick start
Copy `.env.example` to `.env`, set a demo PostgreSQL password, and set `KAFKA_EXTERNAL_HOST` to the VPS public IP if clients outside Docker will connect to Kafka. Then run `docker compose up --build -d`. Compose restarts services after VPS reboots, waits for PostgreSQL and the API before starting dependent services, and stores PostgreSQL, Kafka, and data-lake files in named volumes. Open the required ports in both the VPS firewall and hosting provider firewall.


---

## 🌐 Endpoints & UI Access

| Component | URL | Purpose |
| :--- | :--- | :--- |
| **Interactive Dashboard** | [http://localhost:8501](http://localhost:8501) | Live Fleet Telemetry, Alerts & Profitability UI |
| **Serving REST API** | [http://localhost:8000](http://localhost:8000) | Root API endpoint & health summary |
| **Interactive API Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive Swagger UI documentation |
| **Observability Health** | [http://localhost:8000/health](http://localhost:8000/health) | Pipeline health check, ingestion lag & freshness rule |
| **Observability Metrics** | [http://localhost:8000/metrics](http://localhost:8000/metrics) | Prometheus text exposition (lag, alerts, profitability gauges) |

---

## 📡 API Reference

- **`GET /health`**: Health status, DB engine (`POSTGRES`/`SQLITE`), window count, unresolved alerts, `ingest_lag_seconds` and `data_fresh`. The service reports **`DEGRADED`** when no telemetry window has closed within `STALE_DATA_THRESHOLD_SEC` (default 60s) — the required "no data received in N minutes" rule.
- **`GET /metrics`**: Prometheus exposition format for scraping/monitoring (`fleet_ingest_lag_seconds`, `fleet_stale_data`, `fleet_unresolved_alerts`, `fleet_batch_reconciled_rows`, `fleet_net_profit_total`, ...).
- **`GET /api/fleet/realtime-utilization`**: Latest window per zone with distinct-vehicle status counts, idle ratio and window earnings.
- **`GET /api/fleet/alerts?limit=50&only_unresolved=false`**: Threshold alerts (e.g. vehicle idle $\ge 60\text{s}$). `limit` is bounded to 1–200 to protect the database.
- **`GET /api/fleet/daily-profitability?simulated_date=YYYY-MM-DD&limit=1000`**: Daily per-vehicle reconciliation (gross earnings, fuel, maintenance, net profit, recommendation). Both parameters are optional.
- **`GET /api/fleet/unified-summary?limit=500`**: Serving-layer view merging batch profitability with live alert counts.

---

## 📈 Observability

| Signal | Where | Notes |
| :--- | :--- | :--- |
| Structured JSON logs | every component (`docker compose logs`) | `timestamp`, `level`, `component`, `message` |
| Window summaries | speed layer | zones upserted, alerts written, events processed/invalid/dropped |
| Batch run summaries | batch layer | simulated dates processed/skipped, unprofitable vehicles, fleet net profit, fallback revenue count |
| Health rule | `GET /health` | `data_fresh` + `ingest_lag_seconds` (stale ingestion ⇒ `DEGRADED`) |
| Metrics | `GET /metrics` | Prometheus gauges for lag, alerts, reconciliation results |
| Dashboard | Streamlit | "Telemetry Lag" KPI + alert table + reconciliation views |

---

## 🧪 Automated Testing

The suite exercises the real code paths (not re-implemented arithmetic): window aggregation semantics, profitability rules, the date-scoped telemetry join, database upserts, and the REST endpoints.

```bash
# Install test dependencies (includes httpx, pinned for the FastAPI TestClient)
pip install -r requirements-dev.txt

# Run with pytest (recommended)
pytest

# Or with the standard library only
python -m unittest discover -s tests -p "test_*.py"
```

`conftest.py` + `pytest.ini` put the repository root on `sys.path` and redirect SQLite to a throw-away file, so tests never touch `storage/fleet_db.sqlite` or `data_lake/`.

---

## 📂 Repository Structure

```
big-data-mini-project/
├── docker-compose.yml              # Multi-container orchestration (Kafka, Postgres, Python services, Airflow profile)
├── Dockerfile                      # Application image (non-root user, unbuffered logs)
├── .dockerignore                   # Keeps demo data, databases and .git out of the image
├── requirements.txt                # Runtime Python dependencies
├── requirements-dev.txt            # Test/development dependencies (pytest, httpx)
├── pytest.ini / conftest.py        # Test discovery + isolated SQLite for tests
├── README.md                       # Comprehensive setup and run guide
├── run_standalone.py               # Standalone runner for pure Python/SQLite mode (Zero Docker)
├── config/
│   └── config.yaml                 # System configurations & thresholds
├── data_generator/
│   ├── streaming_producer.py       # Continuous GPS/telemetry Kafka producer
│   └── batch_generator.py          # Periodic daily expense file generator
├── streaming_layer/
│   ├── spark_streaming_job.py      # Speed layer: Kafka consumer -> window aggregates & alerts
│   ├── window_aggregator.py        # Shared stateful tumbling-window engine (distinct vehicles, fare deltas)
│   └── metrics_writer.py           # Dialect-aware writers for speed_fleet_metrics / speed_vehicle_alerts
├── batch_layer/
│   ├── batch_processor.py          # Engine-agnostic reconciliation (date-scoped join, expense + profit upserts)
│   └── scheduled_runner.py         # Automated runner executing batch jobs every simulated day
├── airflow/
│   └── dags/
│       └── fleet_daily_batch_dag.py # Airflow DAG (short-circuits when no drop, archives, executive report)
├── storage/
│   ├── init.sql                    # PostgreSQL database schemas, indexes & unified views
│   └── db_adapter.py               # Dual-engine adapter (PostgreSQL in Docker / SQLite standalone, WAL + caching)
├── serving/
│   ├── api.py                      # FastAPI serving layer: /health, /metrics, fleet endpoints
│   └── dashboard.py                # Streamlit operational & financial dashboard
├── docs/
│   └── report.md                   # Complete 8-15 page Technical Report (Assignment Deliverable)
└── tests/
    ├── test_producers.py           # Telemetry generation + schema/cadence tests
    ├── test_window_aggregator.py   # Window semantics: distinct vehicles, fare deltas, alert cooldown
    ├── test_batch_reconciliation.py# Profitability rules, date-scoped join, end-to-end job
    ├── test_db_adapter.py          # SQLite schema bootstrap, WAL, idempotent upserts
    └── test_api.py                 # REST contract, bounded limits, health rule, /metrics
```

---

## 🛡️ Correctness & Reliability Notes

Fixes applied to the pipeline (all covered by automated tests):

| Area | Issue found | Fix |
| :--- | :--- | :--- |
| Speed layer metrics | Heartbeats were counted as vehicles, so a 25-vehicle fleet reported up to 125 "vehicles" per window and the idle ratio used that inflated denominator | Stateful aggregator keeps the latest state per vehicle → counts are distinct vehicles (`Active + Enroute + Idle ≤ fleet size`) |
| Live earnings | Cumulative trip fare was summed on every 2s heartbeat (≈5× inflation) | Only the fare *delta* per trip is booked, including trips spanning windows |
| Batch revenue | Fare was booked on the *first* `on_trip` heartbeat (smallest value) and every day was reconciled against all-time telemetry | Per-trip **maximum** fare + records bucketed by event date |
| Batch idempotency | Every run re-read every drop file and rewrote every row; parquet files accumulated per run | Dates already reconciled are skipped (opt-out with `skip_existing=False`), upserts are idempotent, parquet is written per simulated date |
| PostgreSQL batch writes | Bulk `execute_values` inserts listed a `CURRENT_TIMESTAMP` column that no row tuple supplied, so the Docker/PostgreSQL batch layer always failed with "INSERT has more target columns than expressions" | Statements now pass an explicit `template`; verified against a live PostgreSQL container and locked in by `tests/test_db_adapter.py` |
| Dead table | `batch_vehicle_expenses` was never populated | Expense drops are now upserted alongside the reconciled results |
| SQLite concurrency | Multi-threaded writers hit `database is locked`; the engine probe cost a TCP timeout per request | WAL + `busy_timeout` pragmas, cached engine resolution, `db_cursor()` helper (sqlite cursors have no context manager) |
| Serving API | Connection leak in `/` and `/health`, unbounded `limit`, CORS `*` combined with `allow_credentials` | Connections always closed, `limit` validated per endpoint, configurable origins |
| Observability | Health only reported row counts; no "no data received" rule | `/health` freshness rule (`data_fresh`, `ingest_lag_seconds`) and Prometheus `/metrics` |
| Airflow DAG | The file-arrival check result was ignored; report task leaked connections on error | `ShortCircuitOperator`, `with` connection handling, run summaries |
| Container/docs | `bitnami/kafka:3.7.0` no longer exists (Docker Hub 404) so `docker compose up` failed; image ran as root with a no-op `CMD`; Kafka/DB ports published on all interfaces | Switched to `apache/kafka`, non-root user + `.dockerignore`, loopback-only port bindings, real default `CMD` |
| Dependencies | `kafka-python==2.0.2` cannot import on Python 3.12; `requests`/`streamlit` had known CVEs | Pinned `kafka-python==2.0.3`, `requests==2.32.3` (CVE-2024-35195), `streamlit==1.37.1` (CVE-2024-42474) |

---

The full written report is available in **[`docs/report.md`](docs/report.md)**, covering:
- In-depth justification of **Lambda vs. Kappa Architecture** and defense against Kappa (evaluating latency, volume, re-processing, cost, and consistency).
- Technology stack selection rationale referencing lecture concepts (OLTP vs OLAP, CAP theorem, throughput vs latency, DAGs).
- Data ingestion & clock simulation specs ($1\text{ Day} = 5\text{ Minutes}$).
- Observability architecture & monitoring rules.
- Production-scale roadmap (Kubernetes, AWS S3/Iceberg Lakehouse).
