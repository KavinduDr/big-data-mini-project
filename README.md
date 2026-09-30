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
│ (Spark/PySpark) │             │ (Raw Data Lake) │             │  (Airflow / Engine)│
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

4. **Launch the Live Dashboard (Terminal 2):**
   ```bash
   streamlit run serving/dashboard.py
   ```
   *Open **[http://localhost:8501](http://localhost:8501)** in your browser.*

---

### Option B: Docker Compose Mode (Full Production Stack)
*Recommended for final evaluation, viva demo, and submission.*

This mode runs the entire stack inside containers: **Apache Kafka (KRaft)**, **PostgreSQL 15**, **Airflow DAGs**, **PySpark Stream Processor**, **FastAPI**, and **Streamlit**.

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

---

## 🌐 Endpoints & UI Access

| Component | URL | Purpose |
| :--- | :--- | :--- |
| **Interactive Dashboard** | [http://localhost:8501](http://localhost:8501) | Live Fleet Telemetry, Alerts & Profitability UI |
| **Serving REST API** | [http://localhost:8000](http://localhost:8000) | Root API endpoint & health summary |
| **Interactive API Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Interactive Swagger UI documentation |
| **Observability Health** | [http://localhost:8000/health](http://localhost:8000/health) | Pipeline health check & metric counts |

---

## 📡 API Reference

- **`GET /health`**: Returns system health status, DB engine (`POSTGRES` or `SQLITE`), recorded streaming window count, and active alerts.
- **`GET /api/fleet/realtime-utilization`**: Real-time fleet utilization breakdown by zone (Downtown, Airport, Uptown, Suburbs) with idle ratios.
- **`GET /api/fleet/alerts`**: Active and recent threshold-based alerts (e.g. vehicles idle $\ge 60\text{s}$).
- **`GET /api/fleet/daily-profitability`**: Daily per-vehicle reconciliation report showing gross earnings, fuel cost, maintenance cost, net profit, and unprofitable flags.
- **`GET /api/fleet/unified-summary`**: Combined serving view merging batch profitability and speed alert status.

---

## 🧪 Automated Testing

Unit tests validate telemetry data generation, schema conformance, and reconciliation math:
```bash
# Run tests with Python's built-in unittest
python -m unittest discover -s tests -p "test_*.py"

# Or with pytest
pytest tests/
```

---

## 📂 Repository Structure

```
big-data-mini-project/
├── docker-compose.yml              # Multi-container orchestration (Kafka, Postgres, Python services)
├── Dockerfile                      # Application Dockerfile
├── requirements.txt                # Python dependencies
├── README.md                       # Comprehensive setup and run guide
├── run_standalone.py               # Standalone runner for pure Python/SQLite mode (Zero Docker)
├── config/
│   └── config.yaml                 # System configurations & thresholds
├── data_generator/
│   ├── streaming_producer.py       # Continuous GPS/telemetry Kafka producer
│   └── batch_generator.py          # Periodic daily expense file generator
├── streaming_layer/
│   └── spark_streaming_job.py      # Speed layer windowed aggregations & alert rules
├── batch_layer/
│   ├── batch_processor.py          # Batch reconciliation joining expenses with trip fares
│   └── scheduled_runner.py         # Automated runner executing batch jobs every simulated day
├── airflow/
│   └── dags/
│       └── fleet_daily_batch_dag.py # Airflow DAG for batch orchestration & archiving
├── storage/
│   ├── init.sql                    # PostgreSQL database schemas & unified views
│   └── db_adapter.py               # Dual-engine adapter (PostgreSQL in Docker / SQLite standalone)
├── serving/
│   ├── api.py                      # FastAPI serving layer with observability health checks
│   └── dashboard.py                # Streamlit operational & financial dashboard
├── docs/
│   └── report.md                   # Complete 8-15 page Technical Report (Assignment Deliverable)
└── tests/
    ├── test_producers.py           # Unit tests for telemetry data generation
    └── test_batch_reconciliation.py# Unit tests for profitability calculations
```

---

## 📄 Academic Technical Report

The full written report is available in **[`docs/report.md`](file:///e:/Github/big-data-mini-project/docs/report.md)**, covering:
- In-depth justification of **Lambda vs. Kappa Architecture** and defense against Kappa (evaluating latency, volume, re-processing, cost, and consistency).
- Technology stack selection rationale referencing lecture concepts (OLTP vs OLAP, CAP theorem, throughput vs latency, DAGs).
- Data ingestion & clock simulation specs ($1\text{ Day} = 5\text{ Minutes}$).
- Observability architecture & monitoring rules.
- Production-scale roadmap (Kubernetes, AWS S3/Iceberg Lakehouse).