# Ride-Hailing Fleet Operations — End-to-End Big Data Platform

> **EC 8203: Applied Big Data Engineering — Mini Project Assessment**  
> **Architecture Pattern:** Lambda Architecture (Speed Layer + Batch Layer + Serving Layer)  
> **Use Case:** Ride-Hailing Fleet Operations & Profitability Reconciliation  

[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://www.docker.com/)
[![Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-Serving-green?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red?logo=streamlit)](https://streamlit.io/)

---

## 📋 Overview

This platform implements an end-to-end **Lambda Architecture** data pipeline that addresses two fundamental operational requirements in ride-hailing fleet management:
1. **Real-Time Fleet Utilization (Speed Layer):** Ingests continuous GPS and trip telemetry every 2 seconds via **Apache Kafka**, performing windowed aggregations of zone utilization, trip counts, earnings, and automated threshold alerts when vehicles idle excessively.
2. **Historical Profitability Reconciliation (Batch Layer):** Periodically reconciles daily vehicle expenses (fuel and garage maintenance) with streaming fare earnings to compute per-vehicle net profit, detect unprofitable vehicles, and generate automated fleet maintenance recommendations.
3. **Serving Layer & Interactive Dashboard:** Unified serving layer in **PostgreSQL** exposed through a **FastAPI** REST interface and an interactive **Streamlit** dashboard.

---

## 🏛️ System Architecture

```
       [ Telemetry Stream (GPS, Fares, Status) ]       [ Daily Batch Expense Files (CSV/JSON) ]
                         │                                                 │
                         ▼                                                 │
              ┌─────────────────────┐                                      │
              │    Apache Kafka     │                                      │
              │  (fleet-telemetry)  │                                      │
              └──────────┬──────────┘                                      │
                         │                                                 │
         ┌───────────────┴───────────────┐                                 │
         ▼                               ▼                                 ▼
┌─────────────────┐             ┌─────────────────┐             ┌────────────────────┐
│   SPEED LAYER   │             │   BATCH LAYER   │             │    BATCH LAYER     │
│ (Spark/PySpark) │             │ (Raw Data Lake) │             │  (Airflow DAG)     │
│  Window Aggs,   │             │ Parquet Storage │             │ Daily Cost & Fare  │
│  Live Earnings, │             │ (Historical Log)│             │  Reconciliation    │
│   Idle Alerts   │             └────────┬────────┘             └─────────┬──────────┘
└────────┬────────┘                      │                                │
         │                               └────────────────┬───────────────┘
         ▼                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────┐
│                       SERVING LAYER (PostgreSQL Database)                          │
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
- **1 Simulated Day = 5 Minutes (300 seconds)**
- **Telemetry Frequency:** Emitted every 2 seconds
- **Batch Drop Cadence:** Drops garage expense CSV/JSON files every 5 minutes

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- [Docker Desktop](https://www.docker.com/) or Docker Engine with Docker Compose in Ubuntu WSL
- Git

### 2. Launch the Platform
From your terminal (or inside Ubuntu WSL):
```bash
# Clone and navigate to repository
cd big-data-mini-project

# Launch all pipeline services
docker compose up --build -d
```

### 3. Access UIs & Endpoints
| Component | URL | Purpose |
| :--- | :--- | :--- |
| **Interactive Dashboard** | [http://localhost:8501](http://localhost:8501) | Live Fleet Telemetry, Alerts & Profitability UI |
| **Serving REST API** | [http://localhost:8000](http://localhost:8000) | Root API endpoint |
| **Interactive API Docs** | [http://localhost:8000/docs](http://localhost:8000/docs) | Swagger UI for exploring endpoints |
| **Observability Health** | [http://localhost:8000/health](http://localhost:8000/health) | Pipeline health check & metric counts |

---

## 📡 API Reference

- **`GET /health`**: Returns system health status, DB connectivity, recorded window count, and active alerts.
- **`GET /api/fleet/realtime-utilization`**: Real-time fleet utilization breakdown by zone (Downtown, Airport, Uptown, Suburbs) with idle ratios.
- **`GET /api/fleet/alerts`**: Active and recent threshold-based alerts (e.g. vehicles idle > 60s).
- **`GET /api/fleet/daily-profitability`**: Daily per-vehicle reconciliation report showing gross earnings, fuel cost, maintenance cost, net profit, and unprofitable flags.
- **`GET /api/fleet/unified-summary`**: Combined serving view merging batch profitability and speed alert status.

---

## 🧪 Automated Testing

Unit tests validate telemetry data generation, schema conformance, and reconciliation math:
```bash
# Run tests locally or inside container
pytest tests/
```

---

## 📂 Repository Structure

```
big-data-mini-project/
├── docker-compose.yml              # Multi-container orchestration (Kafka, Postgres, Python services)
├── Dockerfile                      # Application Dockerfile
├── requirements.txt                # Python dependencies
├── README.md                       # Setup and run instructions
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
│   └── init.sql                    # PostgreSQL database schemas & unified views
├── serving/
│   ├── api.py                      # FastAPI serving layer with observability health checks
│   └── dashboard.py                # Streamlit operational & financial dashboard
├── docs/
│   └── report.md                   # Complete 8-15 page Technical Report (Assignment Deliverable)
└── tests/
    ├── test_producers.py           # Unit tests for data generation
    └── test_batch_reconciliation.py# Unit tests for profitability calculations
```

---

## 📄 Academic Technical Report

The full written report is available in **`docs/report.md`**, covering:
- In-depth justification of **Lambda vs. Kappa Architecture** (evaluating latency, volume, re-processing, cost, and consistency).
- Technology stack selection rationale.
- Data ingestion & clock simulation specs.
- Observability architecture & monitoring rules.
- Production-scale roadmap (Kubernetes, AWS S3/Iceberg Lakehouse).