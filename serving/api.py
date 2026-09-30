import os
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from storage.db_adapter import get_connection

app = FastAPI(
    title="Ride-Hailing Fleet Operations API",
    description="Real-time and batch serving endpoints for Fleet Telemetry & Profitability Reconciliation (Lambda Architecture)",
    version="1.1.0"
)

# Browsers reject `allow_origins=["*"]` together with `allow_credentials=True`,
# so the allowed origins are configurable and credentials are only enabled when
# explicit origins are listed.
def _cors_origins():
    raw = os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:8501,http://127.0.0.1:8501")
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


_cors_origins = _cors_origins()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials="*" not in _cors_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Observability: a window older than this means ingestion has stalled.
STALE_DATA_THRESHOLD_SEC = int(os.getenv("STALE_DATA_THRESHOLD_SEC", 60))


def _dialect(engine_type, query):
    """SQLite uses qmark placeholders, PostgreSQL uses pyformat."""
    return query.replace("%s", "?") if engine_type == "sqlite" else query


def query_db(query, params=None):
    """Runs a read query and returns ``(engine_type, list_of_dict_rows)``."""
    engine_type, conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_dialect(engine_type, query), params) if params else cur.execute(query)
        rows = cur.fetchall()

        if hasattr(rows[0] if rows else None, "keys"):
            return engine_type, [dict(row) for row in rows]

        columns = [description[0] for description in cur.description]
        return engine_type, [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def _parse_timestamp(value):
    """Parses PostgreSQL datetimes and SQLite ISO text into aware UTC datetimes."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _pipeline_freshness():
    """Returns ``(lag_seconds, latest_window_end)`` for the speed layer."""
    _, rows = query_db("SELECT MAX(window_end) AS latest_window_end FROM speed_fleet_metrics;")
    latest = rows[0]["latest_window_end"] if rows else None
    parsed = _parse_timestamp(latest)
    if parsed is None:
        return None, latest
    lag = round((datetime.now(timezone.utc) - parsed).total_seconds(), 2)
    return lag, latest


@app.get("/")
def read_root():
    # query_db() always releases the connection; the previous version leaked a
    # PostgreSQL connection on every call to this endpoint.
    engine_type, _ = query_db("SELECT 1 AS ok;")
    return {
        "service": "Ride-Hailing Fleet Operations Lambda API",
        "status": "online",
        "database_backend": engine_type,
        "architecture": "Lambda Architecture",
        "endpoints": [
            "/health",
            "/metrics",
            "/api/fleet/realtime-utilization",
            "/api/fleet/alerts",
            "/api/fleet/daily-profitability",
            "/api/fleet/unified-summary"
        ]
    }

@app.get("/health")
def health_check():
    """Observability: health check verifying the database and ingestion freshness."""
    db_status = "UNKNOWN"
    total_metrics_count = 0
    recent_alerts_count = 0
    engine_type = "unknown"
    ingest_lag = None
    latest_window_end = None
    database_healthy = False

    try:
        engine_type, res1 = query_db("SELECT COUNT(*) as count FROM speed_fleet_metrics;")
        total_metrics_count = res1[0]["count"] if res1 else 0

        _, res2 = query_db("SELECT COUNT(*) as count FROM speed_vehicle_alerts WHERE resolved = FALSE;")
        recent_alerts_count = res2[0]["count"] if res2 else 0

        ingest_lag, latest_window_end = _pipeline_freshness()
        db_status = f"HEALTHY ({engine_type.upper()})"
        database_healthy = True
    except Exception as e:
        db_status = f"UNHEALTHY: {str(e)}"

    # Assignment observability rule: alert when no data has been received for N
    # seconds (default 60s), i.e. ingestion/processing has stalled.
    data_fresh = ingest_lag is not None and ingest_lag <= STALE_DATA_THRESHOLD_SEC

    payload = {
        "status": "HEALTHY" if (database_healthy and data_fresh) else "DEGRADED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database_health": db_status,
        "database_engine": engine_type,
        "streaming_windows_recorded": total_metrics_count,
        "unresolved_alerts": recent_alerts_count,
        "last_window_end": str(latest_window_end) if latest_window_end is not None else None,
        "ingest_lag_seconds": ingest_lag,
        "stale_data_threshold_sec": STALE_DATA_THRESHOLD_SEC,
        "data_fresh": data_fresh,
    }
    return JSONResponse(status_code=200 if database_healthy else 503, content=payload)

@app.get("/metrics")
def prometheus_metrics():
    """Observability: minimal Prometheus exposition format (no extra dependency).

    Exposes pipeline gauges so a scraper (Prometheus) or the demo viva can show
    ingestion lag, alert rates and reconciliation results.
    """
    gauges = {
        "fleet_db_up": 0,
        "fleet_speed_windows_recorded": 0,
        "fleet_unresolved_alerts": 0,
        "fleet_batch_reconciled_rows": 0,
        "fleet_unprofitable_vehicles": 0,
        "fleet_net_profit_total": 0.0,
        "fleet_ingest_lag_seconds": -1.0,
        "fleet_stale_data": 1,
    }
    try:
        _, rows = query_db("""
            SELECT
                (SELECT COUNT(*) FROM speed_fleet_metrics) AS windows_recorded,
                (SELECT COUNT(*) FROM speed_vehicle_alerts WHERE resolved = FALSE) AS unresolved_alerts,
                (SELECT COUNT(*) FROM batch_daily_profitability) AS reconciled_rows,
                (SELECT COUNT(*) FROM batch_daily_profitability WHERE is_unprofitable = TRUE) AS unprofitable_vehicles,
                (SELECT COALESCE(SUM(net_profit), 0) FROM batch_daily_profitability) AS net_profit_total;
        """)
        if rows:
            row = rows[0]
            gauges["fleet_db_up"] = 1
            gauges["fleet_speed_windows_recorded"] = int(row["windows_recorded"] or 0)
            gauges["fleet_unresolved_alerts"] = int(row["unresolved_alerts"] or 0)
            gauges["fleet_batch_reconciled_rows"] = int(row["reconciled_rows"] or 0)
            gauges["fleet_unprofitable_vehicles"] = int(row["unprofitable_vehicles"] or 0)
            gauges["fleet_net_profit_total"] = float(row["net_profit_total"] or 0.0)

        lag, _latest = _pipeline_freshness()
        if lag is not None:
            gauges["fleet_ingest_lag_seconds"] = lag
            gauges["fleet_stale_data"] = 1 if lag > STALE_DATA_THRESHOLD_SEC else 0
    except Exception:  # pragma: no cover - degraded mode is reported via gauges
        pass

    help_text = {
        "fleet_db_up": "1 when the serving database is queryable",
        "fleet_speed_windows_recorded": "Rows in speed_fleet_metrics",
        "fleet_unresolved_alerts": "Unresolved threshold alerts",
        "fleet_batch_reconciled_rows": "Rows in batch_daily_profitability",
        "fleet_unprofitable_vehicles": "Reconciled vehicles with negative net profit",
        "fleet_net_profit_total": "Sum of net profit across all reconciled days",
        "fleet_ingest_lag_seconds": "Seconds since the newest closed streaming window",
        "fleet_stale_data": "1 when ingestion lag exceeds the stale threshold",
    }
    lines = []
    for name, value in gauges.items():
        lines.append(f"# HELP {name} {help_text[name]}")
        lines.append(f"# TYPE {name} gauge")
        lines.append(f"{name} {value}")
    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

@app.get("/api/fleet/realtime-utilization")
def get_realtime_utilization():
    """Returns live fleet utilization metrics: active vehicles, idle ratio, trips, and earnings by zone."""
    query = """
        WITH ranked_metrics AS (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY grid_zone ORDER BY window_end DESC, window_start DESC
          ) AS zone_rank
          FROM speed_fleet_metrics
        )
        SELECT
            grid_zone,
            COALESCE(active_vehicles, 0) AS active_vehicles,
            COALESCE(idle_vehicles, 0) AS idle_vehicles,
            COALESCE(enroute_vehicles, 0) AS enroute_vehicles,
            (COALESCE(active_vehicles, 0) + COALESCE(idle_vehicles, 0) + COALESCE(enroute_vehicles, 0)) as total_fleet_zone,
            COALESCE(total_trips, 0) AS total_trips,
            COALESCE(total_fare, 0) as zone_earnings,
            COALESCE(avg_speed, 0) AS avg_speed,
            window_start,
            window_end,
            updated_at
        FROM ranked_metrics
        WHERE zone_rank = 1
        ORDER BY window_end DESC, zone_earnings DESC;
    """
    try:
        _, rows = query_db(query)
        deduped_rows = rows
        for row in deduped_rows:
            fleet_in_zone = int(row["total_fleet_zone"] or 0)
            row["idle_ratio_pct"] = round(
                int(row["idle_vehicles"] or 0) / fleet_in_zone * 100, 2
            ) if fleet_in_zone else 0.0

        total_active = sum(int(r["active_vehicles"] or 0) for r in deduped_rows)
        total_idle = sum(int(r["idle_vehicles"] or 0) for r in deduped_rows)
        total_enroute = sum(int(r["enroute_vehicles"] or 0) for r in deduped_rows)
        total_fleet = total_active + total_idle + total_enroute
        overall_idle_ratio = round((total_idle / total_fleet * 100), 2) if total_fleet > 0 else 0.0
        total_earnings = round(sum(float(r["zone_earnings"] or 0.0) for r in deduped_rows), 2)

        ingest_lag, latest_window_end = _pipeline_freshness()

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_active_on_trip": total_active,
                "total_enroute": total_enroute,
                "total_idle": total_idle,
                "total_fleet_observed": total_fleet,
                "overall_idle_ratio_pct": overall_idle_ratio,
                "total_realtime_earnings": total_earnings,
                "last_window_end": str(latest_window_end) if latest_window_end is not None else None,
                "ingest_lag_seconds": ingest_lag,
            },
            "zones": deduped_rows
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/fleet/alerts")
def get_vehicle_alerts(
    limit: int = Query(50, ge=1, le=200, description="Maximum alerts to return (bounded to protect the DB)"),
    only_unresolved: bool = Query(False, description="Return only unresolved alerts"),
):
    """Returns threshold-based alerts (e.g., vehicles idle > threshold)."""
    query = """
        SELECT id, vehicle_id, driver_id, grid_zone, alert_type, idle_duration_sec, alert_message, alert_timestamp, resolved
        FROM speed_vehicle_alerts
        {where_clause}
        ORDER BY alert_timestamp DESC
        LIMIT %s;
    """.format(where_clause="WHERE resolved = FALSE" if only_unresolved else "")
    try:
        _, alerts = query_db(query, (limit,))
        return {
            "count": len(alerts),
            "alerts": alerts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/fleet/daily-profitability")
def get_daily_profitability(
    simulated_date: str = Query(None, description="Optional YYYY-MM-DD filter"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum reconciliation rows to return"),
):
    """Returns daily per-vehicle profitability reconciliation report."""
    query = """
        SELECT 
            simulated_date,
            vehicle_id,
            total_trips,
            gross_earnings,
            fuel_cost,
            maintenance_cost,
            total_expenses,
            net_profit,
            profit_margin_pct,
            is_unprofitable,
            recommendation,
            reconciled_at
        FROM batch_daily_profitability
        {where_clause}
        ORDER BY simulated_date DESC, net_profit ASC
        LIMIT %s;
    """.format(where_clause="WHERE simulated_date = %s" if simulated_date else "")
    try:
        params = (simulated_date, limit) if simulated_date else (limit,)
        _, records = query_db(query, params)
        unprofitable_count = sum(1 for r in records if r["is_unprofitable"] in [1, True, "1"])
        total_net = round(sum(float(r["net_profit"] or 0.0) for r in records), 2)
        return {
            "total_records": len(records),
            "unprofitable_vehicles_count": unprofitable_count,
            "total_fleet_net_profit": total_net,
            "reconciliation_records": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/fleet/unified-summary")
def get_unified_summary(limit: int = Query(500, ge=1, le=5000)):
    """Serving Layer unified view: combines daily profitability with real-time idle alert count."""
    query = """
        SELECT * FROM v_unified_vehicle_summary
        ORDER BY yesterday_net_profit ASC
        LIMIT %s;
    """
    try:
        _, records = query_db(query, (limit,))
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary_records": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
