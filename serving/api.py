import os
import time
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from storage.db_adapter import get_connection

app = FastAPI(
    title="Ride-Hailing Fleet Operations API",
    description="Real-time and batch serving endpoints for Fleet Telemetry & Profitability Reconciliation (Lambda Architecture)",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def query_db(query, params=None):
    engine_type, conn = get_connection()
    try:
        cur = conn.cursor()
        if params:
            # SQLite uses ? while Postgres uses %s
            if engine_type == "sqlite":
                query_mod = query.replace("%s", "?")
                cur.execute(query_mod, params)
            else:
                cur.execute(query, params)
        else:
            cur.execute(query)

        rows = cur.fetchall()
        if engine_type == "sqlite":
            result = [dict(r) for r in rows]
        else:
            # Postgres RealDictCursor or tuple
            if hasattr(rows[0] if rows else None, 'keys'):
                result = [dict(r) for r in rows]
            else:
                cols = [desc[0] for desc in cur.description]
                result = [dict(zip(cols, r)) for r in rows]
        return engine_type, result
    finally:
        conn.close()

@app.get("/")
def read_root():
    engine_type, _ = get_connection()
    return {
        "service": "Ride-Hailing Fleet Operations Lambda API",
        "status": "online",
        "database_backend": engine_type,
        "architecture": "Lambda Architecture",
        "endpoints": [
            "/health",
            "/api/fleet/realtime-utilization",
            "/api/fleet/alerts",
            "/api/fleet/daily-profitability",
            "/api/fleet/unified-summary"
        ]
    }

@app.get("/health")
def health_check():
    """Observability: System health check verifying database and ingestion state."""
    db_status = "UNKNOWN"
    total_metrics_count = 0
    recent_alerts_count = 0
    engine_type = "unknown"
    try:
        engine_type, res1 = query_db("SELECT COUNT(*) as count FROM speed_fleet_metrics;")
        total_metrics_count = res1[0]["count"] if res1 else 0

        _, res2 = query_db("SELECT COUNT(*) as count FROM speed_vehicle_alerts WHERE resolved = 0 OR resolved = FALSE;")
        recent_alerts_count = res2[0]["count"] if res2 else 0
        db_status = f"HEALTHY ({engine_type.upper()})"
    except Exception as e:
        db_status = f"UNHEALTHY: {str(e)}"

    is_healthy = "HEALTHY" in db_status
    return {
        "status": "HEALTHY" if is_healthy else "DEGRADED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database_health": db_status,
        "database_engine": engine_type,
        "streaming_windows_recorded": total_metrics_count,
        "unresolved_alerts": recent_alerts_count
    }

@app.get("/api/fleet/realtime-utilization")
def get_realtime_utilization():
    """Returns live fleet utilization metrics: active vehicles, idle ratio, trips, and earnings by zone."""
    query = """
        SELECT 
            grid_zone,
            active_vehicles,
            idle_vehicles,
            enroute_vehicles,
            (active_vehicles + idle_vehicles + enroute_vehicles) as total_fleet_zone,
            ROUND(CASE 
                WHEN (active_vehicles + idle_vehicles + enroute_vehicles) > 0 
                THEN (CAST(idle_vehicles AS REAL) / (active_vehicles + idle_vehicles + enroute_vehicles)) * 100 
                ELSE 0.0 
            END, 2) as idle_ratio_pct,
            total_trips,
            total_fare as zone_earnings,
            avg_speed,
            updated_at
        FROM speed_fleet_metrics
        ORDER BY updated_at DESC, zone_earnings DESC;
    """
    try:
        _, rows = query_db(query)
        # Deduplicate to keep latest row per zone
        zone_map = {}
        for r in rows:
            z = r["grid_zone"]
            if z not in zone_map:
                zone_map[z] = r
        deduped_rows = list(zone_map.values())

        total_active = sum(r["active_vehicles"] for r in deduped_rows)
        total_idle = sum(r["idle_vehicles"] for r in deduped_rows)
        total_enroute = sum(r["enroute_vehicles"] for r in deduped_rows)
        total_fleet = total_active + total_idle + total_enroute
        overall_idle_ratio = round((total_idle / total_fleet * 100), 2) if total_fleet > 0 else 0.0
        total_earnings = round(sum(float(r["zone_earnings"]) for r in deduped_rows), 2)

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_active_on_trip": total_active,
                "total_enroute": total_enroute,
                "total_idle": total_idle,
                "overall_idle_ratio_pct": overall_idle_ratio,
                "total_realtime_earnings": total_earnings
            },
            "zones": deduped_rows
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/fleet/alerts")
def get_vehicle_alerts(limit: int = 50):
    """Returns threshold-based alerts (e.g., vehicles idle > threshold)."""
    query = """
        SELECT id, vehicle_id, driver_id, grid_zone, alert_type, idle_duration_sec, alert_message, alert_timestamp, resolved
        FROM speed_vehicle_alerts
        ORDER BY alert_timestamp DESC
        LIMIT %s;
    """
    try:
        _, alerts = query_db(query, (limit,))
        return {
            "count": len(alerts),
            "alerts": alerts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/fleet/daily-profitability")
def get_daily_profitability():
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
        ORDER BY simulated_date DESC, net_profit ASC;
    """
    try:
        _, records = query_db(query)
        unprofitable_count = sum(1 for r in records if r["is_unprofitable"] in [1, True, "1"])
        total_net = round(sum(float(r["net_profit"]) for r in records), 2)
        return {
            "total_records": len(records),
            "unprofitable_vehicles_count": unprofitable_count,
            "total_fleet_net_profit": total_net,
            "reconciliation_records": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/fleet/unified-summary")
def get_unified_summary():
    """Serving Layer unified view: combines daily profitability with real-time idle alert count."""
    query = """
        SELECT * FROM v_unified_vehicle_summary
        ORDER BY yesterday_net_profit ASC;
    """
    try:
        _, records = query_db(query)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary_records": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
