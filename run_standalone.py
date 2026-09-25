import json
import logging
import os
import queue
import random
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
import uvicorn
from storage.db_adapter import get_connection

# Structured Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "%(name)s", "message": "%(message)s"}'
)
logger = logging.getLogger("StandalonePipeline")

# Shared streaming in-memory queue simulating Kafka broker
event_bus = queue.Queue(maxsize=10000)

DATA_LAKE_DIR = os.path.join(os.path.dirname(__file__), "data_lake")
DAILY_EXPENSES_DIR = os.path.join(DATA_LAKE_DIR, "daily_expenses")
RAW_TELEMETRY_DIR = os.path.join(DATA_LAKE_DIR, "raw_telemetry")
RECONCILED_DIR = os.path.join(DATA_LAKE_DIR, "reconciled_reports")

os.makedirs(DAILY_EXPENSES_DIR, exist_ok=True)
os.makedirs(RAW_TELEMETRY_DIR, exist_ok=True)
os.makedirs(RECONCILED_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# 1. STREAMING PRODUCER THREAD
# ---------------------------------------------------------------------------
def streaming_producer_thread():
    from data_generator.streaming_producer import FleetSimulator, load_config
    config = load_config()
    simulator = FleetSimulator(config)
    logger.info("Streaming Producer thread started (emitting every 2s).")

    while True:
        try:
            today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            raw_path = os.path.join(RAW_TELEMETRY_DIR, f"telemetry_{today_str}.jsonl")
            
            with open(raw_path, "a") as f:
                for vehicle in simulator.vehicles:
                    event = simulator.generate_event(vehicle)
                    event_bus.put(event)
                    f.write(json.dumps(event) + "\n")

            time.sleep(2)
        except Exception as e:
            logger.error(f"Producer error: {e}")
            time.sleep(2)

# ---------------------------------------------------------------------------
# 2. SPEED LAYER STREAM PROCESSOR THREAD
# ---------------------------------------------------------------------------
def speed_layer_thread():
    logger.info("Speed Layer Stream Processor thread started (10s tumbling windows).")
    window_size_sec = 10
    current_window_start = datetime.now(timezone.utc)
    zone_aggregates = {}
    last_alerted = {}

    while True:
        try:
            # Poll from shared event stream queue
            try:
                event = event_bus.get(timeout=1.0)
            except queue.Empty:
                event = None

            now = datetime.now(timezone.utc)

            if event:
                zone = event.get("grid_zone", "Downtown")
                status = event.get("status", "idle")
                speed = float(event.get("speed", 0.0))
                fare = float(event.get("fare", 0.0))
                idle_sec = int(event.get("idle_duration_sec", 0))
                veh_id = event.get("vehicle_id")

                if zone not in zone_aggregates:
                    zone_aggregates[zone] = {
                        "active": 0, "idle": 0, "enroute": 0,
                        "trips": 0, "fare_sum": 0.0, "speed_sum": 0.0, "count": 0
                    }

                zone_aggregates[zone]["count"] += 1
                zone_aggregates[zone]["speed_sum"] += speed
                zone_aggregates[zone]["fare_sum"] += fare

                if status == "on_trip":
                    zone_aggregates[zone]["active"] += 1
                    zone_aggregates[zone]["trips"] += 1
                elif status == "enroute":
                    zone_aggregates[zone]["enroute"] += 1
                else:
                    zone_aggregates[zone]["idle"] += 1

                # Alert rule: vehicle idle >= 60s
                if status == "idle" and idle_sec >= 60:
                    if time.time() - last_alerted.get(veh_id, 0) > 60:
                        engine, conn = get_connection()
                        cur = conn.cursor()
                        msg = f"Vehicle {veh_id} idle for {idle_sec}s in {zone} (threshold: 60s)."
                        if engine == "sqlite":
                            cur.execute(
                                "INSERT INTO speed_vehicle_alerts (vehicle_id, driver_id, grid_zone, alert_type, idle_duration_sec, alert_message) VALUES (?, ?, ?, ?, ?, ?)",
                                (veh_id, event["driver_id"], zone, "EXCESSIVE_IDLE", idle_sec, msg)
                            )
                        else:
                            cur.execute(
                                "INSERT INTO speed_vehicle_alerts (vehicle_id, driver_id, grid_zone, alert_type, idle_duration_sec, alert_message) VALUES (%s, %s, %s, %s, %s, %s)",
                                (veh_id, event["driver_id"], zone, "EXCESSIVE_IDLE", idle_sec, msg)
                            )
                        conn.commit()
                        conn.close()
                        last_alerted[veh_id] = time.time()
                        logger.warning(f"ALERT: {msg}")

            # Close window
            if (now - current_window_start).total_seconds() >= window_size_sec:
                if zone_aggregates:
                    engine, conn = get_connection()
                    cur = conn.cursor()
                    for zone, stats in zone_aggregates.items():
                        avg_spd = round(stats["speed_sum"] / stats["count"], 2) if stats["count"] > 0 else 0.0
                        if engine == "sqlite":
                            cur.execute("""
                                INSERT INTO speed_fleet_metrics (
                                    window_start, window_end, grid_zone, active_vehicles, 
                                    idle_vehicles, enroute_vehicles, total_trips, total_fare, avg_speed, updated_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                                ON CONFLICT(window_start, grid_zone) DO UPDATE SET
                                    active_vehicles = excluded.active_vehicles,
                                    idle_vehicles = excluded.idle_vehicles,
                                    enroute_vehicles = excluded.enroute_vehicles,
                                    total_trips = excluded.total_trips,
                                    total_fare = excluded.total_fare,
                                    avg_speed = excluded.avg_speed,
                                    updated_at = CURRENT_TIMESTAMP;
                            """, (
                                current_window_start.isoformat(), now.isoformat(), zone,
                                stats["active"], stats["idle"], stats["enroute"],
                                stats["trips"], round(stats["fare_sum"], 2), avg_spd
                            ))
                        else:
                            cur.execute("""
                                INSERT INTO speed_fleet_metrics (
                                    window_start, window_end, grid_zone, active_vehicles, 
                                    idle_vehicles, enroute_vehicles, total_trips, total_fare, avg_speed, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                                ON CONFLICT(window_start, grid_zone) DO UPDATE SET
                                    active_vehicles = EXCLUDED.active_vehicles,
                                    idle_vehicles = EXCLUDED.idle_vehicles,
                                    enroute_vehicles = EXCLUDED.enroute_vehicles,
                                    total_trips = EXCLUDED.total_trips,
                                    total_fare = EXCLUDED.total_fare,
                                    avg_speed = EXCLUDED.avg_speed,
                                    updated_at = CURRENT_TIMESTAMP;
                            """, (
                                current_window_start, now, zone,
                                stats["active"], stats["idle"], stats["enroute"],
                                stats["trips"], round(stats["fare_sum"], 2), avg_spd
                            ))
                    conn.commit()
                    conn.close()
                    logger.info(f"Closed window {current_window_start.strftime('%H:%M:%S')} - {now.strftime('%H:%M:%S')}. Updated {len(zone_aggregates)} zones.")

                zone_aggregates = {}
                current_window_start = now

        except Exception as e:
            logger.error(f"Speed layer error: {e}")
            time.sleep(1)

# ---------------------------------------------------------------------------
# 3. BATCH LAYER RECONCILIATION THREAD (Simulated Day = 60s in local test)
# ---------------------------------------------------------------------------
def batch_layer_thread():
    from data_generator.batch_generator import generate_daily_expense_file
    logger.info("Batch Layer thread started. Generating baseline and reconciling...")
    sim_date = datetime.now(timezone.utc).date()
    generate_daily_expense_file(str(sim_date), 25, DAILY_EXPENSES_DIR)

    # Initial reconciliation
    reconcile_batch(str(sim_date))

    day_idx = 1
    while True:
        try:
            time.sleep(60)  # Reconcile every 60s for immediate local demonstration
            sim_date = datetime.now(timezone.utc).date() + timedelta(days=day_idx)
            generate_daily_expense_file(str(sim_date), 25, DAILY_EXPENSES_DIR)
            reconcile_batch(str(sim_date))
            day_idx += 1
        except Exception as e:
            logger.error(f"Batch thread error: {e}")
            time.sleep(10)

def reconcile_batch(sim_date_str):
    csv_file = os.path.join(DAILY_EXPENSES_DIR, f"vehicle_expenses_{sim_date_str}.csv")
    if not os.path.exists(csv_file):
        return

    df = pd.read_csv(csv_file)
    engine, conn = get_connection()
    cur = conn.cursor()

    reconciled_rows = []
    for _, row in df.iterrows():
        veh_id = row["vehicle_id"]
        fuel = float(row["fuel_cost"])
        maint = float(row["maintenance_cost"])
        tot_exp = round(fuel + maint, 2)
        dist = float(row["distance_covered_km"])

        # Revenue proportional to distance + random variability
        gross = round(dist * random.uniform(0.65, 0.95), 2)
        trips = max(4, int(gross / 16))
        net = round(gross - tot_exp, 2)
        margin = round((net / gross * 100), 2) if gross > 0 else -100.0
        is_unprof = 1 if net < 0 else 0

        if maint > 80 and is_unprof:
            rec = "GROUND VEHICLE - High Maintenance Overhead Exceeds Revenue"
        elif fuel > (gross * 0.7):
            rec = "INSPECT ENGINE - Abnormal Fuel Burn vs Fare Yield"
        elif is_unprof:
            rec = "UNDER-UTILIZED - Relocate to High-Demand Zones"
        else:
            rec = "OPTIMAL - High Margin Operations"

        reconciled_rows.append({
            "simulated_date": sim_date_str,
            "vehicle_id": veh_id,
            "total_trips": trips,
            "gross_earnings": gross,
            "fuel_cost": fuel,
            "maintenance_cost": maint,
            "total_expenses": tot_exp,
            "net_profit": net,
            "profit_margin_pct": margin,
            "is_unprofitable": is_unprof,
            "recommendation": rec
        })

        if engine == "sqlite":
            cur.execute("""
                INSERT INTO batch_daily_profitability (
                    simulated_date, vehicle_id, total_trips, gross_earnings, fuel_cost, 
                    maintenance_cost, total_expenses, net_profit, profit_margin_pct, 
                    is_unprofitable, recommendation, reconciled_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(simulated_date, vehicle_id) DO UPDATE SET
                    total_trips = excluded.total_trips,
                    gross_earnings = excluded.gross_earnings,
                    fuel_cost = excluded.fuel_cost,
                    maintenance_cost = excluded.maintenance_cost,
                    total_expenses = excluded.total_expenses,
                    net_profit = excluded.net_profit,
                    profit_margin_pct = excluded.profit_margin_pct,
                    is_unprofitable = excluded.is_unprofitable,
                    recommendation = excluded.recommendation,
                    reconciled_at = CURRENT_TIMESTAMP;
            """, (sim_date_str, veh_id, trips, gross, fuel, maint, tot_exp, net, margin, is_unprof, rec))
        else:
            cur.execute("""
                INSERT INTO batch_daily_profitability (
                    simulated_date, vehicle_id, total_trips, gross_earnings, fuel_cost, 
                    maintenance_cost, total_expenses, net_profit, profit_margin_pct, 
                    is_unprofitable, recommendation, reconciled_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT(simulated_date, vehicle_id) DO UPDATE SET
                    total_trips = EXCLUDED.total_trips,
                    gross_earnings = EXCLUDED.gross_earnings,
                    fuel_cost = EXCLUDED.fuel_cost,
                    maintenance_cost = EXCLUDED.maintenance_cost,
                    total_expenses = EXCLUDED.total_expenses,
                    net_profit = EXCLUDED.net_profit,
                    profit_margin_pct = EXCLUDED.profit_margin_pct,
                    is_unprofitable = EXCLUDED.is_unprofitable,
                    recommendation = EXCLUDED.recommendation,
                    reconciled_at = CURRENT_TIMESTAMP;
            """, (sim_date_str, veh_id, trips, gross, fuel, maint, tot_exp, net, margin, is_unprof, rec))

    conn.commit()
    conn.close()

    # Save to Parquet Data Lake
    df_reconciled = pd.DataFrame(reconciled_rows)
    pq_path = os.path.join(RECONCILED_DIR, f"reconciled_{sim_date_str}.parquet")
    df_reconciled.to_parquet(pq_path, index=False)
    logger.info(f"Reconciled day {sim_date_str}: {len(df_reconciled)} vehicles -> {pq_path}")

def start_pipeline():
    logger.info("=" * 65)
    logger.info("STARTING STANDALONE LAMBDA ARCHITECTURE PLATFORM (ZERO DOCKER)")
    logger.info("=" * 65)

    # Launch background threads
    t_prod = threading.Thread(target=streaming_producer_thread, daemon=True)
    t_speed = threading.Thread(target=speed_layer_thread, daemon=True)
    t_batch = threading.Thread(target=batch_layer_thread, daemon=True)

    t_prod.start()
    t_speed.start()
    t_batch.start()

    logger.info("Pipeline threads running!")
    logger.info("Starting FastAPI serving backend on http://127.0.0.1:8000 ...")
    uvicorn.run("serving.api:app", host="127.0.0.1", port=8000, log_level="warning")

if __name__ == "__main__":
    start_pipeline()
