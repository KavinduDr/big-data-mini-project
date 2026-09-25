import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# Structured Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "SpeedLayerStreamProcessor", "message": "%(message)s"}'
)
logger = logging.getLogger("SpeedLayerStreamProcessor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "fleet-telemetry")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "fleet_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgrespassword")
IDLE_ALERT_THRESHOLD = int(os.getenv("IDLE_ALERT_THRESHOLD_SEC", 60))

def get_db_connection(max_retries=15, delay=5):
    logger.info(f"Connecting to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}...")
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            conn.autocommit = True
            logger.info("Connected to PostgreSQL serving layer.")
            return conn
        except Exception as e:
            logger.warning(f"PostgreSQL not ready (attempt {attempt}/{max_retries}): {e}. Retrying in {delay}s...")
            time.sleep(delay)
    logger.error("Failed to connect to PostgreSQL.")
    sys.exit(1)

def get_kafka_consumer(topic, bootstrap_servers, max_retries=15, delay=5):
    logger.info(f"Connecting to Kafka topic '{topic}' on {bootstrap_servers}...")
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="speed-layer-group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000
            )
            logger.info("Connected to Kafka consumer.")
            return consumer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not ready (attempt {attempt}/{max_retries}). Retrying in {delay}s...")
            time.sleep(delay)
    logger.error("Failed to connect to Kafka.")
    sys.exit(1)

def persist_zone_metrics(conn, zone_aggregates, window_start, window_end):
    """Upsert real-time aggregated metrics per zone into speed_fleet_metrics."""
    query = """
        INSERT INTO speed_fleet_metrics (
            window_start, window_end, grid_zone, active_vehicles, 
            idle_vehicles, enroute_vehicles, total_trips, total_fare, avg_speed, updated_at
        ) VALUES %s
        ON CONFLICT (window_start, grid_zone) DO UPDATE SET
            active_vehicles = EXCLUDED.active_vehicles,
            idle_vehicles = EXCLUDED.idle_vehicles,
            enroute_vehicles = EXCLUDED.enroute_vehicles,
            total_trips = EXCLUDED.total_trips,
            total_fare = EXCLUDED.total_fare,
            avg_speed = EXCLUDED.avg_speed,
            updated_at = CURRENT_TIMESTAMP;
    """
    records = []
    for zone, stats in zone_aggregates.items():
        avg_spd = round(stats["speed_sum"] / stats["count"], 2) if stats["count"] > 0 else 0.0
        records.append((
            window_start,
            window_end,
            zone,
            stats["active"],
            stats["idle"],
            stats["enroute"],
            stats["trips"],
            round(stats["fare_sum"], 2),
            avg_spd
        ))
    if records:
        with conn.cursor() as cur:
            execute_values(cur, query, records)

def trigger_idle_alert(conn, event):
    """Record threshold alert if vehicle idle exceeds limit."""
    query = """
        INSERT INTO speed_vehicle_alerts (
            vehicle_id, driver_id, grid_zone, alert_type, idle_duration_sec, alert_message
        ) VALUES (%s, %s, %s, %s, %s, %s);
    """
    msg = f"Vehicle {event['vehicle_id']} idle for {event['idle_duration_sec']}s in zone {event['grid_zone']} (threshold: {IDLE_ALERT_THRESHOLD}s)."
    with conn.cursor() as cur:
        cur.execute(query, (
            event["vehicle_id"],
            event["driver_id"],
            event["grid_zone"],
            "EXCESSIVE_IDLE",
            event["idle_duration_sec"],
            msg
        ))
    logger.warning(f"ALERT TRIGGERED: {msg}")

def run_speed_layer():
    conn = get_db_connection()
    consumer = get_kafka_consumer(TELEMETRY_TOPIC, KAFKA_BOOTSTRAP)

    logger.info("Speed Layer Stream Processor running. Window size: 10s micro-batch / tumbling window.")

    window_size_sec = 10
    current_window_start = datetime.now(timezone.utc)
    zone_aggregates = {}
    last_alerted_vehicles = {}

    while True:
        try:
            records = consumer.poll(timeout_ms=1000)
            now = datetime.now(timezone.utc)

            for partition, messages in records.items():
                for msg in messages:
                    data = msg.value
                    zone = data.get("grid_zone", "Unknown")
                    status = data.get("status", "idle")
                    speed = float(data.get("speed", 0.0))
                    fare = float(data.get("fare", 0.0))
                    idle_sec = int(data.get("idle_duration_sec", 0))
                    veh_id = data.get("vehicle_id")

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

                    # Check threshold-based idle alert
                    if status == "idle" and idle_sec >= IDLE_ALERT_THRESHOLD:
                        last_alert_time = last_alerted_vehicles.get(veh_id, 0)
                        if time.time() - last_alert_time > 60:  # Suppress duplicate alerts for 60s
                            trigger_idle_alert(conn, data)
                            last_alerted_vehicles[veh_id] = time.time()

            # Window closure check
            elapsed = (now - current_window_start).total_seconds()
            if elapsed >= window_size_sec:
                if zone_aggregates:
                    persist_zone_metrics(conn, zone_aggregates, current_window_start, now)
                    logger.info(f"Closed streaming window {current_window_start.strftime('%H:%M:%S')} - {now.strftime('%H:%M:%S')}. Updated {len(zone_aggregates)} zones.")
                
                # Reset for next window
                zone_aggregates = {}
                current_window_start = now

        except KeyboardInterrupt:
            logger.info("Stopping stream processor gracefully.")
            break
        except Exception as e:
            logger.error(f"Error in streaming processing: {e}")
            time.sleep(2)

if __name__ == "__main__":
    run_speed_layer()
