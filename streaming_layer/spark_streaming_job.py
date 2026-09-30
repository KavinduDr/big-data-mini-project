"""Speed layer stream processor (Docker mode): Kafka -> windowed metrics.

Consumes the ``fleet-telemetry`` topic, applies the shared tumbling-window
aggregation (``streaming_layer.window_aggregator``) and upserts live zone
metrics plus threshold alerts into PostgreSQL.

The aggregation and persistence helpers are shared with the zero-Docker
standalone runner (``run_standalone.py``) so both modes compute identical
numbers from one implementation.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

try:  # package import (PYTHONPATH=/app, as configured in docker-compose)
    from streaming_layer.metrics_writer import write_alerts, write_speed_window
    from streaming_layer.window_aggregator import FleetWindowAggregator
except ImportError:  # executed as a plain script from inside this directory
    from metrics_writer import write_alerts, write_speed_window
    from window_aggregator import FleetWindowAggregator

# Structured Logging Configuration
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
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
WINDOW_SIZE_SEC = int(os.getenv("WINDOW_SIZE_SEC", 10))


def get_db_connection(max_retries=15, delay=5):
    import psycopg2

    logger.info("Connecting to PostgreSQL at %s:%s/%s...", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.autocommit = True
            logger.info("Connected to PostgreSQL serving layer.")
            return conn
        except Exception as e:
            logger.warning(
                "PostgreSQL not ready (attempt %s/%s): %s. Retrying in %ss...",
                attempt, max_retries, e, delay,
            )
            time.sleep(delay)
    logger.error("Failed to connect to PostgreSQL.")
    sys.exit(1)


def get_kafka_consumer(topic, bootstrap_servers, max_retries=15, delay=5):
    logger.info("Connecting to Kafka topic '%s' on %s...", topic, bootstrap_servers)
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="speed-layer-group",
                client_id="fleet-speed-layer",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            logger.info("Connected to Kafka consumer.")
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                "Kafka broker not ready (attempt %s/%s). Retrying in %ss...",
                attempt, max_retries, delay,
            )
            time.sleep(delay)
    logger.error("Failed to connect to Kafka.")
    sys.exit(1)


def run_speed_layer():
    engine = "postgres"
    conn = get_db_connection()
    consumer = get_kafka_consumer(TELEMETRY_TOPIC, KAFKA_BOOTSTRAP)

    aggregator = FleetWindowAggregator(
        window_size_sec=WINDOW_SIZE_SEC,
        idle_threshold_sec=IDLE_ALERT_THRESHOLD,
    )
    logger.info(
        "Speed Layer Stream Processor running (%ss tumbling windows, idle threshold %ss).",
        WINDOW_SIZE_SEC, IDLE_ALERT_THRESHOLD,
    )

    window_start = datetime.now(timezone.utc)
    pending_alerts = []

    try:
        while True:
            try:
                records = consumer.poll(timeout_ms=1000)
                for _partition, messages in records.items():
                    for msg in messages:
                        alert = aggregator.add_event(msg.value)
                        if alert:
                            pending_alerts.append(alert)

                now = datetime.now(timezone.utc)
                if (now - window_start).total_seconds() < WINDOW_SIZE_SEC:
                    continue

                zones = aggregator.flush(window_start, now)
                if not zones:
                    window_start = now
                    continue

                try:
                    written = write_speed_window(engine, conn, zones, window_start, now)
                    alert_rows = write_alerts(engine, conn, pending_alerts)
                except Exception as e:
                    # Serving layer restarted (e.g. `docker compose restart postgres`):
                    # reconnect rather than exiting the daemon.
                    logger.error("Serving layer write failed (%s); reconnecting...", e)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = get_db_connection()
                    continue

                logger.info(
                    "Closed streaming window %s - %s: %s zones upserted, %s alerts, "
                    "%s events processed, %s invalid.",
                    window_start.strftime("%H:%M:%S"),
                    now.strftime("%H:%M:%S"),
                    written,
                    alert_rows,
                    aggregator.events_processed,
                    aggregator.invalid_events,
                )
                pending_alerts = []
                window_start = now

            except KeyboardInterrupt:
                logger.info("Stopping stream processor gracefully.")
                break
            except Exception as e:
                logger.error("Error in streaming processing: %s", e)
                time.sleep(2)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    run_speed_layer()
