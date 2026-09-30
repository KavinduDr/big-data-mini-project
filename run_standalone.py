"""Zero-Docker standalone runner for the full Lambda pipeline.

Starts three background threads (streaming producer -> in-memory event bus ->
speed layer -> SQLite, and the batch reconciliation loop) and finally the
FastAPI serving layer on http://127.0.0.1:8000.

Usage:
    python run_standalone.py                 # API + pipeline (SQLite)
    LAUNCH_DASHBOARD=1 python run_standalone.py   # also start Streamlit :8501

The speed layer reuses ``streaming_layer.window_aggregator`` and the batch loop
reuses ``batch_layer.batch_processor`` so the standalone mode and the Docker
mode (Kafka + PostgreSQL) compute identical metrics from a single code path.
"""

import json
import logging
import os
import queue
import threading
import time
from datetime import datetime, timedelta, timezone

import uvicorn

# Standalone mode is "zero Docker": default to the embedded database unless the
# operator explicitly asks for PostgreSQL. Must run before db_adapter is used.
os.environ.setdefault("DB_ENGINE", "sqlite")

from storage.db_adapter import get_connection  # noqa: E402
from streaming_layer.metrics_writer import write_alerts, write_speed_window  # noqa: E402
from streaming_layer.window_aggregator import FleetWindowAggregator  # noqa: E402

# Structured Logging Configuration
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "%(name)s", "message": "%(message)s"}'
)
logger = logging.getLogger("StandalonePipeline")

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_LAKE_DIR = os.getenv("DATA_LAKE_DIR", os.path.join(ROOT_DIR, "data_lake"))
DAILY_EXPENSES_DIR = os.getenv("DATA_DROP_DIR", os.path.join(DATA_LAKE_DIR, "daily_expenses"))
RAW_TELEMETRY_DIR = os.getenv("RAW_TELEMETRY_DIR", os.path.join(DATA_LAKE_DIR, "raw_telemetry"))
RECONCILED_DIR = os.getenv("PARQUET_OUTPUT_DIR", os.path.join(DATA_LAKE_DIR, "reconciled_reports"))

for _directory in (DAILY_EXPENSES_DIR, RAW_TELEMETRY_DIR, RECONCILED_DIR):
    os.makedirs(_directory, exist_ok=True)

WINDOW_SIZE_SEC = int(os.getenv("WINDOW_SIZE_SEC", 10))
IDLE_ALERT_THRESHOLD_SEC = int(os.getenv("IDLE_ALERT_THRESHOLD_SEC", 60))
SIMULATED_DAY_SEC = int(os.getenv("SIMULATED_DAY_SEC", 60))
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", 8000))

# Shared streaming in-memory queue simulating the Kafka broker
event_bus = queue.Queue(maxsize=int(os.getenv("EVENT_BUS_MAXSIZE", 10000)))

# Pipeline level counters exposed in logs (basic observability)
pipeline_counters = {
    "events_produced": 0,
    "events_consumed": 0,
    "events_dropped": 0,
    "windows_closed": 0,
    "alerts_written": 0,
    "batch_runs": 0,
}
_counters_lock = threading.Lock()


def _bump(counter, amount=1):
    with _counters_lock:
        pipeline_counters[counter] += amount


# ---------------------------------------------------------------------------
# 1. STREAMING PRODUCER THREAD
# ---------------------------------------------------------------------------
def streaming_producer_thread():
    from data_generator.streaming_producer import FleetSimulator, load_config

    config = load_config()
    simulator = FleetSimulator(config)
    interval = float(config["simulation"].get("telemetry_interval_sec", 2))
    logger.info("Streaming Producer thread started (emitting every %ss).", interval)

    while True:
        try:
            today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            raw_path = os.path.join(RAW_TELEMETRY_DIR, f"telemetry_{today_str}.jsonl")

            with open(raw_path, "a") as f:
                for vehicle in simulator.vehicles:
                    event = simulator.generate_event(vehicle)
                    try:
                        # Non blocking: a saturated bus must never stall the
                        # producer silently, so drops are counted and logged.
                        event_bus.put_nowait(event)
                    except queue.Full:
                        _bump("events_dropped")
                    f.write(json.dumps(event) + "\n")
                    _bump("events_produced")

            if pipeline_counters["events_dropped"] and pipeline_counters["events_produced"] % 500 < len(simulator.vehicles):
                logger.warning(
                    "Event bus saturated: %s events dropped so far (consumer slower than producer).",
                    pipeline_counters["events_dropped"],
                )

            time.sleep(interval)
        except Exception as e:
            logger.error("Producer error: %s", e)
            time.sleep(2)


# ---------------------------------------------------------------------------
# 2. SPEED LAYER STREAM PROCESSOR THREAD
# ---------------------------------------------------------------------------
def speed_layer_thread():
    logger.info(
        "Speed Layer Stream Processor thread started (%ss tumbling windows, idle threshold %ss).",
        WINDOW_SIZE_SEC, IDLE_ALERT_THRESHOLD_SEC,
    )
    aggregator = FleetWindowAggregator(
        window_size_sec=WINDOW_SIZE_SEC,
        idle_threshold_sec=IDLE_ALERT_THRESHOLD_SEC,
    )
    window_start = datetime.now(timezone.utc)
    pending_alerts = []

    while True:
        try:
            # Drain the bus so a burst of events is processed in one pass.
            event = None
            try:
                event = event_bus.get(timeout=0.5)
            except queue.Empty:
                pass

            while event is not None:
                _bump("events_consumed")
                alert = aggregator.add_event(event)
                if alert:
                    pending_alerts.append(alert)
                try:
                    event = event_bus.get_nowait()
                except queue.Empty:
                    event = None

            now = datetime.now(timezone.utc)
            if (now - window_start).total_seconds() >= WINDOW_SIZE_SEC:
                zones = aggregator.flush(window_start, now)
                if zones:
                    engine, conn = get_connection()
                    try:
                        written = write_speed_window(engine, conn, zones, window_start, now)
                        alert_rows = write_alerts(engine, conn, pending_alerts)
                    finally:
                        conn.close()

                    _bump("windows_closed")
                    _bump("alerts_written", alert_rows)
                    logger.info(
                        "Closed window %s-%s: %s zones upserted, %s alerts, %s events processed, "
                        "%s invalid, %s dropped overall.",
                        window_start.strftime("%H:%M:%S"),
                        now.strftime("%H:%M:%S"),
                        written,
                        alert_rows,
                        aggregator.events_processed,
                        aggregator.invalid_events,
                        pipeline_counters["events_dropped"],
                    )
                    pending_alerts = []

                window_start = now

        except Exception as e:
            logger.error("Speed layer error: %s", e)
            time.sleep(1)


# ---------------------------------------------------------------------------
# 3. BATCH LAYER RECONCILIATION THREAD (simulated day = SIMULATED_DAY_SEC)
# ---------------------------------------------------------------------------
def batch_layer_thread():
    """Generates a daily expense drop, then runs the shared reconciliation job.

    The reconciliation itself lives in ``batch_layer.batch_processor`` so the
    standalone mode and the Airflow/Kafka mode execute identical business logic.
    """
    from batch_layer.batch_processor import run_batch_reconciliation
    from data_generator.batch_generator import generate_daily_expense_file

    logger.info("Batch Layer thread started (simulated day = %ss).", SIMULATED_DAY_SEC)
    sim_date = datetime.now(timezone.utc).date()
    generate_daily_expense_file(str(sim_date), 25, DAILY_EXPENSES_DIR)
    _reconcile(sim_date)
    day_idx = 1

    while True:
        try:
            time.sleep(SIMULATED_DAY_SEC)
            sim_date = datetime.now(timezone.utc).date() + timedelta(days=day_idx)
            generate_daily_expense_file(str(sim_date), 25, DAILY_EXPENSES_DIR)
            _reconcile(sim_date)
            day_idx += 1
        except Exception as e:
            logger.error("Batch thread error: %s", e)
            time.sleep(10)


def _reconcile(sim_date):
    from batch_layer.batch_processor import run_batch_reconciliation

    summary = run_batch_reconciliation(
        data_drop_dir=DAILY_EXPENSES_DIR,
        raw_telemetry_dir=RAW_TELEMETRY_DIR,
        parquet_output_dir=RECONCILED_DIR,
    )
    _bump("batch_runs")
    if summary.get("reconciled_records"):
        logger.info(
            "Batch reconciliation for %s: %s records, %s unprofitable vehicles, "
            "fleet net profit %.2f.",
            sim_date,
            summary["reconciled_records"],
            summary["unprofitable_vehicles"],
            summary["total_net_profit"],
        )


def _launch_dashboard_thread():
    """Optionally starts the Streamlit UI (LAUNCH_DASHBOARD=1)."""
    import subprocess
    import sys

    dashboard_path = os.path.join(ROOT_DIR, "serving", "dashboard.py")
    if not os.path.exists(dashboard_path):
        logger.warning("Dashboard entrypoint not found at %s; skipping.", dashboard_path)
        return

    env = dict(os.environ, API_BASE_URL=f"http://{API_HOST}:{API_PORT}")
    try:
        subprocess.Popen(
            [
                sys.executable, "-m", "streamlit", "run", dashboard_path,
                "--server.port", "8501", "--server.address", "0.0.0.0",
            ],
            env=env,
        )
        logger.info("Streamlit dashboard starting on http://localhost:8501 ...")
    except Exception as e:  # pragma: no cover - depends on the local environment
        logger.warning("Could not start the Streamlit dashboard: %s", e)


def start_pipeline():
    logger.info("=" * 65)
    logger.info("STARTING STANDALONE LAMBDA ARCHITECTURE PLATFORM (ZERO DOCKER)")
    logger.info("=" * 65)

    threads = [
        threading.Thread(target=streaming_producer_thread, daemon=True, name="producer"),
        threading.Thread(target=speed_layer_thread, daemon=True, name="speed-layer"),
        threading.Thread(target=batch_layer_thread, daemon=True, name="batch-layer"),
    ]
    for thread in threads:
        thread.start()

    logger.info("Pipeline threads running!")
    if os.getenv("LAUNCH_DASHBOARD", "0") == "1":
        _launch_dashboard_thread()

    logger.info("Starting FastAPI serving backend on http://%s:%s ...", API_HOST, API_PORT)
    logger.info(
        "Endpoints: /health  /metrics  /api/fleet/realtime-utilization  "
        "/api/fleet/alerts  /api/fleet/daily-profitability  /api/fleet/unified-summary"
    )
    uvicorn.run("serving.api:app", host=API_HOST, port=API_PORT, log_level="warning")


if __name__ == "__main__":
    start_pipeline()
