"""Batch layer reconciliation engine.

Reconciles the once-a-day garage/fuel expense drop against the trip fares
observed in the raw telemetry lake and writes per-vehicle profitability into
the serving database plus Parquet cold storage.

Runs against **either** PostgreSQL (Docker mode) or the embedded SQLite
database (standalone mode) through ``storage.db_adapter``, so
``run_standalone.py`` and the Airflow DAG execute the exact same job instead of
maintaining two divergent copies of the business rules.
"""

import glob
import json
import logging
import os
import re
from collections import defaultdict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from storage.db_adapter import db_cursor, get_connection

# Structured Logging Configuration
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "BatchLayerReconciliation", "message": "%(message)s"}'
)
logger = logging.getLogger("BatchLayerReconciliation")

DEFAULT_DROP_DIR = os.getenv("DATA_DROP_DIR", "/app/data_lake/daily_expenses")
DEFAULT_RAW_TELEMETRY_DIR = os.getenv("RAW_TELEMETRY_DIR", "/app/data_lake/raw_telemetry")
DEFAULT_PARQUET_DIR = os.getenv("PARQUET_OUTPUT_DIR", "/app/data_lake/reconciled_reports")

REQUIRED_EXPENSE_COLUMNS = {
    "simulated_date", "vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered_km",
}

# Revenue assumption when a vehicle has no telemetry (cold start / late file).
FALLBACK_FARE_PER_KM = float(os.getenv("FALLBACK_FARE_PER_KM", 0.75))
MIN_TRIPS_PER_DAY = int(os.getenv("MIN_TRIPS_PER_DAY", 4))
AVG_TRIP_FARE = float(os.getenv("AVG_TRIP_FARE", 15.0))
DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")


def compute_profitability(gross_earnings, fuel_cost, maintenance_cost):
    """Pure business rule: fares minus expenses -> profit, margin and action.

    Extracted from the loop so the exact rule set used in production can be
    unit tested directly (see ``tests/test_batch_reconciliation.py``).
    """
    gross = round(float(gross_earnings), 2)
    fuel = round(float(fuel_cost), 2)
    maintenance = round(float(maintenance_cost), 2)
    total_expenses = round(fuel + maintenance, 2)
    net_profit = round(gross - total_expenses, 2)
    profit_margin_pct = round(net_profit / gross * 100, 2) if gross > 0 else -100.0
    is_unprofitable = net_profit < 0
    total_trips = max(MIN_TRIPS_PER_DAY, int(gross / AVG_TRIP_FARE)) if gross > 0 else 0

    if maintenance > 80 and is_unprofitable:
        recommendation = "GROUND VEHICLE - High Maintenance Overhead Exceeds Revenue"
    elif fuel > (gross * 0.7):
        recommendation = "INSPECT ENGINE - Abnormal Fuel Burn vs Fare Yield"
    elif is_unprofitable:
        recommendation = "UNDER-UTILIZED - Relocate to High-Demand Zones"
    else:
        recommendation = "OPTIMAL - High Margin Operations"

    return {
        "total_trips": total_trips,
        "gross_earnings": gross,
        "fuel_cost": fuel,
        "maintenance_cost": maintenance,
        "total_expenses": total_expenses,
        "net_profit": net_profit,
        "profit_margin_pct": profit_margin_pct,
        "is_unprofitable": is_unprofitable,
        "recommendation": recommendation,
    }


def _date_from_filename(path):
    """Best-effort simulated date from ``telemetry_20260925.jsonl`` style names."""
    name = os.path.basename(path)
    compact = re.search(r"(\d{8})", name)
    if compact:
        raw = compact.group(1)
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    match = DATE_PATTERN.search(name)
    return match.group(1) if match else None


def _record_date(record, fallback):
    timestamp = record.get("timestamp")
    if isinstance(timestamp, str) and DATE_PATTERN.fullmatch(timestamp[:10]):
        return timestamp[:10]
    return fallback


def summarize_telemetry_earnings(raw_telemetry_dir, target_dates=None):
    """Aggregates trip revenue from the raw telemetry lake, keyed by date.

    Returns ``{simulated_date: {vehicle_id: {"trips": n, "gross_earnings": x}}}``.

    Two bugs in the previous implementation are fixed here:

    * ``fare`` is the *cumulative* total for the current trip re-emitted on
      every heartbeat. Previously the fare was booked only on the first
      ``on_trip`` heartbeat of each trip, i.e. the smallest possible value,
      drastically under-reporting daily revenue. Now the highest fare observed
      per ``trip_id`` is booked as that trip's revenue.
    * Telemetry was never filtered by date, so every simulated day was
      reconciled against the same all-time fares. Records are now bucketed by
      the telemetry timestamp (falling back to the file name), which keeps the
      join deterministic, replayable and date-scoped.
    """
    raw_files = sorted(glob.glob(os.path.join(raw_telemetry_dir, "*.jsonl")))
    if not raw_files:
        logger.warning(
            "No raw telemetry files found in %s; using distance-based fallback revenue.",
            raw_telemetry_dir,
        )
        return {}

    wanted = set(target_dates) if target_dates else None
    # date -> vehicle -> trip_id -> highest fare observed
    trip_fares = defaultdict(lambda: defaultdict(dict))
    malformed = 0
    scanned = 0

    for file_path in raw_files:
        file_date = _date_from_filename(file_path)
        if wanted and file_date and file_date not in wanted:
            # Cheap skip: a dated file can only contain its own day of telemetry.
            continue
        with open(file_path, "r") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue
                scanned += 1
                if record.get("status") != "on_trip":
                    continue
                trip_id = record.get("trip_id")
                vehicle_id = record.get("vehicle_id")
                if not trip_id or not vehicle_id:
                    continue
                day = _record_date(record, file_date)
                if not day or (wanted and day not in wanted):
                    continue
                try:
                    fare = float(record.get("fare") or 0.0)
                except (TypeError, ValueError):
                    continue
                per_trip = trip_fares[day][vehicle_id]
                if fare > per_trip.get(trip_id, 0.0):
                    per_trip[trip_id] = fare

    if malformed:
        logger.warning("Skipped %s malformed telemetry lines while aggregating fares.", malformed)

    summary = {
        day: {
            vehicle_id: {"trips": len(trips), "gross_earnings": round(sum(trips.values()), 2)}
            for vehicle_id, trips in vehicles.items()
        }
        for day, vehicles in trip_fares.items()
    }
    logger.info(
        "Aggregated %s telemetry records: %s day(s), %s vehicle-day revenue rows.",
        scanned,
        len(summary),
        sum(len(vehicles) for vehicles in summary.values()),
    )
    return summary


# ---------------------------------------------------------------------------
# Dialect aware persistence (PostgreSQL in Docker mode, SQLite standalone)
# ---------------------------------------------------------------------------
_EXPENSE_COLUMNS = (
    "simulated_date, vehicle_id, fuel_cost, maintenance_cost, total_expense, "
    "distance_covered_km, service_flag, ingested_at"
)

SQLITE_EXPENSE_SQL = f"""
    INSERT INTO batch_vehicle_expenses ({_EXPENSE_COLUMNS})
    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(simulated_date, vehicle_id) DO UPDATE SET
        fuel_cost = excluded.fuel_cost,
        maintenance_cost = excluded.maintenance_cost,
        total_expense = excluded.total_expense,
        distance_covered_km = excluded.distance_covered_km,
        service_flag = excluded.service_flag,
        ingested_at = CURRENT_TIMESTAMP;
"""

# execute_values needs an explicit template because `ingested_at` is supplied by
# the database, not by the row tuples. Without it PostgreSQL fails with
# "INSERT has more target columns than expressions" - a latent defect in the
# original implementation that made the Docker/PostgreSQL batch layer unusable.
POSTGRES_EXPENSE_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"

POSTGRES_EXPENSE_SQL = f"""
    INSERT INTO batch_vehicle_expenses ({_EXPENSE_COLUMNS})
    VALUES %s
    ON CONFLICT (simulated_date, vehicle_id) DO UPDATE SET
        fuel_cost = excluded.fuel_cost,
        maintenance_cost = excluded.maintenance_cost,
        total_expense = excluded.total_expense,
        distance_covered_km = excluded.distance_covered_km,
        service_flag = excluded.service_flag,
        ingested_at = CURRENT_TIMESTAMP;
"""

_PROFIT_COLUMNS = (
    "simulated_date, vehicle_id, total_trips, gross_earnings, fuel_cost, "
    "maintenance_cost, total_expenses, net_profit, profit_margin_pct, "
    "is_unprofitable, recommendation, reconciled_at"
)

SQLITE_PROFIT_SQL = f"""
    INSERT INTO batch_daily_profitability ({_PROFIT_COLUMNS})
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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
"""

POSTGRES_PROFIT_SQL = f"""
    INSERT INTO batch_daily_profitability ({_PROFIT_COLUMNS})
    VALUES %s
    ON CONFLICT (simulated_date, vehicle_id) DO UPDATE SET
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
"""

POSTGRES_PROFIT_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"


def _write_rows(engine, conn, sqlite_sql, postgres_sql, rows, template=None):
    if not rows:
        return 0
    with db_cursor(conn) as cur:
        if engine == "sqlite":
            cur.executemany(sqlite_sql, rows)
        else:
            from psycopg2.extras import execute_values

            execute_values(cur, postgres_sql, rows, template=template)
    conn.commit()
    return len(rows)


def write_batch_expenses(engine, conn, rows):
    """Upserts the daily expense drop so ``batch_vehicle_expenses`` is populated.

    Previously this table existed in the schema but was never written to, which
    made the serving layer's expense side unqueryable.
    """
    return _write_rows(
        engine, conn, SQLITE_EXPENSE_SQL, POSTGRES_EXPENSE_SQL, rows,
        template=POSTGRES_EXPENSE_TEMPLATE,
    )


def write_profitability(engine, conn, rows):
    """Upserts the reconciliation result (idempotent on date + vehicle)."""
    return _write_rows(
        engine, conn, SQLITE_PROFIT_SQL, POSTGRES_PROFIT_SQL, rows,
        template=POSTGRES_PROFIT_TEMPLATE,
    )


def existing_reconciled_dates(engine, conn):
    """Dates already present in ``batch_daily_profitability`` (for incremental runs)."""
    with db_cursor(conn) as cur:
        cur.execute("SELECT DISTINCT simulated_date FROM batch_daily_profitability;")
        rows = cur.fetchall()
    dates = set()
    for row in rows:
        value = row[0] if not hasattr(row, "keys") else row["simulated_date"]
        dates.add(str(value)[:10])
    return dates


def _reconcile_row(day, expense_row, day_telemetry, counters):
    """Joins one expense row with that day's telemetry and applies the rules."""
    vehicle_id = expense_row["vehicle_id"]
    fuel_cost = float(expense_row["fuel_cost"])
    maintenance_cost = float(expense_row["maintenance_cost"])
    distance_km = float(expense_row.get("distance_covered_km") or 0.0)
    service_flag = expense_row.get("service_flag") or "NORMAL"

    telemetry = day_telemetry.get(vehicle_id)
    if telemetry and telemetry.get("gross_earnings", 0.0) > 5.0:
        gross_earnings = telemetry["gross_earnings"]
        total_trips = telemetry["trips"]
    else:
        # Cold start / late telemetry: fall back to distance-implied revenue so
        # the daily report is still produced, but record that it happened.
        gross_earnings = round(distance_km * FALLBACK_FARE_PER_KM, 2)
        total_trips = max(MIN_TRIPS_PER_DAY, int(gross_earnings / AVG_TRIP_FARE))
        counters["fallback_vehicles"] += 1

    metrics = compute_profitability(gross_earnings, fuel_cost, maintenance_cost)

    expense_tuple = (
        day, vehicle_id, metrics["fuel_cost"], metrics["maintenance_cost"],
        metrics["total_expenses"], distance_km, service_flag,
    )
    profit_tuple = (
        day, vehicle_id, total_trips, metrics["gross_earnings"], metrics["fuel_cost"],
        metrics["maintenance_cost"], metrics["total_expenses"], metrics["net_profit"],
        metrics["profit_margin_pct"], bool(metrics["is_unprofitable"]), metrics["recommendation"],
    )
    record = {
        "simulated_date": day,
        "vehicle_id": vehicle_id,
        "total_trips": total_trips,
        **{key: metrics[key] for key in (
            "gross_earnings", "fuel_cost", "maintenance_cost", "total_expenses",
            "net_profit", "profit_margin_pct", "is_unprofitable", "recommendation",
        )},
    }
    return expense_tuple, profit_tuple, record


def run_batch_reconciliation(
    data_drop_dir=None,
    raw_telemetry_dir=None,
    parquet_output_dir=None,
    target_dates=None,
    skip_existing=True,
):
    """Executes the batch reconciliation and returns an observability summary.

    ``skip_existing`` makes repeated runs cheap and idempotent: dates already
    present in ``batch_daily_profitability`` are not recomputed (the previous
    implementation re-read every CSV and re-wrote every row on every run).
    """
    drop_dir = data_drop_dir or os.getenv("DATA_DROP_DIR", DEFAULT_DROP_DIR)
    raw_dir = raw_telemetry_dir or os.getenv("RAW_TELEMETRY_DIR", DEFAULT_RAW_TELEMETRY_DIR)
    out_dir = parquet_output_dir or os.getenv("PARQUET_OUTPUT_DIR", DEFAULT_PARQUET_DIR)
    counters = {"fallback_vehicles": 0}
    summary = {
        "expense_files": 0,
        "dates_processed": [],
        "dates_skipped": [],
        "reconciled_records": 0,
        "unprofitable_vehicles": 0,
        "total_net_profit": 0.0,
        "fallback_revenue_vehicles": 0,
    }

    expense_files = sorted(glob.glob(os.path.join(drop_dir, "*.csv")))
    if not expense_files:
        logger.warning("No expense CSV files found in %s. Skipping batch run.", drop_dir)
        return summary

    frames = []
    for file_path in expense_files:
        try:
            frame = pd.read_csv(file_path)
        except Exception as exc:
            logger.error("Could not read expense file %s: %s", file_path, exc)
            continue
        missing = REQUIRED_EXPENSE_COLUMNS - set(frame.columns)
        if missing:
            logger.error("Skipping %s: missing columns %s", file_path, sorted(missing))
            continue
        frames.append(frame)
        summary["expense_files"] += 1

    if not frames:
        logger.warning("No valid expense files found in %s. Skipping batch run.", drop_dir)
        return summary

    expenses = pd.concat(frames, ignore_index=True)
    expenses["simulated_date"] = expenses["simulated_date"].astype(str).str.slice(0, 10)
    if target_dates:
        expenses = expenses[expenses["simulated_date"].isin(set(target_dates))]
    if expenses.empty:
        logger.info("No expense rows match the requested simulated dates.")
        return summary

    os.makedirs(out_dir, exist_ok=True)
    engine, conn = get_connection()
    try:
        reconciled_dates = existing_reconciled_dates(engine, conn) if skip_existing else set()
        all_dates = set(expenses["simulated_date"])
        pending_dates = sorted(all_dates - reconciled_dates)
        summary["dates_skipped"] = sorted(all_dates & reconciled_dates)
        if summary["dates_skipped"]:
            logger.info("Already reconciled (skipped): %s", ", ".join(summary["dates_skipped"]))
        if not pending_dates:
            logger.info("All %s expense date(s) already reconciled; nothing to do.", len(all_dates))
            return summary

        expenses = expenses[expenses["simulated_date"].isin(pending_dates)]
        telemetry = summarize_telemetry_earnings(raw_dir, target_dates=pending_dates)

        for day, day_frame in expenses.groupby("simulated_date"):
            day_telemetry = telemetry.get(day, {})
            expense_rows, profit_rows, records = [], [], []
            for expense_row in day_frame.to_dict("records"):
                expense_tuple, profit_tuple, record = _reconcile_row(
                    day, expense_row, day_telemetry, counters
                )
                expense_rows.append(expense_tuple)
                profit_rows.append(profit_tuple)
                records.append(record)

            write_batch_expenses(engine, conn, expense_rows)
            write_profitability(engine, conn, profit_rows)

            parquet_path = os.path.join(out_dir, f"reconciled_{day}.parquet")
            pq.write_table(pa.Table.from_pandas(pd.DataFrame(records)), parquet_path)

            day_unprofitable = sum(1 for record in records if record["is_unprofitable"])
            day_net = round(sum(record["net_profit"] for record in records), 2)
            summary["dates_processed"].append(day)
            summary["reconciled_records"] += len(records)
            summary["unprofitable_vehicles"] += day_unprofitable
            summary["total_net_profit"] = round(summary["total_net_profit"] + day_net, 2)
            logger.info(
                "Reconciled %s: %s vehicles, %s unprofitable, day net %.2f -> %s",
                day, len(records), day_unprofitable, day_net, parquet_path,
            )
    finally:
        conn.close()

    summary["fallback_revenue_vehicles"] = counters["fallback_vehicles"]
    if counters["fallback_vehicles"]:
        logger.warning(
            "%s vehicle-day row(s) had no usable telemetry and used distance-based revenue.",
            counters["fallback_vehicles"],
        )
    return summary


if __name__ == "__main__":
    print(json.dumps(run_batch_reconciliation(), indent=2, default=str))