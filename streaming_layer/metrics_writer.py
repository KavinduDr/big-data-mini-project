"""Dialect-aware persistence helpers for speed-layer results.

Both stream processors (the Kafka consumer and the standalone in-memory bus)
must write identical rows into ``speed_fleet_metrics`` and
``speed_vehicle_alerts``. Previously each file carried its own copy of the
SQLite ``?`` / PostgreSQL ``%s`` variants, which is how the two implementations
drifted apart. They now share these helpers.
"""

import logging

from storage.db_adapter import db_cursor

logger = logging.getLogger("SpeedLayerWriter")

_METRIC_COLUMNS = (
    "window_start, window_end, grid_zone, active_vehicles, idle_vehicles, "
    "enroute_vehicles, total_trips, total_fare, avg_speed, updated_at"
)

_METRIC_UPDATE = """
    active_vehicles = excluded.active_vehicles,
    idle_vehicles = excluded.idle_vehicles,
    enroute_vehicles = excluded.enroute_vehicles,
    total_trips = excluded.total_trips,
    total_fare = excluded.total_fare,
    avg_speed = excluded.avg_speed,
    updated_at = CURRENT_TIMESTAMP
"""

# ``excluded`` is valid for both engines; PostgreSQL accepts the lower case
# spelling of the pseudo-table, so a single statement body is reused.
SQLITE_METRIC_SQL = f"""
    INSERT INTO speed_fleet_metrics ({_METRIC_COLUMNS})
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(window_start, grid_zone) DO UPDATE SET {_METRIC_UPDATE};
"""

POSTGRES_METRIC_SQL = f"""
    INSERT INTO speed_fleet_metrics ({_METRIC_COLUMNS})
    VALUES %s
    ON CONFLICT (window_start, grid_zone) DO UPDATE SET {_METRIC_UPDATE};
"""

# `execute_values` expands one placeholder per row, so the trailing
# `updated_at` column needs its own template entry (otherwise PostgreSQL raises
# "INSERT has more target columns than expressions").
POSTGRES_METRIC_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)"

SQLITE_ALERT_SQL = """
    INSERT INTO speed_vehicle_alerts (
        vehicle_id, driver_id, grid_zone, alert_type, idle_duration_sec, alert_message
    ) VALUES (?, ?, ?, ?, ?, ?);
"""

POSTGRES_ALERT_SQL = """
    INSERT INTO speed_vehicle_alerts (
        vehicle_id, driver_id, grid_zone, alert_type, idle_duration_sec, alert_message
    ) VALUES %s;
"""


def _timestamp(engine, value):
    """SQLite stores ISO-8601 text, PostgreSQL a real TIMESTAMP."""
    if value is None:
        return None
    if engine == "sqlite":
        return value.isoformat() if hasattr(value, "isoformat") else str(value)
    return value


def build_metric_rows(engine, zones, window_start, window_end):
    """Converts ``{zone: stats}`` into insertable rows (also used by tests)."""
    rows = []
    for zone, stats in zones.items():
        count = stats.get("count", 0)
        avg_speed = round(stats["speed_sum"] / count, 2) if count else 0.0
        rows.append((
            _timestamp(engine, window_start),
            _timestamp(engine, window_end),
            zone,
            int(stats.get("active", 0)),
            int(stats.get("idle", 0)),
            int(stats.get("enroute", 0)),
            int(stats.get("trips", 0)),
            round(float(stats.get("fare_sum", 0.0)), 2),
            avg_speed,
        ))
    return rows


def write_speed_window(engine, conn, zones, window_start, window_end):
    """Upserts one row per zone for the closed window. Returns rows written."""
    rows = build_metric_rows(engine, zones, window_start, window_end)
    if not rows:
        return 0

    with db_cursor(conn) as cur:
        if engine == "sqlite":
            cur.executemany(SQLITE_METRIC_SQL, rows)
        else:
            from psycopg2.extras import execute_values

            execute_values(cur, POSTGRES_METRIC_SQL, rows, template=POSTGRES_METRIC_TEMPLATE)
    conn.commit()
    return len(rows)


def write_alerts(engine, conn, alerts):
    """Persists threshold alerts. Returns rows written."""
    if not alerts:
        return 0

    rows = [
        (
            alert["vehicle_id"],
            alert.get("driver_id") or "UNKNOWN",
            alert.get("grid_zone") or "Unknown",
            alert.get("alert_type") or "EXCESSIVE_IDLE",
            int(alert.get("idle_duration_sec") or 0),
            alert.get("alert_message"),
        )
        for alert in alerts
    ]

    with db_cursor(conn) as cur:
        if engine == "sqlite":
            cur.executemany(SQLITE_ALERT_SQL, rows)
        else:
            from psycopg2.extras import execute_values

            execute_values(cur, POSTGRES_ALERT_SQL, rows)
    conn.commit()
    return len(rows)
