import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager

logger = logging.getLogger("DBAdapter")

_DEFAULT_SQLITE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fleet_db.sqlite")


def _sqlite_db_path():
    return os.getenv("SQLITE_DB_PATH", _DEFAULT_SQLITE_PATH)


def _postgres_settings():
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "dbname": os.getenv("POSTGRES_DB", "fleet_db"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgrespassword"),
        "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", 2)),
    }


# Backwards compatible module level aliases (read by config/debug helpers)
DB_ENGINE = os.getenv("DB_ENGINE", "auto")  # 'postgres', 'sqlite', or 'auto'
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "fleet_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgrespassword")
SQLITE_DB_PATH = _sqlite_db_path()

# ---------------------------------------------------------------------------
# Engine resolution cache
# ---------------------------------------------------------------------------
# Resolving the engine on *every* call is expensive in standalone mode: an
# unreachable PostgreSQL host costs a full TCP connect timeout per request.
# A successful PostgreSQL detection is cached indefinitely, while the SQLite
# fallback is cached for a short TTL so the adapter can still pick PostgreSQL
# up if it starts later.
_ENGINE_CACHE_TTL_SEC = float(os.getenv("DB_ENGINE_RETRY_TTL_SEC", 60))
_engine_cache = {"engine": None, "resolved_at": 0.0}
_engine_lock = threading.Lock()


def reset_engine_cache():
    """Force the next get_connection() call to re-resolve the engine."""
    with _engine_lock:
        _engine_cache["engine"] = None
        _engine_cache["resolved_at"] = 0.0


def _connect_postgres():
    import psycopg2  # lazy import: optional dependency in standalone mode

    conn = psycopg2.connect(**_postgres_settings())
    conn.autocommit = True
    return conn


def _resolve_engine():
    """Returns 'postgres' or 'sqlite' using a small thread-safe cache."""
    configured = os.getenv("DB_ENGINE", "auto").lower()
    if configured in ("postgres", "postgresql"):
        return "postgres"
    if configured == "sqlite":
        return "sqlite"

    now = time.time()
    with _engine_lock:
        cached = _engine_cache["engine"]
        if cached == "postgres":
            return "postgres"
        if cached == "sqlite" and (now - _engine_cache["resolved_at"]) < _ENGINE_CACHE_TTL_SEC:
            return "sqlite"

        try:
            probe = _connect_postgres()
            probe.close()
            _engine_cache["engine"] = "postgres"
            _engine_cache["resolved_at"] = now
            return "postgres"
        except Exception as exc:  # any failure means "fall back to SQLite"
            logger.info("PostgreSQL unavailable (%s); using embedded SQLite.", exc)
            _engine_cache["engine"] = "sqlite"
            _engine_cache["resolved_at"] = now
            return "sqlite"


def get_connection():
    """Returns a DB connection tuple ``(engine_name, connection)``.
    PostgreSQL is preferred when reachable, otherwise the embedded SQLite
    database is used. The engine choice is cached (see ``_resolve_engine``) so
    serving-layer requests do not pay a TCP timeout on every call.
    """
    engine = _resolve_engine()

    if engine == "postgres":
        try:
            return "postgres", _connect_postgres()
        except Exception:
            if os.getenv("DB_ENGINE", "auto").lower() in ("postgres", "postgresql"):
                raise
            logger.warning("PostgreSQL connection lost; serving this call from SQLite.")
            reset_engine_cache()

    return "sqlite", _connect_sqlite()


@contextmanager
def db_cursor(conn):
    """Cursor context manager that works for both sqlite3 and psycopg2.

    ``sqlite3.Cursor`` does not implement the context manager protocol (only
    ``sqlite3.Connection`` does, and as a transaction), so ``with conn.cursor()``
    raises ``TypeError`` on SQLite while working on PostgreSQL. Use this helper
    in code that supports both engines.
    """
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def _connect_sqlite():
    path = _sqlite_db_path()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False, timeout=10.0)
    conn.row_factory = sqlite3.Row
    # WAL + busy_timeout let the producer, speed, batch and API threads read and
    # write concurrently instead of raising "database is locked".
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=10000;")
    init_sqlite_schema(conn)
    return conn

def init_sqlite_schema(conn):
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS speed_fleet_metrics (
            window_start TEXT NOT NULL,
            window_end TEXT NOT NULL,
            grid_zone TEXT NOT NULL,
            active_vehicles INTEGER DEFAULT 0,
            idle_vehicles INTEGER DEFAULT 0,
            enroute_vehicles INTEGER DEFAULT 0,
            total_trips INTEGER DEFAULT 0,
            total_fare REAL DEFAULT 0.0,
            avg_speed REAL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (window_start, grid_zone)
        );

        CREATE INDEX IF NOT EXISTS idx_speed_metrics_window_end
            ON speed_fleet_metrics (window_end DESC);

        CREATE TABLE IF NOT EXISTS speed_vehicle_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            vehicle_id TEXT NOT NULL,
            driver_id TEXT NOT NULL,
            grid_zone TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            idle_duration_sec INTEGER DEFAULT 0,
            alert_message TEXT,
            alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            resolved INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_speed_alerts_unresolved
            ON speed_vehicle_alerts (resolved, vehicle_id);

        CREATE TABLE IF NOT EXISTS batch_vehicle_expenses (
            simulated_date TEXT NOT NULL,
            vehicle_id TEXT NOT NULL,
            fuel_cost REAL NOT NULL,
            maintenance_cost REAL NOT NULL,
            total_expense REAL NOT NULL,
            distance_covered_km REAL NOT NULL,
            service_flag TEXT DEFAULT 'NORMAL',
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (simulated_date, vehicle_id)
        );

        CREATE INDEX IF NOT EXISTS idx_batch_expenses_date
            ON batch_vehicle_expenses (simulated_date DESC);

        CREATE TABLE IF NOT EXISTS batch_daily_profitability (
            simulated_date TEXT NOT NULL,
            vehicle_id TEXT NOT NULL,
            total_trips INTEGER DEFAULT 0,
            gross_earnings REAL DEFAULT 0.0,
            fuel_cost REAL DEFAULT 0.0,
            maintenance_cost REAL DEFAULT 0.0,
            total_expenses REAL DEFAULT 0.0,
            net_profit REAL DEFAULT 0.0,
            profit_margin_pct REAL DEFAULT 0.0,
            is_unprofitable INTEGER DEFAULT 0,
            recommendation TEXT,
            reconciled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (simulated_date, vehicle_id)
        );

        CREATE INDEX IF NOT EXISTS idx_batch_profitability_date
            ON batch_daily_profitability (simulated_date DESC);

        DROP VIEW IF EXISTS v_unified_vehicle_summary;
        CREATE VIEW IF NOT EXISTS v_unified_vehicle_summary AS
        SELECT 
            p.vehicle_id,
            p.simulated_date AS last_reconciled_date,
            p.gross_earnings AS yesterday_gross_earnings,
            p.total_expenses AS yesterday_expenses,
            p.net_profit AS yesterday_net_profit,
            p.is_unprofitable,
            p.recommendation,
            COALESCE(a.alert_count, 0) AS active_alerts_count
        FROM batch_daily_profitability p
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) AS alert_count 
            FROM speed_vehicle_alerts 
            WHERE resolved = 0 
            GROUP BY vehicle_id
        ) a ON p.vehicle_id = a.vehicle_id;
    """)
    conn.commit()
