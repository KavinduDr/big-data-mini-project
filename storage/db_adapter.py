import os
import sqlite3
import logging

logger = logging.getLogger("DBAdapter")

DB_ENGINE = os.getenv("DB_ENGINE", "auto")  # 'postgres', 'sqlite', or 'auto'
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "fleet_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgrespassword")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", os.path.join(os.path.dirname(__file__), "fleet_db.sqlite"))

def get_connection():
    """Returns a DB connection (PostgreSQL if available, otherwise SQLite)."""
    if DB_ENGINE != "sqlite":
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=2
            )
            conn.autocommit = True
            return "postgres", conn
        except Exception:
            if DB_ENGINE == "postgres":
                raise
    
    # Fallback to embedded SQLite
    os.makedirs(os.path.dirname(os.path.abspath(SQLITE_DB_PATH)), exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    init_sqlite_schema(conn)
    return "sqlite", conn

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
