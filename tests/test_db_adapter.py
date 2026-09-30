"""Tests for the dual-engine storage adapter (SQLite path).

Covers the standalone-mode fixes: schema bootstrap, WAL/busy-timeout pragmas
(which stop "database is locked" errors when producer, speed, batch and API
threads write concurrently) and idempotent upserts.
"""

import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from storage import db_adapter
from streaming_layer.metrics_writer import write_alerts, write_speed_window


class TestSqliteAdapter(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="fleet-db-")
        self._env_backup = {key: os.environ.get(key) for key in ("DB_ENGINE", "SQLITE_DB_PATH")}
        os.environ["DB_ENGINE"] = "sqlite"
        os.environ["SQLITE_DB_PATH"] = os.path.join(self.tmpdir, "adapter.sqlite")
        db_adapter.reset_engine_cache()

    def tearDown(self):
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        db_adapter.reset_engine_cache()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_connects_to_sqlite_with_wal_and_schema(self):
        engine, conn = db_adapter.get_connection()
        try:
            self.assertEqual(engine, "sqlite")
            journal_mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            self.assertEqual(journal_mode.lower(), "wal")
            busy_timeout = conn.execute("PRAGMA busy_timeout;").fetchone()[0]
            self.assertEqual(busy_timeout, 10000)

            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';")}
            for expected in (
                "speed_fleet_metrics",
                "speed_vehicle_alerts",
                "batch_vehicle_expenses",
                "batch_daily_profitability",
            ):
                self.assertIn(expected, tables)

            indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index';")}
            self.assertIn("idx_speed_metrics_window_end", indexes)
        finally:
            conn.close()

    def test_window_upsert_is_idempotent(self):
        start = datetime(2026, 9, 30, 10, 0, tzinfo=timezone.utc)
        end = start + timedelta(seconds=10)
        zones = {
            "Downtown": {
                "active": 1, "idle": 2, "enroute": 0, "trips": 1,
                "fare_sum": 12.5, "speed_sum": 60.0, "count": 3,
            }
        }

        engine, conn = db_adapter.get_connection()
        try:
            self.assertEqual(write_speed_window(engine, conn, zones, start, end), 1)
            self.assertEqual(write_speed_window(engine, conn, zones, start, end), 1)

            row_count = conn.execute("SELECT COUNT(*) FROM speed_fleet_metrics;").fetchone()[0]
            self.assertEqual(row_count, 1, "same window + zone must upsert, not duplicate")
            avg_speed = conn.execute("SELECT avg_speed FROM speed_fleet_metrics;").fetchone()[0]
            self.assertAlmostEqual(avg_speed, 20.0, places=2)
        finally:
            conn.close()

    def test_alerts_persist_with_safe_defaults(self):
        engine, conn = db_adapter.get_connection()
        try:
            written = write_alerts(engine, conn, [{
                "vehicle_id": "VEH-100",
                "grid_zone": "Downtown",
                "idle_duration_sec": 75,
                "alert_message": "idle too long",
            }])
            self.assertEqual(written, 1)
            row = conn.execute(
                "SELECT vehicle_id, driver_id, alert_type, resolved FROM speed_vehicle_alerts;"
            ).fetchone()
            self.assertEqual(row["driver_id"], "UNKNOWN")
            self.assertEqual(row["alert_type"], "EXCESSIVE_IDLE")
            self.assertEqual(row["resolved"], 0)
        finally:
            conn.close()

    def test_reconnecting_reinitialises_schema_safely(self):
        db_adapter.get_connection()[1].close()
        engine, conn = db_adapter.get_connection()
        try:
            self.assertEqual(engine, "sqlite")
            rows = conn.execute("SELECT COUNT(*) AS count FROM v_unified_vehicle_summary;").fetchone()
            self.assertEqual(rows["count"], 0)
        finally:
            conn.close()


class TestPostgresStatementTemplates(unittest.TestCase):
    """Regression guard for the PostgreSQL bulk-insert statements.

    ``execute_values`` expands one placeholder per row, so a statement whose
    column list contains a database-supplied column (``CURRENT_TIMESTAMP``) must
    pass a matching ``template``. Without it PostgreSQL rejects the insert with
    "INSERT has more target columns than expressions" - a defect that shipped in
    the original batch layer and could not be caught by the SQLite tests.
    """

    def test_metric_statement_and_template_agree(self):
        from streaming_layer import metrics_writer

        self._assert_statement_matches_template(
            metrics_writer.POSTGRES_METRIC_SQL, metrics_writer.POSTGRES_METRIC_TEMPLATE
        )

    def test_batch_statement_and_template_agree(self):
        from batch_layer import batch_processor

        self._assert_statement_matches_template(
            batch_processor.POSTGRES_EXPENSE_SQL, batch_processor.POSTGRES_EXPENSE_TEMPLATE
        )
        self._assert_statement_matches_template(
            batch_processor.POSTGRES_PROFIT_SQL, batch_processor.POSTGRES_PROFIT_TEMPLATE
        )

    def _assert_statement_matches_template(self, sql, template):
        column_list = sql.split("(", 1)[1].split(")", 1)[0]
        columns = [column.strip() for column in column_list.split(",") if column.strip()]
        self.assertIn("VALUES %s", sql)

        supplied_by_database = template.count("CURRENT_TIMESTAMP")
        placeholders = template.count("%s")
        self.assertEqual(
            placeholders + supplied_by_database,
            len(columns),
            f"template {template!r} does not cover the {len(columns)} columns of {column_list!r}",
        )


if __name__ == "__main__":
    unittest.main()
