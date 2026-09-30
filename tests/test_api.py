"""Serving-layer API contract tests (SQLite backend via FastAPI TestClient).

Covers the endpoint fixes: bounded pagination, optional date filtering, the
stale-data health rule and the Prometheus ``/metrics`` exposition.
"""

import os
import shutil
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

import serving.api as api_module
from serving.api import app
from storage import db_adapter
from streaming_layer.metrics_writer import write_alerts, write_speed_window


class TestServingApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix="fleet-api-")
        cls._env_backup = {key: os.environ.get(key) for key in ("DB_ENGINE", "SQLITE_DB_PATH")}
        os.environ["DB_ENGINE"] = "sqlite"
        os.environ["SQLITE_DB_PATH"] = os.path.join(cls.tmpdir, "api.sqlite")
        api_module.STALE_DATA_THRESHOLD_SEC = 60
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        for key, value in cls._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        db_adapter.reset_engine_cache()
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    def setUp(self):
        self._fresh_database()
        self.seed(window_end=datetime.now(timezone.utc).replace(microsecond=0))

    def _fresh_database(self):
        # A unique database per test avoids WAL/SHM side files leaking state
        # between tests after the .sqlite file is deleted.
        self.db_path = os.path.join(self.tmpdir, f"api-{uuid.uuid4().hex[:8]}.sqlite")
        os.environ["SQLITE_DB_PATH"] = self.db_path
        db_adapter.reset_engine_cache()

    def seed(self, window_end, batch_date="2026-09-30"):
        engine, conn = db_adapter.get_connection()
        try:
            zones = {
                "Downtown": {"active": 2, "idle": 1, "enroute": 0, "trips": 2,
                             "fare_sum": 30.0, "speed_sum": 90.0, "count": 3},
                "Airport": {"active": 0, "idle": 1, "enroute": 0, "trips": 0,
                            "fare_sum": 0.0, "speed_sum": 0.0, "count": 1},
            }
            write_speed_window(engine, conn, zones, window_end - timedelta(seconds=10), window_end)
            write_alerts(engine, conn, [{
                "vehicle_id": "VEH-100", "driver_id": "DRV-500", "grid_zone": "Downtown",
                "alert_type": "EXCESSIVE_IDLE", "idle_duration_sec": 90, "alert_message": "idle",
            }])
            conn.execute(
                """INSERT INTO batch_vehicle_expenses
                   (simulated_date, vehicle_id, fuel_cost, maintenance_cost, total_expense,
                    distance_covered_km, service_flag)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (batch_date, "VEH-100", 10.0, 5.0, 15.0, 90.0, "NORMAL"),
            )
            conn.execute(
                """INSERT INTO batch_daily_profitability
                   (simulated_date, vehicle_id, total_trips, gross_earnings, fuel_cost,
                    maintenance_cost, total_expenses, net_profit, profit_margin_pct,
                    is_unprofitable, recommendation)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (batch_date, "VEH-100", 5, 120.0, 10.0, 5.0, 15.0, 105.0, 87.5, 0,
                 "OPTIMAL - High Margin Operations"),
            )
            conn.commit()
        finally:
            conn.close()

    def test_root_advertises_metrics_endpoint(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("/metrics", response.json()["endpoints"])

    def test_health_is_healthy_with_fresh_stream(self):
        payload = self.client.get("/health").json()
        self.assertEqual(payload["status"], "HEALTHY")
        self.assertTrue(payload["data_fresh"])
        self.assertEqual(payload["database_engine"], "sqlite")
        self.assertLess(payload["ingest_lag_seconds"], 60)
        self.assertEqual(payload["unresolved_alerts"], 1)

    def test_health_degrades_when_no_recent_window(self):
        self._fresh_database()
        self.seed(window_end=datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=10))
        payload = self.client.get("/health").json()
        self.assertEqual(payload["status"], "DEGRADED", "stalled ingestion must be reported")
        self.assertFalse(payload["data_fresh"])
        self.assertGreater(payload["ingest_lag_seconds"], 60)

    def test_metrics_endpoint_uses_prometheus_format(self):
        response = self.client.get("/metrics")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/plain", response.headers["content-type"])
        body = response.text
        self.assertIn("fleet_stale_data 0", body)
        self.assertIn("fleet_batch_reconciled_rows 1", body)
        self.assertIn("# TYPE fleet_ingest_lag_seconds gauge", body)

    def test_realtime_utilization_reports_latest_window_per_zone(self):
        payload = self.client.get("/api/fleet/realtime-utilization").json()
        summary = payload["summary"]
        self.assertEqual(summary["total_active_on_trip"], 2)
        self.assertEqual(summary["total_idle"], 2)
        self.assertEqual(summary["total_fleet_observed"], 4)
        self.assertAlmostEqual(summary["total_realtime_earnings"], 30.0, places=2)
        self.assertEqual(len(payload["zones"]), 2)

    def test_alert_limit_is_bounded(self):
        self.assertEqual(self.client.get("/api/fleet/alerts?limit=10").status_code, 200)
        self.assertEqual(self.client.get("/api/fleet/alerts?limit=0").status_code, 422)
        self.assertEqual(self.client.get("/api/fleet/alerts?limit=100000").status_code, 422)

    def test_daily_profitability_supports_date_filter(self):
        matched = self.client.get("/api/fleet/daily-profitability?simulated_date=2026-09-30").json()
        self.assertEqual(matched["total_records"], 1)
        self.assertEqual(matched["unprofitable_vehicles_count"], 0)

        empty = self.client.get("/api/fleet/daily-profitability?simulated_date=2020-01-01").json()
        self.assertEqual(empty["total_records"], 0)

    def test_unified_summary_joins_batch_and_speed_layers(self):
        records = self.client.get("/api/fleet/unified-summary").json()["summary_records"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["vehicle_id"], "VEH-100")
        self.assertEqual(records[0]["active_alerts_count"], 1)


if __name__ == "__main__":
    unittest.main()
