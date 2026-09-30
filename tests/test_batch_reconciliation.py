"""Tests for the batch layer.

Unlike the previous placeholder test (which re-implemented the arithmetic and
therefore asserted nothing about the codebase), these tests exercise the real
production code paths:

* ``compute_profitability`` - the business rule applied to every vehicle;
* ``summarize_telemetry_earnings`` - the date-scoped join against the raw
  telemetry lake, including cumulative-fare handling;
* ``run_batch_reconciliation`` - the end-to-end job against SQLite + Parquet.
"""

import json
import os
import sqlite3
import tempfile
import unittest

import pandas as pd

from batch_layer.batch_processor import (
    compute_profitability,
    run_batch_reconciliation,
    summarize_telemetry_earnings,
)
from storage import db_adapter

DAY = "2026-09-30"
NEXT_DAY = "2026-10-01"


def telemetry_record(vehicle_id, trip_id, fare, status="on_trip", day=DAY, zone="Downtown"):
    return {
        "vehicle_id": vehicle_id,
        "driver_id": f"DRV-{vehicle_id[-3:]}",
        "trip_id": trip_id,
        "grid_zone": zone,
        "status": status,
        "fare": fare,
        "speed": 30.0,
        "idle_duration_sec": 0,
        "timestamp": f"{day}T04:00:00+00:00",
    }


def write_jsonl(path, records, trailing_garbage=False):
    with open(path, "a") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")
        if trailing_garbage:
            handle.write("{not-json}\n")


class TestProfitabilityRules(unittest.TestCase):
    def test_profitable_vehicle(self):
        metrics = compute_profitability(150.00, 45.00, 25.00)
        self.assertEqual(metrics["net_profit"], 80.00)
        self.assertEqual(metrics["total_expenses"], 70.00)
        self.assertEqual(metrics["profit_margin_pct"], 53.33)
        self.assertFalse(metrics["is_unprofitable"])
        self.assertTrue(metrics["recommendation"].startswith("OPTIMAL"))

    def test_high_maintenance_loss_makes_vehicle_grounded(self):
        metrics = compute_profitability(95.00, 55.00, 90.00)
        self.assertEqual(metrics["net_profit"], -50.00)
        self.assertLess(metrics["profit_margin_pct"], 0)
        self.assertTrue(metrics["is_unprofitable"])
        self.assertTrue(metrics["recommendation"].startswith("GROUND VEHICLE"))

    def test_fuel_heavy_vehicle_is_flagged_for_inspection(self):
        metrics = compute_profitability(120.00, 100.00, 10.00)
        self.assertEqual(metrics["net_profit"], 10.00)
        self.assertFalse(metrics["is_unprofitable"])
        self.assertTrue(metrics["recommendation"].startswith("INSPECT ENGINE"))

    def test_unprofitable_without_maintenance_is_reallocation_candidate(self):
        metrics = compute_profitability(40.00, 20.00, 30.00)
        self.assertEqual(metrics["net_profit"], -10.00)
        self.assertTrue(metrics["recommendation"].startswith("UNDER-UTILIZED"))

    def test_zero_revenue_vehicle(self):
        metrics = compute_profitability(0.0, 10.0, 0.0)
        self.assertEqual(metrics["profit_margin_pct"], -100.0)
        self.assertTrue(metrics["is_unprofitable"])
        self.assertEqual(metrics["total_trips"], 0)


class TestTelemetrySummarization(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="fleet-telemetry-")

    def test_cumulative_fare_is_taken_once_per_trip(self):
        # `fare` is the running total of the trip, re-emitted every heartbeat.
        write_jsonl(
            os.path.join(self.tmpdir, f"telemetry_{DAY.replace('-', '')}.jsonl"),
            [
                telemetry_record("VEH-100", "TRIP-1", 5.0),
                telemetry_record("VEH-100", "TRIP-1", 9.0),
                telemetry_record("VEH-100", "TRIP-1", 14.0),
                telemetry_record("VEH-100", "TRIP-2", 6.0),
                # Enroute heartbeats carry fare 0 and must not create trips.
                telemetry_record("VEH-100", "TRIP-3", 0.0, status="enroute"),
            ],
        )

        summary = summarize_telemetry_earnings(self.tmpdir)
        self.assertEqual(summary[DAY]["VEH-100"]["trips"], 2)
        self.assertAlmostEqual(summary[DAY]["VEH-100"]["gross_earnings"], 20.0, places=2)

    def test_records_are_scoped_to_their_own_simulated_day(self):
        write_jsonl(
            os.path.join(self.tmpdir, f"telemetry_{DAY.replace('-', '')}.jsonl"),
            [telemetry_record("VEH-100", "TRIP-1", 14.0, day=DAY)],
        )
        write_jsonl(
            os.path.join(self.tmpdir, f"telemetry_{NEXT_DAY.replace('-', '')}.jsonl"),
            [telemetry_record("VEH-100", "TRIP-2", 30.0, day=NEXT_DAY)],
        )

        summary = summarize_telemetry_earnings(self.tmpdir)
        self.assertAlmostEqual(summary[DAY]["VEH-100"]["gross_earnings"], 14.0, places=2)
        self.assertAlmostEqual(summary[NEXT_DAY]["VEH-100"]["gross_earnings"], 30.0, places=2)

        filtered = summarize_telemetry_earnings(self.tmpdir, target_dates=[DAY])
        self.assertEqual(set(filtered), {DAY})

    def test_malformed_lines_are_skipped(self):
        write_jsonl(
            os.path.join(self.tmpdir, f"telemetry_{DAY.replace('-', '')}.jsonl"),
            [telemetry_record("VEH-100", "TRIP-1", 12.0)],
            trailing_garbage=True,
        )
        summary = summarize_telemetry_earnings(self.tmpdir)
        self.assertAlmostEqual(summary[DAY]["VEH-100"]["gross_earnings"], 12.0, places=2)

    def test_missing_lake_returns_empty_summary(self):
        self.assertEqual(summarize_telemetry_earnings(os.path.join(self.tmpdir, "nope")), {})


class TestReconciliationEndToEnd(unittest.TestCase):
    """Runs the real job against a temporary SQLite database and Parquet folder."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="fleet-batch-")
        self.drop_dir = os.path.join(self.tmpdir, "daily_expenses")
        self.raw_dir = os.path.join(self.tmpdir, "raw_telemetry")
        self.parquet_dir = os.path.join(self.tmpdir, "reconciled_reports")
        for directory in (self.drop_dir, self.raw_dir, self.parquet_dir):
            os.makedirs(directory, exist_ok=True)

        self._env_backup = {key: os.environ.get(key) for key in ("DB_ENGINE", "SQLITE_DB_PATH")}
        os.environ["DB_ENGINE"] = "sqlite"
        os.environ["SQLITE_DB_PATH"] = os.path.join(self.tmpdir, "test_fleet.sqlite")
        db_adapter.reset_engine_cache()

        pd.DataFrame([
            {"simulated_date": DAY, "vehicle_id": "VEH-100", "fuel_cost": 20.0, "maintenance_cost": 10.0,
             "total_expense": 30.0, "distance_covered_km": 100.0, "service_flag": "NORMAL"},
            {"simulated_date": DAY, "vehicle_id": "VEH-101", "fuel_cost": 60.0, "maintenance_cost": 120.0,
             "total_expense": 180.0, "distance_covered_km": 90.0, "service_flag": "TIRE_REPLACE"},
        ]).to_csv(os.path.join(self.drop_dir, f"vehicle_expenses_{DAY}.csv"), index=False)

        write_jsonl(
            os.path.join(self.raw_dir, f"telemetry_{DAY.replace('-', '')}.jsonl"),
            [
                telemetry_record("VEH-100", "TRIP-1", 40.0),
                telemetry_record("VEH-100", "TRIP-2", 30.0),
                telemetry_record("VEH-101", "TRIP-9", 80.0),
            ],
        )

        self.summary = self.run_job()

    def tearDown(self):
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        db_adapter.reset_engine_cache()

    def run_job(self, **overrides):
        kwargs = {
            "data_drop_dir": self.drop_dir,
            "raw_telemetry_dir": self.raw_dir,
            "parquet_output_dir": self.parquet_dir,
        }
        kwargs.update(overrides)
        return run_batch_reconciliation(**kwargs)

    def _query(self, sql):
        conn = sqlite3.connect(os.environ["SQLITE_DB_PATH"])
        conn.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in conn.execute(sql).fetchall()]
        finally:
            conn.close()

    def test_summary_reports_profitability(self):
        self.assertEqual(self.summary["reconciled_records"], 2)
        self.assertEqual(self.summary["dates_processed"], [DAY])
        self.assertEqual(self.summary["unprofitable_vehicles"], 1)
        self.assertAlmostEqual(self.summary["total_net_profit"], -60.0, places=2)
        self.assertEqual(self.summary["fallback_revenue_vehicles"], 0)

    def test_rows_persisted_in_both_tables(self):
        profitability = {row["vehicle_id"]: row for row in self._query("SELECT * FROM batch_daily_profitability;")}
        self.assertEqual(len(profitability), 2)
        self.assertAlmostEqual(profitability["VEH-100"]["net_profit"], 40.0, places=2)
        self.assertAlmostEqual(profitability["VEH-101"]["net_profit"], -100.0, places=2)
        self.assertEqual(profitability["VEH-101"]["is_unprofitable"], 1)
        self.assertTrue(profitability["VEH-101"]["recommendation"].startswith("GROUND VEHICLE"))
        # The expense drop is now persisted too (it was a dead table before).
        expenses = self._query("SELECT * FROM batch_vehicle_expenses;")
        self.assertEqual(len(expenses), 2)

    def test_parquet_cold_storage_written_per_day(self):
        parquet_path = os.path.join(self.parquet_dir, f"reconciled_{DAY}.parquet")
        self.assertTrue(os.path.exists(parquet_path))
        stored = pd.read_parquet(parquet_path)
        self.assertEqual(len(stored), 2)
        self.assertIn("net_profit", stored.columns)

    def test_second_run_skips_already_reconciled_dates(self):
        second = self.run_job()
        self.assertEqual(second["reconciled_records"], 0)
        self.assertEqual(second["dates_skipped"], [DAY])
        rows = self._query("SELECT COUNT(*) AS count FROM batch_daily_profitability;")
        self.assertEqual(rows[0]["count"], 2)

    def test_forced_rerun_upserts_without_duplicating(self):
        forced = self.run_job(skip_existing=False)
        self.assertEqual(forced["reconciled_records"], 2)
        rows = self._query("SELECT COUNT(*) AS count FROM batch_daily_profitability;")
        self.assertEqual(rows[0]["count"], 2)

    def test_missing_drop_folder_is_reported_not_raised(self):
        summary = self.run_job(data_drop_dir=os.path.join(self.tmpdir, "does-not-exist"))
        self.assertEqual(summary["reconciled_records"], 0)
        self.assertEqual(summary["expense_files"], 0)


if __name__ == "__main__":
    unittest.main()

