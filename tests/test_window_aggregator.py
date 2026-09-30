"""Unit tests for the shared speed-layer windowing engine.

These lock in the two metric-correctness fixes:
* distinct vehicles are counted (not heartbeats), so zone totals never exceed
  the fleet size and the idle ratio is meaningful;
* cumulative trip fare is booked once as a delta, not re-counted on every
  heartbeat.
"""

import unittest

from streaming_layer.window_aggregator import FleetWindowAggregator


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def telemetry(vehicle_index, status="on_trip", fare=0.0, trip_id=None, zone="Downtown", idle=0, speed=30.0):
    return {
        "vehicle_id": f"VEH-{100 + vehicle_index}",
        "driver_id": f"DRV-{500 + vehicle_index}",
        "grid_zone": zone,
        "trip_id": trip_id or f"TRIP-{vehicle_index}",
        "status": status,
        "speed": speed,
        "fare": fare,
        "idle_duration_sec": idle,
        "timestamp": "2026-09-30T10:00:00+00:00",
    }


class TestFleetWindowAggregator(unittest.TestCase):
    def test_counts_distinct_vehicles_not_heartbeats(self):
        aggregator = FleetWindowAggregator()
        # 25 vehicles emit 5 heartbeats each inside the same window.
        for _tick in range(5):
            for index in range(25):
                status = "on_trip" if index < 5 else "idle"
                aggregator.add_event(telemetry(index, status=status))

        zones = aggregator.flush()
        downtown = zones["Downtown"]
        self.assertEqual(downtown["active"], 5)
        self.assertEqual(downtown["idle"], 20)
        observed_fleet = downtown["active"] + downtown["idle"] + downtown["enroute"]
        self.assertEqual(observed_fleet, 25, "fleet totals must never exceed the real fleet size")

    def test_cumulative_fare_is_booked_as_delta_only(self):
        aggregator = FleetWindowAggregator()
        aggregator.add_event(telemetry(1, fare=10.0, trip_id="TRIP-X"))
        aggregator.add_event(telemetry(1, fare=12.0, trip_id="TRIP-X"))
        aggregator.add_event(telemetry(1, fare=15.0, trip_id="TRIP-X"))

        self.assertAlmostEqual(aggregator.flush()["Downtown"]["fare_sum"], 15.0, places=2)

        # Same trip continues into the next window: only the increment is booked.
        aggregator.add_event(telemetry(1, fare=18.0, trip_id="TRIP-X"))
        self.assertAlmostEqual(aggregator.flush()["Downtown"]["fare_sum"], 3.0, places=2)

    def test_fare_watermark_released_when_vehicle_starts_new_trip(self):
        aggregator = FleetWindowAggregator()
        aggregator.add_event(telemetry(1, fare=20.0, trip_id="TRIP-A"))
        aggregator.flush()

        aggregator.add_event(telemetry(1, fare=7.0, trip_id="TRIP-B"))
        zones = aggregator.flush()
        self.assertAlmostEqual(zones["Downtown"]["fare_sum"], 7.0, places=2)

    def test_idle_alert_respects_threshold_and_cooldown(self):
        clock = FakeClock()
        aggregator = FleetWindowAggregator(idle_threshold_sec=60, alert_cooldown_sec=60, clock=clock)

        self.assertIsNone(aggregator.add_event(telemetry(1, status="idle", idle=30)))

        alert = aggregator.add_event(telemetry(1, status="idle", idle=60))
        self.assertIsNotNone(alert)
        self.assertEqual(alert["alert_type"], "EXCESSIVE_IDLE")
        self.assertEqual(alert["vehicle_id"], "VEH-101")
        self.assertIn("VEH-101", alert["alert_message"])

        self.assertIsNone(aggregator.add_event(telemetry(1, status="idle", idle=90)), "cooldown must suppress duplicates")

        clock.advance(61)
        self.assertIsNotNone(aggregator.add_event(telemetry(1, status="idle", idle=120)))
        self.assertEqual(aggregator.alerts_triggered, 2)

    def test_invalid_events_are_counted_and_skipped(self):
        aggregator = FleetWindowAggregator()
        self.assertIsNone(aggregator.add_event({"grid_zone": "Downtown"}))
        self.assertIsNone(aggregator.add_event(telemetry(1, speed="fast")))
        self.assertEqual(aggregator.invalid_events, 2)
        self.assertEqual(aggregator.events_processed, 0)
        self.assertEqual(aggregator.flush(), {})

    def test_zone_change_uses_latest_observed_zone(self):
        aggregator = FleetWindowAggregator()
        aggregator.add_event(telemetry(1, zone="Downtown", speed=10.0))
        aggregator.add_event(telemetry(1, zone="Airport", speed=50.0, trip_id="TRIP-1"))

        zones = aggregator.flush()
        self.assertNotIn("Downtown", zones)
        self.assertEqual(zones["Airport"]["active"], 1)
        self.assertEqual(zones["Airport"]["trips"], 1)
        self.assertAlmostEqual(zones["Airport"]["speed_sum"], 50.0, places=2)

    def test_flush_resets_window_state(self):
        aggregator = FleetWindowAggregator()
        aggregator.add_event(telemetry(1, fare=5.0))
        aggregator.flush()
        self.assertEqual(aggregator.flush(), {}, "flushing twice must not duplicate the window")


if __name__ == "__main__":
    unittest.main()
