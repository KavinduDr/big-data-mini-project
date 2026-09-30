import unittest
from unittest import mock

from data_generator.streaming_producer import FleetSimulator

class TestFleetSimulator(unittest.TestCase):
    def setUp(self):
        self.mock_config = {
            "simulation": {
                "telemetry_interval_sec": 2,
                "num_vehicles": 5,
                "zones": [
                    {"name": "Downtown", "lat": 40.7128, "lon": -74.0060, "base_fare": 25.0},
                    {"name": "Airport", "lat": 40.6413, "lon": -73.7781, "base_fare": 45.0}
                ]
            }
        }

    def test_simulator_initialization(self):
        sim = FleetSimulator(self.mock_config)
        self.assertEqual(len(sim.vehicles), 5)
        for v in sim.vehicles:
            self.assertTrue(v["vehicle_id"].startswith("VEH-"))
            self.assertTrue(v["driver_id"].startswith("DRV-"))
            self.assertIn(v["current_zone"], ["Downtown", "Airport"])

    def test_telemetry_event_schema(self):
        sim = FleetSimulator(self.mock_config)
        vehicle = sim.vehicles[0]
        event = sim.generate_event(vehicle)

        required_keys = [
            "trip_id", "driver_id", "vehicle_id", "grid_zone",
            "lat", "lon", "speed", "status", "idle_duration_sec", "fare", "timestamp"
        ]
        for key in required_keys:
            self.assertIn(key, event, f"Missing key {key} in telemetry event")

        self.assertIn(event["status"], ["idle", "enroute", "on_trip"])
        self.assertIsInstance(event["lat"], float)
        self.assertIsInstance(event["lon"], float)
        self.assertGreaterEqual(event["fare"], 0.0)

    def test_idle_duration_follows_configured_telemetry_interval(self):
        config = {
            "simulation": {
                "telemetry_interval_sec": 5,
                "num_vehicles": 2,
                "zones": [{"name": "Downtown", "lat": 40.7128, "lon": -74.0060, "base_fare": 25.0}],
            }
        }
        simulator = FleetSimulator(config)
        vehicle = simulator.vehicles[0]
        vehicle.update(status="idle", idle_duration=0, fare_accumulated=0.0)

        with mock.patch("random.random", return_value=0.9):  # no status transition
            event = simulator.generate_event(vehicle)

        self.assertEqual(event["status"], "idle")
        self.assertEqual(event["idle_duration_sec"], 5)
        self.assertIsInstance(event["idle_duration_sec"], int)

    def test_new_trip_starts_with_zero_accumulated_fare(self):
        simulator = FleetSimulator(self.mock_config)
        vehicle = simulator.vehicles[0]
        vehicle.update(status="idle", idle_duration=0, fare_accumulated=42.0, current_trip_id="TRIP-OLD")

        with mock.patch("random.random", return_value=0.0):  # force idle -> enroute
            event = simulator.generate_event(vehicle)

        self.assertEqual(event["status"], "enroute")
        self.assertEqual(vehicle["fare_accumulated"], 0.0)
        self.assertEqual(event["fare"], 0.0)
        self.assertNotEqual(event["trip_id"], "TRIP-OLD")

if __name__ == '__main__':
    unittest.main()
