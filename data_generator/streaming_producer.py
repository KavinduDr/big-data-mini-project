import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
try:
    import yaml
except ImportError:
    yaml = None

# Structured Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "StreamingProducer", "message": "%(message)s"}'
)
logger = logging.getLogger("StreamingProducer")

CONFIG_PATH = os.getenv("CONFIG_PATH", "config/config.yaml")

def load_config():
    if yaml and os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f)
    return {
        "simulation": {
            "telemetry_interval_sec": 2,
            "num_vehicles": 25,
            "zones": [
                {"name": "Downtown", "lat": 40.7128, "lon": -74.0060, "base_fare": 25.0},
                {"name": "Airport", "lat": 40.6413, "lon": -73.7781, "base_fare": 45.0},
                {"name": "Uptown", "lat": 40.7831, "lon": -73.9712, "base_fare": 20.0},
                {"name": "Suburbs", "lat": 40.8448, "lon": -73.8648, "base_fare": 15.0}
            ]
        },
        "kafka": {
            "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "telemetry_topic": "fleet-telemetry"
        }
    }

def get_kafka_producer(bootstrap_servers, max_retries=15, delay=5):
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
    except ImportError:
        logger.error("kafka-python package not installed.")
        sys.exit(1)

    logger.info(f"Connecting to Kafka at {bootstrap_servers}...")
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3
            )
            logger.info("Successfully connected to Kafka producer broker.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not ready (attempt {attempt}/{max_retries}). Retrying in {delay}s...")
            time.sleep(delay)
    logger.error("Failed to connect to Kafka broker after multiple retries.")
    sys.exit(1)

class FleetSimulator:
    def __init__(self, config):
        self.num_vehicles = config["simulation"].get("num_vehicles", 25)
        self.zones = config["simulation"].get("zones", [])
        self.vehicles = []
        self.statuses = ["idle", "enroute", "on_trip"]
        self._init_fleet()

    def _init_fleet(self):
        for i in range(1, self.num_vehicles + 1):
            zone = random.choice(self.zones)
            self.vehicles.append({
                "vehicle_id": f"VEH-{100 + i}",
                "driver_id": f"DRV-{500 + i}",
                "current_zone": zone["name"],
                "lat": zone["lat"] + random.uniform(-0.015, 0.015),
                "lon": zone["lon"] + random.uniform(-0.015, 0.015),
                "status": random.choice(self.statuses),
                "current_trip_id": f"TRIP-{random.randint(10000, 99999)}",
                "idle_duration": 0,
                "fare_accumulated": round(random.uniform(5.0, 30.0), 2)
            })

    def generate_event(self, vehicle):
        # Update vehicle state
        status_transition_roll = random.random()
        if status_transition_roll < 0.25:
            if vehicle["status"] == "idle":
                vehicle["status"] = "enroute"
                vehicle["idle_duration"] = 0
                vehicle["current_trip_id"] = f"TRIP-{random.randint(10000, 99999)}"
            elif vehicle["status"] == "enroute":
                vehicle["status"] = "on_trip"
            elif vehicle["status"] == "on_trip":
                vehicle["status"] = "idle"
                vehicle["fare_accumulated"] = 0.0

        if vehicle["status"] == "idle":
            vehicle["speed"] = 0.0
            vehicle["idle_duration"] += 2
            fare = 0.0
        elif vehicle["status"] == "enroute":
            vehicle["speed"] = round(random.uniform(20.0, 50.0), 1)
            vehicle["idle_duration"] = 0
            fare = 0.0
        else:  # on_trip
            vehicle["speed"] = round(random.uniform(15.0, 65.0), 1)
            vehicle["idle_duration"] = 0
            fare_increment = round(random.uniform(0.5, 2.5), 2)
            vehicle["fare_accumulated"] = round(vehicle["fare_accumulated"] + fare_increment, 2)
            fare = vehicle["fare_accumulated"]

        # Small GPS drift within zone
        zone = next((z for z in self.zones if z["name"] == vehicle["current_zone"]), self.zones[0])
        vehicle["lat"] = round(zone["lat"] + random.uniform(-0.02, 0.02), 6)
        vehicle["lon"] = round(zone["lon"] + random.uniform(-0.02, 0.02), 6)

        # Zone change chance
        if random.random() < 0.05:
            new_zone = random.choice(self.zones)
            vehicle["current_zone"] = new_zone["name"]

        return {
            "trip_id": vehicle["current_trip_id"],
            "driver_id": vehicle["driver_id"],
            "vehicle_id": vehicle["vehicle_id"],
            "grid_zone": vehicle["current_zone"],
            "lat": vehicle["lat"],
            "lon": vehicle["lon"],
            "speed": vehicle["speed"],
            "status": vehicle["status"],
            "idle_duration_sec": vehicle["idle_duration"],
            "fare": fare,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

def main():
    config = load_config()
    bootstrap_servers = config["kafka"].get("bootstrap_servers", "kafka:9092")
    topic = config["kafka"].get("telemetry_topic", "fleet-telemetry")
    interval = config["simulation"].get("telemetry_interval_sec", 2)

    producer = get_kafka_producer(bootstrap_servers)
    simulator = FleetSimulator(config)

    logger.info(f"Starting real-time streaming telemetry producer to topic '{topic}'...")

    raw_archive_dir = "/app/data_lake/raw_telemetry"
    os.makedirs(raw_archive_dir, exist_ok=True)

    event_count = 0
    while True:
        try:
            batch_raw_records = []
            for vehicle in simulator.vehicles:
                event = simulator.generate_event(vehicle)
                # Send to Kafka with vehicle_id as partition key to ensure partition ordering
                producer.send(topic, key=vehicle["vehicle_id"].encode("utf-8"), value=event)
                batch_raw_records.append(event)
                event_count += 1

            producer.flush()

            # Lambda Architecture Raw Log: append to raw storage archive for Batch Layer
            today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
            raw_file_path = os.path.join(raw_archive_dir, f"telemetry_{today_str}.jsonl")
            with open(raw_file_path, "a") as f:
                for rec in batch_raw_records:
                    f.write(json.dumps(rec) + "\n")

            if event_count % (len(simulator.vehicles) * 10) == 0:
                logger.info(f"Emitted {event_count} telemetry events to Kafka topic '{topic}'.")

            time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Stopping telemetry producer gracefully.")
            break
        except Exception as e:
            logger.error(f"Error producing telemetry event: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
