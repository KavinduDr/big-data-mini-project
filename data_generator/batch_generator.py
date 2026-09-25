import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
import yaml
import pandas as pd

# Structured Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "component": "BatchExpenseGenerator", "message": "%(message)s"}'
)
logger = logging.getLogger("BatchExpenseGenerator")

CONFIG_PATH = os.getenv("CONFIG_PATH", "config/config.yaml")

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "r") as f:
            return yaml.safe_load(f)
    return {
        "simulation": {
            "day_duration_sec": 300,
            "num_vehicles": 25
        },
        "batch": {
            "data_drop_dir": "/app/data_lake/daily_expenses"
        }
    }

def generate_daily_expense_file(simulated_date_str, num_vehicles, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    records = []

    for i in range(1, num_vehicles + 1):
        vehicle_id = f"VEH-{100 + i}"
        distance_km = round(random.uniform(80.0, 320.0), 2)
        
        # Calculate realistic fuel cost ($0.15 - $0.22 per km)
        fuel_rate = random.uniform(0.15, 0.22)
        fuel_cost = round(distance_km * fuel_rate, 2)

        # Maintenance cost: some vehicles have major maintenance or repair requirements
        is_high_maintenance = (random.random() < 0.20)  # 20% of vehicles undergo service/repairs
        if is_high_maintenance:
            maintenance_cost = round(random.uniform(70.0, 180.0), 2)
            service_flag = random.choice(["TIRE_REPLACE", "ENGINE_TUNE", "BRAKE_SERVICE"])
        else:
            maintenance_cost = round(random.uniform(5.0, 25.0), 2)
            service_flag = "NORMAL"

        total_expense = round(fuel_cost + maintenance_cost, 2)

        records.append({
            "simulated_date": simulated_date_str,
            "vehicle_id": vehicle_id,
            "fuel_cost": fuel_cost,
            "maintenance_cost": maintenance_cost,
            "total_expense": total_expense,
            "distance_covered_km": distance_km,
            "service_flag": service_flag,
            "generated_at": datetime.now(timezone.utc).isoformat()
        })

    # Save as CSV & JSON in data lake drop folder
    csv_file = os.path.join(output_dir, f"vehicle_expenses_{simulated_date_str}.csv")
    json_file = os.path.join(output_dir, f"vehicle_expenses_{simulated_date_str}.json")

    df = pd.DataFrame(records)
    df.to_csv(csv_file, index=False)
    with open(json_file, "w") as f:
        json.dump(records, f, indent=2)

    logger.info(f"Generated daily batch expense file: {csv_file} ({len(records)} vehicles recorded).")
    return csv_file

def main():
    config = load_config()
    day_duration = config["simulation"].get("day_duration_sec", 300)
    num_vehicles = config["simulation"].get("num_vehicles", 25)
    output_dir = config["batch"].get("data_drop_dir", "/app/data_lake/daily_expenses")

    logger.info(f"Starting Batch Expense Generator. Simulated day = {day_duration}s. Target dir: {output_dir}")

    # Generate initial day 0 baseline file right away so pipelines have immediate data to process
    current_simulated_date = datetime.now(timezone.utc).date()
    generate_daily_expense_file(str(current_simulated_date), num_vehicles, output_dir)

    day_counter = 1
    while True:
        try:
            logger.info(f"Waiting for next simulated day ({day_duration}s)...")
            time.sleep(day_duration)
            next_date = current_simulated_date + timedelta(days=day_counter)
            generate_daily_expense_file(str(next_date), num_vehicles, output_dir)
            day_counter += 1
        except KeyboardInterrupt:
            logger.info("Stopping batch generator gracefully.")
            break
        except Exception as e:
            logger.error(f"Error during batch generation: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
