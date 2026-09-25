-- Schema initialization for Ride-Hailing Fleet Operations (Lambda Architecture)

-- Speed Layer Tables (Real-time streaming aggregations)
CREATE TABLE IF NOT EXISTS speed_fleet_metrics (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    grid_zone VARCHAR(50) NOT NULL,
    active_vehicles INT DEFAULT 0,
    idle_vehicles INT DEFAULT 0,
    enroute_vehicles INT DEFAULT 0,
    total_trips INT DEFAULT 0,
    total_fare NUMERIC(10, 2) DEFAULT 0.00,
    avg_speed NUMERIC(5, 2) DEFAULT 0.00,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, grid_zone)
);

CREATE TABLE IF NOT EXISTS speed_vehicle_alerts (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL,
    driver_id VARCHAR(50) NOT NULL,
    grid_zone VARCHAR(50) NOT NULL,
    alert_type VARCHAR(50) NOT NULL, -- e.g., 'EXCESSIVE_IDLE', 'SPEED_VIOLATION'
    idle_duration_sec INT DEFAULT 0,
    alert_message TEXT,
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE
);

-- Batch Layer Tables (Historical and daily reconciled extracts)
CREATE TABLE IF NOT EXISTS batch_vehicle_expenses (
    simulated_date DATE NOT NULL,
    vehicle_id VARCHAR(50) NOT NULL,
    fuel_cost NUMERIC(10, 2) NOT NULL,
    maintenance_cost NUMERIC(10, 2) NOT NULL,
    total_expense NUMERIC(10, 2) NOT NULL,
    distance_covered_km NUMERIC(8, 2) NOT NULL,
    service_flag VARCHAR(20) DEFAULT 'NORMAL',
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (simulated_date, vehicle_id)
);

CREATE TABLE IF NOT EXISTS batch_daily_profitability (
    simulated_date DATE NOT NULL,
    vehicle_id VARCHAR(50) NOT NULL,
    total_trips INT DEFAULT 0,
    gross_earnings NUMERIC(10, 2) DEFAULT 0.00,
    fuel_cost NUMERIC(10, 2) DEFAULT 0.00,
    maintenance_cost NUMERIC(10, 2) DEFAULT 0.00,
    total_expenses NUMERIC(10, 2) DEFAULT 0.00,
    net_profit NUMERIC(10, 2) DEFAULT 0.00,
    profit_margin_pct NUMERIC(6, 2) DEFAULT 0.00,
    is_unprofitable BOOLEAN DEFAULT FALSE,
    recommendation VARCHAR(100),
    reconciled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (simulated_date, vehicle_id)
);

-- Serving Layer: Unified View combining Speed and Batch records
CREATE OR REPLACE VIEW v_unified_vehicle_summary AS
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
    WHERE resolved = FALSE 
    GROUP BY vehicle_id
) a ON p.vehicle_id = a.vehicle_id;
