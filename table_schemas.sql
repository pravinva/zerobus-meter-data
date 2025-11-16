-- =============================================================================
-- Unity Catalog Table Schemas for Zerobus Meter Data Demo
-- =============================================================================
-- Run these DDL statements in Databricks SQL Warehouse
-- Target: energy_australia_demo catalog
-- =============================================================================

-- Create catalog (if not exists)
CREATE CATALOG IF NOT EXISTS energy_australia_demo
COMMENT 'Energy Australia - Zerobus Demo Catalog';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS energy_australia_demo.bronze
COMMENT 'Raw ingestion layer - direct from Zerobus';

CREATE SCHEMA IF NOT EXISTS energy_australia_demo.silver
COMMENT 'Cleaned and enriched data';

CREATE SCHEMA IF NOT EXISTS energy_australia_demo.gold
COMMENT 'Business-level aggregations';

-- =============================================================================
-- Bronze Layer: Raw Ingestion Tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Meter Metadata Table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_australia_demo.bronze.meter_metadata (
    meter_id STRING NOT NULL COMMENT 'Unique meter identifier (NMI format)',
    meter_type STRING NOT NULL COMMENT 'Meter type: residential, commercial, industrial',
    location_state STRING COMMENT 'Australian state: NSW, VIC, QLD, WA, SA, TAS, ACT',
    location_suburb STRING COMMENT 'Suburb name',
    location_postcode INT COMMENT 'Australian postcode (2000-6999)',
    has_solar BOOLEAN COMMENT 'Whether meter has solar installed',
    solar_capacity_kw DOUBLE COMMENT 'Solar panel capacity in kW',
    install_date DATE COMMENT 'Meter installation date',
    manufacturer STRING COMMENT 'Meter manufacturer',
    model STRING COMMENT 'Meter model number',
    firmware_version STRING COMMENT 'Firmware version',
    created_at TIMESTAMP COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Smart meter metadata and configuration'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Add constraints
ALTER TABLE energy_australia_demo.bronze.meter_metadata
ADD CONSTRAINT pk_meter_id PRIMARY KEY (meter_id);

ALTER TABLE energy_australia_demo.bronze.meter_metadata
ADD CONSTRAINT chk_meter_type CHECK (meter_type IN ('residential', 'commercial', 'industrial'));

ALTER TABLE energy_australia_demo.bronze.meter_metadata
ADD CONSTRAINT chk_state CHECK (location_state IN ('NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT'));

-- -----------------------------------------------------------------------------
-- Live Meter Readings Table (Time-Series)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_australia_demo.bronze.live_meter_readings (
    meter_id STRING NOT NULL COMMENT 'Meter identifier (foreign key to metadata)',
    timestamp TIMESTAMP NOT NULL COMMENT '5-minute interval timestamp',
    power_kw DOUBLE COMMENT 'Net power in kW (demand - solar generation)',
    voltage_v DOUBLE COMMENT 'Voltage in volts (nominal 240V)',
    current_a DOUBLE COMMENT 'Current in amperes',
    power_factor DOUBLE COMMENT 'Power factor (0.0-1.0)',
    frequency_hz DOUBLE COMMENT 'Grid frequency in Hz (nominal 50Hz)',
    is_valid BOOLEAN COMMENT 'Data quality flag',
    quality_flag STRING COMMENT 'Quality code: OK, MISSING, ANOMALY',
    ingestion_timestamp TIMESTAMP COMMENT 'Zerobus ingestion timestamp'
)
USING DELTA
PARTITIONED BY (DATE(timestamp))
COMMENT 'Real-time meter readings at 5-minute intervals'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 7 days',
    'delta.logRetentionDuration' = 'interval 30 days'
);

-- Add constraints
ALTER TABLE energy_australia_demo.bronze.live_meter_readings
ADD CONSTRAINT chk_quality_flag CHECK (quality_flag IN ('OK', 'MISSING', 'ANOMALY'));

ALTER TABLE energy_australia_demo.bronze.live_meter_readings
ADD CONSTRAINT chk_power_factor CHECK (power_factor >= 0.0 AND power_factor <= 1.0);

-- =============================================================================
-- Silver Layer: Cleaned and Enriched Tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Cleansed Meter Readings (Valid data only)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_australia_demo.silver.meter_readings_clean (
    meter_id STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    power_kw DOUBLE,
    voltage_v DOUBLE,
    current_a DOUBLE,
    power_factor DOUBLE,
    frequency_hz DOUBLE,
    -- Enrichment from metadata
    meter_type STRING,
    location_state STRING,
    location_suburb STRING,
    has_solar BOOLEAN,
    -- Derived fields
    energy_kwh DOUBLE COMMENT 'Energy consumption in interval (kWh)',
    date_partition DATE,
    hour INT,
    is_peak_hour BOOLEAN COMMENT 'Peak demand hour (6-9 AM, 5-10 PM)',
    processed_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (date_partition, location_state)
COMMENT 'Validated and enriched meter readings'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- =============================================================================
-- Gold Layer: Business Aggregations
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Hourly Demand by State
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_australia_demo.gold.hourly_demand_by_state (
    timestamp_hour TIMESTAMP NOT NULL,
    location_state STRING NOT NULL,
    meter_type STRING,
    meter_count INT COMMENT 'Number of active meters',
    total_demand_kw DOUBLE COMMENT 'Total demand in kW',
    avg_demand_kw DOUBLE COMMENT 'Average demand per meter',
    max_demand_kw DOUBLE COMMENT 'Peak demand',
    total_energy_kwh DOUBLE COMMENT 'Total energy consumed',
    solar_export_kwh DOUBLE COMMENT 'Solar energy exported to grid',
    data_quality_pct DOUBLE COMMENT 'Percentage of valid readings',
    processed_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(timestamp_hour), location_state)
COMMENT 'Hourly aggregated demand by state and meter type'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- -----------------------------------------------------------------------------
-- Daily Solar Generation Summary
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_australia_demo.gold.daily_solar_generation (
    date DATE NOT NULL,
    location_state STRING NOT NULL,
    solar_meter_count INT COMMENT 'Number of solar meters',
    total_capacity_kw DOUBLE COMMENT 'Total installed solar capacity',
    total_generation_kwh DOUBLE COMMENT 'Total solar energy generated',
    avg_generation_kwh_per_meter DOUBLE,
    peak_generation_kw DOUBLE COMMENT 'Peak instantaneous generation',
    peak_hour INT COMMENT 'Hour of peak generation',
    capacity_factor DOUBLE COMMENT 'Actual generation / theoretical max',
    processed_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (date)
COMMENT 'Daily solar generation summary'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- =============================================================================
-- Views for Analytics
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Real-Time Demand View (Last 24 Hours)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW energy_australia_demo.silver.v_realtime_demand AS
SELECT
    r.timestamp,
    r.meter_id,
    m.meter_type,
    m.location_state,
    m.location_suburb,
    r.power_kw,
    r.voltage_v,
    r.current_a,
    r.quality_flag,
    m.has_solar,
    m.solar_capacity_kw,
    CASE
        WHEN HOUR(r.timestamp) BETWEEN 6 AND 9 THEN 'Morning Peak'
        WHEN HOUR(r.timestamp) BETWEEN 17 AND 22 THEN 'Evening Peak'
        WHEN HOUR(r.timestamp) BETWEEN 0 AND 5 THEN 'Overnight'
        ELSE 'Mid-Day'
    END AS demand_period
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 24 HOURS
    AND r.is_valid = TRUE;

-- -----------------------------------------------------------------------------
-- State Demand Summary View
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW energy_australia_demo.gold.v_state_demand_summary AS
SELECT
    m.location_state,
    COUNT(DISTINCT r.meter_id) as active_meters,
    ROUND(SUM(r.power_kw), 2) as total_demand_kw,
    ROUND(AVG(r.power_kw), 2) as avg_demand_kw,
    ROUND(MAX(r.power_kw), 2) as peak_demand_kw,
    ROUND(SUM(CASE WHEN m.has_solar AND r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END), 2) as solar_export_kw,
    ROUND(COUNT(CASE WHEN r.quality_flag = 'OK' THEN 1 END) * 100.0 / COUNT(*), 2) as data_quality_pct,
    MAX(r.timestamp) as last_reading_time
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY m.location_state;

-- =============================================================================
-- Indexes and Optimization
-- =============================================================================

-- Create Z-ORDER indexes for common query patterns
OPTIMIZE energy_australia_demo.bronze.live_meter_readings
ZORDER BY (meter_id, timestamp);

OPTIMIZE energy_australia_demo.bronze.meter_metadata
ZORDER BY (meter_id, meter_type, location_state);

-- =============================================================================
-- Grants and Permissions
-- =============================================================================

-- Grant read access to data analysts
GRANT SELECT ON CATALOG energy_australia_demo TO `data_analysts`;

-- Grant write access to data engineers
GRANT ALL PRIVILEGES ON SCHEMA energy_australia_demo.bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA energy_australia_demo.silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA energy_australia_demo.gold TO `data_engineers`;

-- =============================================================================
-- Success Verification
-- =============================================================================

-- Verify tables exist
SHOW TABLES IN energy_australia_demo.bronze;
SHOW TABLES IN energy_australia_demo.silver;
SHOW TABLES IN energy_australia_demo.gold;

-- Show table properties
DESCRIBE EXTENDED energy_australia_demo.bronze.live_meter_readings;
DESCRIBE EXTENDED energy_australia_demo.bronze.meter_metadata;

-- =============================================================================
-- Cleanup (Use with caution!)
-- =============================================================================

-- To drop all tables and start fresh:
-- DROP SCHEMA IF EXISTS energy_australia_demo.bronze CASCADE;
-- DROP SCHEMA IF EXISTS energy_australia_demo.silver CASCADE;
-- DROP SCHEMA IF EXISTS energy_australia_demo.gold CASCADE;
-- DROP CATALOG IF EXISTS energy_australia_demo CASCADE;
