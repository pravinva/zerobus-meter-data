-- =============================================================================
-- Example Analytics Queries for Zerobus Meter Data Demo
-- =============================================================================
-- These queries demonstrate real-time analytics on 200M+ meter readings
-- Expected query latency: 1-10 seconds with Photon acceleration
-- =============================================================================

-- =============================================================================
-- 1. BASIC VERIFICATION QUERIES
-- =============================================================================

-- Count total records ingested
SELECT COUNT(*) as total_records
FROM energy_australia_demo.bronze.live_meter_readings;
-- Expected: ~201,600,000 for full demo (100K meters × 7 days × 288 readings/day)

-- Count unique meters
SELECT COUNT(DISTINCT meter_id) as unique_meters
FROM energy_australia_demo.bronze.live_meter_readings;
-- Expected: 100,000

-- Date range of data
SELECT
    MIN(timestamp) as earliest_reading,
    MAX(timestamp) as latest_reading,
    DATEDIFF(MAX(timestamp), MIN(timestamp)) as days_of_data
FROM energy_australia_demo.bronze.live_meter_readings;

-- Data quality overview
SELECT
    quality_flag,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM energy_australia_demo.bronze.live_meter_readings
GROUP BY quality_flag
ORDER BY count DESC;
-- Expected: ~98% OK, ~1% MISSING, ~1% ANOMALY

-- =============================================================================
-- 2. REAL-TIME DEMAND MONITORING (< 5 SEC LATENCY)
-- =============================================================================

-- Current demand by state (last 1 hour)
SELECT
    m.location_state,
    COUNT(DISTINCT r.meter_id) as active_meters,
    ROUND(SUM(r.power_kw), 2) as total_demand_kw,
    ROUND(AVG(r.power_kw), 2) as avg_demand_kw_per_meter,
    ROUND(MAX(r.power_kw), 2) as peak_demand_kw,
    MAX(r.timestamp) as last_update
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 1 HOUR
    AND r.is_valid = TRUE
GROUP BY m.location_state
ORDER BY total_demand_kw DESC;

-- Demand by meter type (residential vs commercial vs industrial)
SELECT
    m.meter_type,
    COUNT(DISTINCT r.meter_id) as meters,
    ROUND(SUM(r.power_kw), 2) as total_demand_kw,
    ROUND(AVG(r.power_kw), 3) as avg_demand_kw
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 1 HOUR
    AND r.is_valid = TRUE
GROUP BY m.meter_type
ORDER BY total_demand_kw DESC;

-- Real-time voltage quality monitoring
SELECT
    CASE
        WHEN voltage_v < 228 THEN 'Low (< 228V)'
        WHEN voltage_v > 252 THEN 'High (> 252V)'
        ELSE 'Normal (228-252V)'
    END AS voltage_range,
    COUNT(*) as reading_count,
    ROUND(AVG(voltage_v), 2) as avg_voltage,
    ROUND(MIN(voltage_v), 2) as min_voltage,
    ROUND(MAX(voltage_v), 2) as max_voltage
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
    AND is_valid = TRUE
GROUP BY
    CASE
        WHEN voltage_v < 228 THEN 'Low (< 228V)'
        WHEN voltage_v > 252 THEN 'High (> 252V)'
        ELSE 'Normal (228-252V)'
    END
ORDER BY reading_count DESC;

-- =============================================================================
-- 3. SOLAR GENERATION ANALYSIS
-- =============================================================================

-- Total solar generation vs consumption (last 24 hours)
SELECT
    HOUR(r.timestamp) as hour_of_day,
    COUNT(DISTINCT CASE WHEN m.has_solar THEN r.meter_id END) as solar_meters,
    ROUND(SUM(CASE WHEN m.has_solar AND r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END), 2) as total_solar_export_kw,
    ROUND(SUM(CASE WHEN r.power_kw > 0 THEN r.power_kw ELSE 0 END), 2) as total_consumption_kw,
    ROUND(SUM(r.power_kw), 2) as net_grid_demand_kw
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 24 HOURS
    AND r.is_valid = TRUE
GROUP BY HOUR(r.timestamp)
ORDER BY hour_of_day;

-- Solar capacity factor by state (% of theoretical max)
SELECT
    m.location_state,
    COUNT(DISTINCT m.meter_id) as solar_meters,
    ROUND(SUM(m.solar_capacity_kw), 2) as total_capacity_kw,
    ROUND(SUM(CASE WHEN r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END) / COUNT(*) * 288, 2) as avg_daily_generation_kwh,
    ROUND(
        (SUM(CASE WHEN r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END) / COUNT(*) * 288) /
        (SUM(m.solar_capacity_kw) * 24) * 100,
        2
    ) as capacity_factor_pct
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE m.has_solar = TRUE
    AND r.timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND r.is_valid = TRUE
GROUP BY m.location_state
ORDER BY total_capacity_kw DESC;

-- Top 10 solar generators (by export)
SELECT
    r.meter_id,
    m.location_state,
    m.location_suburb,
    m.solar_capacity_kw,
    ROUND(SUM(CASE WHEN r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END) / 12, 2) as total_export_kwh_last_hour,
    ROUND(SUM(CASE WHEN r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END) / 12 / m.solar_capacity_kw * 100, 2) as utilization_pct
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE m.has_solar = TRUE
    AND r.timestamp >= current_timestamp() - INTERVAL 1 HOUR
    AND r.is_valid = TRUE
GROUP BY r.meter_id, m.location_state, m.location_suburb, m.solar_capacity_kw
ORDER BY total_export_kwh_last_hour DESC
LIMIT 10;

-- =============================================================================
-- 4. LOAD PATTERN ANALYSIS
-- =============================================================================

-- Daily load curve (average hourly demand over last 7 days)
SELECT
    HOUR(timestamp) as hour,
    ROUND(AVG(power_kw), 3) as avg_demand_kw,
    ROUND(PERCENTILE(power_kw, 0.5), 3) as median_demand_kw,
    ROUND(PERCENTILE(power_kw, 0.95), 3) as p95_demand_kw,
    COUNT(*) as sample_size
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND is_valid = TRUE
    AND power_kw > 0  -- Exclude solar export
GROUP BY HOUR(timestamp)
ORDER BY hour;

-- Peak demand periods identification
SELECT
    DATE(timestamp) as date,
    HOUR(timestamp) as hour,
    ROUND(SUM(power_kw), 2) as total_demand_kw,
    COUNT(DISTINCT meter_id) as active_meters,
    ROUND(AVG(power_kw), 3) as avg_demand_kw
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND is_valid = TRUE
GROUP BY DATE(timestamp), HOUR(timestamp)
ORDER BY total_demand_kw DESC
LIMIT 20;

-- Residential vs Commercial load patterns
SELECT
    m.meter_type,
    HOUR(r.timestamp) as hour,
    ROUND(AVG(r.power_kw), 3) as avg_demand_kw,
    ROUND(STDDEV(r.power_kw), 3) as demand_stddev
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND r.is_valid = TRUE
    AND m.meter_type IN ('residential', 'commercial')
GROUP BY m.meter_type, HOUR(r.timestamp)
ORDER BY m.meter_type, hour;

-- =============================================================================
-- 5. DATA QUALITY MONITORING
-- =============================================================================

-- Missing data by meter (identify problematic meters)
SELECT
    meter_id,
    COUNT(*) as total_readings,
    SUM(CASE WHEN quality_flag = 'MISSING' THEN 1 ELSE 0 END) as missing_count,
    SUM(CASE WHEN quality_flag = 'ANOMALY' THEN 1 ELSE 0 END) as anomaly_count,
    ROUND(SUM(CASE WHEN quality_flag = 'OK' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as quality_pct
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY meter_id
HAVING quality_pct < 95.0  -- Meters with < 95% good data
ORDER BY quality_pct ASC
LIMIT 20;

-- Data completeness by hour
SELECT
    DATE(timestamp) as date,
    HOUR(timestamp) as hour,
    COUNT(DISTINCT meter_id) as meters_reporting,
    COUNT(*) as total_readings,
    ROUND(COUNT(*) * 100.0 / (COUNT(DISTINCT meter_id) * 12), 2) as completeness_pct,
    ROUND(AVG(CASE WHEN quality_flag = 'OK' THEN 1.0 ELSE 0.0 END) * 100, 2) as quality_pct
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY DATE(timestamp), HOUR(timestamp)
ORDER BY date DESC, hour DESC
LIMIT 24;

-- Anomaly detection - unusual power readings
SELECT
    r.meter_id,
    m.meter_type,
    m.location_state,
    r.timestamp,
    r.power_kw,
    r.voltage_v,
    r.current_a,
    r.quality_flag
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 1 HOUR
    AND r.quality_flag = 'ANOMALY'
ORDER BY r.timestamp DESC
LIMIT 100;

-- =============================================================================
-- 6. PERFORMANCE BENCHMARKING
-- =============================================================================

-- Query 1: Full table scan (count all records)
-- Expected: 1-2 seconds for 200M records
SELECT COUNT(*) FROM energy_australia_demo.bronze.live_meter_readings;

-- Query 2: Filtered aggregation (last 24 hours)
-- Expected: 2-5 seconds
SELECT
    DATE(timestamp) as date,
    HOUR(timestamp) as hour,
    COUNT(*) as readings,
    ROUND(AVG(power_kw), 2) as avg_power_kw
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY DATE(timestamp), HOUR(timestamp);

-- Query 3: Join with metadata (last 1 hour)
-- Expected: 3-8 seconds
SELECT
    m.location_state,
    m.meter_type,
    COUNT(*) as readings,
    ROUND(SUM(r.power_kw), 2) as total_demand_kw
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY m.location_state, m.meter_type;

-- Query 4: Complex aggregation (7 days)
-- Expected: 5-15 seconds
SELECT
    DATE(r.timestamp) as date,
    m.location_state,
    m.meter_type,
    COUNT(DISTINCT r.meter_id) as meters,
    ROUND(AVG(r.power_kw), 2) as avg_demand_kw,
    ROUND(MAX(r.power_kw), 2) as peak_demand_kw,
    ROUND(SUM(r.power_kw) / 12, 2) as total_energy_kwh
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND r.is_valid = TRUE
GROUP BY DATE(r.timestamp), m.location_state, m.meter_type
ORDER BY date DESC, total_energy_kwh DESC;

-- =============================================================================
-- 7. BUSINESS INSIGHTS
-- =============================================================================

-- Energy consumption by state (last 7 days)
SELECT
    m.location_state,
    COUNT(DISTINCT r.meter_id) as meters,
    ROUND(SUM(CASE WHEN r.power_kw > 0 THEN r.power_kw ELSE 0 END) / 12, 2) as total_consumption_kwh,
    ROUND(SUM(CASE WHEN r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END) / 12, 2) as total_solar_export_kwh,
    ROUND(
        SUM(CASE WHEN r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END) /
        NULLIF(SUM(CASE WHEN r.power_kw > 0 THEN r.power_kw ELSE 0 END), 0) * 100,
        2
    ) as solar_offset_pct
FROM energy_australia_demo.bronze.live_meter_readings r
INNER JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND r.is_valid = TRUE
GROUP BY m.location_state
ORDER BY total_consumption_kwh DESC;

-- Identify high-value customers (top 1% consumers)
WITH meter_consumption AS (
    SELECT
        r.meter_id,
        m.meter_type,
        m.location_state,
        ROUND(SUM(CASE WHEN r.power_kw > 0 THEN r.power_kw ELSE 0 END) / 12, 2) as consumption_kwh
    FROM energy_australia_demo.bronze.live_meter_readings r
    INNER JOIN energy_australia_demo.bronze.meter_metadata m
        ON r.meter_id = m.meter_id
    WHERE r.timestamp >= current_timestamp() - INTERVAL 7 DAYS
        AND r.is_valid = TRUE
    GROUP BY r.meter_id, m.meter_type, m.location_state
)
SELECT
    meter_id,
    meter_type,
    location_state,
    consumption_kwh,
    ROUND(consumption_kwh * 0.30, 2) as estimated_cost_aud  -- Assuming $0.30/kWh
FROM meter_consumption
WHERE consumption_kwh >= (SELECT PERCENTILE(consumption_kwh, 0.99) FROM meter_consumption)
ORDER BY consumption_kwh DESC
LIMIT 100;

-- Solar penetration impact on grid demand
SELECT
    DATE(r.timestamp) as date,
    HOUR(r.timestamp) as hour,
    ROUND(SUM(r.power_kw), 2) as net_demand_kw,
    ROUND(SUM(CASE WHEN r.power_kw > 0 THEN r.power_kw ELSE 0 END), 2) as gross_consumption_kw,
    ROUND(SUM(CASE WHEN r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END), 2) as solar_export_kw,
    ROUND(
        SUM(CASE WHEN r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END) /
        NULLIF(SUM(CASE WHEN r.power_kw > 0 THEN r.power_kw ELSE 0 END), 0) * 100,
        2
    ) as solar_offset_pct
FROM energy_australia_demo.bronze.live_meter_readings r
WHERE r.timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND r.is_valid = TRUE
GROUP BY DATE(r.timestamp), HOUR(r.timestamp)
ORDER BY date DESC, hour;

-- =============================================================================
-- END OF EXAMPLE QUERIES
-- =============================================================================
-- Next steps:
-- 1. Use these queries in Databricks SQL Dashboards
-- 2. Create scheduled alerts based on thresholds
-- 3. Build ML models for demand forecasting
-- 4. Integrate with Databricks Workflows for automated reporting
-- =============================================================================
