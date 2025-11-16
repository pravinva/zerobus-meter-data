# Zerobus SDK Setup Guide - Real Meter Streaming

This guide shows how to set up the **official Databricks Zerobus SDK** to stream meter data in real-time, simulating a real-world IoT deployment.

---

## 🎯 **What You're Building**

```
Your Laptop (Simulated IoT Gateway)
         │
         │ Continuous meter readings every 5 minutes
         │ Streams via Zerobus gRPC protocol
         │ No Kafka required!
         ▼
  Databricks Zerobus API
  (Managed serverless service)
         │
         │ < 5 second latency
         ▼
    Delta Lake Table
   (Queryable immediately)
```

---

## 📋 **Prerequisites Checklist**

- [ ] Databricks workspace (AWS or Azure)
- [ ] Unity Catalog enabled
- [ ] Python 3.9 or higher
- [ ] Workspace ID (found in URL: `https://<instance>.cloud.databricks.com/o=<workspace-id>`)
- [ ] AWS region or Azure region

---

## ⚙️ **Step 1: Install Zerobus SDK**

```bash
# Install official Databricks Zerobus SDK
pip install databricks-zerobus-ingest-sdk

# Or install all requirements:
pip install -r requirements.txt
```

**Verify installation:**
```bash
python -c "from zerobus.sdk.sync import ZerobusSdk; print('✅ Zerobus SDK installed!')"
```

---

## 🔐 **Step 2: Create Service Principal**

Service principals provide secure authentication for programmatic access.

### **In Databricks UI:**

1. Navigate to **Settings** → **Identity and access** → **Service principals**
2. Click **Add service principal**
3. Enter a name: `zerobus-meter-ingestion`
4. Click **Add**
5. **Save the Application ID (UUID)** - this is your `client_id`

### **Generate Client Secret:**

1. Click on your new service principal
2. Go to **OAuth secrets** tab
3. Click **Generate secret**
4. **Copy and save the secret immediately** - you won't see it again!
5. This is your `client_secret`

### **Example values (yours will be different):**
```bash
export DATABRICKS_CLIENT_ID="12345678-1234-1234-1234-123456789abc"
export DATABRICKS_CLIENT_SECRET="dapi1234567890abcdef..."
```

---

## 🗄️ **Step 3: Create Target Table**

Run this in Databricks SQL Warehouse:

```sql
-- Create table for meter readings
CREATE TABLE IF NOT EXISTS energy_australia_demo.bronze.live_meter_readings (
    meter_id STRING COMMENT 'Unique meter identifier (NMI format)',
    timestamp TIMESTAMP COMMENT '5-minute interval reading timestamp',
    power_kw DOUBLE COMMENT 'Net power in kW',
    voltage_v DOUBLE COMMENT 'Voltage in volts',
    current_a DOUBLE COMMENT 'Current in amperes',
    power_factor DOUBLE COMMENT 'Power factor (0.0-1.0)',
    frequency_hz DOUBLE COMMENT 'Grid frequency in Hz',
    is_valid BOOLEAN COMMENT 'Data quality flag',
    quality_flag STRING COMMENT 'Quality code: OK, MISSING, ANOMALY'
)
USING DELTA
PARTITIONED BY (DATE(timestamp))
COMMENT 'Real-time meter readings ingested via Zerobus'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

---

## 🔑 **Step 4: Grant Permissions to Service Principal**

Replace `<UUID>` with your service principal's Application ID:

```sql
-- Grant catalog permissions
GRANT USE CATALOG ON CATALOG energy_australia_demo
TO `<UUID>`;

-- Grant schema permissions
GRANT USE SCHEMA ON SCHEMA energy_australia_demo.bronze
TO `<UUID>`;

-- Grant table permissions
GRANT MODIFY, SELECT ON TABLE energy_australia_demo.bronze.live_meter_readings
TO `<UUID>`;
```

**Verify permissions:**
```sql
SHOW GRANTS ON TABLE energy_australia_demo.bronze.live_meter_readings;
```

---

## 🌐 **Step 5: Configure Environment Variables**

Find your workspace details and set environment variables:

### **Get Workspace ID:**
Look at your Databricks URL:
```
https://dbc-abc123-def456.cloud.databricks.com/o=1234567890123456
                                                     ^^^^^^^^^^^^^^^^^
                                                     This is your workspace ID
```

### **Get Region:**
- **AWS**: Look at your URL (e.g., `us-west-2`, `us-east-1`, `eu-west-1`)
- **Azure**: Look at your URL (e.g., `eastus2`, `westus`, `westeurope`)

### **Set environment variables:**

```bash
# Databricks configuration
export DATABRICKS_WORKSPACE_ID="1234567890123456"
export DATABRICKS_WORKSPACE_URL="https://dbc-abc123-def456.cloud.databricks.com"
export DATABRICKS_REGION="us-west-2"  # or your region

# Service principal credentials
export DATABRICKS_CLIENT_ID="12345678-1234-1234-1234-123456789abc"
export DATABRICKS_CLIENT_SECRET="dapi1234567890abcdef..."

# Optional: Add to ~/.bashrc or ~/.zshrc for persistence
echo 'export DATABRICKS_WORKSPACE_ID="..."' >> ~/.bashrc
echo 'export DATABRICKS_WORKSPACE_URL="..."' >> ~/.bashrc
echo 'export DATABRICKS_REGION="..."' >> ~/.bashrc
echo 'export DATABRICKS_CLIENT_ID="..."' >> ~/.bashrc
echo 'export DATABRICKS_CLIENT_SECRET="..."' >> ~/.bashrc
```

---

## 🚀 **Step 6: Run Your First Test**

### **Quick Test (100 meters for 1 minute):**

```bash
python zerobus_meter_streamer.py \
    --num-meters 100 \
    --duration 5 \
    --interval 5 \
    --speed 1
```

**What you should see:**
```
╔══════════════════════════════════════════════════════════════════╗
║  Zerobus Meter Data Streamer (Official SDK)                     ║
╚══════════════════════════════════════════════════════════════════╝

Configuration:
  Zerobus endpoint: 1234567890123456.zerobus.us-west-2.cloud.databricks.com
  Workspace URL: https://dbc-abc123-def456.cloud.databricks.com
  Target table: energy_australia_demo.bronze.live_meter_readings
  Service principal: 12345678-1234-1234-1234-123456789abc

🔌 Connecting to Zerobus...
✅ Connected! Stream ready for ingestion.

🚀 Starting Meter Fleet Simulation
═══════════════════════════════════════════════════════════════
  Meters: 100
  Duration: 5 minutes (simulated)
  Interval: 5 minutes
  Speed: 1x real-time
  Wait for acks: False

✅ Generated 100 meter profiles:
   Residential: 70
   Commercial: 25
   Industrial: 5
   With solar: 22

📡 Streaming data to Zerobus... (Ctrl+C to stop)

[2025-11-16 10:00:00] ✓ 100 readings | 250 rec/sec | Total: 100 | Interval 1/1 | Real: 0.4s

╔══════════════════════════════════════════════════════════════════╗
║  STREAMING COMPLETE                                              ║
╚══════════════════════════════════════════════════════════════════╝

📊 Statistics:
   Total readings streamed: 100
   Errors: 0
   Simulated time: 5 minutes (1 intervals)
   Real time elapsed: 0.4 seconds
   Average throughput: 250 readings/second

✅ Data now queryable in Databricks:
   SELECT COUNT(*) FROM energy_australia_demo.bronze.live_meter_readings;
```

### **Verify in Databricks:**

```sql
-- Check data arrived
SELECT COUNT(*) as total_readings
FROM energy_australia_demo.bronze.live_meter_readings;
-- Expected: 100

-- Check latest readings
SELECT *
FROM energy_australia_demo.bronze.live_meter_readings
ORDER BY timestamp DESC
LIMIT 10;

-- Measure ingestion latency
SELECT
    MAX(timestamp) as latest_reading_time,
    current_timestamp() as now,
    ROUND(UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(MAX(timestamp)), 2) as latency_seconds
FROM energy_australia_demo.bronze.live_meter_readings;
-- Latency should be < 10 seconds!
```

---

## 🎬 **Step 7: Run Full Demo**

### **Scenario 1: Simulate 1,000 Meters for 1 Hour (Real-Time)**

```bash
python zerobus_meter_streamer.py \
    --num-meters 1000 \
    --duration 60 \
    --interval 5 \
    --speed 1

# What this simulates:
# - 1,000 meters sending readings every 5 minutes
# - Runs for 1 hour of real time
# - Generates 1,000 × 12 = 12,000 readings
# - Proves sustained 3.3 readings/second throughput
```

### **Scenario 2: Simulate 24 Hours at 60x Speed (1 day in 24 minutes)**

```bash
python zerobus_meter_streamer.py \
    --num-meters 10000 \
    --duration 1440 \
    --interval 5 \
    --speed 60

# What this simulates:
# - 10,000 meters × 24 hours of operation
# - Compressed to 24 minutes of real time
# - Generates 10,000 × 288 = 2,880,000 readings
# - Proves ~2,000 readings/second sustained throughput
# - Shows full daily load patterns
```

### **Scenario 3: Prove 100K Meter Scale (Peak Throughput)**

```bash
python zerobus_meter_streamer.py \
    --num-meters 100000 \
    --duration 60 \
    --interval 5 \
    --speed 60

# What this proves:
# - System can handle 100,000 concurrent meter streams
# - Generates 100,000 × 12 = 1,200,000 readings
# - Runs in 1 minute real time
# - Proves ~20,000 readings/second burst throughput
```

---

## 🔍 **Step 8: Monitor Real-Time Ingestion**

While the streamer is running, query in Databricks:

```sql
-- Dashboard 1: Live ingestion rate
SELECT
    DATE_TRUNC('second', timestamp) as second,
    COUNT(*) as readings_per_second
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 60 SECONDS
GROUP BY DATE_TRUNC('second', timestamp)
ORDER BY second DESC;

-- Dashboard 2: Current demand by state (if you have metadata table)
SELECT
    m.location_state,
    COUNT(DISTINCT r.meter_id) as active_meters,
    ROUND(SUM(r.power_kw), 2) as total_demand_kw
FROM energy_australia_demo.bronze.live_meter_readings r
JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY m.location_state
ORDER BY total_demand_kw DESC;

-- Dashboard 3: Data quality metrics
SELECT
    quality_flag,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY quality_flag;
```

---

## 🏆 **Success Criteria**

✅ **You've successfully set up Zerobus when you can:**

1. Stream 100+ meters continuously
2. See < 10 second latency from generation to query
3. Sustain 100+ readings/second throughput
4. Query data in real-time while still ingesting
5. No errors in the streamer output
6. No Kafka cluster needed!

---

## 🐛 **Troubleshooting**

### **Error: "Zerobus SDK not installed"**
```bash
pip install databricks-zerobus-ingest-sdk
```

### **Error: "Authentication failed"**
- Verify service principal client ID and secret
- Check permissions: `SHOW GRANTS ON TABLE ...;`
- Ensure service principal has MODIFY and SELECT

### **Error: "Table not found"**
- Verify table exists: `DESCRIBE TABLE energy_australia_demo.bronze.live_meter_readings;`
- Check catalog and schema names match exactly
- Ensure Unity Catalog is enabled

### **Error: "Connection refused" or "gRPC error"**
- Verify workspace ID and region are correct
- Check zerobus endpoint format: `<workspace-id>.zerobus.<region>.cloud.databricks.com`
- For AWS: use `cloud.databricks.com`
- For Azure: use `azuredatabricks.net`

### **Slow ingestion or timeouts**
- Network bandwidth issues - check your internet connection
- Try reducing `--num-meters` for initial testing
- Add `--wait-acks` flag for guaranteed delivery (slower but more reliable)

### **Data not appearing in queries**
- Wait 5-10 seconds for data to become queryable
- Check for errors in streamer output
- Verify table partitioning: `SHOW PARTITIONS energy_australia_demo.bronze.live_meter_readings;`

---

## 📊 **Next Steps**

1. **Create Databricks SQL Dashboard** showing real-time demand
2. **Set up alerts** for anomalous readings or high demand
3. **Build ML model** for demand forecasting
4. **Compare architecture** vs. Kafka-based ingestion (show < 5 sec latency vs. 45-90+ sec)
5. **Demo Unity Catalog governance** - lineage, access control, audit logs

---

## 🎯 **Demo Talking Points for Energy Australia**

**This setup demonstrates:**

✅ **No Kafka cluster** - Eliminated AWS MSK complexity and costs
✅ **< 5 second latency** - vs. 45-90+ seconds with Kafka
✅ **Serverless scaling** - Handles 100K+ meters automatically
✅ **Unity Catalog native** - Governance from ingestion point
✅ **30-40% cost reduction** - No Kafka infrastructure or ops
✅ **Real-time analytics** - Query while ingesting
✅ **gRPC protocol** - Efficient, production-grade streaming

**Your laptop simulates the IoT gateway** that would run in EA's infrastructure, continuously streaming meter data from the field to Databricks - proving the architecture works at scale!
