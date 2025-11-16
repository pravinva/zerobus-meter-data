# Zerobus Meter Data Ingestion Demo

**Generate and ingest 200 million smart meter records to Databricks Unity Catalog using Zerobus**

This repository demonstrates Databricks Zerobus for high-throughput meter data ingestion, eliminating the need for Apache Kafka in IoT data pipelines.

## 🎯 What This Demo Proves

**Current Architecture** (Energy Australia):
```
IoT Gateway → AWS Managed Kafka → Databricks → Delta Lake
```

**With Zerobus**:
```
IoT Gateway → Zerobus API → Unity Catalog Delta Lake
```

### Key Benefits

- **No Kafka Required**: Direct HTTP/gRPC ingestion to Delta Lake
- **< 5 Second Latency**: From ingestion to query-ready data
- **500K-1M Records/Sec**: Enterprise-scale throughput
- **Unity Catalog Native**: Governance from the ingestion point
- **30-40% Cost Reduction**: Eliminates Kafka cluster management

## 📊 Demo Scale

| Metric | Test | Full Demo |
|--------|------|-----------|
| **Meters** | 10,000 | 100,000 |
| **Days** | 1 | 7 |
| **Total Records** | 2.88M | 201.6M |
| **Data Size** | ~500 MB | ~30 GB |
| **Generation Time** | 15 min | 2-3 hours |
| **Ingestion Time** | < 1 min | 3-7 min |

## 🏗️ Architecture

### Data Flow

```
┌─────────────────────┐
│  Python Generators  │  ← You are here
│  (This Repository)  │
└──────────┬──────────┘
           │
           │ 1. Generate parquet files (2-3 hours)
           │
           ▼
    ┌──────────────┐
    │ Parquet Files│  ~30 GB on local disk
    │  (Batched)   │
    └──────┬───────┘
           │
           │ 2. Zerobus client pushes (5-10 min)
           │
           ▼
┌──────────────────────┐
│ Databricks Zerobus   │  HTTPS/gRPC API
│ /api/2.0/zerobus     │
└──────────┬───────────┘
           │
           │ 3. Direct write (< 5 sec)
           │
           ▼
┌────────────────────────────────┐
│  Unity Catalog Delta Table     │
│  energy_australia_demo.bronze. │
│  live_meter_readings           │
└────────────┬───────────────────┘
             │
             │ Physical storage
             │
             ▼
    ┌──────────────────┐
    │ S3 Delta Lake    │  s3://databricks-.../
    │ ~30 GB compressed│  unity-catalog/.../
    └──────────────────┘
```

### Storage Location

- **Logical**: `energy_australia_demo.bronze.live_meter_readings` (Unity Catalog)
- **Physical**: `s3://databricks-{workspace-id}/unity-catalog/{catalog}/{schema}/{table}/`
- **Format**: Delta Lake (Parquet + transaction log)
- **Region**: ap-southeast-2 (Sydney)

## 🚀 Quick Start

### Prerequisites

```bash
# Python 3.8+
python3 --version

# Required packages
pip install pandas pyarrow numpy requests
```

### 1. Setup Databricks

```sql
-- Run in Databricks SQL Warehouse

-- Create catalog and schemas
CREATE CATALOG IF NOT EXISTS energy_australia_demo;
CREATE SCHEMA IF NOT EXISTS energy_australia_demo.bronze;
CREATE SCHEMA IF NOT EXISTS energy_australia_demo.silver;

-- Create meter metadata table
CREATE TABLE IF NOT EXISTS energy_australia_demo.bronze.meter_metadata (
    meter_id STRING,
    meter_type STRING,
    location_state STRING,
    location_suburb STRING,
    location_postcode INT,
    has_solar BOOLEAN,
    solar_capacity_kw DOUBLE,
    install_date DATE,
    manufacturer STRING,
    model STRING,
    firmware_version STRING,
    created_at TIMESTAMP
) USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Create meter readings table
CREATE TABLE IF NOT EXISTS energy_australia_demo.bronze.live_meter_readings (
    meter_id STRING,
    timestamp TIMESTAMP,
    power_kw DOUBLE,
    voltage_v DOUBLE,
    current_a DOUBLE,
    power_factor DOUBLE,
    frequency_hz DOUBLE,
    is_valid BOOLEAN,
    quality_flag STRING
) USING DELTA
PARTITIONED BY (DATE(timestamp))
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

### 2. Configure Environment

```bash
# Set Databricks credentials
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."  # Generate in User Settings > Developer > Access Tokens
```

### 3. Generate Test Data (10K Meters)

```bash
# Step 1: Generate meter metadata (2 minutes)
python generate_meter_metadata.py \
    --num-meters 10000 \
    --output metadata_test.parquet

# Step 2: Generate 1 day of readings (15 minutes)
python meter_reading_generator.py \
    --metadata metadata_test.parquet \
    --start-date 2025-11-16 \
    --days 1 \
    --output meter_readings_test.parquet
```

**Output**: 2.88M records (~500 MB)

### 4. Ingest to Databricks via Zerobus

```bash
python zerobus_ingestion_client.py \
    --workspace-url $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --catalog energy_australia_demo \
    --schema bronze \
    --table live_meter_readings \
    --input meter_readings_test.parquet \
    --batch-size 5000 \
    --workers 8
```

**Expected**: ~1 minute to ingest 2.88M records at 500K+ rec/sec

### 5. Query Immediately

```sql
-- Verify ingestion
SELECT COUNT(*) FROM energy_australia_demo.bronze.live_meter_readings;
-- Result: ~2,880,000

-- Real-time analytics (< 5 seconds)
SELECT
    HOUR(timestamp) as hour,
    COUNT(*) as readings,
    ROUND(AVG(power_kw), 2) as avg_power_kw,
    ROUND(SUM(power_kw), 2) as total_demand_kw
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 1 DAY
GROUP BY HOUR(timestamp)
ORDER BY hour;
```

## 📈 Full Demo (100K Meters, 200M Records)

### 1. Generate Full Dataset

```bash
# Step 1: Generate 100K meter metadata (5 minutes)
python generate_meter_metadata.py \
    --num-meters 100000 \
    --output metadata_100k.parquet

# Step 2: Generate 7 days of readings (2-3 hours)
# Recommended: Run on Databricks cluster or EC2 (16+ cores, 32GB+ RAM)
python meter_reading_generator.py \
    --metadata metadata_100k.parquet \
    --start-date 2025-11-09 \
    --days 7 \
    --output meter_readings_100k_7d/ \
    --batch-size 1000 \
    --workers 16
```

**Output**: 201.6M records (100K meters × 7 days × 288 readings/day) = ~30 GB

### 2. Ingest Full Dataset

```bash
python zerobus_ingestion_client.py \
    --workspace-url $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --catalog energy_australia_demo \
    --schema bronze \
    --table live_meter_readings \
    --input "meter_readings_100k_7d/*.parquet" \
    --batch-size 10000 \
    --workers 16
```

**Expected**: 3-7 minutes to ingest 201.6M records

### 3. Production-Quality Analytics

```sql
-- State-level demand aggregation
SELECT
    m.location_state,
    COUNT(DISTINCT r.meter_id) as meters,
    ROUND(SUM(r.power_kw), 2) as total_demand_kw,
    ROUND(AVG(r.power_kw), 2) as avg_demand_kw
FROM energy_australia_demo.bronze.live_meter_readings r
JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY m.location_state
ORDER BY total_demand_kw DESC;

-- Solar generation analysis
SELECT
    DATE(r.timestamp) as date,
    HOUR(r.timestamp) as hour,
    COUNT(DISTINCT CASE WHEN m.has_solar THEN r.meter_id END) as solar_meters,
    ROUND(SUM(CASE WHEN m.has_solar AND r.power_kw < 0 THEN ABS(r.power_kw) ELSE 0 END), 2) as total_solar_export_kw
FROM energy_australia_demo.bronze.live_meter_readings r
JOIN energy_australia_demo.bronze.meter_metadata m
    ON r.meter_id = m.meter_id
WHERE r.timestamp >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY DATE(r.timestamp), HOUR(r.timestamp)
ORDER BY date, hour;

-- Data quality monitoring
SELECT
    quality_flag,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM energy_australia_demo.bronze.live_meter_readings
WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY quality_flag;
```

## 📂 Repository Structure

```
zerobus-meter-data/
├── README.md                          # This file
├── generate_meter_metadata.py         # Step 1: Generate meter metadata
├── meter_reading_generator.py         # Step 2: Generate time-series readings
├── zerobus_ingestion_client.py        # Step 3: Ingest to Databricks
├── zerobus_implementation_checklist.csv  # 17-step execution checklist
├── table_schemas.sql                  # Unity Catalog DDL
├── example_queries.sql                # Sample analytics queries
├── requirements.txt                   # Python dependencies
└── .env.example                       # Environment variables template
```

## 🔧 Configuration

### Python Scripts

#### `generate_meter_metadata.py`

Generates realistic meter metadata:
- **Meter types**: Residential (70%), Commercial (25%), Industrial (5%)
- **States**: NSW, VIC, QLD, WA, SA, TAS, ACT (population-weighted)
- **Solar penetration**: 30% residential, 15% commercial, 5% industrial
- **Output**: Parquet file with meter metadata

**Options**:
```bash
--num-meters    # Number of meters (10000 for test, 100000 for demo)
--output        # Output parquet file path
--seed          # Random seed for reproducibility (default: 42)
```

#### `meter_reading_generator.py`

Generates realistic 5-minute interval readings:
- **Load patterns**: Morning/evening peaks (residential), business hours (commercial), 24/7 (industrial)
- **Solar curves**: Dawn to dusk generation with peak at noon
- **Seasonal factors**: Australian seasons (summer: Dec-Feb, winter: Jun-Aug)
- **Data quality**: 1% missing, 1% anomalies
- **Grid parameters**: 240V, 50Hz

**Options**:
```bash
--metadata      # Input metadata parquet file
--start-date    # Start date (YYYY-MM-DD)
--days          # Number of days to generate
--output        # Output path (file or directory)
--batch-size    # Meters per batch (default: 1000)
--workers       # Parallel workers (default: 4)
--seed          # Random seed (default: 42)
```

#### `zerobus_ingestion_client.py`

Ingests data to Databricks via Zerobus API:
- **Protocol**: HTTPS POST to `/api/2.0/zerobus/ingest`
- **Authentication**: Personal Access Token
- **Batching**: Configurable batch size (5K-10K recommended)
- **Parallelism**: Multi-threaded for high throughput
- **Target**: Unity Catalog managed tables

**Options**:
```bash
--workspace-url  # Databricks workspace URL
--token          # Personal Access Token (or use $DATABRICKS_TOKEN)
--catalog        # Unity Catalog catalog name
--schema         # Schema name
--table          # Table name
--input          # Input parquet file(s) - glob pattern supported
--batch-size     # Records per batch (default: 5000)
--workers        # Parallel workers (default: 8)
--dry-run        # Test without ingesting
```

## 📋 Implementation Checklist

See `zerobus_implementation_checklist.csv` for the complete 17-step execution plan:

| Phase | Steps | Duration |
|-------|-------|----------|
| **Setup** | Create catalogs, tables, tokens | 45 min |
| **Test** | 10K meters, 1 day | 30 min |
| **Scale** | 100K meters, 7 days | 3-4 hours |
| **Verify** | Analytics queries | 10 min |
| **Demo** | Dashboards, ML (optional) | 1-2 hours |

**Total**: ~4-6 hours (mostly data generation)

## 🎯 Expected Performance

### Data Generation

| Scale | Meters | Days | Records | Size | Time | Hardware |
|-------|--------|------|---------|------|------|----------|
| Test | 10K | 1 | 2.88M | 500 MB | 15 min | Laptop (8 cores) |
| Demo | 100K | 7 | 201.6M | 30 GB | 2-3 hours | EC2/Databricks (16 cores) |

### Zerobus Ingestion

| Records | Batches | Workers | Throughput | Time | Latency |
|---------|---------|---------|------------|------|---------|
| 2.88M | 576 | 8 | 500K/sec | < 1 min | < 5 sec |
| 201.6M | 20,160 | 16 | 1M/sec | 3-7 min | < 5 sec |

### Query Performance (Photon-accelerated)

| Query Type | Records Scanned | Latency |
|------------|-----------------|---------|
| COUNT(*) | 200M | 1-2 sec |
| Aggregation (1 hour) | ~1.2M | 1-3 sec |
| Aggregation (7 days) | 200M | 5-10 sec |
| Join with metadata | 200M | 10-15 sec |

## 🔍 Data Specifications

### Meter Metadata

```python
{
    'meter_id': 'NMI1234567890',           # Australian NMI format
    'meter_type': 'residential',            # residential, commercial, industrial
    'location_state': 'NSW',                # Australian state
    'location_suburb': 'Sydney CBD',
    'location_postcode': 2000,              # Australian postcode
    'has_solar': True,
    'solar_capacity_kw': 6.5,               # 3-10 kW residential
    'install_date': '2024-03-15',
    'manufacturer': 'Landis+Gyr',
    'model': 'SM-745',
    'firmware_version': '2.3.14',
    'created_at': '2025-11-16T10:30:00'
}
```

### Meter Readings

```python
{
    'meter_id': 'NMI1234567890',
    'timestamp': '2025-11-16T10:30:00',     # 5-minute intervals
    'power_kw': 2.45,                       # Net power (demand - solar)
    'voltage_v': 238.5,                     # 240V ± 5%
    'current_a': 10.3,                      # Calculated from power/voltage
    'power_factor': 0.92,                   # 0.85 - 0.98
    'frequency_hz': 50.0,                   # Australian grid frequency
    'is_valid': True,
    'quality_flag': 'OK'                    # OK, MISSING, ANOMALY
}
```

### Load Patterns

**Residential** (Morning & Evening Peaks):
- 6-9 AM: 1.5× baseline
- 5-10 PM: 1.8× baseline
- Overnight: 0.3× baseline

**Commercial** (Business Hours):
- 8 AM - 6 PM: 1.2-1.5× baseline
- Overnight: 0.2× baseline

**Industrial** (24/7):
- Steady 0.9-1.1× baseline with slight variation

**Solar Generation**:
- 6 AM - 6 PM (peak at noon)
- Seasonal variation: 0.8× winter, 1.2× summer
- Cloud cover randomness: 0.7-1.0×

## 🛠️ Troubleshooting

### Common Issues

**1. Memory Error During Generation**
```bash
# Reduce batch size and workers
python meter_reading_generator.py --batch-size 500 --workers 4 ...
```

**2. Zerobus API Rate Limiting**
```bash
# Reduce workers and increase batch size
python zerobus_ingestion_client.py --batch-size 10000 --workers 4 ...
```

**3. Authentication Error**
```bash
# Verify token is valid
curl -H "Authorization: Bearer $DATABRICKS_TOKEN" \
     $DATABRICKS_HOST/api/2.0/clusters/list
```

**4. Table Not Found**
```sql
-- Verify catalog and schema exist
SHOW CATALOGS;
SHOW SCHEMAS IN energy_australia_demo;
SHOW TABLES IN energy_australia_demo.bronze;
```

## 📚 References

- [Databricks Zerobus Documentation](https://docs.databricks.com/ingestion/zerobus.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [Delta Lake Performance Tuning](https://docs.databricks.com/delta/optimizations/index.html)
- [Australian NMI Format](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-systems-and-processes/national-metering-identifier)

## 🤝 Support

For questions or issues:
1. Check `zerobus_implementation_checklist.csv` for step-by-step guidance
2. Review example queries in `example_queries.sql`
3. Consult Databricks documentation

## 📄 License

MIT License - See LICENSE file for details

---

**Built for Energy Australia - Databricks Zerobus Demo**

*Eliminate Kafka. Simplify ingestion. Scale to billions.*
