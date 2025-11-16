# Alternative Options If OAuth Service Principal Is a Blocker

## 🤔 **The Context**

Zerobus requires OAuth service principals - no way around it. But if that's a blocker (permissions, setup time, security approvals), here are your **other ingestion options** for the demo.

---

## 📊 **Your Options (Ranked by Ease)**

| Option | Auth Method | Latency | Throughput | Setup Time | Demo Quality |
|--------|-------------|---------|------------|------------|--------------|
| **1. Auto Loader** ⭐ | PAT (easier!) | < 10 sec | High | 15 min | ⭐⭐⭐⭐⭐ |
| **2. COPY INTO** | PAT | Minutes | Very high | 5 min | ⭐⭐⭐⭐ |
| **3. Delta Live Tables** | PAT | < 10 sec | High | 30 min | ⭐⭐⭐⭐⭐ |
| **4. Databricks SQL API** | PAT | 10-30 sec | Medium | 20 min | ⭐⭐⭐ |
| **5. Zerobus (OAuth)** | OAuth only | < 5 sec | Very high | 1-2 hours | ⭐⭐⭐⭐⭐ |

---

## ⭐ **Option 1: Auto Loader (BEST Alternative)**

**Uses:** Personal Access Token (PAT) - much easier to get than OAuth!

### **Why It's Good:**
- ✅ **< 10 second latency** (close to Zerobus's < 5 sec)
- ✅ **Continuous streaming** (real-time ingestion)
- ✅ **Just needs a PAT** (no service principal required)
- ✅ **Production-quality** (what EA would actually use)
- ✅ **Shows "eliminate Kafka"** value prop

### **Architecture:**
```
Your Laptop → Writes JSON files → DBFS/S3 → Auto Loader → Delta Lake
                                              (< 10 sec)
```

### **Setup (15 minutes):**

**1. Get a PAT (super easy):**
```
Databricks UI → User Settings → Developer → Access Tokens
→ Generate New Token → Copy token
```

```bash
export DATABRICKS_TOKEN="dapi1234567890abcdef..."
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
```

**2. Create Databricks notebook with Auto Loader:**
```python
# Databricks notebook: auto_loader_meter_ingestion

from pyspark.sql.functions import *

# Configure Auto Loader to monitor DBFS location
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "dbfs:/schemas/meter_readings")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("dbfs:/streaming/meter_data/")
)

# Add ingestion timestamp
df_with_metadata = df.withColumn("ingestion_time", current_timestamp())

# Write to Delta table
query = (df_with_metadata.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/checkpoints/meter_readings")
    .trigger(processingTime="5 seconds")  # Check for new files every 5 seconds
    .toTable("energy_australia_demo.bronze.live_meter_readings")
)

# Display stream status
display(query.status)
```

**3. Run your modified generator:**
```python
# Modified streaming_meter_generator.py to write to DBFS

import json
from datetime import datetime

class DBFSStreamer:
    def __init__(self, output_path="streaming_output"):
        self.output_path = output_path
        os.makedirs(output_path, exist_ok=True)

    def stream_reading(self, reading):
        # Write individual JSON files
        timestamp = datetime.now()
        filename = f"{self.output_path}/reading_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.json"

        with open(filename, 'w') as f:
            json.dump(reading, f)

    def upload_to_dbfs(self):
        # Upload batch to DBFS
        import subprocess
        subprocess.run([
            "databricks", "fs", "cp",
            self.output_path,
            "dbfs:/streaming/meter_data/",
            "--recursive", "--overwrite"
        ])

# Use in your simulator
streamer = DBFSStreamer()
for reading in generate_readings():
    streamer.stream_reading(reading)

    # Upload every 100 records
    if count % 100 == 0:
        streamer.upload_to_dbfs()
```

**4. Run the demo:**
```bash
# Terminal 1: Start Auto Loader in Databricks
# (Run the notebook above)

# Terminal 2: Generate and upload data
python streaming_meter_generator.py --mode files --num-meters 1000 --duration 60 --speed 12

# Terminal 3: Query in real-time
# SELECT COUNT(*) FROM energy_australia_demo.bronze.live_meter_readings;
```

**Latency:** < 10 seconds (Auto Loader checks every 5 seconds)

---

## ⚡ **Option 2: COPY INTO (Simplest, Batch)**

**Uses:** Personal Access Token (PAT)

### **Why It's Good:**
- ✅ **Simplest setup** (5 minutes)
- ✅ **Just SQL** - no Python SDK needed
- ✅ **Just needs a PAT**
- ✅ **Highest throughput** for batch loads

### **Downside:**
- ⚠️ Not real-time (batch processing)
- ⚠️ Manual trigger (or schedule with jobs)

### **Setup:**

**1. Generate data as before:**
```bash
python meter_reading_generator.py \
    --metadata metadata.parquet \
    --start-date 2025-11-16 \
    --days 7 \
    --output meter_readings_100k_7d/ \
    --batch-size 1000 \
    --workers 16
```

**2. Upload to DBFS:**
```bash
databricks fs cp meter_readings_100k_7d/ dbfs:/data/meter_readings/ --recursive
```

**3. Ingest with SQL:**
```sql
-- In Databricks SQL Warehouse
COPY INTO energy_australia_demo.bronze.live_meter_readings
FROM 'dbfs:/data/meter_readings/'
FILEFORMAT = PARQUET
COPY_OPTIONS ('mergeSchema' = 'true');

-- Check results
SELECT COUNT(*) FROM energy_australia_demo.bronze.live_meter_readings;
-- Result: 201.6M records in 2-5 minutes
```

**Good for:** Proving scale (200M records), not real-time streaming

---

## 🔥 **Option 3: Delta Live Tables (Most Production-Like)**

**Uses:** Personal Access Token (PAT)

### **Why It's Good:**
- ✅ **Production architecture** (what EA should use)
- ✅ **Full governance** (lineage, quality checks, monitoring)
- ✅ **Just needs a PAT**
- ✅ **Continuous streaming** with Auto Loader built-in
- ✅ **Best demo** for EA (shows end-to-end solution)

### **Setup (30 minutes):**

**1. Create DLT pipeline definition:**
```python
# dlt_meter_ingestion.py

import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="live_meter_readings_bronze",
    comment="Real-time meter readings from streaming ingestion",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def meter_readings_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("dbfs:/streaming/meter_data/")
    )

@dlt.table(
    name="live_meter_readings_silver",
    comment="Validated and enriched meter readings",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL")
@dlt.expect_or_drop("valid_power", "power_kw IS NOT NULL")
@dlt.expect("quality_ok", "quality_flag = 'OK'")
def meter_readings_silver():
    return (
        dlt.read_stream("live_meter_readings_bronze")
            .withColumn("ingestion_time", current_timestamp())
            .filter(col("is_valid") == True)
    )

@dlt.table(
    name="hourly_demand_gold",
    comment="Hourly aggregated demand metrics"
)
def hourly_demand():
    return (
        dlt.read_stream("live_meter_readings_silver")
            .groupBy(
                window("timestamp", "1 hour"),
                "quality_flag"
            )
            .agg(
                count("*").alias("reading_count"),
                avg("power_kw").alias("avg_power_kw"),
                sum("power_kw").alias("total_power_kw")
            )
    )
```

**2. Create DLT pipeline in Databricks UI:**
- Go to **Workflows** → **Delta Live Tables**
- Click **Create pipeline**
- Add your notebook path
- Set target catalog: `energy_australia_demo`
- Click **Create**
- Click **Start**

**3. Stream data as before**

**Latency:** < 10 seconds, with full data quality monitoring!

---

## 🔌 **Option 4: Databricks SQL Statement API (PAT-based)**

**Uses:** Personal Access Token (PAT)

### **Setup:**

```python
#!/usr/bin/env python3
"""
SQL API Meter Streamer - Uses PAT instead of OAuth
"""
import requests
import json
import time
from datetime import datetime

class SQLAPIStreamer:
    def __init__(self, workspace_url, token, warehouse_id, catalog, schema, table):
        self.workspace_url = workspace_url
        self.token = token
        self.warehouse_id = warehouse_id
        self.table_name = f"{catalog}.{schema}.{table}"

        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        self.endpoint = f"{workspace_url}/api/2.0/sql/statements"

    def stream_batch(self, readings):
        # Convert readings to VALUES clause
        values = []
        for r in readings:
            values.append(f"""(
                '{r['meter_id']}',
                '{r['timestamp']}',
                {r['power_kw'] if r['power_kw'] is not None else 'NULL'},
                {r['voltage_v'] if r['voltage_v'] is not None else 'NULL'},
                {r['current_a'] if r['current_a'] is not None else 'NULL'},
                {r['power_factor']},
                {r['frequency_hz']},
                {r['is_valid']},
                '{r['quality_flag']}'
            )""")

        sql = f"""
        INSERT INTO {self.table_name}
        (meter_id, timestamp, power_kw, voltage_v, current_a,
         power_factor, frequency_hz, is_valid, quality_flag)
        VALUES {', '.join(values)}
        """

        payload = {
            'statement': sql,
            'warehouse_id': self.warehouse_id,
            'wait_timeout': '30s'
        }

        try:
            response = requests.post(
                self.endpoint,
                headers=self.headers,
                json=payload,
                timeout=60
            )

            if response.status_code == 200:
                return True
            else:
                print(f"Error: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Exception: {e}")
            return False

# Usage
streamer = SQLAPIStreamer(
    workspace_url=os.getenv('DATABRICKS_HOST'),
    token=os.getenv('DATABRICKS_TOKEN'),  # Just a PAT!
    warehouse_id='your-warehouse-id',
    catalog='energy_australia_demo',
    schema='bronze',
    table='live_meter_readings'
)

# Stream in batches
batch = []
for reading in generate_readings():
    batch.append(reading)

    if len(batch) >= 100:
        streamer.stream_batch(batch)
        batch = []
```

**Latency:** 10-30 seconds (SQL warehouse execution time)

---

## 🎯 **My Recommendation**

### **For EA Demo - Choose Based on Your Constraints:**

#### **If you have 1-2 hours:**
→ **Set up OAuth service principal for Zerobus** ⭐⭐⭐⭐⭐
- Best demo quality
- True Kafka replacement story
- < 5 second latency

#### **If you have 30 minutes:**
→ **Use Auto Loader** ⭐⭐⭐⭐⭐
- Just needs a PAT (30 seconds to get)
- < 10 second latency (close enough!)
- Still shows continuous streaming
- Production-quality solution

#### **If you have 15 minutes:**
→ **Use Delta Live Tables** ⭐⭐⭐⭐⭐
- Just needs a PAT
- Best overall demo (shows governance + streaming)
- < 10 second latency
- This is what EA should actually implement

#### **If you have 5 minutes:**
→ **Use COPY INTO** ⭐⭐⭐⭐
- Just needs a PAT
- Not real-time, but proves 200M record scale
- Good for "data volume" part of demo

---

## 📝 **Quick Comparison: OAuth vs PAT**

| Aspect | OAuth Service Principal | Personal Access Token (PAT) |
|--------|------------------------|------------------------------|
| **How to get** | 1-2 hours (create SP, permissions) | 30 seconds (UI → Generate) |
| **Works with Zerobus** | ✅ Yes | ❌ No |
| **Works with Auto Loader** | ✅ Yes | ✅ Yes |
| **Works with COPY INTO** | ✅ Yes | ✅ Yes |
| **Works with DLT** | ✅ Yes | ✅ Yes |
| **Works with SQL API** | ✅ Yes | ✅ Yes |
| **Validity** | 1-2 years | 90 days (default) |
| **Production recommended** | ✅ Yes | ⚠️ For dev/test only |

---

## 💡 **Hybrid Approach (Best of Both Worlds)**

**Demo with PAT + Auto Loader NOW, show OAuth path for production:**

```
Today's Demo (30 min setup):
Your Laptop → JSON files → DBFS → Auto Loader (PAT) → Delta Lake
                                    (< 10 sec latency)

Production Recommendation (show architecture):
Smart Meters → IoT Gateway → Zerobus (OAuth) → Delta Lake
                              (< 5 sec latency)
```

**Talking points:**
- "Today I'm using Auto Loader to prove the concept quickly"
- "For production, we'd recommend Zerobus with OAuth service principals"
- "Both eliminate Kafka - that's the key win"
- "Auto Loader: < 10 sec, Zerobus: < 5 sec - both massive improvements over 45-90 sec Kafka latency"

---

## ✅ **Summary: Your Other Options**

**If OAuth service principal is a blocker:**

1. **Auto Loader** - Best alternative (< 10 sec latency, PAT auth, 30 min setup)
2. **Delta Live Tables** - Best production demo (governance + streaming, PAT auth)
3. **COPY INTO** - Simplest (batch only, but proves 200M scale)
4. **SQL API** - Works but slower (10-30 sec latency)

**All of these use Personal Access Tokens (PAT) instead of OAuth!**

Want me to create a working implementation for any of these alternatives?
