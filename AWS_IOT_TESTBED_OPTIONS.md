# AWS IoT Test Bed Options for Zerobus Demo

## 🎯 **The Key Question: Should You Use AWS IoT?**

**Short Answer:** For the Zerobus demo, **your laptop simulator is better** than AWS IoT Core. Here's why:

### **Zerobus Demo Goal:**
```
Smart Meters → IoT Gateway → Zerobus → Delta Lake
                              ^^^^^^^^
                              This is what you're proving!
                              (Kafka replacement)
```

### **If You Add AWS IoT Core:**
```
Smart Meters → IoT Gateway → AWS IoT Core → Lambda → Zerobus → Delta Lake
                              ^^^^^^^^^^^^^^^^^^^^^
                              You just added back the complexity
                              you're trying to eliminate!
```

**AWS IoT Core would be an extra hop** - defeating the point of "eliminate hops" that Zerobus provides.

---

## 📊 **AWS IoT Options Available**

### ❌ **Option 1: AWS IoT Device Simulator (DEPRECATED)**

**Status:** Deprecated January 29, 2025 - No longer maintained

This was AWS's official solution but is now archived:
- GitHub: `aws-solutions/iot-device-simulator` (archived)
- Could simulate thousands of devices
- Had GUI for configuration
- **Don't use this** - no updates since Jan 2025

---

### ✅ **Option 2: Custom Python Simulator + AWS IoT Core (Overkill for Demo)**

**Architecture:**
```python
# Your simulator sends data to AWS IoT Core
import awsiot
from awscrt import mqtt

# Connect to AWS IoT Core
mqtt_connection = mqtt.Connection(
    endpoint="your-endpoint.iot.us-west-2.amazonaws.com",
    client_id="meter-simulator",
    cert="certificates/device.pem.crt",
    key="certificates/private.pem.key"
)

# Publish meter readings
mqtt_connection.publish(
    topic="meters/NMI1234567890/telemetry",
    payload=json.dumps(reading)
)
```

Then use **AWS IoT Rules** to route to Lambda → Zerobus:
```sql
-- IoT Rules SQL
SELECT * FROM 'meters/+/telemetry'
```

**Costs:**
- AWS IoT Core: $1 per million messages + $0.08/GB data transfer
- Lambda invocations: $0.20 per million
- **Your demo:** 2.88M messages = ~$3-5 for 10K meters

**Complexity:**
- ⚠️ Certificate management (X.509 certs for each device)
- ⚠️ IoT Core setup and configuration
- ⚠️ Lambda function to bridge IoT → Zerobus
- ⚠️ Extra latency (adds 5-15 seconds)

**When to use:**
- If EA specifically asks "how would this integrate with our existing AWS IoT infrastructure?"
- If you want to show Zerobus as a **sink** for AWS IoT data
- If demonstrating migration path from IoT Core → Zerobus

---

### ⭐ **Option 3: Your Current Approach (BEST FOR DEMO)**

**What you have:**
```bash
python zerobus_meter_streamer.py --num-meters 100000
```

**Why it's better:**
- ✅ **Direct simulation** of IoT gateway (the real integration point)
- ✅ **No extra AWS costs** ($0 vs. $3-5 for AWS IoT)
- ✅ **Lowest latency** (< 5 sec vs. 10-20 sec with IoT Core)
- ✅ **Simpler architecture** (fewer moving parts)
- ✅ **Proves the point:** Zerobus replaces Kafka, not adds to it

**Your laptop = IoT Gateway** that aggregates meter data and pushes to Zerobus.

In a real deployment, this gateway could be:
- On-premises edge server
- EC2 instance in AWS
- Databricks cluster (for ultimate simplicity)

---

## 🏗️ **When AWS IoT Core WOULD Make Sense**

### **Scenario A: Hybrid Architecture (Existing IoT Investment)**

If Energy Australia already has AWS IoT Core deployed:

```
Existing Meters → AWS IoT Core → Lambda → Zerobus → Delta Lake
                  ^^^^^^^^^^^^^   ^^^^^^   ^^^^^^^^
                  Already exists  Bridge   New!
```

**Benefits:**
- Leverage existing IoT infrastructure
- Gradual migration (can run parallel with Kafka)
- Shows Zerobus as flexible sink

**Implementation:**
```python
# Lambda function to bridge IoT Core → Zerobus
import json
from zerobus.sdk.sync import ZerobusSdk

def lambda_handler(event, context):
    # event = IoT Core message
    sdk = ZerobusSdk(ZEROBUS_ENDPOINT, WORKSPACE_URL)
    stream = sdk.create_stream(CLIENT_ID, CLIENT_SECRET, table_props)

    # Transform IoT Core format to Delta table schema
    reading = {
        'meter_id': event['deviceId'],
        'timestamp': event['timestamp'],
        'power_kw': event['power'],
        ...
    }

    stream.ingest_record(json.dumps(reading))
    return {'statusCode': 200}
```

---

### **Scenario B: Testing at MASSIVE Scale (1M+ meters)**

If you want to prove **extreme scale** beyond laptop capacity:

**Option: AWS Lambda as Distributed Simulator**

```python
# deploy_simulators.py
import boto3

lambda_client = boto3.client('lambda')

# Launch 1000 Lambda functions
# Each simulates 1000 meters
# = 1,000,000 meters total

for i in range(1000):
    lambda_client.invoke(
        FunctionName='meter-simulator',
        InvocationType='Event',  # Async
        Payload=json.dumps({
            'meter_start': i * 1000,
            'meter_count': 1000,
            'duration_minutes': 60
        })
    )
```

**Costs:** ~$10-20 for 1M meters × 1 hour

**When to use:**
- EA asks "can it handle 1 million meters?"
- Laptop can't handle the load
- Want to prove extreme scale

---

## 🎬 **Recommended Demo Approach**

### **For Initial Demo (90% of cases):**

**Use your current implementation:**
```bash
python zerobus_meter_streamer.py --num-meters 100000 --duration 60 --speed 60
```

**Talking points:**
- "My laptop simulates your IoT gateway"
- "In production, this would be an EC2 instance or on-prem server"
- "Notice: **no AWS IoT Core, no Kafka** - direct to Delta Lake"
- "This eliminates 2-3 hops from your current architecture"

---

### **If EA Asks: "How does this work with our existing AWS IoT?"**

**Then show the hybrid approach:**

1. **Create IoT Core thing:**
```bash
aws iot create-thing --thing-name meter-simulator-001
aws iot create-keys-and-certificate --set-as-active
```

2. **Modify simulator to publish to IoT Core:**
```python
# iot_to_zerobus_bridge.py
from awscrt import mqtt
from zerobus.sdk.sync import ZerobusSdk

# Connect to both IoT Core and Zerobus
iot_connection = connect_to_iot_core()
zerobus_stream = connect_to_zerobus()

# Forward messages
def on_message(topic, payload):
    reading = json.loads(payload)
    zerobus_stream.ingest_record(json.dumps(reading))

iot_connection.subscribe(topic="meters/+/telemetry", callback=on_message)
```

3. **Show in demo:**
- "Here's how Zerobus integrates with existing AWS IoT investment"
- "But notice - we still eliminate Kafka"
- "And we get Unity Catalog governance on the data"

---

## 📊 **Cost Comparison**

| Approach | Setup Time | Demo Cost | Complexity | Latency | Best For |
|----------|------------|-----------|------------|---------|----------|
| **Laptop simulator** ⭐ | 5 min | $0 | ⭐☆☆☆☆ | < 5 sec | **Pure Zerobus demo** |
| **AWS IoT Core + Zerobus** | 2 hours | $3-5 | ⭐⭐⭐☆☆ | 10-20 sec | Hybrid/migration story |
| **Lambda simulators** | 1 day | $10-20 | ⭐⭐⭐⭐☆ | < 5 sec | Extreme scale proof |

---

## 🎯 **My Recommendation**

**For the Energy Australia Zerobus demo:**

1. **Use `zerobus_meter_streamer.py`** (your current approach) ⭐
   - Cleanest architecture
   - Lowest latency
   - Proves the core value prop
   - No additional AWS costs

2. **Have AWS IoT Core integration ready as backup**
   - Only if they specifically ask
   - Shows migration path
   - Demonstrates flexibility

3. **Don't use AWS IoT Device Simulator**
   - It's deprecated (Jan 2025)
   - Your simulator is better anyway

---

## 💡 **Third-Party Alternatives (If Needed)**

If you absolutely need a realistic "device fleet" simulator:

### **Option 1: HiveMQ Swarm (Commercial)**
- Simulates millions of MQTT devices
- Can integrate with any endpoint
- Commercial license required
- Website: hivemq.com/swarm

### **Option 2: Bevywise IoT Simulator (Free/Commercial)**
- Free tier: 50 devices
- Commercial: unlimited
- MQTT protocol support
- Website: bevywise.com/iot-simulator

### **Option 3: Eclipse IoT Test Bed (Open Source)**
- MQTT broker + simulators
- Completely free
- More setup required

**But honestly:** Your Python simulator is more flexible and fits the demo better!

---

## ✅ **Bottom Line**

**For Zerobus demo → Use your laptop simulator**

It's:
- ✅ Simpler
- ✅ Faster
- ✅ Cheaper ($0)
- ✅ More accurate (direct integration)
- ✅ Better demonstrates "eliminate Kafka" value prop

**Only use AWS IoT Core if:**
- EA specifically has AWS IoT Core deployed
- They ask about hybrid architecture
- You want to show migration path

**The whole point of Zerobus is to eliminate hops - don't add them back for the demo!**

---

## 🚀 **Quick Decision Tree**

```
Does EA currently use AWS IoT Core?
│
├─ NO → Use zerobus_meter_streamer.py ⭐
│        (your current approach)
│
└─ YES → Do they want to keep it?
         │
         ├─ NO → Use zerobus_meter_streamer.py ⭐
         │        (show migration away from IoT Core)
         │
         └─ YES → Build IoT Core → Zerobus bridge
                  (show hybrid integration)
```

---

**99% of the time: Your current laptop simulator is the right choice!** 🎯
