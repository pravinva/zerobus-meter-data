#!/usr/bin/env python3
"""
Real Zerobus SDK Meter Data Streamer
Uses official Databricks Zerobus SDK to stream meter data in real-time

This simulates an IoT gateway or meter fleet continuously pushing data
to Databricks via Zerobus - demonstrating the Kafka replacement architecture.
"""
import argparse
import time
import json
import random
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List
import os

# Official Databricks Zerobus SDK
try:
    from zerobus.sdk.sync import ZerobusSdk
    from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties
    ZEROBUS_AVAILABLE = True
except ImportError:
    print("⚠️  Zerobus SDK not installed. Install with:")
    print("   pip install databricks-zerobus-ingest-sdk")
    print("")
    ZEROBUS_AVAILABLE = False

class MeterDataStreamer:
    """
    Streams simulated meter data to Databricks using Zerobus SDK

    This simulates:
    - IoT gateway aggregating data from meters
    - Continuous real-time streaming (not batch)
    - Direct to Delta Lake (no Kafka)
    - < 5 second latency
    """

    def __init__(
        self,
        workspace_id: str,
        workspace_url: str,
        region: str,
        client_id: str,
        client_secret: str,
        catalog: str,
        schema: str,
        table: str
    ):
        """
        Initialize Zerobus meter data streamer

        Args:
            workspace_id: Databricks workspace ID (from URL: /o=<workspace-id>)
            workspace_url: Full workspace URL (e.g., https://dbc-xxx.cloud.databricks.com)
            region: AWS region (e.g., us-west-2) or Azure region
            client_id: Service principal client ID
            client_secret: Service principal client secret
            catalog: Unity Catalog catalog name
            schema: Schema name
            table: Table name
        """

        # Construct Zerobus endpoint
        # Format: <workspace-id>.zerobus.<region>.cloud.databricks.com
        self.zerobus_endpoint = f"{workspace_id}.zerobus.{region}.cloud.databricks.com"
        self.workspace_url = workspace_url

        # Authentication
        self.client_id = client_id
        self.client_secret = client_secret

        # Table
        self.table_name = f"{catalog}.{schema}.{table}"

        print(f"╔══════════════════════════════════════════════════════════════════╗")
        print(f"║  Zerobus Meter Data Streamer (Official SDK)                     ║")
        print(f"╚══════════════════════════════════════════════════════════════════╝")
        print(f"")
        print(f"Configuration:")
        print(f"  Zerobus endpoint: {self.zerobus_endpoint}")
        print(f"  Workspace URL: {self.workspace_url}")
        print(f"  Target table: {self.table_name}")
        print(f"  Service principal: {self.client_id}")
        print(f"")

        if not ZEROBUS_AVAILABLE:
            raise ImportError("Zerobus SDK not installed")

        # Initialize SDK
        self.sdk = ZerobusSdk(self.zerobus_endpoint, self.workspace_url)

        # Create table properties
        self.table_properties = TableProperties(self.table_name)

        # Stream options (using JSON for simplicity)
        self.stream_options = StreamConfigurationOptions(
            record_type=RecordType.JSON,
            max_inflight_records=1000,  # Allow 1000 records in-flight
            recovery_timeout_ms=30000    # 30 second timeout
        )

        # Create stream
        print(f"🔌 Connecting to Zerobus...")
        self.stream = self.sdk.create_stream(
            self.client_id,
            self.client_secret,
            self.table_properties,
            self.stream_options
        )
        print(f"✅ Connected! Stream ready for ingestion.")
        print(f"")

    def generate_meter_reading(
        self,
        meter_id: str,
        meter_type: str,
        timestamp: datetime,
        has_solar: bool = False,
        solar_capacity_kw: float = 0.0
    ) -> Dict[str, Any]:
        """
        Generate a single realistic meter reading

        Returns dict matching Unity Catalog table schema
        """
        hour = timestamp.hour + timestamp.minute / 60.0

        # Load pattern based on meter type
        if meter_type == 'residential':
            base_load = 1.5
            # Morning peak (6-9 AM), evening peak (5-10 PM)
            if 6 <= hour <= 9:
                base_load *= 1.5
            elif 17 <= hour <= 22:
                base_load *= 1.8
            elif 0 <= hour <= 5:
                base_load *= 0.3
        elif meter_type == 'commercial':
            base_load = 15.0 if 8 <= hour <= 18 else 2.0
        else:  # industrial
            base_load = 150.0

        # Add variation
        power_kw = base_load * random.uniform(0.85, 1.15)

        # Solar generation
        if has_solar and 6 <= hour <= 18:
            solar_factor = math.exp(-((hour - 12) ** 2) / 8.0)
            solar_kw = -solar_capacity_kw * solar_factor * random.uniform(0.7, 1.0)
            power_kw += solar_kw

        # Electrical parameters
        voltage_v = 240.0 * random.uniform(0.95, 1.05)
        current_a = (power_kw * 1000) / voltage_v if voltage_v > 0 else 0.0
        power_factor = random.uniform(0.85, 0.98)

        # Data quality
        quality_flag = 'OK'
        is_valid = True
        if random.random() < 0.01:  # 1% missing
            quality_flag = 'MISSING'
            is_valid = False
            power_kw = None
            voltage_v = None
            current_a = None
        elif random.random() < 0.01:  # 1% anomaly
            quality_flag = 'ANOMALY'
            power_kw *= random.uniform(3.0, 5.0)

        return {
            'meter_id': meter_id,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'power_kw': round(power_kw, 3) if power_kw is not None else None,
            'voltage_v': round(voltage_v, 2) if voltage_v is not None else None,
            'current_a': round(current_a, 2) if current_a is not None else None,
            'power_factor': round(power_factor, 3),
            'frequency_hz': 50.0,
            'is_valid': is_valid,
            'quality_flag': quality_flag
        }

    def stream_reading(self, reading: Dict[str, Any], wait_for_ack: bool = False):
        """
        Stream a single reading to Databricks via Zerobus

        Args:
            reading: Meter reading dict
            wait_for_ack: If True, wait for Databricks acknowledgment (durability guarantee)
        """
        # Serialize to JSON
        json_record = json.dumps(reading)

        # Ingest via Zerobus SDK
        ack = self.stream.ingest_record(json_record)

        # Optionally wait for acknowledgment
        if wait_for_ack:
            ack.wait_for_ack()

    def simulate_meter_fleet(
        self,
        num_meters: int = 100,
        duration_minutes: int = 60,
        interval_minutes: int = 5,
        speed_multiplier: int = 1,
        wait_for_acks: bool = False
    ):
        """
        Simulate a fleet of meters continuously streaming data

        Args:
            num_meters: Number of meters to simulate
            duration_minutes: How long to run simulation (simulated time)
            interval_minutes: Reading interval (5 min standard)
            speed_multiplier: Speed up time (60 = 1 hour in 1 minute)
            wait_for_acks: Wait for Databricks acknowledgments (slower but guaranteed)
        """

        print(f"🚀 Starting Meter Fleet Simulation")
        print(f"═══════════════════════════════════════════════════════════════")
        print(f"  Meters: {num_meters:,}")
        print(f"  Duration: {duration_minutes} minutes (simulated)")
        print(f"  Interval: {interval_minutes} minutes")
        print(f"  Speed: {speed_multiplier}x real-time")
        print(f"  Wait for acks: {wait_for_acks}")
        print(f"")

        # Generate meter metadata
        meters = []
        for i in range(num_meters):
            meter_type = random.choices(
                ['residential', 'commercial', 'industrial'],
                weights=[0.70, 0.25, 0.05]
            )[0]

            has_solar = random.random() < (0.30 if meter_type == 'residential' else 0.15)
            solar_capacity = random.uniform(3, 10) if has_solar else 0.0

            meters.append({
                'meter_id': f'NMI{1000000000 + i}',
                'meter_type': meter_type,
                'has_solar': has_solar,
                'solar_capacity_kw': solar_capacity
            })

        print(f"✅ Generated {num_meters:,} meter profiles:")
        print(f"   Residential: {sum(1 for m in meters if m['meter_type'] == 'residential'):,}")
        print(f"   Commercial: {sum(1 for m in meters if m['meter_type'] == 'commercial'):,}")
        print(f"   Industrial: {sum(1 for m in meters if m['meter_type'] == 'industrial'):,}")
        print(f"   With solar: {sum(1 for m in meters if m['has_solar']):,}")
        print(f"")

        # Simulation loop
        current_sim_time = datetime.now()
        num_intervals = duration_minutes // interval_minutes
        total_readings = 0
        total_errors = 0

        start_real_time = time.time()

        print(f"📡 Streaming data to Zerobus... (Ctrl+C to stop)")
        print(f"")

        try:
            for interval_num in range(num_intervals):
                interval_start_time = time.time()
                readings_this_interval = 0

                # Generate and stream readings for all meters at this timestamp
                for meter in meters:
                    reading = self.generate_meter_reading(
                        meter_id=meter['meter_id'],
                        meter_type=meter['meter_type'],
                        timestamp=current_sim_time,
                        has_solar=meter['has_solar'],
                        solar_capacity_kw=meter['solar_capacity_kw']
                    )

                    try:
                        self.stream_reading(reading, wait_for_ack=wait_for_acks)
                        readings_this_interval += 1
                        total_readings += 1
                    except Exception as e:
                        total_errors += 1
                        if total_errors <= 10:  # Only show first 10 errors
                            print(f"❌ Error streaming reading: {e}")

                # Calculate metrics
                interval_elapsed = time.time() - interval_start_time
                throughput = readings_this_interval / interval_elapsed if interval_elapsed > 0 else 0

                # Progress update
                elapsed_real = time.time() - start_real_time
                print(f"[{current_sim_time.strftime('%Y-%m-%d %H:%M:%S')}] "
                      f"✓ {readings_this_interval:,} readings | "
                      f"{throughput:.0f} rec/sec | "
                      f"Total: {total_readings:,} | "
                      f"Interval {interval_num + 1}/{num_intervals} | "
                      f"Real: {elapsed_real:.1f}s")

                # Advance simulated time
                current_sim_time += timedelta(minutes=interval_minutes)

                # Wait for next interval (respecting speed multiplier)
                wait_time = (interval_minutes * 60 / speed_multiplier) - interval_elapsed
                if wait_time > 0:
                    time.sleep(wait_time)

        except KeyboardInterrupt:
            print(f"\n⚠️  Stream interrupted by user")

        # Summary
        total_elapsed = time.time() - start_real_time
        avg_throughput = total_readings / total_elapsed if total_elapsed > 0 else 0

        print(f"")
        print(f"╔══════════════════════════════════════════════════════════════════╗")
        print(f"║  STREAMING COMPLETE                                              ║")
        print(f"╚══════════════════════════════════════════════════════════════════╝")
        print(f"")
        print(f"📊 Statistics:")
        print(f"   Total readings streamed: {total_readings:,}")
        print(f"   Errors: {total_errors}")
        print(f"   Simulated time: {duration_minutes} minutes ({num_intervals} intervals)")
        print(f"   Real time elapsed: {total_elapsed:.1f} seconds")
        print(f"   Average throughput: {avg_throughput:.0f} readings/second")
        print(f"")
        print(f"✅ Data now queryable in Databricks:")
        print(f"   SELECT COUNT(*) FROM {self.table_name};")
        print(f"")

    def close(self):
        """Close the Zerobus stream"""
        if hasattr(self, 'stream') and self.stream:
            self.stream.close()
            print("🔌 Zerobus stream closed")

def main():
    parser = argparse.ArgumentParser(
        description='Stream meter data to Databricks using Zerobus SDK',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Prerequisites:
  1. Install Zerobus SDK: pip install databricks-zerobus-ingest-sdk
  2. Create service principal in Databricks
  3. Grant permissions: USE CATALOG, USE SCHEMA, MODIFY, SELECT
  4. Create target table in Unity Catalog

Example:
  python zerobus_meter_streamer.py \\
      --workspace-id 1234567890123456 \\
      --workspace-url https://dbc-xxx.cloud.databricks.com \\
      --region us-west-2 \\
      --client-id <service-principal-client-id> \\
      --client-secret <service-principal-secret> \\
      --catalog energy_australia_demo \\
      --schema bronze \\
      --table live_meter_readings \\
      --num-meters 1000 \\
      --duration 60 \\
      --speed 12

Environment Variables (alternative to flags):
  DATABRICKS_WORKSPACE_ID
  DATABRICKS_WORKSPACE_URL
  DATABRICKS_REGION
  DATABRICKS_CLIENT_ID
  DATABRICKS_CLIENT_SECRET
        """
    )

    # Databricks configuration
    parser.add_argument('--workspace-id', help='Databricks workspace ID (from URL: /o=<id>)')
    parser.add_argument('--workspace-url', help='Full workspace URL')
    parser.add_argument('--region', help='AWS/Azure region (e.g., us-west-2, eastus2)')
    parser.add_argument('--client-id', help='Service principal client ID')
    parser.add_argument('--client-secret', help='Service principal client secret')

    # Table configuration
    parser.add_argument('--catalog', default='energy_australia_demo', help='Unity Catalog catalog')
    parser.add_argument('--schema', default='bronze', help='Schema name')
    parser.add_argument('--table', default='live_meter_readings', help='Table name')

    # Simulation parameters
    parser.add_argument('--num-meters', type=int, default=100, help='Number of meters to simulate')
    parser.add_argument('--duration', type=int, default=60, help='Duration in minutes (simulated time)')
    parser.add_argument('--interval', type=int, default=5, help='Reading interval in minutes')
    parser.add_argument('--speed', type=int, default=1, help='Speed multiplier (60 = 1 hour in 1 minute)')
    parser.add_argument('--wait-acks', action='store_true', help='Wait for Databricks acknowledgments')

    args = parser.parse_args()

    # Get configuration from args or environment
    workspace_id = args.workspace_id or os.getenv('DATABRICKS_WORKSPACE_ID')
    workspace_url = args.workspace_url or os.getenv('DATABRICKS_WORKSPACE_URL')
    region = args.region or os.getenv('DATABRICKS_REGION')
    client_id = args.client_id or os.getenv('DATABRICKS_CLIENT_ID')
    client_secret = args.client_secret or os.getenv('DATABRICKS_CLIENT_SECRET')

    # Validate required parameters
    if not all([workspace_id, workspace_url, region, client_id, client_secret]):
        parser.error("Missing required configuration. Set via flags or environment variables.")

    # Initialize streamer
    streamer = MeterDataStreamer(
        workspace_id=workspace_id,
        workspace_url=workspace_url,
        region=region,
        client_id=client_id,
        client_secret=client_secret,
        catalog=args.catalog,
        schema=args.schema,
        table=args.table
    )

    try:
        # Run simulation
        streamer.simulate_meter_fleet(
            num_meters=args.num_meters,
            duration_minutes=args.duration,
            interval_minutes=args.interval,
            speed_multiplier=args.speed,
            wait_for_acks=args.wait_acks
        )
    finally:
        streamer.close()

if __name__ == '__main__':
    main()
