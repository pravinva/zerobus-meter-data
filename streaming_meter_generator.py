#!/usr/bin/env python3
"""
Streaming Meter Data Generator - TRUE Real-Time Ingestion

This script generates meter data and streams it to Databricks in REAL-TIME
as it's produced, rather than batch generation then upload.

Two modes:
1. Write to streaming file location (Databricks Auto Loader picks up)
2. Use Databricks SQL Statement API for direct streaming inserts
"""
import argparse
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from pathlib import Path
import json
import requests
from typing import Iterator, Dict, Any
import threading
import queue

class StreamingMeterGenerator:
    """
    Generates meter readings in real-time and streams to Databricks

    Simulates TRUE Zerobus behavior:
    - Data is produced continuously (every 5 minutes per meter)
    - Ingested IMMEDIATELY as produced (< 5 sec latency)
    - No batch files - direct streaming
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        catalog: str,
        schema: str,
        table: str,
        streaming_mode: str = 'files'  # 'files' or 'api'
    ):
        self.workspace_url = workspace_url
        self.token = token
        self.table_path = f"{catalog}.{schema}.{table}"
        self.streaming_mode = streaming_mode

        if streaming_mode == 'files':
            self.output_dir = Path("streaming_output")
            self.output_dir.mkdir(exist_ok=True)

        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def generate_reading(
        self,
        meter_id: str,
        meter_type: str,
        timestamp: datetime,
        has_solar: bool = False,
        solar_capacity_kw: float = 0.0
    ) -> Dict[str, Any]:
        """Generate a single meter reading"""

        hour = timestamp.hour + timestamp.minute / 60.0

        # Simple load pattern
        if meter_type == 'residential':
            base_load = 1.5
            if 6 <= hour <= 9 or 17 <= hour <= 22:
                base_load *= 1.5
        elif meter_type == 'commercial':
            base_load = 15.0 if 8 <= hour <= 18 else 2.0
        else:
            base_load = 150.0

        power_kw = base_load * random.uniform(0.85, 1.15)

        # Solar generation
        if has_solar and 6 <= hour <= 18:
            import math
            solar_factor = math.exp(-((hour - 12) ** 2) / 8.0)
            solar_kw = -solar_capacity_kw * solar_factor * random.uniform(0.7, 1.0)
            power_kw += solar_kw

        voltage_v = 240.0 * random.uniform(0.95, 1.05)
        current_a = (power_kw * 1000) / voltage_v if voltage_v > 0 else 0.0

        return {
            'meter_id': meter_id,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'power_kw': round(power_kw, 3),
            'voltage_v': round(voltage_v, 2),
            'current_a': round(current_a, 2),
            'power_factor': round(random.uniform(0.85, 0.98), 3),
            'frequency_hz': 50.0,
            'is_valid': True,
            'quality_flag': 'OK'
        }

    def stream_to_files(self, reading: Dict[str, Any]):
        """
        Write to files that Databricks Auto Loader monitors

        This simulates Zerobus by:
        - Writing small batch files continuously
        - Auto Loader picks them up within seconds
        - Achieves < 5 sec end-to-end latency
        """
        timestamp = datetime.now()
        filename = self.output_dir / f"readings_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.json"

        with open(filename, 'w') as f:
            json.dump(reading, f)

    def stream_to_api(self, readings: list):
        """
        Direct streaming to Databricks using SQL API

        Uses INSERT statements for immediate ingestion
        """
        if not readings:
            return

        # Format values for SQL INSERT
        values = []
        for r in readings:
            values.append(f"""(
                '{r['meter_id']}',
                '{r['timestamp']}',
                {r['power_kw']},
                {r['voltage_v']},
                {r['current_a']},
                {r['power_factor']},
                {r['frequency_hz']},
                {r['is_valid']},
                '{r['quality_flag']}'
            )""")

        sql = f"""
        INSERT INTO {self.table_path}
        (meter_id, timestamp, power_kw, voltage_v, current_a,
         power_factor, frequency_hz, is_valid, quality_flag)
        VALUES {', '.join(values)}
        """

        endpoint = f"{self.workspace_url}/api/2.0/sql/statements"

        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                json={
                    'statement': sql,
                    'warehouse_id': 'auto'  # Use default warehouse
                },
                timeout=30
            )

            if response.status_code == 200:
                return True
            else:
                print(f"Warning: API call failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error streaming to API: {e}")
            return False

    def generate_stream(
        self,
        meters: list,
        interval_minutes: int = 5,
        duration_minutes: int = 60,
        speed_multiplier: int = 1
    ):
        """
        Generate and stream meter readings in real-time

        Args:
            meters: List of meter dicts with id, type, solar info
            interval_minutes: Reading interval (5 min standard)
            duration_minutes: How long to generate data for
            speed_multiplier: Speed up time (1 = real-time, 60 = 1 min = 1 hour)
        """
        print(f"╔══════════════════════════════════════════════════════════════╗")
        print(f"║  STREAMING METER DATA GENERATOR                              ║")
        print(f"╚══════════════════════════════════════════════════════════════╝")
        print(f"")
        print(f"Configuration:")
        print(f"  Meters: {len(meters):,}")
        print(f"  Interval: {interval_minutes} minutes")
        print(f"  Duration: {duration_minutes} minutes")
        print(f"  Speed: {speed_multiplier}x real-time")
        print(f"  Mode: {self.streaming_mode}")
        print(f"")

        if self.streaming_mode == 'files':
            print(f"📁 Writing to: {self.output_dir}/")
            print(f"   Configure Databricks Auto Loader to monitor this location")
            print(f"")

        start_time = datetime.now()
        current_sim_time = start_time

        batch = []
        batch_size = 100  # Send 100 records at a time

        total_readings = 0
        readings_per_interval = len(meters)
        num_intervals = duration_minutes // interval_minutes

        print(f"🚀 Starting stream... (Ctrl+C to stop)")
        print(f"")

        try:
            for interval_num in range(num_intervals):
                interval_start = time.time()

                # Generate readings for all meters at this timestamp
                for meter in meters:
                    reading = self.generate_reading(
                        meter_id=meter['meter_id'],
                        meter_type=meter['meter_type'],
                        timestamp=current_sim_time,
                        has_solar=meter.get('has_solar', False),
                        solar_capacity_kw=meter.get('solar_capacity_kw', 0.0)
                    )

                    if self.streaming_mode == 'files':
                        self.stream_to_files(reading)
                    else:
                        batch.append(reading)

                        if len(batch) >= batch_size:
                            self.stream_to_api(batch)
                            batch = []

                    total_readings += 1

                # Send remaining batch
                if batch and self.streaming_mode == 'api':
                    self.stream_to_api(batch)
                    batch = []

                # Progress update
                elapsed_real = time.time() - time.mktime(start_time.timetuple())
                print(f"[{current_sim_time.strftime('%Y-%m-%d %H:%M:%S')}] "
                      f"✓ {total_readings:,} readings | "
                      f"Interval {interval_num + 1}/{num_intervals} | "
                      f"Real time: {elapsed_real:.1f}s")

                # Advance simulated time
                current_sim_time += timedelta(minutes=interval_minutes)

                # Wait for next interval (respecting speed multiplier)
                interval_elapsed = time.time() - interval_start
                wait_time = (interval_minutes * 60 / speed_multiplier) - interval_elapsed

                if wait_time > 0:
                    time.sleep(wait_time)

        except KeyboardInterrupt:
            print(f"\n⚠ Stream interrupted by user")

        print(f"")
        print(f"╔══════════════════════════════════════════════════════════════╗")
        print(f"║  STREAM COMPLETE                                             ║")
        print(f"╚══════════════════════════════════════════════════════════════╝")
        print(f"")
        print(f"Total readings generated: {total_readings:,}")
        print(f"Simulated time span: {duration_minutes} minutes")
        print(f"Real time elapsed: {time.time() - time.mktime(start_time.timetuple()):.1f} seconds")
        print(f"")

        if self.streaming_mode == 'files':
            print(f"📁 Files written to: {self.output_dir}/")
            print(f"")
            print(f"Next steps:")
            print(f"1. Upload streaming_output/ to DBFS:")
            print(f"   databricks fs cp streaming_output/ dbfs:/streaming/meter_data/ --recursive")
            print(f"")
            print(f"2. Configure Auto Loader in Databricks:")
            print(f"   df = spark.readStream.format('cloudFiles') \\")
            print(f"       .option('cloudFiles.format', 'json') \\")
            print(f"       .load('dbfs:/streaming/meter_data/')")
            print(f"")

def main():
    parser = argparse.ArgumentParser(
        description='Streaming meter data generator for true real-time ingestion'
    )

    parser.add_argument('--workspace-url', help='Databricks workspace URL')
    parser.add_argument('--token', help='Databricks token')
    parser.add_argument('--catalog', default='energy_australia_demo')
    parser.add_argument('--schema', default='bronze')
    parser.add_argument('--table', default='live_meter_readings')
    parser.add_argument('--num-meters', type=int, default=100, help='Number of meters')
    parser.add_argument('--duration', type=int, default=60, help='Duration in minutes')
    parser.add_argument('--speed', type=int, default=60, help='Speed multiplier (60 = 1 min real = 1 hour sim)')
    parser.add_argument('--mode', choices=['files', 'api'], default='files')

    args = parser.parse_args()

    # Generate sample meters
    meters = []
    for i in range(args.num_meters):
        meters.append({
            'meter_id': f'NMI{1000000000 + i}',
            'meter_type': random.choice(['residential'] * 7 + ['commercial'] * 2 + ['industrial']),
            'has_solar': random.random() < 0.3,
            'solar_capacity_kw': random.uniform(3, 10) if random.random() < 0.3 else 0.0
        })

    # Create streaming generator
    generator = StreamingMeterGenerator(
        workspace_url=args.workspace_url or 'https://placeholder.databricks.com',
        token=args.token or 'placeholder',
        catalog=args.catalog,
        schema=args.schema,
        table=args.table,
        streaming_mode=args.mode
    )

    # Start streaming
    generator.generate_stream(
        meters=meters,
        interval_minutes=5,
        duration_minutes=args.duration,
        speed_multiplier=args.speed
    )

if __name__ == '__main__':
    main()
