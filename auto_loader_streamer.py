#!/usr/bin/env python3
"""
Auto Loader Meter Data Streamer
Alternative to Zerobus - Uses PAT instead of OAuth service principal

This streams meter data continuously via Auto Loader:
- Generates readings → Writes JSON files → Auto Loader → Delta Lake
- < 10 second latency (vs Zerobus < 5 sec)
- Uses Personal Access Token (much easier than OAuth!)
- Production-quality solution
"""
import argparse
import json
import os
import time
import random
import math
from datetime import datetime, timedelta
from pathlib import Path
import subprocess
from typing import Dict, Any, List

class AutoLoaderMeterStreamer:
    """
    Streams meter data via Auto Loader (PAT-based alternative to Zerobus)

    Architecture:
    Generate → Write JSON → DBFS → Auto Loader → Delta Lake
                            (uploads via Databricks CLI)
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        output_dir: str = "streaming_output",
        dbfs_path: str = "dbfs:/streaming/meter_data/"
    ):
        """
        Initialize Auto Loader streamer

        Args:
            workspace_url: Databricks workspace URL
            token: Personal Access Token (PAT)
            output_dir: Local directory for JSON files
            dbfs_path: DBFS path where Auto Loader monitors
        """
        self.workspace_url = workspace_url
        self.token = token
        self.output_dir = Path(output_dir)
        self.dbfs_path = dbfs_path

        # Create output directory
        self.output_dir.mkdir(exist_ok=True, parents=True)

        print(f"╔══════════════════════════════════════════════════════════════════╗")
        print(f"║  Auto Loader Meter Data Streamer (PAT Authentication)           ║")
        print(f"╚══════════════════════════════════════════════════════════════════╝")
        print(f"")
        print(f"Configuration:")
        print(f"  Workspace URL: {self.workspace_url}")
        print(f"  Auth method: Personal Access Token (PAT)")
        print(f"  Local output: {self.output_dir}/")
        print(f"  DBFS path: {self.dbfs_path}")
        print(f"")
        print(f"⚡ Auto Loader will pick up files with < 10 second latency")
        print(f"")

        # Configure Databricks CLI
        self._configure_databricks_cli()

    def _configure_databricks_cli(self):
        """Configure Databricks CLI with workspace and token"""
        # Set environment variables for CLI
        os.environ['DATABRICKS_HOST'] = self.workspace_url
        os.environ['DATABRICKS_TOKEN'] = self.token

        print(f"✓ Databricks CLI configured")

    def generate_meter_reading(
        self,
        meter_id: str,
        meter_type: str,
        timestamp: datetime,
        has_solar: bool = False,
        solar_capacity_kw: float = 0.0
    ) -> Dict[str, Any]:
        """Generate a single realistic meter reading"""

        hour = timestamp.hour + timestamp.minute / 60.0

        # Load pattern based on meter type
        if meter_type == 'residential':
            base_load = 1.5
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

        power_kw = base_load * random.uniform(0.85, 1.15)

        # Solar generation
        if has_solar and 6 <= hour <= 18:
            solar_factor = math.exp(-((hour - 12) ** 2) / 8.0)
            solar_kw = -solar_capacity_kw * solar_factor * random.uniform(0.7, 1.0)
            power_kw += solar_kw

        voltage_v = 240.0 * random.uniform(0.95, 1.05)
        current_a = (power_kw * 1000) / voltage_v if voltage_v > 0 else 0.0
        power_factor = random.uniform(0.85, 0.98)

        # Data quality
        quality_flag = 'OK'
        is_valid = True
        if random.random() < 0.01:
            quality_flag = 'MISSING'
            is_valid = False
            power_kw = None
            voltage_v = None
            current_a = None

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

    def write_reading(self, reading: Dict[str, Any]):
        """Write reading to local JSON file"""
        timestamp = datetime.now()
        filename = self.output_dir / f"reading_{timestamp.strftime('%Y%m%d_%H%M%S_%f')}.json"

        with open(filename, 'w') as f:
            json.dump(reading, f)

    def upload_to_dbfs(self):
        """Upload local files to DBFS for Auto Loader to pick up"""
        try:
            # Use Databricks CLI to copy files
            cmd = [
                "databricks", "fs", "cp",
                str(self.output_dir),
                self.dbfs_path,
                "--recursive",
                "--overwrite"
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode == 0:
                # Clean up local files after successful upload
                for file in self.output_dir.glob("*.json"):
                    file.unlink()
                return True
            else:
                print(f"⚠️  Upload warning: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Upload error: {e}")
            return False

    def simulate_meter_fleet(
        self,
        num_meters: int = 100,
        duration_minutes: int = 60,
        interval_minutes: int = 5,
        speed_multiplier: int = 1,
        upload_frequency: int = 100
    ):
        """
        Simulate meter fleet with Auto Loader streaming

        Args:
            num_meters: Number of meters to simulate
            duration_minutes: Duration (simulated time)
            interval_minutes: Reading interval
            speed_multiplier: Speed up time
            upload_frequency: Upload to DBFS every N readings
        """
        print(f"🚀 Starting Meter Fleet Simulation (Auto Loader Mode)")
        print(f"═══════════════════════════════════════════════════════════════")
        print(f"  Meters: {num_meters:,}")
        print(f"  Duration: {duration_minutes} minutes (simulated)")
        print(f"  Interval: {interval_minutes} minutes")
        print(f"  Speed: {speed_multiplier}x real-time")
        print(f"  Upload every: {upload_frequency} readings")
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

        print(f"✅ Generated {num_meters:,} meter profiles")
        print(f"")

        # Simulation loop
        current_sim_time = datetime.now()
        num_intervals = duration_minutes // interval_minutes
        total_readings = 0
        total_uploads = 0

        start_real_time = time.time()

        print(f"📡 Streaming data via Auto Loader... (Ctrl+C to stop)")
        print(f"")

        try:
            for interval_num in range(num_intervals):
                interval_start_time = time.time()
                readings_this_interval = 0

                # Generate readings for all meters
                for meter in meters:
                    reading = self.generate_meter_reading(
                        meter_id=meter['meter_id'],
                        meter_type=meter['meter_type'],
                        timestamp=current_sim_time,
                        has_solar=meter['has_solar'],
                        solar_capacity_kw=meter['solar_capacity_kw']
                    )

                    self.write_reading(reading)
                    readings_this_interval += 1
                    total_readings += 1

                    # Upload to DBFS periodically
                    if total_readings % upload_frequency == 0:
                        upload_success = self.upload_to_dbfs()
                        if upload_success:
                            total_uploads += 1
                            print(f"  ↑ Uploaded batch {total_uploads} to DBFS")

                # Calculate metrics
                interval_elapsed = time.time() - interval_start_time
                throughput = readings_this_interval / interval_elapsed if interval_elapsed > 0 else 0

                # Progress update
                elapsed_real = time.time() - start_real_time
                print(f"[{current_sim_time.strftime('%Y-%m-%d %H:%M:%S')}] "
                      f"✓ {readings_this_interval:,} readings | "
                      f"{throughput:.0f} rec/sec | "
                      f"Total: {total_readings:,} | "
                      f"Uploads: {total_uploads} | "
                      f"Interval {interval_num + 1}/{num_intervals}")

                # Advance simulated time
                current_sim_time += timedelta(minutes=interval_minutes)

                # Wait for next interval
                wait_time = (interval_minutes * 60 / speed_multiplier) - interval_elapsed
                if wait_time > 0:
                    time.sleep(wait_time)

        except KeyboardInterrupt:
            print(f"\n⚠️  Stream interrupted by user")

        # Final upload
        print(f"\n📤 Uploading final batch...")
        self.upload_to_dbfs()
        total_uploads += 1

        # Summary
        total_elapsed = time.time() - start_real_time

        print(f"")
        print(f"╔══════════════════════════════════════════════════════════════════╗")
        print(f"║  STREAMING COMPLETE                                              ║")
        print(f"╚══════════════════════════════════════════════════════════════════╝")
        print(f"")
        print(f"📊 Statistics:")
        print(f"   Total readings generated: {total_readings:,}")
        print(f"   Total uploads to DBFS: {total_uploads}")
        print(f"   Real time elapsed: {total_elapsed:.1f} seconds")
        print(f"")
        print(f"🔍 Next Steps:")
        print(f"   1. Ensure Auto Loader is running in Databricks notebook")
        print(f"   2. Query: SELECT COUNT(*) FROM energy_australia_demo.bronze.live_meter_readings;")
        print(f"   3. Data should appear within 10 seconds of upload")
        print(f"")

def main():
    parser = argparse.ArgumentParser(
        description='Stream meter data via Auto Loader (PAT authentication)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Prerequisites:
  1. Install Databricks CLI: pip install databricks-cli
  2. Get Personal Access Token from Databricks UI
  3. Set up Auto Loader in Databricks notebook (see ALTERNATIVES_TO_OAUTH.md)

Environment Variables:
  DATABRICKS_HOST      - Workspace URL
  DATABRICKS_TOKEN     - Personal Access Token

Example:
  python auto_loader_streamer.py \\
      --num-meters 1000 \\
      --duration 60 \\
      --speed 12 \\
      --upload-freq 100
        """
    )

    parser.add_argument('--workspace-url', help='Databricks workspace URL')
    parser.add_argument('--token', help='Personal Access Token')
    parser.add_argument('--num-meters', type=int, default=100, help='Number of meters')
    parser.add_argument('--duration', type=int, default=60, help='Duration in minutes')
    parser.add_argument('--interval', type=int, default=5, help='Reading interval')
    parser.add_argument('--speed', type=int, default=1, help='Speed multiplier')
    parser.add_argument('--upload-freq', type=int, default=100, help='Upload every N readings')
    parser.add_argument('--output-dir', default='streaming_output', help='Local output directory')
    parser.add_argument('--dbfs-path', default='dbfs:/streaming/meter_data/', help='DBFS path')

    args = parser.parse_args()

    # Get configuration
    workspace_url = args.workspace_url or os.getenv('DATABRICKS_HOST')
    token = args.token or os.getenv('DATABRICKS_TOKEN')

    if not workspace_url or not token:
        parser.error("Missing workspace URL or token. Set via flags or environment variables.")

    # Initialize streamer
    streamer = AutoLoaderMeterStreamer(
        workspace_url=workspace_url,
        token=token,
        output_dir=args.output_dir,
        dbfs_path=args.dbfs_path
    )

    # Run simulation
    streamer.simulate_meter_fleet(
        num_meters=args.num_meters,
        duration_minutes=args.duration,
        interval_minutes=args.interval,
        speed_multiplier=args.speed,
        upload_frequency=args.upload_freq
    )

if __name__ == '__main__':
    main()
