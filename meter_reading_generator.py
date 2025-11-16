#!/usr/bin/env python3
"""
Meter Reading Generator for Zerobus Demo
Generates realistic 5-minute interval meter readings with:
- Seasonal and daily load patterns
- Solar generation curves
- Data quality issues (2% missing/anomalous)
- Australian grid parameters (240V, 50Hz)
"""
import argparse
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import multiprocessing as mp
from pathlib import Path
import math

# Constants for Australian electrical grid
VOLTAGE_NOMINAL = 240.0  # Volts (Australian standard)
FREQUENCY = 50.0  # Hz (Australian grid frequency)
READING_INTERVAL_MINUTES = 5  # Standard smart meter interval
READINGS_PER_DAY = 24 * 60 // READING_INTERVAL_MINUTES  # 288 readings per day

# Data quality parameters
MISSING_DATA_RATE = 0.01  # 1% missing readings
ANOMALY_RATE = 0.01  # 1% anomalous readings

def generate_daily_load_pattern(meter_type: str, hour: float) -> float:
    """
    Generate realistic load factor based on time of day

    Args:
        meter_type: 'residential', 'commercial', or 'industrial'
        hour: Hour of day (0-24, fractional)

    Returns:
        Load factor (0.0 - 2.0)
    """
    if meter_type == 'residential':
        # Morning peak (6-9 AM), evening peak (5-10 PM), low overnight
        morning_peak = 1.5 * math.exp(-((hour - 7.5) ** 2) / 2.0)
        evening_peak = 1.8 * math.exp(-((hour - 19.0) ** 2) / 3.0)
        baseline = 0.3
        return baseline + morning_peak + evening_peak

    elif meter_type == 'commercial':
        # Business hours (8 AM - 6 PM), low overnight
        if 8 <= hour <= 18:
            return 1.2 + 0.3 * math.sin((hour - 8) * math.pi / 10)
        else:
            return 0.2

    else:  # industrial
        # Steady 24/7 with slight variation
        return 0.9 + 0.2 * math.sin(hour * math.pi / 12)

def generate_solar_generation(
    hour: float,
    solar_capacity_kw: float,
    season_factor: float = 1.0
) -> float:
    """
    Generate solar generation curve (dawn to dusk)

    Args:
        hour: Hour of day (0-24, fractional)
        solar_capacity_kw: Installed solar capacity in kW
        season_factor: Seasonal adjustment (0.7 winter - 1.2 summer)

    Returns:
        Solar generation in kW (negative value for export)
    """
    if solar_capacity_kw == 0:
        return 0.0

    # Solar generation hours: 6 AM - 6 PM (peak at noon)
    if hour < 6 or hour > 18:
        return 0.0

    # Bell curve centered at noon
    peak_hour = 12.0
    solar_factor = math.exp(-((hour - peak_hour) ** 2) / 8.0)

    # Add some randomness for cloud cover
    cloud_factor = random.uniform(0.7, 1.0)

    generation = solar_capacity_kw * solar_factor * cloud_factor * season_factor

    return -generation  # Negative for export to grid

def generate_season_factor(date: datetime) -> float:
    """
    Generate seasonal factor for load and solar

    Args:
        date: Date for reading

    Returns:
        Seasonal factor (0.8 winter - 1.2 summer in Australia)
    """
    # Australian seasons (Southern Hemisphere):
    # Summer: Dec-Feb, Autumn: Mar-May, Winter: Jun-Aug, Spring: Sep-Nov
    month = date.month

    if month in [12, 1, 2]:  # Summer
        return 1.2
    elif month in [3, 4, 5]:  # Autumn
        return 1.0
    elif month in [6, 7, 8]:  # Winter
        return 0.8
    else:  # Spring
        return 1.0

def generate_readings_for_meter(
    meter_id: str,
    meter_type: str,
    avg_demand_kw: float,
    has_solar: bool,
    solar_capacity_kw: float,
    start_date: datetime,
    num_days: int
) -> list:
    """
    Generate time-series readings for a single meter

    Args:
        meter_id: Unique meter identifier
        meter_type: 'residential', 'commercial', or 'industrial'
        avg_demand_kw: Average demand in kW
        has_solar: Whether meter has solar
        solar_capacity_kw: Solar capacity in kW
        start_date: Start date for readings
        num_days: Number of days to generate

    Returns:
        List of reading dictionaries
    """
    readings = []
    current_time = start_date

    total_readings = num_days * READINGS_PER_DAY

    for i in range(total_readings):
        # Calculate hour of day
        hour = current_time.hour + current_time.minute / 60.0

        # Get seasonal factor
        season_factor = generate_season_factor(current_time)

        # Calculate base load
        load_pattern = generate_daily_load_pattern(meter_type, hour)
        demand_kw = avg_demand_kw * load_pattern * season_factor

        # Add random variation
        demand_kw *= random.uniform(0.85, 1.15)

        # Calculate solar generation
        solar_kw = 0.0
        if has_solar:
            solar_kw = generate_solar_generation(hour, solar_capacity_kw, season_factor)

        # Net power (demand - solar generation)
        power_kw = demand_kw + solar_kw  # solar_kw is negative

        # Calculate voltage (with small variations)
        voltage_v = VOLTAGE_NOMINAL * random.uniform(0.95, 1.05)

        # Calculate current from power and voltage
        current_a = (power_kw * 1000) / voltage_v if voltage_v > 0 else 0.0

        # Calculate power factor (typical range 0.85 - 0.98)
        power_factor = random.uniform(0.85, 0.98)

        # Data quality simulation
        is_valid = True
        quality_flag = 'OK'

        # Simulate missing data
        if random.random() < MISSING_DATA_RATE:
            is_valid = False
            quality_flag = 'MISSING'
            power_kw = None
            voltage_v = None
            current_a = None

        # Simulate anomalies
        elif random.random() < ANOMALY_RATE:
            quality_flag = 'ANOMALY'
            # Spike or drop
            if random.random() < 0.5:
                power_kw *= random.uniform(3.0, 5.0)  # Spike
            else:
                power_kw *= random.uniform(0.1, 0.3)  # Drop

        reading = {
            'meter_id': meter_id,
            'timestamp': current_time,
            'power_kw': round(power_kw, 3) if power_kw is not None else None,
            'voltage_v': round(voltage_v, 2) if voltage_v is not None else None,
            'current_a': round(current_a, 2) if current_a is not None else None,
            'power_factor': round(power_factor, 3),
            'frequency_hz': FREQUENCY,
            'is_valid': is_valid,
            'quality_flag': quality_flag,
        }

        readings.append(reading)

        # Move to next interval
        current_time += timedelta(minutes=READING_INTERVAL_MINUTES)

    return readings

def generate_readings_for_meters_batch(args):
    """
    Worker function for parallel processing

    Args:
        args: Tuple of (meters_batch, start_date, num_days, batch_id)

    Returns:
        DataFrame with readings
    """
    meters_batch, start_date, num_days, batch_id = args

    print(f"  [Batch {batch_id}] Processing {len(meters_batch)} meters...")

    all_readings = []

    for idx, meter in enumerate(meters_batch):
        readings = generate_readings_for_meter(
            meter_id=meter['meter_id'],
            meter_type=meter['meter_type'],
            avg_demand_kw=meter['avg_demand_kw'],
            has_solar=meter['has_solar'],
            solar_capacity_kw=meter['solar_capacity_kw'],
            start_date=start_date,
            num_days=num_days
        )
        all_readings.extend(readings)

        if (idx + 1) % 100 == 0:
            print(f"  [Batch {batch_id}] Processed {idx + 1}/{len(meters_batch)} meters...")

    df = pd.DataFrame(all_readings)
    print(f"  [Batch {batch_id}] Complete: {len(df):,} readings generated")

    return df

def generate_meter_readings(
    metadata_path: str,
    start_date: datetime,
    num_days: int,
    output_path: str,
    batch_size: int = 1000,
    workers: int = 4
):
    """
    Generate meter readings from metadata

    Args:
        metadata_path: Path to meter metadata parquet file
        start_date: Start date for readings
        num_days: Number of days to generate
        output_path: Output path (file or directory for partitioned output)
        batch_size: Number of meters to process per batch
        workers: Number of parallel workers
    """
    print(f"Loading meter metadata from {metadata_path}...")
    metadata_df = pd.read_parquet(metadata_path)

    num_meters = len(metadata_df)
    total_readings = num_meters * num_days * READINGS_PER_DAY

    print(f"\nConfiguration:")
    print(f"  Meters: {num_meters:,}")
    print(f"  Date range: {start_date.date()} to {(start_date + timedelta(days=num_days-1)).date()}")
    print(f"  Days: {num_days}")
    print(f"  Interval: {READING_INTERVAL_MINUTES} minutes")
    print(f"  Expected readings: {total_readings:,}")
    print(f"  Workers: {workers}")
    print(f"  Batch size: {batch_size} meters/batch")

    # Add average demand based on meter type
    meter_type_demand = {
        'residential': 1.5,
        'commercial': 15.0,
        'industrial': 150.0,
    }
    metadata_df['avg_demand_kw'] = metadata_df['meter_type'].map(meter_type_demand)

    # Prepare batches
    meters_list = metadata_df.to_dict('records')
    batches = []

    for i in range(0, len(meters_list), batch_size):
        batch = meters_list[i:i + batch_size]
        batches.append((batch, start_date, num_days, i // batch_size + 1))

    print(f"\nGenerating readings using {workers} workers...")
    print(f"Total batches: {len(batches)}")

    # Process in parallel
    if workers > 1:
        with mp.Pool(processes=workers) as pool:
            results = pool.map(generate_readings_for_meters_batch, batches)
    else:
        results = [generate_readings_for_meters_batch(batch) for batch in batches]

    # Combine results
    print(f"\nCombining results...")
    combined_df = pd.concat(results, ignore_index=True)

    print(f"\n✓ Generated {len(combined_df):,} readings")

    # Save output
    output_path_obj = Path(output_path)

    if output_path_obj.suffix == '.parquet':
        # Single file output
        print(f"\nSaving to {output_path}...")
        combined_df.to_parquet(output_path, index=False, compression='snappy')

        import os
        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"  Size: {file_size_mb:.2f} MB")

    else:
        # Partitioned output (directory)
        output_path_obj.mkdir(parents=True, exist_ok=True)
        print(f"\nSaving to partitioned directory {output_path}...")

        # Partition by date for efficient querying
        combined_df['date'] = combined_df['timestamp'].dt.date

        for date, group in combined_df.groupby('date'):
            output_file = output_path_obj / f"readings_date={date}.parquet"
            group.drop('date', axis=1).to_parquet(output_file, index=False, compression='snappy')
            print(f"  Saved {date}: {len(group):,} readings")

    # Statistics
    print(f"\n=== Statistics ===")
    print(f"Total readings: {len(combined_df):,}")
    print(f"Valid readings: {len(combined_df[combined_df.is_valid]):,}")
    print(f"Missing: {len(combined_df[combined_df.quality_flag == 'MISSING']):,} ({len(combined_df[combined_df.quality_flag == 'MISSING'])/len(combined_df)*100:.2f}%)")
    print(f"Anomalies: {len(combined_df[combined_df.quality_flag == 'ANOMALY']):,} ({len(combined_df[combined_df.quality_flag == 'ANOMALY'])/len(combined_df)*100:.2f}%)")
    print(f"Average power: {combined_df[combined_df.is_valid]['power_kw'].mean():.2f} kW")
    print(f"Max power: {combined_df[combined_df.is_valid]['power_kw'].max():.2f} kW")

def main():
    parser = argparse.ArgumentParser(
        description='Generate meter readings for Zerobus demo',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 1 day of readings for 10K meters (test)
  python meter_reading_generator.py \\
      --metadata metadata.parquet \\
      --start-date 2025-11-16 \\
      --days 1 \\
      --output meter_readings_test.parquet

  # Generate 7 days for 100K meters (full demo, partitioned output)
  python meter_reading_generator.py \\
      --metadata metadata_100k.parquet \\
      --start-date 2025-11-09 \\
      --days 7 \\
      --output meter_readings_100k_7d/ \\
      --batch-size 1000 \\
      --workers 16
        """
    )

    parser.add_argument(
        '--metadata',
        type=str,
        required=True,
        help='Path to meter metadata parquet file'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date (YYYY-MM-DD format)'
    )

    parser.add_argument(
        '--days',
        type=int,
        required=True,
        help='Number of days to generate'
    )

    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='Output path (file.parquet for single file, directory/ for partitioned)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of meters per batch (default: 1000)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )

    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    args = parser.parse_args()

    # Set random seed
    random.seed(args.seed)
    np.random.seed(args.seed)

    # Parse start date
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')

    # Generate readings
    start_time = datetime.now()

    generate_meter_readings(
        metadata_path=args.metadata,
        start_date=start_date,
        num_days=args.days,
        output_path=args.output,
        batch_size=args.batch_size,
        workers=args.workers
    )

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n✓ Complete!")
    print(f"  Time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"\nNext step: Ingest using zerobus_ingestion_client.py")

if __name__ == '__main__':
    main()
