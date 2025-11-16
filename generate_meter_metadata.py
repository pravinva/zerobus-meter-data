#!/usr/bin/env python3
"""
Meter Metadata Generator for Zerobus Demo
Generates realistic meter metadata for 10K-100K+ smart meters across Australia
"""
import argparse
import random
import pandas as pd
import numpy as np
from datetime import datetime
import uuid

# Australian states and their population distribution
AUSTRALIAN_STATES = {
    'NSW': 0.32,  # New South Wales - 32%
    'VIC': 0.26,  # Victoria - 26%
    'QLD': 0.20,  # Queensland - 20%
    'WA': 0.11,   # Western Australia - 11%
    'SA': 0.07,   # South Australia - 7%
    'TAS': 0.02,  # Tasmania - 2%
    'ACT': 0.02,  # Australian Capital Territory - 2%
}

# Meter types and their characteristics
METER_TYPES = {
    'residential': {
        'weight': 0.70,
        'avg_demand_kw': 1.5,
        'solar_penetration': 0.30,  # 30% have solar
    },
    'commercial': {
        'weight': 0.25,
        'avg_demand_kw': 15.0,
        'solar_penetration': 0.15,  # 15% have solar
    },
    'industrial': {
        'weight': 0.05,
        'avg_demand_kw': 150.0,
        'solar_penetration': 0.05,  # 5% have solar
    }
}

# Australian suburbs for realistic location data
SUBURBS_BY_STATE = {
    'NSW': ['Sydney CBD', 'Parramatta', 'Newcastle', 'Wollongong', 'Bondi', 'Chatswood', 'Penrith'],
    'VIC': ['Melbourne CBD', 'Geelong', 'Ballarat', 'Bendigo', 'Frankston', 'Dandenong'],
    'QLD': ['Brisbane CBD', 'Gold Coast', 'Sunshine Coast', 'Townsville', 'Cairns', 'Toowoomba'],
    'WA': ['Perth CBD', 'Fremantle', 'Joondalup', 'Mandurah', 'Bunbury'],
    'SA': ['Adelaide CBD', 'Port Adelaide', 'Mount Gambier', 'Whyalla'],
    'TAS': ['Hobart', 'Launceston', 'Devonport'],
    'ACT': ['Canberra City', 'Belconnen', 'Tuggeranong', 'Woden'],
}

def generate_meter_id():
    """Generate a realistic meter ID (NMI - National Metering Identifier format)"""
    # Australian NMI format: 10-11 digit alphanumeric
    return f"NMI{random.randint(1000000000, 9999999999)}"

def generate_meter_metadata(num_meters: int) -> pd.DataFrame:
    """
    Generate realistic meter metadata

    Args:
        num_meters: Number of meters to generate

    Returns:
        DataFrame with meter metadata
    """
    print(f"Generating metadata for {num_meters:,} meters...")

    meters = []

    # Determine meter counts per type
    meter_type_counts = {
        mtype: int(num_meters * props['weight'])
        for mtype, props in METER_TYPES.items()
    }

    # Adjust for rounding
    total_assigned = sum(meter_type_counts.values())
    meter_type_counts['residential'] += (num_meters - total_assigned)

    meter_idx = 0

    for meter_type, count in meter_type_counts.items():
        print(f"  Generating {count:,} {meter_type} meters...")

        for _ in range(count):
            # Select state based on population distribution
            state = random.choices(
                list(AUSTRALIAN_STATES.keys()),
                weights=list(AUSTRALIAN_STATES.values())
            )[0]

            # Select random suburb in state
            suburb = random.choice(SUBURBS_BY_STATE[state])

            # Determine if meter has solar
            has_solar = random.random() < METER_TYPES[meter_type]['solar_penetration']

            # Generate solar capacity if applicable (3-10 kW for residential, higher for commercial)
            solar_capacity_kw = 0.0
            if has_solar:
                if meter_type == 'residential':
                    solar_capacity_kw = round(random.uniform(3.0, 10.0), 2)
                elif meter_type == 'commercial':
                    solar_capacity_kw = round(random.uniform(20.0, 100.0), 2)
                else:  # industrial
                    solar_capacity_kw = round(random.uniform(100.0, 500.0), 2)

            meter = {
                'meter_id': generate_meter_id(),
                'meter_type': meter_type,
                'location_state': state,
                'location_suburb': suburb,
                'location_postcode': random.randint(2000, 6999),  # Australian postcodes
                'has_solar': has_solar,
                'solar_capacity_kw': solar_capacity_kw,
                'install_date': pd.Timestamp(
                    datetime(2020, 1, 1) + pd.Timedelta(days=random.randint(0, 1800))
                ),
                'manufacturer': random.choice(['Landis+Gyr', 'Itron', 'Elster', 'Schneider Electric']),
                'model': f"SM-{random.randint(100, 999)}",
                'firmware_version': f"{random.randint(1, 3)}.{random.randint(0, 9)}.{random.randint(0, 20)}",
                'created_at': pd.Timestamp.now(),
            }

            meters.append(meter)
            meter_idx += 1

            if meter_idx % 10000 == 0:
                print(f"    Generated {meter_idx:,} / {num_meters:,} meters...")

    df = pd.DataFrame(meters)

    print(f"\n✓ Generated {len(df):,} meter metadata records")
    print(f"  - Residential: {len(df[df.meter_type == 'residential']):,}")
    print(f"  - Commercial: {len(df[df.meter_type == 'commercial']):,}")
    print(f"  - Industrial: {len(df[df.meter_type == 'industrial']):,}")
    print(f"  - With Solar: {len(df[df.has_solar]):,} ({len(df[df.has_solar])/len(df)*100:.1f}%)")

    return df

def main():
    parser = argparse.ArgumentParser(
        description='Generate meter metadata for Zerobus demo',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 10K meters for testing
  python generate_meter_metadata.py --num-meters 10000 --output metadata.parquet

  # Generate 100K meters for full demo
  python generate_meter_metadata.py --num-meters 100000 --output metadata_100k.parquet
        """
    )

    parser.add_argument(
        '--num-meters',
        type=int,
        required=True,
        help='Number of meters to generate (e.g., 10000 for test, 100000 for demo)'
    )

    parser.add_argument(
        '--output',
        type=str,
        required=True,
        help='Output parquet file path (e.g., metadata.parquet)'
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

    # Generate metadata
    start_time = datetime.now()
    df = generate_meter_metadata(args.num_meters)

    # Save to parquet
    print(f"\nSaving to {args.output}...")
    df.to_parquet(args.output, index=False, compression='snappy')

    file_size_mb = pd.io.common.file_exists(args.output)
    import os
    file_size_mb = os.path.getsize(args.output) / (1024 * 1024)

    elapsed = (datetime.now() - start_time).total_seconds()

    print(f"\n✓ Complete!")
    print(f"  Output: {args.output}")
    print(f"  Size: {file_size_mb:.2f} MB")
    print(f"  Time: {elapsed:.1f} seconds")
    print(f"\nNext step: Generate meter readings using meter_reading_generator.py")

if __name__ == '__main__':
    main()
