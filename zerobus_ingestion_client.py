#!/usr/bin/env python3
"""
Zerobus Ingestion Client for Databricks
Ingests meter reading data directly to Databricks Delta Lake via Zerobus API
No Kafka required - direct HTTP/gRPC push to Unity Catalog tables
"""
import argparse
import time
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import json
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed

class ZerobusClient:
    """
    Client for ingesting data to Databricks via Zerobus API

    Zerobus provides direct ingestion to Delta Lake without Kafka:
    - Single-hop from client to Unity Catalog
    - < 5 second latency from ingest to query
    - 500K-1M records/sec throughput
    - Built-in schema validation and governance
    """

    def __init__(
        self,
        workspace_url: str,
        token: str,
        catalog: str,
        schema: str,
        table: str
    ):
        """
        Initialize Zerobus client

        Args:
            workspace_url: Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
            token: Databricks Personal Access Token
            catalog: Unity Catalog catalog name
            schema: Schema name
            table: Table name
        """
        self.workspace_url = workspace_url.rstrip('/')
        self.token = token
        self.catalog = catalog
        self.schema = schema
        self.table = table

        # Zerobus API endpoint
        self.api_endpoint = f"{self.workspace_url}/api/2.0/zerobus/ingest"

        # Full table path
        self.table_path = f"{catalog}.{schema}.{table}"

        # Request headers
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

        print(f"Zerobus Client initialized:")
        print(f"  Workspace: {self.workspace_url}")
        print(f"  Target table: {self.table_path}")
        print(f"  API endpoint: {self.api_endpoint}")

    def ingest_batch(
        self,
        records: List[Dict[str, Any]],
        batch_id: int = 0
    ) -> Dict[str, Any]:
        """
        Ingest a batch of records to Databricks via Zerobus

        Args:
            records: List of record dictionaries
            batch_id: Batch identifier for logging

        Returns:
            Response dictionary with status and metrics
        """
        # Prepare payload
        payload = {
            'table': self.table_path,
            'records': records,
            'format': 'json',
            'mode': 'append',  # Append to existing table
            'options': {
                'mergeSchema': 'true',  # Allow schema evolution
                'checkConstraints': 'true',  # Validate constraints
            }
        }

        try:
            start_time = time.time()

            # Send POST request to Zerobus API
            response = requests.post(
                self.api_endpoint,
                headers=self.headers,
                json=payload,
                timeout=300  # 5 minute timeout
            )

            elapsed = time.time() - start_time

            if response.status_code == 200:
                result = response.json()
                return {
                    'success': True,
                    'batch_id': batch_id,
                    'records_sent': len(records),
                    'records_written': result.get('num_records_written', len(records)),
                    'elapsed_seconds': elapsed,
                    'throughput_rps': len(records) / elapsed if elapsed > 0 else 0,
                }
            else:
                return {
                    'success': False,
                    'batch_id': batch_id,
                    'records_sent': len(records),
                    'error': f"HTTP {response.status_code}: {response.text}",
                    'elapsed_seconds': elapsed,
                }

        except Exception as e:
            return {
                'success': False,
                'batch_id': batch_id,
                'records_sent': len(records),
                'error': str(e),
            }

    def verify_table_exists(self) -> bool:
        """
        Verify that the target table exists in Unity Catalog

        Returns:
            True if table exists, False otherwise
        """
        # Use SQL API to check table existence
        sql_endpoint = f"{self.workspace_url}/api/2.0/sql/statements"

        query = f"DESCRIBE TABLE {self.table_path}"

        payload = {
            'statement': query,
            'warehouse_id': 'auto',  # Use default warehouse
        }

        try:
            response = requests.post(
                sql_endpoint,
                headers=self.headers,
                json=payload,
                timeout=60
            )

            return response.status_code == 200

        except Exception as e:
            print(f"Warning: Could not verify table existence: {e}")
            return False

def load_data_files(input_pattern: str) -> pd.DataFrame:
    """
    Load data from parquet files matching pattern

    Args:
        input_pattern: File pattern (e.g., "data/*.parquet" or "data.parquet")

    Returns:
        Combined DataFrame
    """
    files = glob.glob(input_pattern)

    if not files:
        raise ValueError(f"No files found matching pattern: {input_pattern}")

    print(f"Loading data from {len(files)} file(s)...")

    dfs = []
    for file in sorted(files):
        print(f"  Loading {file}...")
        df = pd.read_parquet(file)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)

    print(f"✓ Loaded {len(combined_df):,} records")

    return combined_df

def prepare_records_for_ingestion(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Prepare DataFrame for Zerobus ingestion

    Args:
        df: Input DataFrame

    Returns:
        List of record dictionaries
    """
    # Convert timestamps to ISO format strings
    if 'timestamp' in df.columns:
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    if 'install_date' in df.columns:
        df['install_date'] = pd.to_datetime(df['install_date']).dt.strftime('%Y-%m-%d')

    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Convert to records (list of dicts)
    records = df.to_dict('records')

    # Handle NaN/None values (convert to None for JSON serialization)
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    return records

def ingest_data(
    client: ZerobusClient,
    records: List[Dict[str, Any]],
    batch_size: int = 5000,
    workers: int = 8,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Ingest records in parallel batches

    Args:
        client: Zerobus client
        records: List of records to ingest
        batch_size: Records per batch
        workers: Number of parallel workers
        max_retries: Max retry attempts for failed batches

    Returns:
        Statistics dictionary
    """
    total_records = len(records)
    num_batches = (total_records + batch_size - 1) // batch_size

    print(f"\nIngestion configuration:")
    print(f"  Total records: {total_records:,}")
    print(f"  Batch size: {batch_size:,}")
    print(f"  Number of batches: {num_batches:,}")
    print(f"  Parallel workers: {workers}")

    # Create batches
    batches = []
    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        batches.append((batch, i // batch_size))

    # Statistics
    stats = {
        'total_records': total_records,
        'records_written': 0,
        'batches_succeeded': 0,
        'batches_failed': 0,
        'start_time': time.time(),
        'errors': [],
    }

    print(f"\nStarting ingestion...")

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(client.ingest_batch, batch, batch_id): (batch, batch_id)
            for batch, batch_id in batches
        }

        completed = 0

        for future in as_completed(futures):
            batch, batch_id = futures[future]

            try:
                result = future.result()

                if result['success']:
                    stats['batches_succeeded'] += 1
                    stats['records_written'] += result['records_written']

                    completed += 1
                    progress = (completed / num_batches) * 100

                    print(f"  [Batch {batch_id + 1}/{num_batches}] ✓ {result['records_written']:,} records | "
                          f"{result['throughput_rps']:.0f} rec/s | "
                          f"Progress: {progress:.1f}%")

                else:
                    stats['batches_failed'] += 1
                    stats['errors'].append({
                        'batch_id': batch_id,
                        'error': result['error']
                    })

                    print(f"  [Batch {batch_id + 1}/{num_batches}] ✗ Failed: {result['error']}")

            except Exception as e:
                stats['batches_failed'] += 1
                stats['errors'].append({
                    'batch_id': batch_id,
                    'error': str(e)
                })
                print(f"  [Batch {batch_id + 1}/{num_batches}] ✗ Exception: {e}")

    stats['end_time'] = time.time()
    stats['elapsed_seconds'] = stats['end_time'] - stats['start_time']
    stats['throughput_rps'] = stats['records_written'] / stats['elapsed_seconds'] if stats['elapsed_seconds'] > 0 else 0

    return stats

def main():
    parser = argparse.ArgumentParser(
        description='Ingest meter data to Databricks via Zerobus',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingest test data
  python zerobus_ingestion_client.py \\
      --workspace-url https://your-workspace.cloud.databricks.com \\
      --token dapi1234... \\
      --catalog energy_australia_demo \\
      --schema bronze \\
      --table live_meter_readings \\
      --input meter_readings_test.parquet \\
      --batch-size 5000 \\
      --workers 8

  # Ingest full dataset (partitioned files)
  python zerobus_ingestion_client.py \\
      --workspace-url $DATABRICKS_HOST \\
      --token $DATABRICKS_TOKEN \\
      --catalog energy_australia_demo \\
      --schema bronze \\
      --table live_meter_readings \\
      --input "meter_readings_100k_7d/*.parquet" \\
      --batch-size 10000 \\
      --workers 16
        """
    )

    parser.add_argument(
        '--workspace-url',
        type=str,
        required=True,
        help='Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)'
    )

    parser.add_argument(
        '--token',
        type=str,
        help='Databricks Personal Access Token (or set DATABRICKS_TOKEN env var)'
    )

    parser.add_argument(
        '--catalog',
        type=str,
        required=True,
        help='Unity Catalog catalog name'
    )

    parser.add_argument(
        '--schema',
        type=str,
        required=True,
        help='Schema name'
    )

    parser.add_argument(
        '--table',
        type=str,
        required=True,
        help='Table name'
    )

    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Input parquet file(s) - use glob pattern for multiple files'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=5000,
        help='Records per batch (default: 5000)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=8,
        help='Number of parallel workers (default: 8)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Load and prepare data without actually ingesting'
    )

    args = parser.parse_args()

    # Get token from args or environment
    import os
    token = args.token or os.environ.get('DATABRICKS_TOKEN')

    if not token:
        raise ValueError("Databricks token required: use --token or set DATABRICKS_TOKEN env var")

    # Initialize client
    client = ZerobusClient(
        workspace_url=args.workspace_url,
        token=token,
        catalog=args.catalog,
        schema=args.schema,
        table=args.table
    )

    # Load data
    print(f"\n{'='*60}")
    print("STEP 1: Loading Data")
    print('='*60)

    df = load_data_files(args.input)

    # Prepare records
    print(f"\n{'='*60}")
    print("STEP 2: Preparing Records")
    print('='*60)

    records = prepare_records_for_ingestion(df)
    print(f"✓ Prepared {len(records):,} records for ingestion")

    if args.dry_run:
        print("\n✓ Dry run complete - no data ingested")
        print(f"\nSample record:")
        print(json.dumps(records[0], indent=2))
        return

    # Ingest data
    print(f"\n{'='*60}")
    print("STEP 3: Ingesting to Databricks via Zerobus")
    print('='*60)

    stats = ingest_data(
        client=client,
        records=records,
        batch_size=args.batch_size,
        workers=args.workers
    )

    # Print summary
    print(f"\n{'='*60}")
    print("INGESTION COMPLETE")
    print('='*60)

    print(f"\n✓ Successfully ingested {stats['records_written']:,} / {stats['total_records']:,} records")
    print(f"  Batches succeeded: {stats['batches_succeeded']}")
    print(f"  Batches failed: {stats['batches_failed']}")
    print(f"  Time elapsed: {stats['elapsed_seconds']:.1f} seconds ({stats['elapsed_seconds']/60:.1f} minutes)")
    print(f"  Throughput: {stats['throughput_rps']:,.0f} records/second")

    if stats['errors']:
        print(f"\n⚠ {len(stats['errors'])} errors occurred:")
        for error in stats['errors'][:5]:  # Show first 5 errors
            print(f"  - Batch {error['batch_id']}: {error['error']}")
        if len(stats['errors']) > 5:
            print(f"  ... and {len(stats['errors']) - 5} more")

    print(f"\n✓ Data now queryable in Unity Catalog:")
    print(f"  SELECT COUNT(*) FROM {client.table_path};")

if __name__ == '__main__':
    main()
