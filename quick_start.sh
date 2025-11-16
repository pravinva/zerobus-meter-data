#!/bin/bash
# Quick Start Script for Zerobus Meter Data Demo
# This script runs the complete test workflow (10K meters, 1 day)

set -e  # Exit on error

echo "============================================================"
echo "Zerobus Meter Data Demo - Quick Start"
echo "============================================================"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 is not installed"
    exit 1
fi

echo "✓ Python 3 found: $(python3 --version)"
echo ""

# Check environment variables
if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
    echo "⚠ Warning: DATABRICKS_HOST and DATABRICKS_TOKEN not set"
    echo "Please set environment variables:"
    echo "  export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com"
    echo "  export DATABRICKS_TOKEN=dapi..."
    echo ""
    read -p "Do you want to continue anyway? (y/n) " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Install dependencies
echo "============================================================"
echo "Step 1: Installing Python dependencies"
echo "============================================================"
pip install -q pandas pyarrow numpy requests
echo "✓ Dependencies installed"
echo ""

# Generate metadata
echo "============================================================"
echo "Step 2: Generating meter metadata (10,000 meters)"
echo "============================================================"
python3 generate_meter_metadata.py \
    --num-meters 10000 \
    --output metadata_test.parquet

echo ""

# Generate readings
echo "============================================================"
echo "Step 3: Generating meter readings (1 day)"
echo "============================================================"
echo "This will take approximately 15 minutes..."
python3 meter_reading_generator.py \
    --metadata metadata_test.parquet \
    --start-date $(date +%Y-%m-%d) \
    --days 1 \
    --output meter_readings_test.parquet \
    --workers 4

echo ""

# Check if ingestion should run
if [ -n "$DATABRICKS_HOST" ] && [ -n "$DATABRICKS_TOKEN" ]; then
    echo "============================================================"
    echo "Step 4: Ingesting data to Databricks via Zerobus"
    echo "============================================================"

    python3 zerobus_ingestion_client.py \
        --workspace-url "$DATABRICKS_HOST" \
        --token "$DATABRICKS_TOKEN" \
        --catalog energy_australia_demo \
        --schema bronze \
        --table live_meter_readings \
        --input meter_readings_test.parquet \
        --batch-size 5000 \
        --workers 8

    echo ""
    echo "============================================================"
    echo "✓ Quick Start Complete!"
    echo "============================================================"
    echo ""
    echo "Next steps:"
    echo "1. Query your data in Databricks:"
    echo "   SELECT COUNT(*) FROM energy_australia_demo.bronze.live_meter_readings;"
    echo ""
    echo "2. For full demo (100K meters, 7 days, 200M records), see README.md"
    echo ""
else
    echo "============================================================"
    echo "✓ Data Generation Complete!"
    echo "============================================================"
    echo ""
    echo "Files created:"
    echo "  - metadata_test.parquet (~2 MB)"
    echo "  - meter_readings_test.parquet (~500 MB)"
    echo ""
    echo "To ingest to Databricks:"
    echo "1. Set environment variables:"
    echo "   export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com"
    echo "   export DATABRICKS_TOKEN=dapi..."
    echo ""
    echo "2. Run ingestion:"
    echo "   python3 zerobus_ingestion_client.py \\"
    echo "       --workspace-url \$DATABRICKS_HOST \\"
    echo "       --token \$DATABRICKS_TOKEN \\"
    echo "       --catalog energy_australia_demo \\"
    echo "       --schema bronze \\"
    echo "       --table live_meter_readings \\"
    echo "       --input meter_readings_test.parquet"
    echo ""
fi
