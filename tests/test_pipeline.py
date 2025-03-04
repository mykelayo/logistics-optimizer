import sys
from pathlib import Path
import pytest
import pandas as pd
import io
import sqlite3
from minio import Minio
from ingest.load_csv import load_csv
from optimize.route_optimizer import optimize_routes
from store.sqlite_writer import store_routes
from config.settings import Settings
import os
import geopandas as gpd
import structlog
import shutil

logger = structlog.get_logger()

# Add project root to sys.path for portability
sys.path.append(str(Path(__file__).parent.parent))

@pytest.fixture
def mock_data(tmp_path):
    """Fixture to create mock CSV data, upload to MinIO, and configure settings."""
    # Create mock data similar to TLC format
    data = {
        "delivery_id": [1, 2],
        "pickup_lat": [40.7128, 40.7130],
        "pickup_lon": [-74.0060, -74.0050],
        "dropoff_lat": [40.7140, 40.7150],
        "dropoff_lon": [-74.0070, -74.0060],
        "timestamp": ["2025-02-20 10:00:00", "2025-02-20 10:05:00"]
    }
    df = pd.DataFrame(data)
    csv_file = tmp_path / "deliveries.csv"
    df.to_csv(csv_file, index=False)

    # Configure test settings
    settings = Settings(
        minio_endpoint="localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket="test-logistics-data",
        minio_secure=False,
        data_dir=str(tmp_path),
        db_path=str(tmp_path / "test_logistics.db"),
        log_file=str(tmp_path / "test_logistics.log"),
        batch_size=2
    )

    # Set up MinIO client and upload mock data
    client = Minio(settings.minio_endpoint, settings.minio_access_key, settings.minio_secret_key, secure=False)
    client.make_bucket(settings.minio_bucket)
    client.fput_object(settings.minio_bucket, "raw/sample_deliveries.csv", str(csv_file))
    yield settings

    # Cleanup: Remove MinIO bucket and local files
    try:
        client.remove_object(settings.minio_bucket, "raw/sample_deliveries.csv")
        client.remove_bucket(settings.minio_bucket)
        if os.path.exists(settings.db_path):
            os.remove(settings.db_path)
        if os.path.exists(settings.map_output_dir):
            shutil.rmtree(settings.map_output_dir)
    except Exception as e:
        logger.warning("Cleanup failed", error=str(e))

def test_full_pipeline(mock_data):
    """Test the entire pipeline: ingest, optimize, and store the data"""
    settings = mock_data

    # Step 1: Ingest
    logger.debug("Testing pipeline ingestion")
    csv_data = load_csv(settings)
    assert isinstance(csv_data, list)
    assert len(csv_data) == 1
    assert isinstance(csv_data[0], io.StringIO)
    df = pd.read_csv(csv_data[0])
    assert all(col in df.columns for col in ["delivery_id", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp"])
    assert len(df) == 2

    # Step 2: Optimize
    logger.debug("Testing pipeline optimization")
    optimized = optimize_routes(csv_data, batch_size=settings.batch_size)
    assert isinstance(optimized, gpd.GeoDataFrame)
    assert len(optimized) == 2 
    assert "geometry" in optimized.columns
    assert optimized.crs == "EPSG:4326"

    # Step 3: Store
    logger.debug("Testing pipeline storage")
    store_routes(optimized, settings)
    conn = sqlite3.connect(settings.db_path)
    result = pd.read_sql("SELECT * FROM optimized_routes", conn)
    assert len(result) == 2
    assert "geometry" in result.columns
    conn.close()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])