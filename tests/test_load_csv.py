import io
import pytest
from unittest.mock import patch, Mock
from ingest.load_csv import load_csv, IngestError
from config.settings import Settings

@pytest.fixture
def settings():
    """Fixture for mock settings."""
    return Settings(
        minio_endpoint="localhost:9000",
        minio_access_key="testkey",
        minio_secret_key="testsecret",
        minio_bucket="test-bucket",
        minio_secure=False,
        data_dir="/tmp/data",
        db_path="/tmp/store/logistics.db",
        log_file="/tmp/logs/logistics.log",
        batch_size=100000
    )

def test_load_csv_success(settings):
    """Test successful CSV ingestion."""
    class MockMinio:
        def __init__(self, *args, **kwargs):
            pass

        def bucket_exists(self, bucket_name):
            return True

        def list_objects(self, bucket_name, prefix='', recursive=True):
            return [Mock(object_name="raw/deliveries.csv", size=1000)]

        def get_object(self, bucket_name, object_name):
            data = io.StringIO(
                "delivery_id,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,timestamp\n"
                "1,40.7128,-74.0060,40.7140,-74.0070,2025-02-20 10:00:00\n"
            )
            return Mock(read=lambda: data.getvalue().encode("utf-8"), close=lambda: None, release_conn=lambda: None)

    with patch('ingest.load_csv.Minio', MockMinio), patch('ingest.load_csv.tqdm', lambda x, *_, **__: x):
        csvs = load_csv(settings)
        assert len(csvs) == 1
        csvs[0].seek(0)
        assert "delivery_id" in csvs[0].getvalue()

def test_load_csv_no_files(settings):
    """Test failure when no CSVs are found."""
    class MockMinioNoFiles:
        def __init__(self, *args, **kwargs):
            pass

        def bucket_exists(self, bucket_name):
            return True

        def list_objects(self, bucket_name, prefix='', recursive=True):
            return []

        def get_object(self, bucket_name, object_name):
            raise ValueError("No object")

    with patch('ingest.load_csv.Minio', MockMinioNoFiles), patch('ingest.load_csv.tqdm', lambda x, *_, **__: x):
        with pytest.raises(IngestError, match="No CSV files found"):
            load_csv(settings)

def test_load_csv_fallback_to_full_prefix(settings):
    """Test fallback to full prefix when sample CSV is not found."""
    class MockMinioFallback:
        def __init__(self, *args, **kwargs):
            pass

        def bucket_exists(self, bucket_name):
            return True

        def list_objects(self, bucket_name, prefix='', recursive=True):
            if prefix == "raw/sample_deliveries.csv":
                return []
            elif prefix == "raw/":
                return [Mock(object_name="raw/deliveries.csv", size=1000)]
            return []

        def get_object(self, bucket_name, object_name):
            data = io.StringIO(
                "delivery_id,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,timestamp\n"
                "1,40.7128,-74.0060,40.7140,-74.0070,2025-02-20 10:00:00\n"
            )
            return Mock(read=lambda: data.getvalue().encode("utf-8"), close=lambda: None, release_conn=lambda: None)

    with patch('ingest.load_csv.Minio', MockMinioFallback), patch('ingest.load_csv.tqdm', lambda x, *_, **__: x):
        csvs = load_csv(settings)
        assert len(csvs) == 1
        csvs[0].seek(0)
        assert "delivery_id" in csvs[0].getvalue()

def test_load_csv_invalid_csv_skipped(settings):
    """Test that invalid CSVs (missing required columns) are skipped."""
    class MockMinioInvalid:
        def __init__(self, *args, **kwargs):
            pass

        def bucket_exists(self, bucket_name):
            return True

        def list_objects(self, bucket_name, prefix='', recursive=True):
            return [Mock(object_name="raw/invalid.csv", size=1000)]

        def get_object(self, bucket_name, object_name):
            data = io.StringIO("wrong_col1,wrong_col2\n1,2")
            return Mock(read=lambda: data.getvalue().encode("utf-8"), close=lambda: None, release_conn=lambda: None)

    with patch('ingest.load_csv.Minio', MockMinioInvalid), patch('ingest.load_csv.tqdm', lambda x, *_, **__: x):
        required_cols = ["delivery_id", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp"]
        with pytest.raises(IngestError, match="No valid CSV content found"):
            load_csv(settings, required_cols=required_cols)

def test_load_csv_retry_success(settings):
    """Test that retries succeed after transient failures."""
    class MockMinioRetry:
        def __init__(self, *args, **kwargs):
            self.attempts = 0

        def bucket_exists(self, bucket_name):
            return True

        def list_objects(self, bucket_name, prefix='', recursive=True):
            return [Mock(object_name="raw/deliveries.csv", size=1000)]

        def get_object(self, bucket_name, object_name):
            self.attempts += 1
            if self.attempts < 3:
                raise Exception("Transient failure")
            data = io.StringIO(
                "delivery_id,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,timestamp\n"
                "1,40.7128,-74.0060,40.7140,-74.0070,2025-02-20 10:00:00\n"
            )
            return Mock(read=lambda: data.getvalue().encode("utf-8"), close=lambda: None, release_conn=lambda: None)

    with patch('ingest.load_csv.Minio', MockMinioRetry), patch('ingest.load_csv.tqdm', lambda x, *_, **__: x):
        csvs = load_csv(settings)
        assert len(csvs) == 1
        csvs[0].seek(0)
        assert "delivery_id" in csvs[0].getvalue()

def test_load_csv_bucket_not_found(settings):
    """Test failure when the bucket does not exist."""
    class MockMinioNoBucket:
        def __init__(self, *args, **kwargs):
            pass

        def bucket_exists(self, bucket_name):
            return False

        def list_objects(self, bucket_name, prefix='', recursive=True):
            return []

        def get_object(self, bucket_name, object_name):
            raise ValueError("No object")

    with patch('ingest.load_csv.Minio', MockMinioNoBucket), patch('ingest.load_csv.tqdm', lambda x, *_, **__: x):
        with pytest.raises(IngestError, match="Bucket 'test-bucket' does not exist"):
            load_csv(settings)