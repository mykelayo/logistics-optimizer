from minio import Minio
from minio.error import S3Error
from config.settings import Settings
import structlog
import io
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm
import pandas as pd
from typing import List, Optional

logger = structlog.get_logger()

class IngestError(Exception):
    """Custom exception raised when CSV ingestion from MinIO fails."""

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
def get_minio_object(client: Minio, bucket: str, name: str) -> io.BytesIO:
    """Fetch an object from MinIO with retries.

    Args:
        client: MinIO client instance.
        bucket: MinIO bucket name.
        name: Object name in the bucket.

    Returns:
        BytesIO object containing the raw data.

    Raises:
        S3Error: If MinIO operation fails after retries.
    """
    return client.get_object(bucket, name)

def list_csv_objects(client: Minio, bucket: str, prefix: str) -> List:
    """List CSV objects in a MinIO bucket under a given prefix.

    Args:
        client: MinIO client instance.
        bucket: MinIO bucket name.
        prefix: Prefix to filter objects (e.g., 'raw/').

    Returns:
        List of MinIO object metadata for CSV files.
    """
    return [obj for obj in client.list_objects(bucket, prefix=prefix, recursive=True) if obj.object_name.endswith(".csv")]

def validate_csv(csv: io.StringIO, object_name: str, required_cols: List[str]) -> bool:
    """Validate a CSV file for emptiness and required columns.

    Args:
        csv: StringIO object containing CSV data.
        object_name: Name of the object for logging.
        required_cols: List of column names that must be present.

    Returns:
        True if valid, False otherwise.
    """
    try:
        df = pd.read_csv(csv, nrows=1)
        if df.empty:
            logger.warning("Empty CSV file", object=object_name)
            return False
        if not all(col in df.columns for col in required_cols):
            logger.warning("Missing required columns", object=object_name, missing=[col for col in required_cols if col not in df.columns])
            return False
        csv.seek(0)
        return True
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        logger.warning("Invalid CSV format", object=object_name, error=str(e))
        return False

def load_csv(
    settings: Settings,
    sample_prefix: str = "raw/sample_deliveries.csv",
    full_prefix: str = "raw/",
    required_cols: Optional[List[str]] = None
) -> List[io.StringIO]:
    """
    Load CSV files from a MinIO bucket into memory as StringIO objects with progress tracking.

    This function first attempts to load a sample CSV (e.g., 'raw/sample_deliveries.csv'). If not found,
    it falls back to all CSVs under a full prefix (e.g., 'raw/').

    Args:
        settings: Settings object with MinIO configuration (endpoint, access_key, secret_key, bucket, secure).
        sample_prefix: MinIO prefix for the sample CSV file (default: 'raw/sample_deliveries.csv').
        full_prefix: MinIO prefix for fallback full dataset CSVs (default: 'raw/').
        required_cols: Optional list of column names required in the CSV (default: None).

    Returns:
        List of StringIO objects containing valid CSV data.

    Raises:
        ValueError: If settings object lacks required MinIO attributes.
        IngestError: If MinIO connection, bucket access, or CSV loading fails after retries.
    """
    # Default required columns if not provided
    if required_cols is None:
        required_cols = ["delivery_id", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp"]

    # Validate settings
    required_attrs = ["minio_endpoint", "minio_access_key", "minio_secret_key", "minio_bucket"]
    if not all(hasattr(settings, attr) for attr in required_attrs):
        raise ValueError(f"Settings object missing required attributes: {', '.join(attr for attr in required_attrs if not hasattr(settings, attr))}")

    # Initialize MinIO client
    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure
    )

    # Check bucket existence
    if not client.bucket_exists(settings.minio_bucket):
        raise IngestError(f"Bucket '{settings.minio_bucket}' does not exist")

    try:
        # Try the sample CSV first
        logger.debug("Attempting to load sample CSV", bucket=settings.minio_bucket, prefix=sample_prefix)
        csv_objects = list_csv_objects(client, settings.minio_bucket, sample_prefix)
        if not csv_objects:
            logger.warning("Sample CSV not found, falling back to full dataset")
            logger.debug("Listing all CSV files", bucket=settings.minio_bucket, prefix=full_prefix)
            csv_objects = list_csv_objects(client, settings.minio_bucket, full_prefix)
            if not csv_objects:
                raise IngestError(f"No CSV files found in {settings.minio_bucket}/{full_prefix}")

        csv_data = []
        for obj in tqdm(csv_objects, desc="Loading CSVs from MinIO", unit="file"):
            logger.debug("Retrieving CSV", object=obj.object_name)
            response = get_minio_object(client, settings.minio_bucket, obj.object_name)
            try:
                # Stream decode to handle large files incrementally
                data = response.read().decode("utf-8")
                if not data.strip():
                    logger.warning("Empty CSV content", object=obj.object_name)
                    continue

                # Use context manager for StringIO
                with io.StringIO(data) as csv:
                    valid = validate_csv(csv, obj.object_name, required_cols)
                    if valid:
                        csv_data.append(io.StringIO(csv.getvalue()))
                        logger.info("Loaded CSV", object=obj.object_name, size=obj.size)
            except UnicodeDecodeError:
                logger.warning("Non-UTF-8 encoding detected", object=obj.object_name)
                continue
            except MemoryError:
                logger.error("Insufficient memory to load CSV", object=obj.object_name)
                raise IngestError(f"Memory error loading {obj.object_name}")
            finally:
                response.close()
                response.release_conn()

        if not csv_data:
            raise IngestError(f"No valid CSV content found in {settings.minio_bucket}/{full_prefix}")

        logger.info("Completed CSV ingestion", count=len(csv_data))
        return csv_data

    except S3Error as e:
        logger.error("MinIO S3 error", error=str(e))
        raise IngestError(f"MinIO S3 error: {e}")
    except Exception as e:
        logger.error("Failed to load CSVs from MinIO", error=str(e))
        raise IngestError(f"MinIO ingestion failed: {e}")

if __name__ == "__main__":
    settings = Settings.create()
    csvs = load_csv(settings)
    for csv in csvs:
        df = pd.read_csv(csv)
        print(df.head())
        csv.close()