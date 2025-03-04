import io
import pandas as pd
import geopandas as gpd
from pathlib import Path
from minio import Minio
from config.settings import Settings
import structlog
from typing import Tuple

logger = structlog.get_logger()

class ConversionError(Exception):
    """Custom exception raised when conversion or upload fails."""

def convert_tlc_to_csv(
    input_file: str = "data/yellow_tripdata_2024-01.parquet",
    shapefile_dir: str = "data/taxi_zones",
    max_rows: int = 1_000_000,
    sample_rows: int = 10_000,
    output_prefix: str = "raw/deliveries_2025_01",
    sample_output: str = "raw/sample_deliveries.csv",
    lat_bounds: Tuple[float, float] = (40.5, 41.0),
    lon_bounds: Tuple[float, float] = (-74.3, -73.7)
) -> None:
    """
    Convert TLC Yellow Taxi Parquet data to CSVs and upload to MinIO bucket,
    mapping location IDs to coordinates using taxi zones shapefile centroids.

    Args:
        input_file: Path to TLC Parquet file.
        shapefile_dir: Directory containing taxi zones shapefile.
        max_rows: Maximum number of rows to process.
        sample_rows: Number of rows for the sample file.
        output_prefix: MinIO path prefix for full dataset.
        sample_output: MinIO path for sample file.
        lat_bounds: Latitude bounds for filtering (min, max).
        lon_bounds: Longitude bounds for filtering (min, max).

    Raises:
        FileNotFoundError: If input or shapefile is not found.
        ValueError: For invalid parameters or MinIO bucket issues.
        ConversionError: For processing or upload failures.
    """
    try:
        # Validate inputs
        if not Path(input_file).is_file():
            raise FileNotFoundError(f"Input file not found: {input_file}")
        if not Path(shapefile_dir).is_dir() or not any(Path(shapefile_dir).glob("*.shp")):
            raise FileNotFoundError(f"Shapefile directory not found or missing .shp: {shapefile_dir}")
        if sample_rows > max_rows:
            raise ValueError(f"sample_rows ({sample_rows}) cannot exceed max_rows ({max_rows})")

        # Load taxi zones shapefile
        logger.info("Loading taxi zones shapefile", path=shapefile_dir)
        try:
            zones = gpd.read_file(f"{shapefile_dir}/taxi_zones.shp")
            zone_coords = zones.set_index("LocationID").geometry.centroid.to_crs(epsg=4326)
            zone_coords = {loc_id: (point.y, point.x) for loc_id, point in zone_coords.items() if point is not None}
        except Exception as e:
            raise ConversionError(f"Failed to load shapefile: {e}")

        # Initialize MinIO client
        settings = Settings.create()
        client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure
        )
        if not client.bucket_exists(settings.minio_bucket):
            raise ValueError(f"MinIO bucket {settings.minio_bucket} does not exist")

        # Read Parquet file
        columns = ["tpep_pickup_datetime", "PULocationID", "DOLocationID"]
        logger.info("Loading Parquet file", input_file=input_file, max_rows=max_rows)
        df = pd.read_parquet(input_file, engine="pyarrow", columns=columns).head(max_rows)

        # Transform data
        df["delivery_id"] = range(1, len(df) + 1)
        df["pickup_lat"] = df["PULocationID"].map(lambda x: zone_coords.get(x, (40.75, -73.98))[0])
        df["pickup_lon"] = df["PULocationID"].map(lambda x: zone_coords.get(x, (40.75, -73.98))[1])
        df["dropoff_lat"] = df["DOLocationID"].map(lambda x: zone_coords.get(x, (40.75, -73.98))[0])
        df["dropoff_lon"] = df["DOLocationID"].map(lambda x: zone_coords.get(x, (40.75, -73.98))[1])
        df = df.rename(columns={"tpep_pickup_datetime": "timestamp"})
        df = df[["delivery_id", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp"]]

        # Clean and filter
        df = df.dropna(subset=["pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon"])
        df = df[
            (df["pickup_lat"].between(lat_bounds[0], lat_bounds[1])) &
            (df["pickup_lon"].between(lon_bounds[0], lon_bounds[1])) &
            (df["dropoff_lat"].between(lat_bounds[0], lat_bounds[1])) &
            (df["dropoff_lon"].between(lon_bounds[0], lon_bounds[1]))
        ]
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").map(
            lambda x: x.replace(year=2025) if pd.notna(x) else x
        )
        invalid_timestamps = df["timestamp"].isna().sum()
        if invalid_timestamps > 0:
            logger.warning("Dropped %d rows due to invalid timestamps", invalid_timestamps)

        # Verify row count
        if len(df) < max_rows * 0.9:  # Warn if <90% of expected
            logger.warning("Processed %d rows, expected ~%d", len(df), max_rows)
            print(f"Warning: Processed {len(df):,} rows, expected ~{max_rows:,}")

        # Save full dataset to MinIO
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        csv_data = csv_buffer.getvalue()
        client.put_object(
            settings.minio_bucket,
            f"{output_prefix}.csv",
            io.BytesIO(csv_data.encode("utf-8")),
            length=len(csv_data),
            part_size=10*1024*1024
        )
        logger.info("Uploaded full dataset", rows=len(df), path=f"{output_prefix}.csv")
        print(f"Uploaded full dataset with {len(df):,} rows to {settings.minio_bucket}/{output_prefix}.csv")

        # Save sample to MinIO
        if not df.empty:
            sample_df = df.head(sample_rows)
            sample_csv_buffer = io.StringIO()
            sample_df.to_csv(sample_csv_buffer, index=False)
            sample_csv_buffer.seek(0)
            sample_data = sample_csv_buffer.getvalue()
            client.put_object(
                settings.minio_bucket,
                sample_output,
                io.BytesIO(sample_data.encode("utf-8")),
                length=len(sample_data),
                part_size=10*1024*1024
            )
            logger.info("Uploaded sample", rows=len(sample_df), path=sample_output)
            print(f"Uploaded sample with {len(sample_df):,} rows to {settings.minio_bucket}/{sample_output}")
        else:
            logger.warning("No sample data generated due to empty filtered dataset")

    except FileNotFoundError as e:
        logger.error("File not found", error=str(e))
        raise
    except ValueError as e:
        logger.error("Validation error", error=str(e))
        raise
    except Exception as e:
        logger.error("Unexpected error during conversion or upload", error=str(e))
        raise ConversionError(f"Error during conversion or upload: {e}")

if __name__ == "__main__":
    convert_tlc_to_csv()