import sqlite3
import geopandas as gpd
from config.settings import Settings
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential
import pandas as pd
from shapely.geometry import Point
from pathlib import Path

logger = structlog.get_logger()

class StorageError(Exception):
    """Custom exception raised when storage operations fail."""

def create_routes_table(conn: sqlite3.Connection) -> None:
    """Create the optimized_routes table with an auto-incrementing delivery_id."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS optimized_routes (
            delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pickup_lat REAL,
            pickup_lon REAL,
            dropoff_lat REAL,
            dropoff_lon REAL,
            timestamp TEXT,
            geometry TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS geom_idx ON optimized_routes (geometry)")
    conn.commit()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
def store_routes(optimized_routes: gpd.GeoDataFrame, settings: Settings = None) -> None:
    """
    Store optimized delivery routes in SQLite database.

    Creates or updates a table 'optimized_routes' with GeoDataFrame data, using SQLite's spatial
    extension (via WKT) for geometry storage. Retries on transient failures like disk I/O issues.

    Args:
        optimized_routes: GeoDataFrame containing optimized delivery routes (CRS: EPSG:4326).
        settings: Settings object with db_path (defaults to Settings.create() if None).

    Raises:
        StorageError: If database connection, table creation, or insertion fails after retries.
    """
    if settings is None:
        settings = Settings.create()

    if not Path(settings.db_path).parent.is_dir():
        raise StorageError(f"Database directory does not exist: {settings.db_path}")

    conn = None
    try:
        logger.debug("Validating GeoDataFrame input", columns=optimized_routes.columns.tolist())
        if not isinstance(optimized_routes, gpd.GeoDataFrame) or "geometry" not in optimized_routes.columns:
            raise StorageError("Invalid GeoDataFrame: Missing geometry column or incorrect type")
        
        required_cols = ['pickup_lon', 'pickup_lat']
        if not all(col in optimized_routes.columns for col in required_cols):
            raise StorageError(f"Missing required coordinate columns: {required_cols}")

        invalid_geom = optimized_routes['geometry'].apply(lambda g: not isinstance(g, Point) or g is None)
        if invalid_geom.any():
            logger.debug("Fixing invalid geometries", count=invalid_geom.sum())
            optimized_routes.loc[invalid_geom, 'geometry'] = [
                Point(row['pickup_lon'], row['pickup_lat']) if pd.notna(row['pickup_lon']) and pd.notna(row['pickup_lat']) else None
                for _, row in optimized_routes[invalid_geom].iterrows()
            ]
            optimized_routes = gpd.GeoDataFrame(optimized_routes, geometry='geometry', crs="EPSG:4326")

        if optimized_routes.crs != "EPSG:4326":
            logger.debug("Converting CRS to EPSG:4326")
            optimized_routes = optimized_routes.to_crs("EPSG:4326")

        conn = sqlite3.connect(settings.db_path)
        create_routes_table(conn)

        logger.debug("Converting geometry to WKT")
        optimized_routes['geometry_wkt'] = optimized_routes['geometry'].apply(
            lambda geom: geom.wkt if geom is not None else None
        )
        df = pd.DataFrame(optimized_routes.drop(columns=['geometry', 'geometry_wkt']))
        df['geometry'] = optimized_routes['geometry_wkt']

        # Drop delivery_id to let AUTOINCREMENT handle it
        df = df.drop(columns=['delivery_id'], errors='ignore')

        logger.debug("Writing data to SQLite with auto-incremented delivery_id", rows=len(df))
        df.to_sql('optimized_routes', conn, if_exists='append', index=False)
        conn.commit()
        logger.info("Stored optimized routes in SQLite", rows=len(optimized_routes), db_path=settings.db_path)
    except sqlite3.Error as e:
        logger.error("SQLite error", error=str(e), db_path=settings.db_path)
        raise StorageError(f"Database error: {e}")
    except ValueError as e:
        logger.error("Data validation error", error=str(e), db_path=settings.db_path)
        raise StorageError(f"Data validation error: {e}")
    except Exception as e:
        logger.error("Failed to store routes in SQLite", error=str(e), db_path=settings.db_path)
        raise StorageError(f"Storage error: {e}")
    finally:
        if conn is not None:
            logger.debug("Closing SQLite connection", db_path=settings.db_path)
            conn.close()

if __name__ == "__main__":
    from optimize.route_optimizer import optimize_routes
    from config.settings import Settings
    from ingest.load_csv import load_csv

    settings = Settings.create()
    logger.info("Running SQLite storage with MinIO data")
    csv_data = load_csv(settings)
    routes = optimize_routes(csv_data, batch_size=100)
    store_routes(routes, settings)