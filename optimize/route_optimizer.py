import geopandas as gpd
from shapely.geometry import Point
import duckdb
import structlog
import networkx as nx
from tenacity import retry, stop_after_attempt, wait_exponential
import pandas as pd
import io
from multiprocessing import Pool, cpu_count
from typing import List

logger = structlog.get_logger()

class OptimizationError(Exception):
    """Custom exception raised when route optimization fails."""

def validate_csv_schema(csv_data: io.StringIO) -> None:
    """Validate that CSV data matches the expected schema."""
    expected_columns = ["delivery_id", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp"]
    try:
        df = pd.read_csv(csv_data)
    except pd.errors.EmptyDataError:
        raise OptimizationError("CSV data is empty or lacks a header")
    csv_data.seek(0)
    if not all(col in df.columns for col in expected_columns):
        raise OptimizationError(f"CSV schema mismatch: Expected {expected_columns}, found {df.columns.tolist()}")
    if df['delivery_id'].duplicated().any():
        raise OptimizationError("Duplicate delivery_id values found")

    required_numeric = ["pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon"]
    required_int = ["delivery_id"]
    required_datetime = ["timestamp"]

    for col in required_numeric:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise OptimizationError(f"Column {col} must be numeric, found {df[col].dtype}")
        if df[col].isnull().any():
            raise OptimizationError(f"Column {col} contains null values")
        if col.endswith("lat") and not df[col].between(-90, 90).all():
            raise OptimizationError(f"{col} out of valid latitude range (-90 to 90)")
        if col.endswith("lon") and not df[col].between(-180, 180).all():
            raise OptimizationError(f"{col} out of valid longitude range (-180 to 180)")

    for col in required_int:
        if not pd.api.types.is_integer_dtype(df[col]):
            raise OptimizationError(f"Column {col} must be integer, found {df[col].dtype}")
        if df[col].isnull().any():
            raise OptimizationError(f"Column {col} contains null values")

    for col in required_datetime:
        try:
            pd.to_datetime(df[col], errors="raise")
        except ValueError:
            raise OptimizationError(f"Column {col} must be datetime, found invalid values")

def get_utm_crs(lon: float, lat: float) -> str:
    """Determine UTM CRS based on longitude and latitude."""
    utm_zone = int((lon + 180) / 6) + 1
    return f"EPSG:326{utm_zone:02d}" if lat >= 0 else f"EPSG:327{utm_zone:02d}"

def build_delivery_graph(batch_df: pd.DataFrame) -> nx.DiGraph:
    """Build a directed graph for delivery optimization using row index as node identifier."""
    required_cols = ['pickup_lat', 'pickup_lon', 'dropoff_lat', 'dropoff_lon']
    missing_cols = [col for col in required_cols if col not in batch_df.columns]
    if missing_cols:
        logger.error("Missing required columns in batch_df", missing=missing_cols)
        raise ValueError(f"batch_df missing required columns: {missing_cols}")

    G = nx.DiGraph()
    for idx, row in batch_df.iterrows():
        pickup_id = f"p_{idx}"
        dropoff_id = f"d_{idx}"
        pickup_data = row.to_dict()
        dropoff_data = row.to_dict()
        pickup_data['point_type'] = 'pickup'
        pickup_data['geometry'] = Point(row['pickup_lon'], row['pickup_lat'])
        dropoff_data['point_type'] = 'dropoff'
        dropoff_data['geometry'] = Point(row['dropoff_lon'], row['dropoff_lat'])
        G.add_node(pickup_id, **pickup_data)
        G.add_node(dropoff_id, **dropoff_data)
        G.add_edge(pickup_id, dropoff_id, weight=0)
    return G

def optimize_batch(batch_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Optimize delivery routes for a batch using a greedy nearest-neighbor approach."""
    if batch_df.empty:
        return gpd.GeoDataFrame([], columns=["delivery_id", "pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp", "geometry"], crs="EPSG:4326")

    logger.debug("Optimizing batch with %d rows", len(batch_df))
    try:
        utm_crs = get_utm_crs(batch_df.iloc[0]['pickup_lon'], batch_df.iloc[0]['pickup_lat'])
        G = build_delivery_graph(batch_df)

        nodes_df = pd.DataFrame.from_dict(dict(G.nodes(data=True)), orient='index')
        gdf = gpd.GeoDataFrame(nodes_df, geometry='geometry', crs="EPSG:4326").to_crs(utm_crs)

        pickups_gdf = gdf[gdf.index.str.startswith('p_')]
        dropoffs_gdf = gdf[gdf.index.str.startswith('d_')]
        distances = pickups_gdf.geometry.apply(lambda p: dropoffs_gdf.geometry.distance(p))
        for i, u in enumerate(pickups_gdf.index):
            for j, v in enumerate(dropoffs_gdf.index):
                if u[2:] != v[2:]:
                    G.add_edge(u, v, weight=distances.iloc[i, j])

        start_node = next(iter(pickups_gdf.index), None)
        if not start_node:
            raise OptimizationError("No pickup nodes in batch")

        current_node = start_node
        path = [current_node]
        visited = {current_node}
        while len(visited) < len(G.nodes()):
            neighbors = [(n, G[current_node][n]['weight']) for n in G.neighbors(current_node) if n not in visited]
            if not neighbors:
                unvisited = [n for n in G.nodes() if n not in visited]
                if unvisited:
                    current_node = unvisited[0]
                    path.append(current_node)
                    visited.add(current_node)
                else:
                    break
            else:
                next_node = min(neighbors, key=lambda x: x[1])[0]
                current_node = next_node
                path.append(current_node)
                visited.add(current_node)

        path_data = [dict(G.nodes[node]) for node in path]
        path_df = pd.DataFrame(path_data)
        path_gdf = gpd.GeoDataFrame(path_df, geometry='geometry', crs="EPSG:4326")
        optimized_route = path_gdf[path_gdf['point_type'] == 'pickup'].drop(columns=['point_type']).drop_duplicates(subset=['delivery_id'])
        
        logger.info("Completed batch optimization", rows=len(optimized_route), batch_id=id(batch_df))
        return optimized_route
    except (IndexError, KeyError) as e:
        logger.error("Invalid batch data", error=str(e), batch_id=id(batch_df))
        raise OptimizationError(f"Batch data error: {e}")
    except Exception as e:
        logger.error("Batch optimization failed", error=str(e), batch_id=id(batch_df))
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10), reraise=True)
def optimize_routes(csv_data_list: List[io.StringIO], batch_size: int = 100) -> gpd.GeoDataFrame:
    """Optimize delivery routes across multiple CSV files using batch processing

    Args:
        csv_data_list (List[io.StringIO]): List of StringIO objects containing CSV data to process.
        batch_size (int, optional): Number of rows per batch for parallel processing. Defaults to 100.

    Returns:
        gpd.GeoDataFrame: Concatenated optimized routes from all batches, with geometry in EPSG:4326.

    Raises:
        ValueError: If csv_data_list is empty or contains invalid types.
        OptimizationError: If schema validation or optimization fails after retries.
    """
    if not csv_data_list or not all(isinstance(d, io.StringIO) for d in csv_data_list):
        raise ValueError("csv_data_list must be a non-empty list of StringIO objects")

    try:
        with duckdb.connect() as conn:
            logger.debug("Validating CSV schemas", count=len(csv_data_list))
            for i, csv_data in enumerate(csv_data_list):
                csv_data.seek(0)
                validate_csv_schema(csv_data)

            logger.debug("Loading CSVs into DuckDB")
            for i, csv_data in enumerate(csv_data_list):
                csv_data.seek(0)
                df = pd.read_csv(csv_data)
                conn.register(f"deliveries_{i}", df)
                conn.execute(f"CREATE OR REPLACE TABLE deliveries_{i} AS SELECT * FROM deliveries_{i}")

            logger.debug("Combining deliveries tables")
            conn.execute("CREATE TABLE deliveries AS " + " UNION ALL ".join(f"SELECT * FROM deliveries_{i}" for i in range(len(csv_data_list))))

            total_rows = conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
            batches = (total_rows + batch_size - 1) // batch_size
            logger.info("Starting route optimization", total_rows=total_rows, batches=batches)

            optimized_dfs = []
            with Pool(processes=max(1, cpu_count() - 1)) as pool:
                offsets = [batch_num * batch_size for batch_num in range(batches)]
                batch_queries = [f"SELECT * FROM deliveries LIMIT {batch_size} OFFSET {offset}" for offset in offsets]
                batch_dfs = [conn.execute(query).fetch_df() for query in batch_queries]
                logger.debug("Starting parallel optimization", batch_count=len(batch_dfs))
                results = pool.map(optimize_batch, batch_dfs)
                optimized_dfs.extend(results)

            logger.debug("Concatenating optimized batches")
            optimized = pd.concat(optimized_dfs, ignore_index=True)
            optimized = gpd.GeoDataFrame(optimized, geometry='geometry', crs="EPSG:4326")
            
            logger.info("Route optimization completed", total_rows=len(optimized))
            for csv_data in csv_data_list:
                csv_data.close()
            return optimized
    except duckdb.Error as e:
        logger.error("DuckDB processing failed", error=str(e))
        raise OptimizationError(f"DuckDB error: {e}")

if __name__ == "__main__":
    from config.settings import Settings
    from ingest.load_csv import load_csv

    settings = Settings.create()
    logger.info("Running route optimization manually")
    csv_data = load_csv(settings)
    optimized_routes = optimize_routes(csv_data, batch_size=100)
    print(optimized_routes.head())