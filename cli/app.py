import typer
import logging
from ingest.load_csv import load_csv
from optimize.route_optimizer import optimize_routes
from store.sqlite_writer import store_routes
import structlog
import os
from config.settings import Settings
import io

logger = structlog.get_logger()

# Create settings instance
settings = Settings.create()

app = typer.Typer(
    help="Logistics Delivery Batch Optimization Pipeline CLI", 
    add_completion=False
)

@app.command()
def run_pipeline(
    batch_size: int = settings.batch_size,
    verbose: bool = typer.Option(False, "--verbose", help="Increase logging verbosity"),
    log_level: str = typer.Option("INFO", "--log-level", help="Set logging level (DEBUG, INFO, WARNING, ERROR)"),
    use_sample: bool = typer.Option(False, "--use-sample", help="Use 5 row sample data instead of MinIO for testing")
):
    """
    Run the full logistics optimization pipeline: ingest, optimize, store , and visualize.

    Orchestrates the pipeline by ingesting CSV data (from MinIO or sample), optimizing delivery routes
    in batches, and storing the results in SQLite. 
    Uses structured logging for tracking.

    Args:
        batch_size: Number of rows per batch for optimization (default to config value).
        verbose: Enable detailed logging.
        log_level: Set the logging level (DEBUG, INFO, WARNING, ERROR).
        use_sample: Use 5 row sample data instead of MinIO for testing (default: False).

    Raises:
        typer.Exit: Exits with code 1 if pipeline step fails.
    """
    try:
        # Configure logging level using logging module
        numeric_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {log_level}")
        logging.getLogger('').setLevel(numeric_level if not verbose else logging.DEBUG)
        logger.info("Starting pipeline", batch_size=batch_size, settings=settings.model_dump(), log_level=log_level, use_sample=use_sample)

        # Check access
        dir_path = os.path.dirname(settings.db_path)
        if not os.access(dir_path, os.W_OK):
            logger.error("Path not writable", path=dir_path)
            raise typer.Exit(code=1)

        # Step 1: Ingest CSV data (sample or MinIO)
        logger.debug("Initiating CSV ingestion")
        if use_sample:
            logger.info("Using 5 row sample data for testing")
            sample_data = io.StringIO("""delivery_id,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,timestamp
10001,40.7128,-74.0060,40.7140,-74.0070,2025-02-20 10:00:00
10002,40.7130,-74.0050,40.7150,-74.0060,2025-02-20 10:05:00
10003,40.7140,-74.0070,40.7128,-74.0060,2025-02-20 10:10:00
10004,40.7150,-74.0080,40.7160,-74.0090,2025-02-20 10:16:00
10005,40.7160,-74.0090,40.7170,-74.0100,2025-02-20 10:20:00                                                              
""".strip())
            sample_data.seek(0)
            csv_data = [sample_data]
        else:
            try:
                csv_data = load_csv(settings)
                logger.info("Ingested CSVs", count=len(csv_data))
            except Exception as e:
                logger.error("CSV ingestion from MinIO failed", error=str(e), step="ingest")
                raise typer.Exit(code=1)
            
        # Step 2: Optimize routes in batches
        logger.debug("Starting route optimization with batch size %d", batch_size)
        try:
            optimized_routes = optimize_routes(csv_data, batch_size=batch_size)
            logger.info("Optimized routes", rows=len(optimized_routes))
        except Exception as e:
            logger.error("Route optimization failed", error=str(e), step="optimize")
            raise typer.Exit(code=1)

        # Step 3: Store optimized routes in SQLite 
        logger.debug("Storing optimized routes in SQLite at %s",settings.db_path)
        try:
            store_routes(optimized_routes, settings)
            logger.info("Stored optimized routes in SQLite", db_path=settings.db_path)
        except Exception as e:
            logger.error("Storage failed", error=str(e), step="store")
            raise typer.Exit(code=1)
        
        logger.info("Pipeline completed successfully")
    except ValueError as ve:
        logger.error("Invalid configuration", error=str(ve), step="setup")
        raise typer.Exit(code=1)
    except Exception as e:
        logger.error("Pipeline failed", error=str(e), step="pipeline")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()