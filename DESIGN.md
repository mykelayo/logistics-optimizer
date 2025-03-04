# Technical Design

## Introduction
This document outlines the technical design of a CLI tool that batch-processes NYC TLC yellow taxi trip data, optimizes delivery-like routes, and stores results in SQLite.

## Workflow
1. **Data Preparation**: Convert NYC TLC Parquet (`yellow_tripdata_2024-01.parquet`) to logistics CSVs using `scripts/convert_tlc_to_csv.py`, saving locally and uploading to MinIO (`raw/deliveries_2025_01.csv`, `raw/sample_deliveries.csv`).
2. **Ingest**: Retrieve CSVs from MinIO with `ingest/load_csv.py`, using retries, progress tracking, and structured logging.
3. **Optimize**: Batch-process data with `optimize/route_optimizer.py` using DuckDB, GeoPandas, and NetworkX for TSP approximation, leveraging parallel processing for scalability.
4. **Store**: Save optimized routes to SQLite with `store/sqlite_writer.py`, serializing geometry as WKT and also using retries for robustness.
5. **CLI**: Orchestrate the pipeline via Typer CLI with `cli/app.py`, logging all steps to `logs/logistics.log`.

## Features
- **Batching**: Processes data in configurable batches (default: 50,000 rows) for memory efficiency.
- **Error Handling**: Custom exceptions with `tenacity` retries (3 attempts, exponential backoff: 4s–10s) for all critical operations.
- **Logging**: Structured JSON logging via `structlog` for traceability, written to `logs/logistics.log` with file handler configuration.
- **Config**: Type-safe settings with Pydantic and `.env` support in `config/settings.py`.
- **CLI Interface**: Typer-driven interface for pipeline execution with sample data option.
- **Tests**: Pytest suite for ingestion, optimization, settings, and pipeline integration.

## Limitations
- Route optimization uses a greedy TSP heuristic (NetworkX), not optimal for large-scale logistics. We should consider `ortools` or `vrpy` for exact VRP solutions.
- MinIO assumes local setup; secure production with HTTPS and authentication.
- Reprocessing TLC files appends duplicates unless deduplicated upstream.

## Dependencies
- See `requirements.txt`.

## Scalability
- Supports ~1M+ deliveries with batching and parallelization.
- Use Dask or Spark for petabyte-scale data, replacing DuckDB/GeoPandas.

## Logging Fixes
- Ensured `logs/logistics.log` is writable with `mkdir -p logs; chmod -R 755 logs`.
- Configured `structlog` with a `logging.FileHandler` in `config/settings.py` to write JSON logs to file.

## Common Issues
- See `TROUBLESHOOTING.md` for detailed troubleshooting steps, including logging, MinIO, and SQLite issues.