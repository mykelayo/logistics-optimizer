# Troubleshooting

- **MinIO Connection Error**:
  - Check `.env` credentials (`MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`).
  - Ensure MinIO is running: `curl http://localhost:9000`.
  - Verify port (default: 9000, update `.env` if using 9001): `sudo netstat -tuln | grep 9000`.
  - Ensure `minio` is running: `ps aux | grep minio`.

- **No CSV Files Found**:
  - Verify CSVs are uploaded to MinIO: `mc ls local/logistics-data/raw/`.
  - Upload with: `mc cp data/sample_deliveries.csv local/logistics-data/raw/`.
  - Ensure `prefix="raw/"` in `ingest/load_csv.py` matches your upload path.

- **SQLite Write Failure**:
  - Ensure `DB_PATH` directory exists and is writable: `mkdir -p store; chmod -R 755 store`.
  - Check permissions: `ls -l store/logistics.db`.
  - Verify SQLite installation: `python -c "import sqlite3"`.

- **Import Errors**:
  - Run from project root: `cd ~/workflow/logistics-optimizer`.
  - Set `PYTHONPATH`: `export PYTHONPATH=~/workflow/logistics-optimizer`.
  - Ensure `__init__.py` exists in `ingest/`, `optimize/`, `store/`, `cli/`, `config/`, and `tests/`.
  - Use `Python -m dir.script` to run, instead of the standalone command as it run the modules as part of a package.

- **Pipeline Failure or No Logs**:
  - Check `logs/logistics.log` for JSON errors. If no logs appear:
    - Ensure `logs/` exists and is writable: `mkdir -p logs; chmod -R 755 logs`.
    - Verify `.env` `LOG_FILE=logs/logistics.log` is correct.
    - Run `python -c "from config.settings import settings; settings.validate_settings()"`, then check `logs/logistics.log`.
    - Run `python -m cli.app --use-sample` to use the sample data to run the pipeline (default: False).
  - Verify all dependencies in `requirements.txt` are installed: `pip list | grep -E "pandas|minio|..."`.

- **Route Optimization Error**:
  - Verify MinIO CSVs: `mc ls local/logistics-data/raw/` and `mc cat local/logistics-data/raw/deliveries_2025_01.csv | head -n 5`.
  - Ensure CSVs have `delivery_id`, `pickup_lat`, `pickup_lon`, `dropoff_lat`, `dropoff_lon`, `timestamp`, with numeric and datetime types.
  - Test with a small batch: `python -c "from optimize.route_optimizer import optimize_routes; from ingest.load_csv import load_csv; csvs = load_csv(); optimized = optimize_routes(csvs, batch_size=10); print(optimized.head())"`.