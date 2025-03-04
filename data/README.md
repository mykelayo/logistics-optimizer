# Data Directory

Placeholder for logistics CSV files and raw data (not tracked due to size).

## How to Obtain Data
- **TLC Yellow Taxi Data**: Generate logistics data from NYC TLC yellow taxi trips (~1M+ deliveries).
- Download:
  ```bash
  mkdir -p data
  wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet -O data/yellow_tripdata_2024-01.parquet
  wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip -O data/taxi_zones.zip
  unzip data/taxi_zones.zip -d data/taxi_zones
  rm data/taxi_zones.zip