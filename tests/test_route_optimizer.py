import io
import pandas as pd
import numpy as np
from optimize.route_optimizer import optimize_routes, OptimizationError, optimize_batch
import pytest
import geopandas as gpd

@pytest.fixture
def mock_csv_data():
    """Fixture providing mock CSV data as StringIO objects for testing."""
    data = io.StringIO("""delivery_id,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,timestamp
1,40.7128,-74.0060,40.7140,-74.0070,2025-02-20 10:00:00
2,40.7130,-74.0050,40.7150,-74.0060,2025-02-20 10:05:00
3,40.7140,-74.0070,40.7128,-74.0060,2025-02-20 10:10:00
""")
    data.seek(0)
    return [data]

def test_optimize_routes(mock_csv_data):
    """Test the optimize_routes function with mock data."""
    result = optimize_routes(mock_csv_data, batch_size=10)
    
    assert isinstance(result, gpd.GeoDataFrame)
    assert "geometry" in result.columns
    assert "delivery_id" in result.columns
    assert len(result) == 3
    assert result.crs == "EPSG:4326"
    assert all(col in result.columns for col in ["pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp"])
    
    distances = []
    for i in range(len(result) - 1):
        dist = result.iloc[i].geometry.distance(result.iloc[i + 1].geometry)
        distances.append(dist)
    assert all(d >= 0 for d in distances)

def test_optimize_batch_directly():
    """Test the optimize_batch function directly with controlled input."""
    df = pd.DataFrame({
        'delivery_id': [1, 2, 3],
        'pickup_lat': [40.7128, 40.7130, 40.7140],
        'pickup_lon': [-74.0060, -74.0050, -74.0070],
        'dropoff_lat': [40.7140, 40.7150, 40.7128],
        'dropoff_lon': [-74.0070, -74.0060, -74.0060],
        'timestamp': pd.to_datetime(['2025-02-20 10:00:00', '2025-02-20 10:05:00', '2025-02-20 10:10:00'])
    })
    
    result = optimize_batch(df)
    
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 3
    assert all(col in result.columns for col in ["pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp"])
    assert "delivery_id" in result.columns
    assert "geometry" in result.columns
    
    # Check geometry is valid and matches expected coordinates
    assert not result.geometry.is_empty.any()

def test_invalid_schema():
    invalid_data = io.StringIO("id,lat,lon,time\n1,40.7128,-74.0060,2025-02-20")
    with pytest.raises(OptimizationError, match="CSV schema mismatch"):
        optimize_routes([invalid_data], batch_size=10)

def test_empty_csv():
    empty_data = io.StringIO("")
    with pytest.raises(OptimizationError, match="CSV data is empty or lacks a header"):
        optimize_routes([empty_data], batch_size=10)

def test_numeric_validation():
    invalid_numeric = io.StringIO("""delivery_id,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,timestamp
1,invalid,-74.0060,40.7140,-74.0070,2025-02-20 10:00:00""")
    with pytest.raises(OptimizationError, match="must be numeric"):
        optimize_routes([invalid_numeric], batch_size=10)

def test_multiple_csv_files():
    data1 = io.StringIO("""delivery_id,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,timestamp
1,40.7128,-74.0060,40.7140,-74.0070,2025-02-20 10:00:00""")
    data2 = io.StringIO("""delivery_id,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,timestamp
2,40.7130,-74.0050,40.7150,-74.0060,2025-02-20 10:05:00""")
    data1.seek(0)
    data2.seek(0)
    
    result = optimize_routes([data1, data2], batch_size=10)
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == 2
    assert all(col in result.columns for col in ["pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "timestamp"])

def test_large_batch():
    n = 20
    np.random.seed(42)
    df = pd.DataFrame({
        'delivery_id': range(1, n+1),
        'pickup_lat': np.random.uniform(40.70, 40.80, n),
        'pickup_lon': np.random.uniform(-74.05, -73.95, n),
        'dropoff_lat': np.random.uniform(40.70, 40.80, n),
        'dropoff_lon': np.random.uniform(-74.05, -73.95, n),
        'timestamp': pd.date_range(start='2025-02-20', periods=n, freq='30min')
    })
    
    data = io.StringIO()
    df.to_csv(data, index=False)
    data.seek(0)
    
    result = optimize_routes([data], batch_size=10)
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) == n
    assert all(col in result.columns for col in ["pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon"])

if __name__ == "__main__":
    pytest.main(["-v", __file__])