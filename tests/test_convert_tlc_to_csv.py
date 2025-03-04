import warnings
import pytest
from unittest.mock import patch, Mock
import pandas as pd
from shapely.geometry import Point
from scripts.convert_tlc_to_csv import convert_tlc_to_csv
from config.settings import Settings

@pytest.fixture
def settings():
    """Fixture for mock settings."""
    return Settings(
        minio_endpoint="localhost:9000",
        minio_access_key="test_access",
        minio_secret_key="test_secret",
        minio_bucket="test-bucket",
        minio_secure=False,
        data_dir="data",
        db_path="store/test.db",
        log_file="logs/test.log",
        batch_size=100,
        map_output_dir="visualize/maps"
    )

def test_convert_tlc_to_csv_success(settings, caplog):
    """Test successful conversion and upload."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
        with caplog.at_level("INFO"):
            # Mock Path
            with patch("scripts.convert_tlc_to_csv.Path") as mock_path:
                mock_path.return_value.is_file.return_value = True
                mock_path.return_value.is_dir.return_value = True
                mock_path.return_value.glob.return_value = [Mock()]

                # Mock geopandas
                mock_zones = Mock()
                mock_zones.set_index.return_value.geometry.centroid.to_crs.return_value = pd.Series(
                    [Point(-73.98, 40.75), Point(-73.99, 40.76)], index=[1, 2]
                )
                with patch("scripts.convert_tlc_to_csv.gpd.read_file", return_value=mock_zones):

                    # Mock pandas
                    sample_df = pd.DataFrame({
                        "tpep_pickup_datetime": ["2024-01-01 12:00:00", "2024-01-01 13:00:00"],
                        "PULocationID": [1, 999],
                        "DOLocationID": [2, 2]
                    })
                    with patch("scripts.convert_tlc_to_csv.pd.read_parquet", return_value=sample_df):

                        # Mock MinIO
                        mock_minio = Mock()
                        mock_minio.bucket_exists.return_value = True
                        with patch("scripts.convert_tlc_to_csv.Minio", return_value=mock_minio), \
                             patch("scripts.convert_tlc_to_csv.Settings.create", return_value=settings):
                            convert_tlc_to_csv(input_file="test.parquet", shapefile_dir="test_shapefile", max_rows=2, sample_rows=1)
                            assert mock_minio.put_object.call_count == 2
                            assert "Uploaded full dataset" in caplog.text
                            assert "Uploaded sample" in caplog.text

def test_convert_tlc_to_csv_file_not_found():
    """Test failure when input file is not found."""
    with patch("scripts.convert_tlc_to_csv.Path") as mock_path:
        mock_path.return_value.is_file.return_value = False
        with pytest.raises(FileNotFoundError, match="Input file not found"):
            convert_tlc_to_csv(input_file="nonexistent.parquet")

def test_convert_tlc_to_csv_invalid_sample_rows():
    """Test failure when sample_rows exceeds max_rows."""
    with patch("scripts.convert_tlc_to_csv.Path") as mock_path:
        mock_path.return_value.is_file.return_value = True
        mock_path.return_value.is_dir.return_value = True
        mock_path.return_value.glob.return_value = [Mock()]
        with pytest.raises(ValueError, match="sample_rows.*cannot exceed max_rows"):
            convert_tlc_to_csv(max_rows=10, sample_rows=20)