import os
import pytest
from config.settings import Settings

@pytest.fixture
def clean_env():
    """Fixture to clear environment variables before each test."""
    original_env = os.environ.copy()
    for key in list(os.environ):
        if key.startswith("MINIO_") or key in ["DATA_DIR", "DB_PATH", "LOG_FILE", "BATCH_SIZE"]:
            os.environ.pop(key, None)
    yield
    os.environ.clear()
    os.environ.update(original_env)

def test_default_values(clean_env):
    """Test default settings values."""
    settings_instance = Settings.create()
    assert settings_instance.minio_endpoint == "localhost:9000"
    assert settings_instance.minio_access_key == "minioadmin"
    assert settings_instance.minio_secret_key == "minioadmin"
    assert settings_instance.minio_bucket == "logistics-data"
    assert settings_instance.minio_secure is False
    assert settings_instance.data_dir.endswith("data")
    assert settings_instance.db_path.endswith("store/logistics.db")
    assert settings_instance.log_file.endswith("logs/logistics.log")
    assert settings_instance.batch_size == 50000

def test_validation_empty(clean_env):
    """Test validation for empty or invalid fields."""
    with pytest.raises(ValueError, match="minio_endpoint must not be empty"):
        Settings(minio_endpoint="")
    
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        Settings(batch_size=0)

def test_from_env(clean_env):
    """Test settings loading from environment variables."""
    os.environ["MINIO_ENDPOINT"] = "test:9000"
    os.environ["MINIO_ACCESS_KEY"] = "testkey"
    os.environ["MINIO_SECRET_KEY"] = "testsecret"
    os.environ["MINIO_BUCKET"] = "test-bucket"
    os.environ["MINIO_SECURE"] = "true"
    os.environ["DATA_DIR"] = "/custom/data"
    os.environ["DB_PATH"] = "/custom/db.sqlite"
    os.environ["LOG_FILE"] = "/custom/log.log"
    os.environ["BATCH_SIZE"] = "50000"

    settings_instance = Settings.create()
    assert settings_instance.minio_endpoint == "test:9000"
    assert settings_instance.minio_access_key == "testkey"
    assert settings_instance.minio_secret_key == "testsecret"
    assert settings_instance.minio_bucket == "test-bucket"
    assert settings_instance.minio_secure is True
    assert settings_instance.data_dir == "/custom/data"
    assert settings_instance.db_path == "/custom/db.sqlite"
    assert settings_instance.log_file == "/custom/log.log"
    assert settings_instance.batch_size == 50000