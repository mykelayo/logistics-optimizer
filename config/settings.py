from pydantic import BaseModel, field_validator, ValidationInfo
from dotenv import load_dotenv
import os
from pathlib import Path
import structlog
import logging

load_dotenv(override=True)

class Settings(BaseModel):
    """Settings for MinIO, data, database, logging, and batch processing"""
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool
    data_dir: str
    db_path: str
    log_file: str
    batch_size: int

    @field_validator("minio_endpoint", "minio_access_key", "minio_secret_key", "minio_bucket", "data_dir", "db_path", "log_file")
    @classmethod
    def not_empty(cls, v: str, info: ValidationInfo) -> str:
        field_name = info.field_name
        if not v or v.strip() == "":
            raise ValueError(f"{field_name} must not be empty")
        return v

    @field_validator("batch_size")
    @classmethod
    def batch_size_positive(cls, v: int, info: ValidationInfo) -> int:
        field_name = info.field_name
        if v <= 0:
            raise ValueError(f"{field_name} must be a positive integer")
        return v

    def validate_settings(self) -> None:
        log_dir = os.path.dirname(self.log_file)
        os.makedirs(log_dir, exist_ok=True)
        if not os.access(log_dir, os.W_OK):
            raise ValueError(f"Log directory {log_dir} is not writable")
        logger.debug("Validating settings with environment", env_vars={k: v for k, v in os.environ.items() if k.startswith("MINIO_") or k in ["DATA_DIR", "DB_PATH", "LOG_FILE", "BATCH_SIZE", "MAP_OUTPUT_DIR"]})
        logger.info("Settings validated successfully", settings=self.model_dump())

    @classmethod
    def create(cls) -> 'Settings':
        load_dotenv(override=False)
        # print("Environment variables at creation:", {k: v for k, v in os.environ.items() if k.startswith("MINIO_") or k in ["DATA_DIR", "DB_PATH", "LOG_FILE", "BATCH_SIZE", "MAP_OUTPUT_DIR"]})
        return cls(
            minio_endpoint=os.getenv("MINIO_ENDPOINT") or "localhost:9000",
            minio_access_key=os.getenv("MINIO_ACCESS_KEY") or "minioadmin",
            minio_secret_key=os.getenv("MINIO_SECRET_KEY") or "minioadmin",
            minio_bucket=os.getenv("MINIO_BUCKET") or "logistics-data",
            minio_secure=(os.getenv("MINIO_SECURE") or "False").lower() in ("true", "1", "t"),
            data_dir=os.path.abspath(os.path.expanduser(os.getenv("DATA_DIR") or str(Path(__file__).parent.parent / "data"))),
            db_path=os.path.abspath(os.path.expanduser(os.getenv("DB_PATH") or str(Path(__file__).parent.parent / "store" / "logistics.db"))),
            log_file=os.path.abspath(os.path.expanduser(os.getenv("LOG_FILE") or str(Path(__file__).parent.parent / "logs" / "logistics.log"))),
            batch_size=int(os.getenv("BATCH_SIZE") or "50000")
        )

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

default_settings = Settings.create()
file_handler = logging.FileHandler(default_settings.log_file)
file_handler.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger('').addHandler(file_handler)
logging.getLogger('').setLevel(logging.DEBUG)
logger = structlog.get_logger()