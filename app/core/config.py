# app/core/config.py
import os
import logging
import json
from typing import List, Optional, Union, Any, Dict
from pydantic import (
    AnyHttpUrl, PostgresDsn, field_validator, ValidationInfo, BaseModel,
    EmailStr, SecretStr
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    """
    Application Settings using Pydantic BaseSettings.
    Loads from environment variables and .env files.
    """
    model_config = SettingsConfigDict(
        env_file=('.env.prod', '.env'),
        env_file_encoding='utf-8',
        case_sensitive=True,
        extra='ignore'
    )

    # --- Core Application Settings ---
    PROJECT_NAME: str = "Axiom Flow"
    API_V1_STR: str = "/api/v1"
    DEBUG: bool = False

    # --- Security Settings ---
    SECRET_KEY: SecretStr = SecretStr("09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7 # 1 week
    ALGORITHM: str = "HS256"

    # --- Google OAuth Client ID (Optional) ---
    GOOGLE_OAUTH_CLIENT_ID: Optional[str] = None

    # --- Database Settings ---
    POSTGRES_SERVER: str = "db"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "dicom_processor_user"
    POSTGRES_PASSWORD: SecretStr = SecretStr("changeme")
    POSTGRES_DB: str = "dicom_processor_db"
    SQLALCHEMY_DATABASE_URI: Optional[PostgresDsn] = None

    @field_validator("SQLALCHEMY_DATABASE_URI", mode='before')
    @classmethod
    def assemble_db_connection(cls, v: Optional[str], info: ValidationInfo) -> Any:
        if isinstance(v, str): return v
        values = info.data
        password = values.get("POSTGRES_PASSWORD")
        return PostgresDsn.build(
            scheme="postgresql+psycopg",
            username=values.get("POSTGRES_USER"),
            password=password.get_secret_value() if isinstance(password, SecretStr) else password,
            host=values.get("POSTGRES_SERVER"),
            port=values.get("POSTGRES_PORT"),
            path=f"{values.get('POSTGRES_DB') or ''}",
        )

    # --- CORS Settings ---
    BACKEND_CORS_ORIGINS: List[Union[str, AnyHttpUrl]] = ["http://localhost:5173", "http://127.0.0.1:5173"]

    @field_validator("BACKEND_CORS_ORIGINS", mode='before')
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> List[Union[str, AnyHttpUrl]]:
        if isinstance(v, str):
            if v.startswith("["):
                 try: return json.loads(v)
                 except json.JSONDecodeError: raise ValueError("Invalid JSON string for BACKEND_CORS_ORIGINS")
            else: return [origin.strip() for origin in v.split(",") if origin.strip()]
        elif isinstance(v, list): return v
        raise ValueError("Invalid format for BACKEND_CORS_ORIGINS")

    # --- Superuser Settings (for initial setup) ---
    FIRST_SUPERUSER_EMAIL: EmailStr = "admin@axiomflow.com"
    FIRST_SUPERUSER_PASSWORD: SecretStr = SecretStr("changeme")

    # --- Celery / Message Broker Settings ---
    RABBITMQ_HOST: str = "rabbitmq"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: SecretStr = SecretStr("guest")
    RABBITMQ_VHOST: str = "/"
    CELERY_BROKER_URL: Optional[str] = None

    # --- Redis Settings ---
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None

    # --- Celery Task Settings ---
    CELERY_TASK_DEFAULT_QUEUE: str = "default"
    CELERY_TASK_MAX_RETRIES: int = 3
    CELERY_TASK_RETRY_DELAY: int = 60

    # --- REMOVED Static DICOM Listener Settings ---
    # DICOM_LISTENER_AET: str = "AXIOM_FLOW"
    # DICOM_LISTENER_PORT: int = 11112
    # DEFAULT_SCP_SOURCE_ID: str = "dicom_scp_main"
    # --- END REMOVED ---
    # Keep LISTENER_HOST as it determines the bind address (usually 0.0.0.0)
    LISTENER_HOST: str = "0.0.0.0"


    # --- Storage & File Handling Settings ---
    DICOM_STORAGE_PATH: Path = Path("/dicom_data/incoming")
    DICOM_ERROR_PATH: Path = Path("/dicom_data/errors")
    TEMP_DIR: Optional[Path] = None
    DELETE_ON_SUCCESS: bool = True
    DELETE_UNMATCHED_FILES: bool = False
    DELETE_ON_NO_DESTINATION: bool = False
    MOVE_TO_ERROR_ON_PARTIAL_FAILURE: bool = True

    # --- DICOMweb Poller Settings ---
    DICOMWEB_POLLER_DEFAULT_FALLBACK_DAYS: int = 7
    DICOMWEB_POLLER_OVERLAP_MINUTES: int = 5
    DICOMWEB_POLLER_QIDO_LIMIT: int = 5000
    DICOMWEB_POLLER_MAX_SOURCES: int = 100

    # --- Static Known Input Sources (Base List - REMOVED SCP ID) ---
    KNOWN_INPUT_SOURCES: List[str] = [
        # DEFAULT_SCP_SOURCE_ID removed here
        "api_json",
        "stow_rs",
    ]

    @field_validator("KNOWN_INPUT_SOURCES", mode='before')
    @classmethod
    def assemble_known_sources(cls, v: Union[str, List[str]], info: ValidationInfo) -> List[str]:
        # Keep default list empty now, rely on env var or static values above
        default_sources: List[str] = [] # Start empty or use only static ones above
        static_sources_in_code = ["api_json", "stow_rs"] # Define static ones here

        parsed_sources = []
        if isinstance(v, str):
            if v.startswith("["):
                 try: parsed_sources = json.loads(v)
                 except json.JSONDecodeError: raise ValueError("Invalid JSON string for KNOWN_INPUT_SOURCES")
            else: parsed_sources = [i.strip() for i in v.split(",") if i.strip()]
        elif isinstance(v, list): parsed_sources = v

        # Combine static code defaults with parsed env var values
        combined = set(static_sources_in_code); combined.update(parsed_sources)
        return sorted(list(combined))

    # --- Dynamic URL Builders & Directory Creation ---
    def model_post_init(self, __context: Any) -> None:
        """Build derived settings like URLs after initial loading."""
        # Build Celery Broker URL
        if self.CELERY_BROKER_URL is None:
            try:
                user = self.RABBITMQ_USER
                pw = self.RABBITMQ_PASSWORD.get_secret_value() if isinstance(self.RABBITMQ_PASSWORD, SecretStr) else self.RABBITMQ_PASSWORD
                host, port, vhost = self.RABBITMQ_HOST, self.RABBITMQ_PORT, "" if self.RABBITMQ_VHOST == "/" else f"/{self.RABBITMQ_VHOST.lstrip('/')}"
                self.CELERY_BROKER_URL = f"amqp://{user}:{pw}@{host}:{port}{vhost}"
            except Exception as e: logger.error(f"Error building Celery Broker URL: {e}")

        # Build REDIS_URL if not set
        if self.REDIS_URL is None:
             try: self.REDIS_URL = f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
             except Exception as e: logger.error(f"Error building Redis URL: {e}")

        # Build Celery Result Backend URL (using Redis URL)
        if self.CELERY_RESULT_BACKEND is None and self.REDIS_URL:
            self.CELERY_RESULT_BACKEND = self.REDIS_URL
        elif self.CELERY_RESULT_BACKEND is None:
             logger.error("Could not build Celery Result Backend URL (Redis URL missing/invalid)")

        # Ensure directories exist
        paths_to_create = [self.DICOM_STORAGE_PATH, self.DICOM_ERROR_PATH]
        if self.TEMP_DIR: paths_to_create.append(self.TEMP_DIR)
        for path in paths_to_create:
            if path:
                try: path.mkdir(parents=True, exist_ok=True); logger.debug(f"Ensured directory exists: {path}")
                except Exception as e: logger.error(f"Error creating directory {path}: {e}")

# Instantiate the settings
settings = Settings()

# Log essential settings on startup
logger.info("--- Axiom Flow Configuration Loaded ---")
logger.info(f"DEBUG mode: {settings.DEBUG}")
db_uri_display = str(settings.SQLALCHEMY_DATABASE_URI).split('@')[-1] if settings.SQLALCHEMY_DATABASE_URI else 'Not Set'
logger.info(f"Database URI (host/db): {db_uri_display}")
logger.info(f"Celery Result Backend: {settings.CELERY_RESULT_BACKEND or 'Not Set'}")
logger.info(f"Redis URL: {settings.REDIS_URL or 'Not Set'}")
# --- REMOVED Static Listener Log ---
# logger.info(f"DICOM SCP AE Title: {settings.DICOM_LISTENER_AET} Port: {settings.DICOM_LISTENER_PORT}")
# --- END REMOVED ---
logger.info(f"Incoming DICOM Path: {settings.DICOM_STORAGE_PATH}")
logger.info(f"Error DICOM Path: {settings.DICOM_ERROR_PATH}")
logger.info(f"Known Input Sources (Static + Env): {settings.KNOWN_INPUT_SOURCES}")
logger.info("---------------------------------------")
