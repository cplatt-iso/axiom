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
    model_config = SettingsConfigDict(
        env_file=('.env.prod', '.env'),
        env_file_encoding='utf-8',
        case_sensitive=True,
        extra='ignore'
    )

    PROJECT_NAME: str = "Axiom Flow"
    PROJECT_VERSION: str = "pre-alpha 0.04.25.01" # Add if missing
    ENVIRONMENT: str = "development" # Add if missing
    API_V1_STR: str = "/api/v1"
    DEBUG: bool = False

    SECRET_KEY: SecretStr = SecretStr("09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7
    ALGORITHM: str = "HS256"

    GOOGLE_OAUTH_CLIENT_ID: Optional[str] = None

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

    FIRST_SUPERUSER_EMAIL: EmailStr = "admin@axiomflow.com"
    FIRST_SUPERUSER_PASSWORD: SecretStr = SecretStr("changeme")

    RABBITMQ_HOST: str = "rabbitmq"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: SecretStr = SecretStr("guest")
    RABBITMQ_VHOST: str = "/"
    CELERY_BROKER_URL: Optional[str] = None

    OPENAI_API_KEY: Optional[SecretStr] = None

    # --- Vertex AI Gemini Settings ---
    VERTEX_AI_PROJECT: Optional[str] = None # Your GCP Project ID
    VERTEX_AI_LOCATION: Optional[str] = "us-central1" # Default, but make configurable
    VERTEX_AI_MODEL_NAME: str = "gemini-2.5-flash-preview-04-17" # Or gemini-1.0-pro, etc.
    VERTEX_AI_CREDENTIALS_SECRET_ID: Optional[str] = None # Optional: GCP Secret Manager ID for Service Account JSON key
    VERTEX_AI_CREDENTIALS_JSON_PATH: Optional[str] = None # Optional: Path to local Service Account JSON key file (less secure, useful for local dev)
    VERTEX_AI_MAX_OUTPUT_TOKENS_VOCAB: int = 150
    VERTEX_AI_MAX_OUTPUT_TOKENS_BODYPART: int = 150
    VERTEX_AI_TEMPERATURE_VOCAB: float = 0.2
    VERTEX_AI_TOP_P_VOCAB: float = 0.8
    VERTEX_AI_TOP_K_VOCAB: int = 40

    # --- AI Invocation Tracking ---
    AI_INVOCATION_COUNTER_ENABLED: bool = True
    AI_INVOCATION_COUNTER_KEY_PREFIX: str = "axiom:ai:invocations"
    AI_THREAD_POOL_WORKERS: int = 2
    AI_SYNC_WRAPPER_TIMEOUT: int = 30

    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_URL: Optional[str] = None

    CELERY_RESULT_BACKEND: Optional[str] = None
    CELERY_TASK_DEFAULT_QUEUE: str = "default"
    CELERY_TASK_MAX_RETRIES: int = 3
    CELERY_TASK_RETRY_DELAY: int = 60
    CELERY_WORKER_CONCURRENCY: int = 4         # Default concurrency
    CELERY_PREFETCH_MULTIPLIER: int = 1        # Default prefetch multiplier
    CELERY_ACKS_LATE: bool = True              # Default ack setting

    LISTENER_HOST: str = "0.0.0.0"

    DICOM_STORAGE_PATH: Path = Path("/dicom_data/incoming")
    DICOM_ERROR_PATH: Path = Path("/dicom_data/errors")
    FILESYSTEM_STORAGE_PATH: Path = Path("/dicom_data/processed")
    TEMP_DIR: Optional[Path] = None
    DELETE_ON_SUCCESS: bool = True
    DELETE_UNMATCHED_FILES: bool = False
    DELETE_ON_NO_DESTINATION: bool = False
    MOVE_TO_ERROR_ON_PARTIAL_FAILURE: bool = True

    LOG_ORIGINAL_ATTRIBUTES: bool = True

    # DICOMweb Poller Settings
    DICOMWEB_POLLER_DEFAULT_FALLBACK_DAYS: int = 7
    DICOMWEB_POLLER_OVERLAP_MINUTES: int = 5
    DICOMWEB_POLLER_QIDO_LIMIT: int = 5000
    DICOMWEB_POLLER_MAX_SOURCES: int = 100

    # DIMSE Q/R Poller Settings
    DIMSE_QR_POLLER_MAX_SOURCES: int = 100 # Added this setting
    DIMSE_ACSE_TIMEOUT: int = 30     # Timeout for establishing association (seconds)
    DIMSE_DIMSE_TIMEOUT: int = 60    # Timeout for DIMSE message responses (seconds)
    DIMSE_NETWORK_TIMEOUT: int = 30  # Low-level network activity timeout (seconds)

    KNOWN_INPUT_SOURCES: List[str] = [
        "api_json",
        "stow_rs",
    ]

    @field_validator("KNOWN_INPUT_SOURCES", mode='before')
    @classmethod
    def assemble_known_sources(cls, v: Union[str, List[str]], info: ValidationInfo) -> List[str]:
        default_sources: List[str] = []
        static_sources_in_code = ["api_json", "stow_rs"]
        parsed_sources = []
        if isinstance(v, str):
            if v.startswith("["):
                 try: parsed_sources = json.loads(v)
                 except json.JSONDecodeError: raise ValueError("Invalid JSON string for KNOWN_INPUT_SOURCES")
            else: parsed_sources = [i.strip() for i in v.split(",") if i.strip()]
        elif isinstance(v, list): parsed_sources = v
        combined = set(static_sources_in_code); combined.update(parsed_sources)
        return sorted(list(combined))


    def model_post_init(self, __context: Any) -> None:
        if self.CELERY_BROKER_URL is None:
            try:
                user = self.RABBITMQ_USER
                pw = self.RABBITMQ_PASSWORD.get_secret_value() if isinstance(self.RABBITMQ_PASSWORD, SecretStr) else self.RABBITMQ_PASSWORD
                host, port, vhost = self.RABBITMQ_HOST, self.RABBITMQ_PORT, "" if self.RABBITMQ_VHOST == "/" else f"/{self.RABBITMQ_VHOST.lstrip('/')}"
                self.CELERY_BROKER_URL = f"amqp://{user}:{pw}@{host}:{port}{vhost}"
            except Exception as e: logger.error(f"Error building Celery Broker URL: {e}")
        if self.VERTEX_AI_PROJECT:
            logger.info(f"Vertex AI Project: {self.VERTEX_AI_PROJECT}")
            logger.info(f"Vertex AI Location: {self.VERTEX_AI_LOCATION}")
            logger.info(f"Vertex AI Model: {self.VERTEX_AI_MODEL_NAME}")
            if self.VERTEX_AI_CREDENTIALS_SECRET_ID:
                logger.info(f"Vertex AI Credentials: Using Secret Manager ID {self.VERTEX_AI_CREDENTIALS_SECRET_ID}")
            elif self.VERTEX_AI_CREDENTIALS_JSON_PATH:
                 logger.info(f"Vertex AI Credentials: Using JSON key path {self.VERTEX_AI_CREDENTIALS_JSON_PATH}")
            else:
                logger.info("Vertex AI Credentials: Using Application Default Credentials (ADC). Ensure ADC are configured.")
        else:
            logger.warning("Vertex AI Project (VERTEX_AI_PROJECT) not set. Gemini features will be unavailable.")
        if self.REDIS_URL is None:
             try: self.REDIS_URL = f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
             except Exception as e: logger.error(f"Error building Redis URL: {e}")

        if self.CELERY_RESULT_BACKEND is None and self.REDIS_URL:
            self.CELERY_RESULT_BACKEND = self.REDIS_URL
        elif self.CELERY_RESULT_BACKEND is None:
             logger.error("Could not build Celery Result Backend URL (Redis URL missing/invalid)")

        paths_to_create = [self.DICOM_STORAGE_PATH, self.DICOM_ERROR_PATH]
        if self.TEMP_DIR: paths_to_create.append(self.TEMP_DIR)
        for path in paths_to_create:
            if path:
                try: path.mkdir(parents=True, exist_ok=True); logger.debug(f"Ensured directory exists: {path}")
                except Exception as e: logger.error(f"Error creating directory {path}: {e}")

settings = Settings()

# ... (rest of the logging remains the same) ...
logger.info("--- Axiom Flow Configuration Loaded ---")
logger.info(f"DEBUG mode: {settings.DEBUG}")
db_uri_display = str(settings.SQLALCHEMY_DATABASE_URI).split('@')[-1] if settings.SQLALCHEMY_DATABASE_URI else 'Not Set'
logger.info(f"Database URI (host/db): {db_uri_display}")
logger.info(f"Celery Result Backend: {settings.CELERY_RESULT_BACKEND or 'Not Set'}")
logger.info(f"Redis URL: {settings.REDIS_URL or 'Not Set'}")
logger.info(f"Incoming DICOM Path: {settings.DICOM_STORAGE_PATH}")
logger.info(f"Error DICOM Path: {settings.DICOM_ERROR_PATH}")
logger.info(f"Log Original Attributes: {settings.LOG_ORIGINAL_ATTRIBUTES}")
logger.info(f"Known Input Sources (Static + Env): {settings.KNOWN_INPUT_SOURCES}")
logger.info(f"OpenAI API Key Loaded: {'Yes' if settings.OPENAI_API_KEY else 'No'}")
logger.info(f"DICOMweb Max Sources: {settings.DICOMWEB_POLLER_MAX_SOURCES}") # Log the existing one
logger.info(f"DIMSE Q/R Max Sources: {settings.DIMSE_QR_POLLER_MAX_SOURCES}") # Log the new one
logger.info(f"Vertex AI Configured: {'Yes' if settings.VERTEX_AI_PROJECT else 'No'}")
logger.info(f"AI Invocation Counting Enabled: {settings.AI_INVOCATION_COUNTER_ENABLED}")
logger.info("---------------------------------------")
 
