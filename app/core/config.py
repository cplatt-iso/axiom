# app/core/config.py
import os
import json
from typing import List, Optional, Union, Any, Dict, Sequence
from pydantic import (
    AnyHttpUrl, PostgresDsn, field_validator, ValidationInfo, BaseModel,
    EmailStr, SecretStr, Field
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

import pydicom
import pydicom.uid

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging # Fallback if structlog isn't there (should be, but defensive)
    logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=('.env.prod', '.env'),
        env_file_encoding='utf-8',
        case_sensitive=True,
        extra='ignore'
    )

    PROJECT_NAME: str = "Axiom Flow"
    PROJECT_VERSION: str = "hallucination pahse" 
    ENVIRONMENT: str = "development" # Add if missing
    API_V1_STR: str = "/api/v1"
    DEBUG: bool = False
    LOG_LEVEL: str = "DEBUG"

    SECRET_KEY: SecretStr
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7
    ALGORITHM: str = "HS256"
    SECRET_CACHE_MAX_SIZE: int = int(os.getenv("SECRET_CACHE_MAX_SIZE", 100))
    SECRET_CACHE_TTL_SECONDS: int = int(os.getenv("SECRET_CACHE_TTL_SECONDS", 3600)) # Default 1 hour

    GOOGLE_OAUTH_CLIENT_ID: Optional[str] = None

    POSTGRES_SERVER: str = "db"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "dicom_processor_user"
    POSTGRES_PASSWORD: SecretStr = SecretStr("changeme")
    POSTGRES_DB: str = "dicom_processor_db"
    SQLALCHEMY_DATABASE_URI: Optional[PostgresDsn] = None

    STALE_DATA_CLEANUP_AGE_DAYS: int = 30 # For general old data, not used yet by this task directly for files
    STALE_RETRY_IN_PROGRESS_AGE_HOURS: int = 6 # How long before a RETRY_IN_PROGRESS is considered stuck
    CLEANUP_BATCH_SIZE: int = 100 # How many records to process per cleanup run
    CLEANUP_STALE_DATA_INTERVAL_HOURS: int = 24 # How often to run the cleanup task (e.g., daily)

    CELERY_RESULT_BACKEND: Optional[str] = None
    CELERY_TASK_DEFAULT_QUEUE: str = "default"
    CELERY_TASK_MAX_RETRIES: int = 3
    CELERY_TASK_RETRY_DELAY: int = 60
    CELERY_WORKER_CONCURRENCY: int = 4         # Default concurrency
    CELERY_PREFETCH_MULTIPLIER: int = 1        # Default prefetch multiplier
    CELERY_ACKS_LATE: bool = True              # Default ack setting

    EXCEPTION_RETRY_BATCH_SIZE: int = 10
    EXCEPTION_RETRY_MAX_BACKOFF_SECONDS: int = 3600 # 1 hour
    EXCEPTION_RETRY_INTERVAL_MINUTES: int = 5 # For Celery Beat schedule
    EXCEPTION_MAX_RETRIES: int = CELERY_TASK_MAX_RETRIES # Defaults to Celery's max retries
    EXCEPTION_RETRY_DELAY_SECONDS: int = CELERY_TASK_RETRY_DELAY # Defaults to Celery's base delay

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
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Sequence[Union[str, AnyHttpUrl]]:
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
    OPENAI_MODEL_NAME_RULE_GEN: Optional[str] = None # Optional: Model name for OpenAI rule generation
    OPENAI_TEMPERATURE_RULE_GEN: float = 0.3
    OPENAI_MAX_TOKENS_RULE_GEN: int = 512


    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_URL: Optional[str] = None

    # --- Vertex AI Gemini Settings ---
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/etc/gcp/axiom-flow-gcs-key.json")
    VERTEX_AI_PROJECT: Optional[str] = None # Your GCP Project ID
    VERTEX_AI_LOCATION: Optional[str] = "us-central1" # Default, but make configurable
    VERTEX_AI_MODEL_NAME: str = "gemini-2.5-flash-preview-05-20" # Or gemini-1.0-pro, etc.
    VERTEX_AI_CREDENTIALS_SECRET_ID: Optional[str] = None # Optional: GCP Secret Manager ID for Service Account JSON key
    VERTEX_AI_CREDENTIALS_JSON_PATH: Optional[str] = None # Optional: Path to local Service Account JSON key file (less secure, useful for local dev)
    VERTEX_AI_MAX_OUTPUT_TOKENS_VOCAB: int = 150
    VERTEX_AI_MAX_OUTPUT_TOKENS_BODYPART: int = 150
    VERTEX_AI_TEMPERATURE_VOCAB: float = 0.2
    VERTEX_AI_TOP_P_VOCAB: float = 0.8
    VERTEX_AI_TOP_K_VOCAB: int = 40
    VERTEX_AI_CONFIG_PROJECT_ID: Optional[str] = None # Optional: Project ID for Vertex AI (if different from GCP project)

    # --- AI Invocation Tracking ---
    AI_INVOCATION_COUNTER_ENABLED: bool = True
    AI_INVOCATION_COUNTER_KEY_PREFIX: str = "axiom:ai:invocations"
    AI_THREAD_POOL_WORKERS: int = 2
    AI_SYNC_WRAPPER_TIMEOUT: int = 30

    AI_VOCAB_CACHE_ENABLED: bool = bool(os.getenv("AI_VOCAB_CACHE_ENABLED", "True").lower() in ("true", "1", "yes"))
    AI_VOCAB_CACHE_TTL_SECONDS: int = int(os.getenv("AI_VOCAB_CACHE_TTL_SECONDS", 60 * 60 * 24 * 30)) # Default 30 days
    AI_VOCAB_CACHE_KEY_PREFIX: str = os.getenv("AI_VOCAB_CACHE_KEY_PREFIX", "ax_ai_vocab")

    LISTENER_HOST: str = "0.0.0.0"

    DICOM_STORAGE_PATH: Path = Path("/dicom_data/incoming")
    DICOM_ERROR_PATH: Path = Path("/dicom_data/errors")
    FILESYSTEM_STORAGE_PATH: Path = Path("/dicom_data/processed")
    DICOM_RETRY_STAGING_PATH: Path = Path("/dicom_data/retry_staging")
    TEMP_DIR: Optional[Path] = None
    DELETE_ON_SUCCESS: bool = True
    DELETE_UNMATCHED_FILES: bool = False
    DELETE_ON_NO_DESTINATION: bool = False
    DELETE_ON_PARTIAL_FAILURE_IF_MODIFIED: bool = False # <-- ADD THIS LINE (defaulting to False)

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

    RULES_CACHE_ENABLED: bool = Field(default=True)
    # RULES_CACHE_ENABLED: bool = False
    RULES_CACHE_TTL_SECONDS: int = Field(default=60)  # in seconds

    PYDICOM_IMPLEMENTATION_UID: str = "1.2.826.0.1.3680043.8.498.1" # Example: generate a new UID or use a predefined one
    IMPLEMENTATION_VERSION_NAME: str = "AXIOM_FLOW_V" # Also ensure this is defined if not already

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

        paths_to_create = [self.DICOM_STORAGE_PATH, self.DICOM_ERROR_PATH, self.DICOM_RETRY_STAGING_PATH]
        if self.TEMP_DIR: paths_to_create.append(self.TEMP_DIR)
        for path in paths_to_create:
            if path:
                try: path.mkdir(parents=True, exist_ok=True); logger.debug(f"Ensured directory exists: {path}")
                except Exception as e: logger.error(f"Error creating directory {path}: {e}")

settings = Settings() # type: ignore[call-arg]

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

