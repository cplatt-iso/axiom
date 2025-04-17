# app/core/config.py
import os
from typing import List, Optional, Union, Any
from pydantic import AnyHttpUrl, PostgresDsn, field_validator, ValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

# Determine the base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent.parent

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
    PROJECT_NAME: str = "Axiom flow"
    API_V1_STR: str = "/api/v1"
    DEBUG: bool = False
    BACKEND_CORS_ORIGINS: List[Union[str, AnyHttpUrl]] = ["http://localhost:3000"]

    @field_validator("BACKEND_CORS_ORIGINS", mode='before')
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, list):
            return v
        raise ValueError(v)

    # --- Database Settings ---
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "dicom_processor_user"
    POSTGRES_PASSWORD: str = "changeme"
    POSTGRES_DB: str = "dicom_processor_db"
    SQLALCHEMY_DATABASE_URI: Optional[str] = None

    # --- RabbitMQ Settings (Celery Broker) ---
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_VHOST: str = "/"
    CELERY_BROKER_URL: Optional[str] = None

    # --- Celery Worker Settings ---
    CELERY_RESULT_BACKEND: Optional[str] = None
    CELERY_TASK_DEFAULT_QUEUE: str = "default"

    # --- DICOM SCP (Receiver) Settings ---
    DICOM_SCP_AE_TITLE: str = "DICOM_PROCESSOR"
    DICOM_SCP_PORT: int = 11112
    DICOM_STORAGE_PATH: str = "/dicom_data/incoming"
    LISTENER_HOST: str = "0.0.0.0"
    # Default source identifier for the main SCP listener
    DEFAULT_SCP_SOURCE_ID: str = "dicom_scp_main" # Use a specific ID

    # Alias LISTENER_PORT for consistency
    @property
    def LISTENER_PORT(self) -> int:
        return self.DICOM_SCP_PORT
    @property
    def LISTENER_AE_TITLE(self) -> str:
        return self.DICOM_SCP_AE_TITLE

    # --- Storage Backend Configuration (Example) ---
    DEFAULT_STORAGE_BACKEND: str = "filesystem"
    FILESYSTEM_STORAGE_PATH: str = "/dicom_data/processed"
    CSTORE_DESTINATION_AE_TITLE: str = "REMOTE_PACS"
    CSTORE_DESTINATION_HOST: str = "192.168.88.115"
    CSTORE_DESTINATION_PORT: int = 11113
    GCS_BUCKET_NAME: Optional[str] = None
    GCS_PROJECT_ID: Optional[str] = None

    # --- Security Settings ---
    SECRET_KEY: str = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7 # 1 week
    ALGORITHM: str = "HS256"

    # --- Google OAuth Client ID ---
    GOOGLE_OAUTH_CLIENT_ID: str = ""

    # --- Redis Settings ---
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_URL: Optional[str] = None

    # --- Known Input Source Identifiers ---
    # Define the canonical identifiers for different ways data can enter the system.
    # These will be used in the UI and for rule matching.
    # Default list defined here, can be overridden by ENV var (comma-separated)
    KNOWN_INPUT_SOURCES: List[str] = [
        "dicom_scp_main", # Default identifier for the main C-STORE SCP
        "api_json",       # Identifier for the upcoming JSON API endpoint
        "dicomweb_stow",  # Identifier for the future DICOMweb STOW-RS input
        "filesystem_poll" # Identifier for a future filesystem polling input
    ]

    @field_validator("KNOWN_INPUT_SOURCES", mode='before')
    @classmethod
    def assemble_known_sources(cls, v: Union[str, List[str]]) -> List[str]:
        """Allow defining known sources as a comma-separated string in ENV VARS."""
        default_sources = [ # Define the default list locally
            "dicom_scp_main", "api_json", "dicomweb_stow", "filesystem_poll"
        ]
        if isinstance(v, str) and not v.startswith("["):
            # Parse from comma-separated string
            sources = [i.strip() for i in v.split(",") if i.strip()]
            return sources if sources else default_sources # Return parsed or default if empty
        elif isinstance(v, list):
            # Use the list provided (e.g., from code default)
             return v if v else default_sources # Return provided or default if empty
        # Fallback to default if invalid type provided
        return default_sources

    # --- Function to build derived settings after initialization ---
    def model_post_init(self, __context: Any) -> None:
        """Build derived settings and perform post-initialization validation."""
        # Build Database URI if not provided directly
        if self.SQLALCHEMY_DATABASE_URI is None:
            try:
                self.SQLALCHEMY_DATABASE_URI = str(PostgresDsn.build(
                    scheme="postgresql+psycopg", # Use psycopg driver
                    username=self.POSTGRES_USER,
                    password=self.POSTGRES_PASSWORD,
                    host=self.POSTGRES_SERVER,
                    port=self.POSTGRES_PORT,
                    path=f"{self.POSTGRES_DB or ''}",
                ))
            except Exception as e:
                 print(f"Error building SQLAlchemy URI: {e}")
                 # Decide how to handle this - raise error? Set to None?
                 self.SQLALCHEMY_DATABASE_URI = None # Set to None if build fails

        # Build Celery Broker URL if not provided directly
        if self.CELERY_BROKER_URL is None:
            try:
                user = self.RABBITMQ_USER
                password = self.RABBITMQ_PASSWORD
                host = self.RABBITMQ_HOST
                port = self.RABBITMQ_PORT
                vhost = self.RABBITMQ_VHOST
                vhost_part = "" if vhost == "/" else f"/{vhost.lstrip('/')}"
                self.CELERY_BROKER_URL = f"amqp://{user}:{password}@{host}:{port}{vhost_part}"
            except Exception as e:
                 print(f"Error building Celery Broker URL: {e}")
                 self.CELERY_BROKER_URL = None

        # Build Redis URL if not provided directly
        if self.REDIS_URL is None:
             try:
                  self.REDIS_URL = f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
             except Exception as e:
                  print(f"Error building Redis URL: {e}")
                  self.REDIS_URL = None

        # --- Post-init Validation/Correction ---
        # Ensure the default SCP source ID is actually in the known list AFTER initialization
        if self.DEFAULT_SCP_SOURCE_ID not in self.KNOWN_INPUT_SOURCES:
             print(f"Warning: DEFAULT_SCP_SOURCE_ID '{self.DEFAULT_SCP_SOURCE_ID}' was not in the initial KNOWN_INPUT_SOURCES list. Adding it.")
             # Prepend it to ensure it's present
             self.KNOWN_INPUT_SOURCES.insert(0, self.DEFAULT_SCP_SOURCE_ID)
             # Remove duplicates if it happened to be added some other way
             self.KNOWN_INPUT_SOURCES = sorted(list(set(self.KNOWN_INPUT_SOURCES)))

# Instantiate the settings
settings = Settings()

# --- Ensure necessary directories exist ---
try:
    Path(settings.DICOM_STORAGE_PATH).mkdir(parents=True, exist_ok=True)
    if settings.DEFAULT_STORAGE_BACKEND == 'filesystem' and settings.FILESYSTEM_STORAGE_PATH:
         Path(settings.FILESYSTEM_STORAGE_PATH).mkdir(parents=True, exist_ok=True)
except Exception as e:
     print(f"Error creating storage directories: {e}")


# Optional: Print key settings on startup (consider removing sensitive ones)
print(f"--- Axiom Flow Configuration ---")
print(f"DEBUG mode: {settings.DEBUG}")
db_uri_display = '*** masked ***' if settings.SQLALCHEMY_DATABASE_URI else 'Not Set'
print(f"Database URI: {db_uri_display}")
broker_url_display = '*** masked ***' if settings.CELERY_BROKER_URL else 'Not Set'
print(f"Celery Broker URL: {broker_url_display}")
print(f"Redis URL: {settings.REDIS_URL or 'Not Set'}")
print(f"DICOM SCP AE Title: {settings.LISTENER_AE_TITLE} Port: {settings.LISTENER_PORT} Host: {settings.LISTENER_HOST}")
print(f"DICOM SCP Default Source ID: {settings.DEFAULT_SCP_SOURCE_ID}")
print(f"Incoming DICOM Path: {settings.DICOM_STORAGE_PATH}")
print(f"Known Input Sources: {settings.KNOWN_INPUT_SOURCES}")
print(f"------------------------------")
