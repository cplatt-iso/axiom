# app/core/config.py
import os
from typing import List, Optional, Union
from pydantic import AnyHttpUrl, PostgresDsn, field_validator, ValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

# Determine the base directory of the project
# This assumes config.py is in app/core/
# Adjust if the location changes
BASE_DIR = Path(__file__).resolve().parent.parent.parent

class Settings(BaseSettings):
    """
    Application Settings using Pydantic BaseSettings.
    Loads from environment variables and .env files.
    """
    model_config = SettingsConfigDict(
        # `.env.prod` takes priority over `.env`
        env_file=('.env.prod', '.env'),
        env_file_encoding='utf-8',
        case_sensitive=True,
        extra='ignore' # Ignore extra fields from env file
    )

    # --- Core Application Settings ---
    PROJECT_NAME: str = "Axiom flow"
    API_V1_STR: str = "/api/v1"
    # Set DEBUG = False in production
    DEBUG: bool = False
    # Origins allowed for CORS (Cross-Origin Resource Sharing)
    # Use ["*"] for development ONLY if necessary, be specific in production
    BACKEND_CORS_ORIGINS: List[Union[str, AnyHttpUrl]] = ["http://localhost:3000"] # Example: React frontend address

    @field_validator("BACKEND_CORS_ORIGINS", mode='before')
    @classmethod
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, list):
            return v
        raise ValueError(v)

    # --- Database Settings ---
    # Assemble Postgres DSN from components if needed, or use full URL
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "dicom_processor_user"
    POSTGRES_PASSWORD: str = "changeme"
    POSTGRES_DB: str = "dicom_processor_db"
    SQLALCHEMY_DATABASE_URI: Optional[PostgresDsn] = None

    @field_validator("SQLALCHEMY_DATABASE_URI", mode="before")
    @classmethod
    def assemble_db_connection(cls, v: Optional[str], info: ValidationInfo) -> str:
        if isinstance(v, str):
            return v # If the full URI is provided, use it
        values = info.data
        return str(PostgresDsn.build(
            scheme="postgresql+psycopg", # Use psycopg (psycopg2 binary) driver
            username=values.get("POSTGRES_USER"),
            password=values.get("POSTGRES_PASSWORD"),
            host=values.get("POSTGRES_SERVER"),
            port=values.get("POSTGRES_PORT"),
            path=f"{values.get('POSTGRES_DB') or ''}",
        ))

    # --- RabbitMQ Settings (Celery Broker) ---
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_VHOST: str = "/"
    CELERY_BROKER_URL: Optional[str] = None

    @field_validator("CELERY_BROKER_URL", mode="before")
    @classmethod
    def assemble_celery_broker_url(cls, v: Optional[str], info: ValidationInfo) -> str:
        if isinstance(v, str):
            return v # If the full URI is provided, use it
        values = info.data
        user = values.get("RABBITMQ_USER")
        password = values.get("RABBITMQ_PASSWORD")
        host = values.get("RABBITMQ_HOST")
        port = values.get("RABBITMQ_PORT")
        vhost = values.get("RABBITMQ_VHOST", "/")
        # Ensure vhost starts with / if it's not empty
        if vhost and not vhost.startswith("/"):
             vhost = "/" + vhost
        elif vhost == "/": # Celery URL needs empty string for default vhost "/"
            vhost = ""

        return f"amqp://{user}:{password}@{host}:{port}{vhost}"

    # --- Celery Worker Settings ---
    CELERY_RESULT_BACKEND: Optional[str] = None # Optional: Can be Redis, DB, etc. if results needed
    CELERY_TASK_DEFAULT_QUEUE: str = "default" # Default queue name

    # --- DICOM SCP (Receiver) Settings ---
    DICOM_SCP_AE_TITLE: str = "DICOM_PROCESSOR"
    DICOM_SCP_PORT: int = 11112
    DICOM_STORAGE_PATH: str = "/dicom_data/incoming" # Temporary storage before processing

    # --- Storage Backend Configuration ---
    # Example: configure which storage backend to use by default or based on rules
    # This would likely become more complex, involving rule-based destination selection
    DEFAULT_STORAGE_BACKEND: str = "filesystem" # Options: filesystem, cstore, gcs
    FILESYSTEM_STORAGE_PATH: str = "/dicom_data/processed"
    CSTORE_DESTINATION_AE_TITLE: str = "REMOTE_PACS"
    CSTORE_DESTINATION_HOST: str = "192.168.88.115"
    CSTORE_DESTINATION_PORT: int = 11113
    # Optional: GCS settings (only needed if gcs backend is used)
    GCS_BUCKET_NAME: Optional[str] = None
    GCS_PROJECT_ID: Optional[str] = None
    # GOOGLE_APPLICATION_CREDENTIALS environment variable should point to the service account key file

    # --- Security Settings ---
    # Generate using: openssl rand -hex 32
    SECRET_KEY: str = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
    # Needs configuration depending on IdP choice (e.g., Keycloak, Okta)
    # Placeholder for now
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    ALGORITHM: str = "HS256" # For JWT token signing

    # --- Add Google Client ID ---
    GOOGLE_OAUTH_CLIENT_ID: str = ""

# Instantiate the settings
settings = Settings()

# --- Ensure necessary directories exist ---
Path(settings.DICOM_STORAGE_PATH).mkdir(parents=True, exist_ok=True)
if settings.DEFAULT_STORAGE_BACKEND == 'filesystem' or 'filesystem' in settings.FILESYSTEM_STORAGE_PATH: # Simple check
     Path(settings.FILESYSTEM_STORAGE_PATH).mkdir(parents=True, exist_ok=True)

print(f"DEBUG mode is: {settings.DEBUG}")
print(f"Database URI: {settings.SQLALCHEMY_DATABASE_URI}")
print(f"Celery Broker URL: {settings.CELERY_BROKER_URL}")
print(f"DICOM SCP AE Title: {settings.DICOM_SCP_AE_TITLE} Port: {settings.DICOM_SCP_PORT}")
