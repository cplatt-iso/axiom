# app/core/config.py
import os
import json # <--- Import json
from typing import List, Optional, Union, Any, Dict # <--- Add Dict
from pydantic import (
    AnyHttpUrl, PostgresDsn, field_validator, ValidationInfo, BaseModel as PydanticBaseModel, # Import BaseModel
    constr # Import constr for constrained strings
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

# Determine the base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# --- Define Pydantic Models for DICOMweb Configuration ---

class DicomWebSourceAuth(PydanticBaseModel):
    """Authentication details for a DICOMweb source."""
    type: str = 'none' # e.g., 'none', 'basic', 'bearer', 'apikey'
    username: Optional[str] = None
    password: Optional[str] = None # Store securely, consider secrets management
    token: Optional[str] = None # For bearer tokens
    header_name: Optional[str] = None # For apikey in header
    header_value: Optional[str] = None # For apikey in header

class DicomWebSourceConfig(PydanticBaseModel):
    """Configuration for a single DICOMweb source poller."""
    name: constr(strip_whitespace=True, min_length=1) # Unique name, acts as source_identifier
    qido_url_prefix: AnyHttpUrl
    wado_url_prefix: AnyHttpUrl
    is_enabled: bool = True
    polling_interval_seconds: int = 300 # Default: 5 minutes
    qido_query_params: Dict[str, str] = {"Modality": "CT", "StudyDate": "-1"} # Example: CTs from yesterday, '-1' means yesterday
    auth: DicomWebSourceAuth = DicomWebSourceAuth() # Default to no auth
    request_headers: Optional[Dict[str, str]] = None # e.g., {"Accept": "application/dicom+json"}

# --- Main Settings Class ---

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
    # --- Celery Task Retry Settings (NEW) ---
    CELERY_TASK_MAX_RETRIES: int = 3        # Default max retries for tasks
    CELERY_TASK_RETRY_DELAY: int = 60       # Default delay (seconds) before retrying

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

    # --- Error Handling & Worker Behavior ---
    DICOM_ERROR_PATH: str = "/dicom_data/errors"
    DELETE_UNMATCHED_FILES: bool = False # Delete files if no rules match?
    DELETE_ON_NO_DESTINATION: bool = False # Delete if rules match but no destinations?
    DELETE_ON_SUCCESS: bool = True # Delete original file after all destinations succeed?
    MOVE_TO_ERROR_ON_PARTIAL_FAILURE: bool = False # Move to error dir if some destinations fail?

    # --- Temporary File Handling (NEW - Optional but recommended) ---
    TEMP_DIR: Optional[str] = None # Base directory for temporary files (e.g., STOW), None uses system default

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
    # Default list defined here, can be overridden by ENV var (comma-separated)
    # We will automatically add DICOMweb source names to this list later.
    KNOWN_INPUT_SOURCES: List[str] = [
        "dicom_scp_main", # Default identifier for the main C-STORE SCP
        "api_json",       # Identifier for the JSON API endpoint
        "filesystem_poll" # Identifier for a future filesystem polling input
        # DICOMweb source names will be added dynamically
    ]

    @field_validator("KNOWN_INPUT_SOURCES", mode='before')
    @classmethod
    def assemble_known_sources(cls, v: Union[str, List[str]]) -> List[str]:
        """Allow defining known sources as a comma-separated string in ENV VARS."""
        # Initial default list (without dynamic sources yet)
        default_sources = ["dicom_scp_main", "api_json", "filesystem_poll"]
        parsed_sources = []
        if isinstance(v, str) and not v.startswith("["):
            parsed_sources = [i.strip() for i in v.split(",") if i.strip()]
        elif isinstance(v, list):
            parsed_sources = v # Use the list provided

        # Return parsed sources if available, otherwise default
        # We'll add DICOMweb sources in model_post_init
        return parsed_sources if parsed_sources else default_sources


    # --- DICOMweb Poller Configuration ---
    # Can be overridden by setting DICOMWEB_SOURCES_JSON environment variable
    # to a JSON string representing a list of DicomWebSourceConfig objects.
    DICOMWEB_SOURCES_JSON: Optional[str] = None # Allow override via ENV
    DICOMWEB_SOURCES: List[DicomWebSourceConfig] = [
        # Example configuration (add more as needed, or override via ENV):
        # DicomWebSourceConfig(
        #     name="orthanc_local",
        #     qido_url_prefix="http://localhost:8042/dicom-web",
        #     wado_url_prefix="http://localhost:8042/dicom-web",
        #     is_enabled=True,
        #     polling_interval_seconds=60,
        #     qido_query_params={"StudyDate": "-1"}, # Studies from yesterday
        #     auth=DicomWebSourceAuth(type='basic', username='orthanc', password='changeme')
        # ),
        # DicomWebSourceConfig(
        #     name="public_idc_cr",
        #     qido_url_prefix="https://proxy.cancerimagingarchive.net/proxy/idc-external-016/dicomweb",
        #     wado_url_prefix="https://proxy.cancerimagingarchive.net/proxy/idc-external-016/dicomweb",
        #     is_enabled=False, # Disabled by default
        #     polling_interval_seconds=3600,
        #     qido_query_params={"Modality": "CR", "StudyDate": "20220101-20220105"}, # Example date range
        #     auth=DicomWebSourceAuth(type='none')
        # ),
    ]

    @field_validator('DICOMWEB_SOURCES', mode='before')
    def assemble_dicomweb_sources(cls, v: Any, info: ValidationInfo) -> List[Dict[str, Any]]:
        """Loads DICOMweb sources from JSON string if provided in environment."""
        json_str = info.data.get('DICOMWEB_SOURCES_JSON')
        sources = []
        if json_str:
            try:
                sources = json.loads(json_str)
                if not isinstance(sources, list):
                    raise ValueError("DICOMWEB_SOURCES_JSON must be a JSON array.")
                print(f"Loaded {len(sources)} DICOMweb sources from DICOMWEB_SOURCES_JSON environment variable.")
                return sources # Use the JSON from ENV VAR if valid
            except json.JSONDecodeError as e:
                print(f"Error decoding DICOMWEB_SOURCES_JSON: {e}. Falling back to code default.")
                return v # Fallback to default list defined in code
            except ValueError as e:
                print(f"Error validating DICOMWEB_SOURCES_JSON structure: {e}. Falling back to code default.")
                return v # Fallback to default list defined in code
        else:
            # If JSON env var is not set, use the default list from code
            print(f"Using default DICOMweb sources defined in code ({len(v)} sources).")
            return v


    # --- Function to build derived settings after initialization ---
    def model_post_init(self, __context: Any) -> None:
        """Build derived settings and perform post-initialization validation."""
        # Build Database URI
        if self.SQLALCHEMY_DATABASE_URI is None:
            try:
                self.SQLALCHEMY_DATABASE_URI = str(PostgresDsn.build(
                    scheme="postgresql+psycopg", # Changed from psycopg2 for wider compatibility? Or keep psycopg2 if installed.
                    username=self.POSTGRES_USER, password=self.POSTGRES_PASSWORD,
                    host=self.POSTGRES_SERVER, port=self.POSTGRES_PORT,
                    path=f"{self.POSTGRES_DB or ''}",
                ))
            except Exception as e:
                 print(f"Error building SQLAlchemy URI: {e}")
                 self.SQLALCHEMY_DATABASE_URI = None

        # Build Celery Broker URL
        if self.CELERY_BROKER_URL is None:
            try:
                user, pw = self.RABBITMQ_USER, self.RABBITMQ_PASSWORD
                host, port = self.RABBITMQ_HOST, self.RABBITMQ_PORT
                vhost_part = "" if self.RABBITMQ_VHOST == "/" else f"/{self.RABBITMQ_VHOST.lstrip('/')}"
                self.CELERY_BROKER_URL = f"amqp://{user}:{pw}@{host}:{port}{vhost_part}"
            except Exception as e:
                 print(f"Error building Celery Broker URL: {e}")
                 self.CELERY_BROKER_URL = None

        # Build Redis URL (needed for Celery result backend if not set explicitly)
        if self.REDIS_URL is None:
             try: self.REDIS_URL = f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
             except Exception as e: print(f"Error building Redis URL: {e}"); self.REDIS_URL = None
        # Build Celery Result Backend using Redis URL if not set directly
        if self.CELERY_RESULT_BACKEND is None and self.REDIS_URL:
            self.CELERY_RESULT_BACKEND = self.REDIS_URL

        # --- Post-init Validation/Correction ---

        # Ensure known sources are unique and include defaults/DICOMweb
        current_known_sources = set(self.KNOWN_INPUT_SOURCES)
        # Add the default SCP source if missing
        current_known_sources.add(self.DEFAULT_SCP_SOURCE_ID)
        # Add names from configured DICOMweb sources
        # Note: Need to ensure DICOMWEB_SOURCES has been parsed into models here.
        # The validator runs *before* model_post_init, so we work with the raw list here
        # if loaded from JSON, or models if using the default.
        if self.DICOMWEB_SOURCES:
            if isinstance(self.DICOMWEB_SOURCES[0], DicomWebSourceConfig): # Check if already parsed
                 dicomweb_source_names = {src.name for src in self.DICOMWEB_SOURCES}
            else: # Assume it's still a list of dicts from JSON override
                 dicomweb_source_names = {src.get('name') for src in self.DICOMWEB_SOURCES if src.get('name')}
            current_known_sources.update(dicomweb_source_names)

        # Update the list, keeping it sorted
        self.KNOWN_INPUT_SOURCES = sorted(list(current_known_sources))


# Instantiate the settings
settings = Settings()

# --- Ensure necessary directories exist ---
try:
    Path(settings.DICOM_STORAGE_PATH).mkdir(parents=True, exist_ok=True)
    if settings.DEFAULT_STORAGE_BACKEND == 'filesystem' and settings.FILESYSTEM_STORAGE_PATH:
         Path(settings.FILESYSTEM_STORAGE_PATH).mkdir(parents=True, exist_ok=True)
    # Create error path too
    if settings.DICOM_ERROR_PATH:
        Path(settings.DICOM_ERROR_PATH).mkdir(parents=True, exist_ok=True)
    # Create temp dir if specified
    if settings.TEMP_DIR:
        Path(settings.TEMP_DIR).mkdir(parents=True, exist_ok=True)

except Exception as e:
     print(f"Error creating storage/error/temp directories: {e}")


# Optional: Print key settings on startup (consider removing sensitive ones)
print(f"--- Axiom Flow Configuration ---")
print(f"DEBUG mode: {settings.DEBUG}")
db_uri_display = '***' if settings.POSTGRES_PASSWORD and settings.SQLALCHEMY_DATABASE_URI else settings.SQLALCHEMY_DATABASE_URI or 'Not Set'
print(f"Database URI: {db_uri_display}")
broker_url_display = '***' if settings.RABBITMQ_PASSWORD and settings.CELERY_BROKER_URL else settings.CELERY_BROKER_URL or 'Not Set'
print(f"Celery Broker URL: {broker_url_display}")
print(f"Celery Result Backend: {settings.CELERY_RESULT_BACKEND or 'Not Set'}") # Use the derived value
print(f"Redis URL: {settings.REDIS_URL or 'Not Set'}")
print(f"DICOM SCP AE Title: {settings.LISTENER_AE_TITLE} Port: {settings.LISTENER_PORT} Host: {settings.LISTENER_HOST}")
print(f"DICOM SCP Default Source ID: {settings.DEFAULT_SCP_SOURCE_ID}")
print(f"Incoming DICOM Path: {settings.DICOM_STORAGE_PATH}")
print(f"Error DICOM Path: {settings.DICOM_ERROR_PATH}")
print(f"Temporary Files Path: {settings.TEMP_DIR or 'System Default'}")
# Print DICOMweb sources count/names (optional)
# Correctly handle parsed models vs dicts after potential override
dicomweb_names_to_print = []
if settings.DICOMWEB_SOURCES:
    if isinstance(settings.DICOMWEB_SOURCES[0], DicomWebSourceConfig):
        dicomweb_names_to_print = [s.name for s in settings.DICOMWEB_SOURCES]
    else: # Assume list of dicts
        dicomweb_names_to_print = [s.get('name', '?') for s in settings.DICOMWEB_SOURCES]
print(f"Configured DICOMweb Sources ({len(settings.DICOMWEB_SOURCES)}): {', '.join(dicomweb_names_to_print) if settings.DICOMWEB_SOURCES else 'None'}")

print(f"Known Input Sources: {settings.KNOWN_INPUT_SOURCES}")
print(f"------------------------------")
