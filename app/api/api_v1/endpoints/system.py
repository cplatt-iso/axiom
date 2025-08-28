# app/api/api_v1/endpoints/system.py
import logging # Fallback if structlog isn't there (should be, but defensive)
import socket
import os
from pathlib import Path
import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import text, select # Import select
from celery import Celery

# Application imports
from app.core.config import settings
from app.api import deps
from app.db import models # Import models
from app import crud # Import top-level crud package
from app import schemas # Import top-level schemas package
from app.core import gcp_utils
from app.services import ai_assist_service

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
# Import the specific error from base_backend if needed
# from app.services.storage_backends import StorageBackendError # Or handle generically

# Schemas used specifically in this endpoint file
from app.schemas.health import HealthCheckResponse, ComponentStatus
from app.schemas.system import (
    DicomWebPollersStatusResponse,
    DicomWebSourceStatus,
    DimseListenerStatus,
    DimseListenersStatusResponse,
    DimseListenerFullStatus,
    DimseListenersFullStatusResponse,
    DimseQrSourceStatus,
    DimseQrSourcesStatusResponse,
    GoogleHealthcareSourceStatus,
    GoogleHealthcareSourcesStatusResponse,
    DiskUsageStats, # Import updated schema
    DirectoryUsageStats, # Import new schema
    SystemInfo
)

router = APIRouter()

# Constants
LISTENER_HEARTBEAT_TIMEOUT_SECONDS = 90

# --- Input Sources Endpoint ---
@router.get(
    "/input-sources",
    response_model=List[str], # Response is still just a list of names
    summary="List Known and Configured Input Sources", # Updated summary
    description="Retrieve a list of configured identifiers for known system input sources, including configured pollers and listeners.", # Updated description
    dependencies=[Depends(deps.get_current_active_user)], # Requires authentication
    tags=["System Info"],
)
def list_input_sources(db: Session = Depends(deps.get_db)) -> List[str]: # Add DB dependency
    """
    Returns known input source identifiers defined in settings *plus* names
    of configured DICOMweb sources, DIMSE Listeners, and DIMSE Q/R sources.
    """
    # 1. Start with static sources from settings
    known_sources = set(settings.KNOWN_INPUT_SOURCES)
    logger.debug(f"Initial known sources from settings: {known_sources}")

    # 2. Add configured DICOMweb source names
    try:
        dicomweb_sources = crud.dicomweb_source.get_multi(db, limit=1000)
        # Explicitly check for non-empty string to aid type checker
        web_names = {
            source.source_name
            for source in dicomweb_sources
            if source.source_name.isnot(None) and source.source_name.ne("")
        }
        # Convert Column[str] to str
        web_names_str = {str(name) for name in web_names}
        logger.debug(f"Found {len(web_names_str)} configured DICOMweb source names: {web_names_str}")
        known_sources.update(web_names_str)
    except Exception as e:
        logger.error(f"Failed to fetch DICOMweb source names for input list: {e}", exc_info=True)

    # 3. Add configured DIMSE Listener names
    try:
        # Corrected to fetch DIMSE Listener states/configs
        # Assuming DimseListenerState has a 'listener_id' or similar 'name' attribute
        # and crud.crud_dimse_listener_state has a get_multi method.
        # If not, adjust to the correct CRUD method and model attribute.
        listener_states = crud.crud_dimse_listener_state.get_all_listener_states(db, limit=1000) # MODIFIED: Changed get_multi to get_all_listener_states
        listener_names = {state.listener_id for state in listener_states if state.listener_id and state.listener_id != ""} # Assuming listener_id is the name
        logger.debug(f"Found {len(listener_names)} configured DIMSE Listener names: {listener_names}")
        known_sources.update(listener_names)
    except Exception as e:
        logger.error(f"Failed to fetch DIMSE Listener config names for input list: {e}", exc_info=True)

    # 4. Add configured DIMSE Q/R source names
    try:
        qr_sources = crud.crud_dimse_qr_source.get_multi(db, limit=1000)
        # Explicitly check for non-empty string
        qr_names = {source.name for source in qr_sources if source.name and source.name != ""}
        logger.debug(f"Found {len(qr_names)} configured DIMSE Q/R source names: {qr_names}")
        known_sources.update(qr_names)
    except Exception as e:
        logger.error(f"Failed to fetch DIMSE Q/R source names for input list: {e}", exc_info=True)


    # 5. Return sorted unique list
    sorted_sources = sorted(list(known_sources))
    logger.info(f"Returning combined input sources: {sorted_sources}")
    return sorted_sources

# --- Health Check Endpoint ---
@router.get(
    "/health",
    summary="Basic Health Check",
    description="Performs a basic health check, primarily verifying database connectivity.",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheckResponse,
    tags=["System Status"],
)
async def health_check(db: Session = Depends(deps.get_db)):
    """Checks database connection health."""
    components: Dict[str, ComponentStatus] = {}
    db_status: Literal["ok", "error"] = "error"
    db_details = "Connection failed"

    try:
        db.execute(text("SELECT 1"))
        db_status = "ok"
        db_details = "Connection successful"
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}", exc_info=False)

    components["database"] = ComponentStatus(status=db_status, details=db_details)
    overall_status: Literal["ok", "error"] = db_status

    return HealthCheckResponse(
        status=overall_status,
        components=components
    )

# --- DICOMweb Poller Status Endpoint ---
@router.get(
    "/dicomweb-pollers/status",
    response_model=DicomWebPollersStatusResponse,
    summary="Get Status of Configured DICOMweb Pollers",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["System Status"],
)
def get_dicomweb_pollers_status(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100
) -> DicomWebPollersStatusResponse:
    """
    Retrieves the current configuration and state for all DICOMweb source pollers
    from the database.
    """
    logger.debug("Request received for DICOMweb poller status.")
    try:
        # Use crud.dicomweb_source which operates on DicomWebSourceState model
        poller_states_db: List[models.DicomWebSourceState] = crud.dicomweb_source.get_multi(db=db, skip=skip, limit=limit)
        # Use the correct schema DicomWebSourceStatus for validation
        response_pollers = [DicomWebSourceStatus.model_validate(p) for p in poller_states_db]
        return DicomWebPollersStatusResponse(pollers=response_pollers)
    except AttributeError as ae:
        logger.error(f"AttributeError accessing CRUD for DICOMweb poller status: {ae}. Check method names/imports.", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error (pollers).")
    except Exception as e:
        logger.error(f"Error retrieving DICOMweb poller status from DB: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error retrieving poller status.")


# --- DIMSE Listener Status Endpoint ---
@router.get(
    "/dimse-listeners/status",
    response_model=DimseListenersFullStatusResponse,
    summary="Get Status of All DIMSE Listeners",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["System Status"],
)
def get_dimse_listeners_status(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100
) -> DimseListenersFullStatusResponse:
    """
    Retrieves the current status of all configured DIMSE listeners, including 
    both pynetdicom and dcm4che listener types, with their runtime status if available.
    """
    logger.debug("Request received for all DIMSE Listeners status (full configuration + runtime status).")
    try:
        # Get all configured listeners with their status
        listener_data: List[tuple] = crud.crud_dimse_listener_state.get_all_listeners_with_status(db=db, skip=skip, limit=limit)
        
        response_listeners = []
        for config, listener_state in listener_data:
            # Build the combined status object
            full_status = DimseListenerFullStatus(
                # Configuration fields
                config_id=config.id,
                name=config.name,
                description=config.description,
                listener_type=config.listener_type,
                ae_title=config.ae_title,
                port=config.port,
                is_enabled=config.is_enabled,
                instance_id=config.instance_id,
                tls_enabled=config.tls_enabled,
                
                # Runtime status fields (optional if listener is not reporting)
                runtime_status=listener_state.status if listener_state else None,
                status_message=listener_state.status_message if listener_state else None,
                runtime_host=listener_state.host if listener_state else None,
                last_heartbeat=listener_state.last_heartbeat if listener_state else None,
                received_instance_count=listener_state.received_instance_count if listener_state else 0,
                processed_instance_count=listener_state.processed_instance_count if listener_state else 0,
            )
            response_listeners.append(full_status)
            
        return DimseListenersFullStatusResponse(listeners=response_listeners)
    except AttributeError as ae:
        logger.error(f"AttributeError accessing CRUD method for DIMSE listener status: {ae}. Verify 'get_all_listeners_with_status' exists.", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error (listeners).")
    except Exception as e:
        logger.error(f"Error retrieving DIMSE Listeners status from DB: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error retrieving listener status.")

# --- DIMSE Q/R Source Status Endpoint ---
@router.get(
    "/dimse-qr-sources/status",
    response_model=schemas.system.DimseQrSourcesStatusResponse,
    summary="Get Status of Configured DIMSE Q/R Sources",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["System Status"],
)
def get_dimse_qr_sources_status(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records."),
) -> schemas.system.DimseQrSourcesStatusResponse:
    """
    Retrieves the current configuration and state for all DIMSE Q/R sources
    from the database.
    """
    logger.debug("Request received for DIMSE Q/R source status.")
    try:
        qr_sources_db: List[models.DimseQueryRetrieveSource] = crud.crud_dimse_qr_source.get_multi(db=db, skip=skip, limit=limit)
        # Convert each DB model to the Pydantic schema model
        response_sources = [schemas.system.DimseQrSourceStatus.model_validate(qs) for qs in qr_sources_db]
        return schemas.system.DimseQrSourcesStatusResponse(sources=response_sources)
    except Exception as e:
        logger.error(f"Error retrieving DIMSE Q/R source status from DB: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error retrieving DIMSE Q/R source status.")


# --- Google Healthcare Source Status Endpoint ---
@router.get(
    "/google-healthcare-sources/status",
    response_model=GoogleHealthcareSourcesStatusResponse,
    summary="Get Status of Configured Google Healthcare Sources",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["System Status"],
)
def get_google_healthcare_sources_status(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records."),
) -> GoogleHealthcareSourcesStatusResponse:
    """
    Retrieves the current configuration and health status for all Google Healthcare sources
    from the database.
    """
    logger.debug("Request received for Google Healthcare source status.")
    try:
        ghc_sources_db: List[models.GoogleHealthcareSource] = crud.google_healthcare_source.get_multi(db=db, skip=skip, limit=limit)
        # Convert each DB model to the Pydantic schema model
        response_sources = [GoogleHealthcareSourceStatus.model_validate(gs) for gs in ghc_sources_db]
        return GoogleHealthcareSourcesStatusResponse(sources=response_sources)
    except Exception as e:
        logger.error(f"Error retrieving Google Healthcare source status from DB: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error retrieving Google Healthcare source status.")


# --- Helper Function to Get Directory Size ---
def get_directory_size(directory: Path) -> int:
    """Recursively calculates the total size of files within a directory."""
    total_size = 0
    # Add a check if the directory exists before iterating
    if not directory.is_dir():
        logger.warning(f"Directory not found for size calculation: {directory}")
        return 0 # Return 0 if dir doesn't exist

    try:
        for entry in directory.rglob('*'): # Recursive glob
            if entry.is_file() and not entry.is_symlink(): # Check if file and not symlink
                try:
                    total_size += entry.stat().st_size
                except OSError as stat_err:
                    # Log warning but continue calculating other files
                    logger.warning(f"Could not stat file {entry} while calculating directory size: {stat_err}")
            elif entry.is_symlink():
                 logger.debug(f"Skipping symlink during size calculation: {entry}")
            elif entry.is_dir():
                 pass # Handled by rglob
            else:
                 logger.debug(f"Skipping non-file/non-dir entry: {entry}")

    except PermissionError as perm_err:
         logger.error(f"Permission denied accessing directory {directory} or its contents during size calculation: {perm_err}")
         # Reraising a generic error might be better than a custom one not defined here
         raise OSError(f"Permission denied for {directory}") from perm_err
    except OSError as walk_err:
         logger.error(f"OS error accessing directory {directory} or its contents during size calculation: {walk_err}", exc_info=True)
         raise OSError(f"OS error accessing {directory}") from walk_err
    except Exception as e:
        logger.error(f"Unexpected error calculating size for {directory}: {e}", exc_info=True)
        # Indicate error by returning -1
        return -1

    return total_size

# --- Disk Usage Endpoint ---
@router.get(
    "/disk-usage",
    response_model=DiskUsageStats, # Use updated schema
    summary="Get Specific Directory and Filesystem Usage",
    description="Retrieves filesystem disk space usage (total, free) and calculates content size for key DICOM directories.",
    dependencies=[Depends(deps.require_role("Admin"))], # Requires Admin
    tags=["System Status"],
)
async def get_disk_usage_stats() -> DiskUsageStats:
    """
    Returns overall filesystem usage and content size for configured DICOM paths.
    Requires Admin privileges.
    """
    # Define the key directories to check based on settings
    paths_to_check = {
        "incoming": settings.DICOM_STORAGE_PATH,
        "processed": settings.FILESYSTEM_STORAGE_PATH, # Assuming this setting exists and is Path type
        "errors": settings.DICOM_ERROR_PATH,
    }

    # Use one path (e.g., incoming) to get overall filesystem stats
    # Assumes all these paths reside on the same filesystem/volume mount
    reference_path = settings.DICOM_STORAGE_PATH
    filesystem_total = -1
    filesystem_free = -1

    if not reference_path.exists():
         logger.warning(f"Reference path {reference_path} for filesystem stats does not exist.")
    else:
        try:
            stat = os.statvfs(reference_path)
            filesystem_total = stat.f_frsize * stat.f_blocks
            filesystem_free = stat.f_frsize * stat.f_bavail
            logger.debug(f"Filesystem stats from {reference_path}: Total={filesystem_total}, Free={filesystem_free}")
        except OSError as e:
            logger.error(f"Error getting filesystem stats via {reference_path}: {e}", exc_info=True)
        except Exception as e:
             logger.error(f"Unexpected error getting filesystem stats: {e}", exc_info=True)

    # Calculate size for each defined directory
    directory_stats: List[DirectoryUsageStats] = []
    for name, path_obj in paths_to_check.items():
        if path_obj: # Check if the setting has a value
             logger.info(f"Calculating content size for '{name}' directory: {path_obj}")
             # Ensure path_obj is a Path object
             current_path = Path(path_obj) if not isinstance(path_obj, Path) else path_obj
             try:
                  content_size = get_directory_size(current_path) # Returns -1 on error
                  directory_stats.append(DirectoryUsageStats(
                      path=str(current_path), # Store path as string
                      content_bytes=content_size # Store calculated size or -1
                  ))
                  if content_size == -1:
                      logger.warning(f"Calculation failed for directory: {current_path}")
             except Exception as calc_err:
                 # Catch errors from get_directory_size if it raises them
                 logger.error(f"Failed to get size for directory {current_path}: {calc_err}")
                 directory_stats.append(DirectoryUsageStats(
                     path=str(current_path),
                     content_bytes=-1 # Indicate error
                 ))
        else:
             logger.warning(f"Path for '{name}' is not configured in settings. Skipping size calculation.")

    if filesystem_total == -1 or filesystem_free == -1:
        logger.warning("Could not determine overall filesystem statistics.")
        # Optionally raise an error if FS stats are critical

    return DiskUsageStats(
        filesystem_total_bytes=filesystem_total,
        filesystem_free_bytes=filesystem_free,
        directories=directory_stats,
    )

@router.get(
    "/info",
    response_model=SystemInfo,
    summary="Get Comprehensive System Information & Configuration",
    description="Retrieves comprehensive system information including all configuration settings, service statuses, and runtime parameters.",
    dependencies=[Depends(deps.require_role("Admin"))], # Requires Admin
    tags=["System Info", "System Configuration"], # Add relevant tags
)
async def get_system_info(db: Session = Depends(deps.get_db)) -> SystemInfo:
    """
    Returns comprehensive system settings and configuration values.
    Includes both static configuration from settings and dynamic configuration from database.
    Requires Admin privileges.
    """
    logger.info("Fetching comprehensive system information.")
    
    try:
        from app.utils.config_helpers import (
            get_config_value,
            get_processing_config,
            get_dustbin_config,
            get_batch_processing_config,
            get_celery_config,
            get_dicomweb_config,
            get_ai_config
        )
        
        # Get dynamic configuration values
        processing_config = get_processing_config(db)
        dustbin_config = get_dustbin_config(db)
        batch_config = get_batch_processing_config(db)
        celery_config = get_celery_config(db)
        dicomweb_config = get_dicomweb_config(db)
        ai_config = get_ai_config(db)
        
        # Test various service connections
        services_status = {}
        
        # Test database connection
        try:
            db.execute(text("SELECT 1"))
            services_status["database"] = {"status": "connected", "error": None}
        except Exception as e:
            services_status["database"] = {"status": "error", "error": str(e)}
        
        # Test Redis connection
        try:
            from app.core.redis_client import redis_client
            if redis_client is not None:
                redis_client.ping()
                services_status["redis"] = {"status": "connected", "error": None}
            else:
                services_status["redis"] = {"status": "error", "error": "Redis client not initialized"}
        except Exception as e:
            services_status["redis"] = {"status": "error", "error": str(e)}
        
        # Convert Path objects to strings for the response model
        temp_dir_str = str(settings.TEMP_DIR) if isinstance(settings.TEMP_DIR, Path) else settings.TEMP_DIR
        
        # Build comprehensive system info
        info = SystemInfo(
            # Basic Project Information
            project_name=settings.PROJECT_NAME,
            project_version=settings.PROJECT_VERSION,
            environment=settings.ENVIRONMENT,
            debug_mode=settings.DEBUG,
            log_level=settings.LOG_LEVEL,
            
            # API Configuration
            api_v1_str=settings.API_V1_STR,
            cors_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
            
            # Authentication Configuration
            access_token_expire_minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES,
            algorithm=settings.ALGORITHM,
            google_oauth_configured=bool(settings.GOOGLE_OAUTH_CLIENT_ID),
            
            # Database Configuration
            postgres_server=settings.POSTGRES_SERVER,
            postgres_port=settings.POSTGRES_PORT,
            postgres_user=settings.POSTGRES_USER,
            postgres_db=settings.POSTGRES_DB,
            database_connected=services_status.get("database", {}).get("status") == "connected",
            
            # File Processing Configuration (using dynamic config)
            log_original_attributes=processing_config["log_original_attributes"],
            delete_on_success=processing_config["delete_on_success"],
            delete_unmatched_files=processing_config["delete_unmatched_files"],
            delete_on_no_destination=processing_config["delete_on_no_destination"],
            move_to_error_on_partial_failure=processing_config["move_to_error_on_partial_failure"],
            
            # Dustbin System Configuration
            use_dustbin_system=dustbin_config["use_dustbin_system"],
            dustbin_retention_days=dustbin_config["retention_days"],
            dustbin_verification_timeout_hours=dustbin_config["verification_timeout_hours"],
            
            # File Storage Paths
            dicom_storage_path=str(settings.DICOM_STORAGE_PATH),
            dicom_error_path=str(settings.DICOM_ERROR_PATH),
            filesystem_storage_path=str(settings.FILESYSTEM_STORAGE_PATH),
            dicom_retry_staging_path=str(settings.DICOM_RETRY_STAGING_PATH),
            dicom_dustbin_path=str(settings.DICOM_DUSTBIN_PATH),
            temp_dir=temp_dir_str,
            
            # Exam Batch Processing
            exam_batch_completion_timeout=batch_config["completion_timeout"],
            exam_batch_check_interval=batch_config["check_interval"],
            exam_batch_max_concurrent=batch_config["max_concurrent"],
            exam_batch_send_interval=settings.EXAM_BATCH_SEND_INTERVAL,
            
            # Celery Configuration
            celery_broker_configured=bool(settings.CELERY_BROKER_URL),
            celery_result_backend_configured=bool(settings.CELERY_RESULT_BACKEND),
            celery_worker_concurrency=celery_config["worker_concurrency"],
            celery_prefetch_multiplier=celery_config["prefetch_multiplier"],
            celery_task_max_retries=celery_config["task_max_retries"],
            celery_task_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
            
            # Cleanup Configuration
            stale_data_cleanup_age_days=settings.STALE_DATA_CLEANUP_AGE_DAYS,
            stale_retry_in_progress_age_hours=settings.STALE_RETRY_IN_PROGRESS_AGE_HOURS,
            cleanup_batch_size=settings.CLEANUP_BATCH_SIZE,
            cleanup_stale_data_interval_hours=settings.CLEANUP_STALE_DATA_INTERVAL_HOURS,
            
            # AI Configuration
            openai_configured=bool(settings.OPENAI_API_KEY),
            openai_model_name_rule_gen=settings.OPENAI_MODEL_NAME_RULE_GEN,
            vertex_ai_configured=bool(settings.VERTEX_AI_PROJECT),
            vertex_ai_project=settings.VERTEX_AI_PROJECT,
            vertex_ai_location=settings.VERTEX_AI_LOCATION,
            vertex_ai_model_name=settings.VERTEX_AI_MODEL_NAME,
            ai_invocation_counter_enabled=settings.AI_INVOCATION_COUNTER_ENABLED,
            ai_vocab_cache_enabled=ai_config["vocab_cache_enabled"],
            ai_vocab_cache_ttl_seconds=ai_config["vocab_cache_ttl_seconds"],
            
            # Redis Configuration
            redis_configured=bool(settings.REDIS_URL),
            redis_host=settings.REDIS_HOST,
            redis_port=settings.REDIS_PORT,
            redis_db=settings.REDIS_DB,
            
            # RabbitMQ Configuration
            rabbitmq_host=settings.RABBITMQ_HOST,
            rabbitmq_port=settings.RABBITMQ_PORT,
            rabbitmq_user=settings.RABBITMQ_USER,
            rabbitmq_vhost=settings.RABBITMQ_VHOST,
            
            # DICOM Configuration
            listener_host=settings.LISTENER_HOST,
            pydicom_implementation_uid=settings.PYDICOM_IMPLEMENTATION_UID,
            implementation_version_name=settings.IMPLEMENTATION_VERSION_NAME,
            
            # DICOMweb Poller Configuration
            dicomweb_poller_default_fallback_days=dicomweb_config["poller_default_fallback_days"],
            dicomweb_poller_overlap_minutes=settings.DICOMWEB_POLLER_OVERLAP_MINUTES,
            dicomweb_poller_qido_limit=dicomweb_config["poller_qido_limit"],
            dicomweb_poller_max_sources=dicomweb_config["poller_max_sources"],
            
            # DIMSE Q/R Configuration
            dimse_qr_poller_max_sources=settings.DIMSE_QR_POLLER_MAX_SOURCES,
            dimse_acse_timeout=settings.DIMSE_ACSE_TIMEOUT,
            dimse_dimse_timeout=settings.DIMSE_DIMSE_TIMEOUT,
            dimse_network_timeout=settings.DIMSE_NETWORK_TIMEOUT,
            
            # DCM4CHE Configuration
            dcm4che_prefix=settings.DCM4CHE_PREFIX,
            
            # Rules Engine Configuration
            rules_cache_enabled=settings.RULES_CACHE_ENABLED,
            rules_cache_ttl_seconds=settings.RULES_CACHE_TTL_SECONDS,
            
            # Known Input Sources
            known_input_sources=settings.KNOWN_INPUT_SOURCES,
            
            # Logging Integration Configuration
            elasticsearch_configured=bool(settings.ELASTICSEARCH_HOST),
            elasticsearch_host=settings.ELASTICSEARCH_HOST,
            elasticsearch_port=settings.ELASTICSEARCH_PORT,
            elasticsearch_tls_enabled=(settings.ELASTICSEARCH_SCHEME.lower() == 'https'),
            elasticsearch_auth_enabled=bool(settings.ELASTICSEARCH_USERNAME),
            elasticsearch_username=settings.ELASTICSEARCH_USERNAME if settings.ELASTICSEARCH_USERNAME else None,
            elasticsearch_password_configured=bool(settings.ELASTICSEARCH_PASSWORD),
            elasticsearch_cert_verification=settings.ELASTICSEARCH_VERIFY_CERTS,
            elasticsearch_ca_cert_path=settings.ELASTICSEARCH_CA_CERT_PATH,
            elasticsearch_index_pattern=settings.ELASTICSEARCH_LOG_INDEX_PATTERN,
            
            # Fluentd Integration Configuration (Fluent Bit)
            fluentd_configured=True,  # Fluentd is enabled via Docker Compose logging driver
            fluentd_host="fluent-bit",  # Docker service name
            fluentd_port=24224,  # Standard Fluentd port
            fluentd_tag_prefix="axiom",  # From Docker Compose tag configuration
            fluentd_buffer_size="10m",  # From Docker Compose max-buffer-size
            
            # Service Status
            services_status=services_status
        )
        
        logger.info("Successfully compiled comprehensive system information.")
        return info
        
    except AttributeError as e:
        logger.error(f"AttributeError fetching system info: Setting '{e.name}' not found in config.", exc_info=False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Server configuration error: Missing setting '{e.name}'.",
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching system info: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while fetching system information.",
        )

# --- Dashboard Status Endpoint ---
@router.get(
    "/dashboard/status",
    summary="Get Combined System Status for Dashboard",
    response_model=HealthCheckResponse,
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["Dashboard", "System Status"],
)
async def get_dashboard_status(
    db: Session = Depends(deps.get_db)
) -> HealthCheckResponse:
    """
    Provides a combined status overview of key system components for the dashboard UI.
    Checks DB, Broker, Workers, and Listener DB state.
    """
    component_statuses: Dict[str, ComponentStatus] = {
        "database": ComponentStatus(status="unknown", details=None),
        "redis": ComponentStatus(status="unknown", details=None),
        "message_broker": ComponentStatus(status="unknown", details=None),
        "api_service": ComponentStatus(status="ok", details="Responding"),
        "dicom_listener": ComponentStatus(status="unknown", details=None),
        "celery_workers": ComponentStatus(status="unknown", details=None),
    }
    logger.debug("Compiling dashboard status...")

    # 1. Database Check
    try:
        db.execute(text("SELECT 1"))
        component_statuses["database"] = ComponentStatus(status="ok", details="Connected")
    except Exception as e:
        component_statuses["database"] = ComponentStatus(status="error", details="Connection failed")
        logger.warning(f"Dashboard Status: DB check failed: {e}")

    # 2. Redis Check
    try:
        from app.core.redis_client import redis_client
        if redis_client is not None:
            redis_client.ping()
            component_statuses["redis"] = ComponentStatus(status="ok", details="Connected")
        else:
            component_statuses["redis"] = ComponentStatus(status="error", details="Redis client not initialized")
    except Exception as e:
        component_statuses["redis"] = ComponentStatus(status="error", details=f"Connection failed: {type(e).__name__}")
        logger.warning(f"Dashboard Status: Redis check failed: {e}")

    # 3. Message Broker Check
    broker_reachable = False
    try:
        with socket.create_connection((settings.RABBITMQ_HOST, settings.RABBITMQ_PORT), timeout=1):
             component_statuses["message_broker"] = ComponentStatus(status="ok", details="Broker port reachable")
             broker_reachable = True
    except Exception as e:
         component_statuses["message_broker"] = ComponentStatus(status="error", details=f"Broker port check failed: {type(e).__name__}")
         logger.warning(f"Dashboard Status: Broker check failed: {e}")

    # 4. DICOM Listener Check
    try:
        listener_states: List[models.DimseListenerState] = crud.crud_dimse_listener_state.get_all_listener_states(db, limit=10)
        if not listener_states:
            component_statuses["dicom_listener"] = ComponentStatus(status="unknown", details="No listener status found in database.")
        else:
            now_utc = datetime.now(timezone.utc)
            running_ok, stale_found, error_found = False, False, False
            listener_details = []
            for state in listener_states:
                # Check if heartbeat is None before comparison
                if state.last_heartbeat is None:
                    is_stale = True # Treat missing heartbeat as stale
                    logger.warning(f"Listener {state.listener_id} has NULL last_heartbeat.")
                else:
                    is_stale = (now_utc - state.last_heartbeat) > timedelta(seconds=LISTENER_HEARTBEAT_TIMEOUT_SECONDS)

                listener_details.append(f"{state.listener_id}: {state.status}{' (STALE)' if is_stale else ''}")
                if state.status == 'running' and not is_stale: running_ok = True
                elif state.status != 'running': error_found = True
                elif is_stale: stale_found = True

            if running_ok and not error_found and not stale_found:
                 component_statuses["dicom_listener"] = ComponentStatus(status="ok", details=f"{len(listener_states)} listener(s) reporting OK.")
            elif error_found:
                 component_statuses["dicom_listener"] = ComponentStatus(status="error", details=f"Error state reported. Details: {'; '.join(listener_details)}")
            elif stale_found:
                 component_statuses["dicom_listener"] = ComponentStatus(status="degraded", details=f"Heartbeat stale. Details: {'; '.join(listener_details)}")
            else: # Covers cases like all stopped, or only stale+stopped
                 component_statuses["dicom_listener"] = ComponentStatus(status="unknown", details=f"Listener status check inconclusive. Details: {'; '.join(listener_details)}")
    except Exception as e:
        component_statuses["dicom_listener"] = ComponentStatus(status="error", details="DB query failed.")
        logger.error(f"Dashboard Status: Error checking listener DB status: {e}", exc_info=settings.DEBUG)

    # 5. Celery Worker Check
    if broker_reachable:
        celery_inspect_timeout = 1.5
        try:
            temp_celery_app = Celery(broker=settings.CELERY_BROKER_URL, backend='rpc://')
            inspector = temp_celery_app.control.inspect(timeout=celery_inspect_timeout)
            active_workers = inspector.ping()
            if active_workers:
                component_statuses["celery_workers"] = ComponentStatus(status="ok", details=f"{len(active_workers)} worker(s) responded.")
            else:
                component_statuses["celery_workers"] = ComponentStatus(status="error", details=f"No workers responded.")
                logger.warning(f"Dashboard Status: No Celery workers responded.")
        except Exception as e:
            component_statuses["celery_workers"] = ComponentStatus(status="error", details="Inspection failed.")
            logger.error(f"Dashboard Status: Celery worker inspection failed: {e}", exc_info=settings.DEBUG)
    else:
        component_statuses["celery_workers"] = ComponentStatus(status="unknown", details="Broker unreachable.")
        logger.warning("Dashboard Status: Broker unreachable, skipping worker check.")

    # Determine overall status
    overall_status: Literal["ok", "error", "degraded", "unknown"] = "ok"
    if any(comp.status == "error" for comp in component_statuses.values()): overall_status = "error"
    elif any(comp.status == "degraded" for comp in component_statuses.values()): overall_status = "degraded"
    elif any(comp.status == "unknown" for comp in component_statuses.values()): overall_status = "unknown"

    logger.debug(f"Dashboard status compiled: overall={overall_status}")
    return HealthCheckResponse(
        status=overall_status,
        components=component_statuses
    )

@router.post(
    "/cache/ai-vocab/clear",
    summary="Clear the AI Vocabulary Cache from Redis",
    status_code=status.HTTP_200_OK,
    response_model=Dict[str, Any] # Define a response model if you want more structure
)
def clear_ai_vocab_cache_endpoint(
    prompt_config_id: Optional[int] = Query(None, description="Specific AIPromptConfig ID to clear entries for. If omitted, affects all AI vocab cache based on other params."),
    input_value: Optional[str] = Query(None, description="Specific input value to clear (requires prompt_config_id)."),
    # current_user: models.User = Depends(deps.get_current_active_superuser) # TODO: UNCOMMENT AND PROTECT
):
    """
    Clears the AI Vocabulary cache.
    - Call with no parameters to clear ALL AI vocabulary cache entries.
    - Provide `prompt_config_id` to clear all entries for that specific prompt configuration.
    - Provide `prompt_config_id` AND `input_value` to clear a single specific cache entry.
    """
    # Add authentication/authorization checks here if current_user is enabled

    if input_value and prompt_config_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="If 'input_value' is provided, 'prompt_config_id' must also be provided to clear a specific entry."
        )

    try:
        result = ai_assist_service.clear_ai_vocab_cache(
            prompt_config_id=prompt_config_id,
            input_value=input_value
        )
        
        if result.get("status") == "error":
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=result.get("message", "Unknown error during cache clearing."))
        if result.get("status") == "warning": # e.g. cache not enabled
             raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=result.get("message", "Cache operation could not be fully performed."))
        
        return result # {"status": "success", "message": "...", "keys_deleted": X}

    except Exception as e:
        # This catches unexpected errors from the service call itself, though the utility should handle its own.
        logger.error(f"API error calling clear_ai_vocab_cache utility: {e}", exc_info=True) # Ensure logger is available here or use print
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"An unexpected error occurred: {str(e)}")

@router.post("/cache/secrets/clear", summary="Clear the cached secrets", status_code=status.HTTP_200_OK)
def clear_secrets_cache_endpoint(
    secret_id: Optional[str] = Query(None, description="Specific secret ID to clear (requires version and project_id too)"),
    version: Optional[str] = Query(None, description="Specific secret version to clear"),
    project_id: Optional[str] = Query(None, description="Specific GCP project ID for the secret"),
    # current_user: models.User = Depends(deps.get_current_active_superuser) # Protect this endpoint
):
    # Ensure current_user has rights if you uncomment the Depends above
    result = gcp_utils.clear_secret_cache(secret_id, version, project_id)
    if result["status"] == "warning":
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=result["message"])
    return result