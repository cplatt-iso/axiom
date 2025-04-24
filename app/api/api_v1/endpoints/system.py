# app/api/api_v1/endpoints/system.py

import logging
import socket
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from celery import Celery # For worker inspection

# Application imports
from app.core.config import settings
from app.api import deps
from app.db import models
from app import crud # Import top-level crud package
from app import schemas # Import top-level schemas package

# Schemas used specifically in this endpoint file
from app.schemas.health import HealthCheckResponse, ComponentStatus
from app.schemas.system import (
    DicomWebPollersStatusResponse,
    DicomWebSourceStatus,
    DimseListenerStatus,
    DimseListenersStatusResponse,
    DimseQrSourceStatus,          
    DimseQrSourcesStatusResponse, 
)

logger = logging.getLogger(__name__)
router = APIRouter()

# Constants
LISTENER_HEARTBEAT_TIMEOUT_SECONDS = 90 # How long before considering a listener heartbeat stale

# --- Input Sources Endpoint ---
@router.get(
    "/input-sources",
    response_model=List[str],
    summary="List Known Input Sources",
    description="Retrieve the list of configured identifiers for known system input sources (e.g., listeners, API endpoints).",
    dependencies=[Depends(deps.get_current_active_user)], # Requires authentication
    tags=["System Info"],
)
def list_input_sources() -> List[str]:
    """Returns known input source identifiers defined in the application settings."""
    # Note: Currently returns static list from settings. Might be extended later
    # to dynamically include sources configured in the database (like DICOMweb pollers).
    return settings.KNOWN_INPUT_SOURCES

# --- Health Check Endpoint ---
@router.get(
    "/health",
    summary="Basic Health Check",
    description="Performs a basic health check, primarily verifying database connectivity.",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheckResponse,
    tags=["System Status"],
    # No authentication needed for this basic check
)
async def health_check(db: Session = Depends(deps.get_db)):
    """Checks database connection health."""
    components: Dict[str, ComponentStatus] = {}
    db_status: Literal["ok", "error"] = "error" # Default to error
    db_details = "Connection failed"

    try:
        # Execute a minimal query to confirm DB is responsive
        db.execute(text("SELECT 1"))
        db_status = "ok"
        db_details = "Connection successful"
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}", exc_info=False)
        # Status remains 'error', details remain 'Connection failed'

    components["database"] = ComponentStatus(status=db_status, details=db_details)

    # For this basic check, overall status mirrors DB status
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
    dependencies=[Depends(deps.get_current_active_user)], # Requires authentication
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
        # Assumes crud.dicomweb_state refers to the DicomWebSourceStateCRUD instance
        poller_states_db: List[models.DicomWebSourceState] = crud.dicomweb_state.get_all(db=db, skip=skip, limit=limit)

        # Convert database models to Pydantic response schemas
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
    response_model=DimseListenersStatusResponse,
    summary="Get Status of All DIMSE Listeners",
    dependencies=[Depends(deps.get_current_active_user)], # Requires authentication
    tags=["System Status"],
)
def get_dimse_listeners_status(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100
) -> DimseListenersStatusResponse:
    """
    Retrieves the current status of all DIMSE C-STORE listeners reporting to the database.
    """
    logger.debug("Request received for all DIMSE Listeners status.")
    try:
        # Use the correct CRUD object and method name
        listener_states_db: List[models.DimseListenerState] = crud.crud_dimse_listener_state.get_all_listener_states(db=db, skip=skip, limit=limit)

        # Convert database models to Pydantic response schemas
        response_listeners = [DimseListenerStatus.model_validate(ls) for ls in listener_states_db]

        return DimseListenersStatusResponse(listeners=response_listeners)
    except AttributeError as ae:
        logger.error(f"AttributeError accessing CRUD method for DIMSE listener status: {ae}. Verify 'get_all_listener_states' exists.", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server configuration error (listeners).")
    except Exception as e:
        logger.error(f"Error retrieving DIMSE Listeners status from DB: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERqVER_ERROR, detail="Database error retrieving listener status.")

# --- DIMSE Q/R Source Status Endpoint ---
@router.get(
    "/dimse-qr-sources/status",
    response_model=schemas.system.DimseQrSourcesStatusResponse, # Use new response schema
    summary="Get Status of Configured DIMSE Q/R Sources",
    dependencies=[Depends(deps.get_current_active_user)], # Requires authentication
    tags=["System Status"],
)
def get_dimse_qr_sources_status(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records."),
) -> schemas.system.DimseQrSourcesStatusResponse: # Use new schema in type hint
    """
    Retrieves the current configuration and state for all DIMSE Q/R sources
    from the database.
    """
    logger.debug("Request received for DIMSE Q/R source status.")
    try:
        # Use the correct CRUD object
        qr_sources_db: List[models.DimseQueryRetrieveSource] = crud.crud_dimse_qr_source.get_multi(db=db, skip=skip, limit=limit)

        # Pydantic handles conversion via ORM mode in the response model
        return schemas.system.DimseQrSourcesStatusResponse(sources=qr_sources_db)
    except Exception as e:
        logger.error(f"Error retrieving DIMSE Q/R source status from DB: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error retrieving DIMSE Q/R source status.")

# --- Dashboard Status Endpoint (More comprehensive) ---
# This endpoint combines checks for a quick overview, distinct from individual status endpoints.
@router.get(
    "/dashboard/status", # Keep original dashboard path for now
    summary="Get Combined System Status for Dashboard",
    response_model=HealthCheckResponse, # Re-use HealthCheckResponse structure
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["Dashboard", "System Status"], # Add Dashboard tag too
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

    # 2. Message Broker Check (Port Reachability)
    broker_reachable = False
    try:
        with socket.create_connection((settings.RABBITMQ_HOST, settings.RABBITMQ_PORT), timeout=1):
             component_statuses["message_broker"] = ComponentStatus(status="ok", details="Broker port reachable")
             broker_reachable = True
    except Exception as e:
         component_statuses["message_broker"] = ComponentStatus(status="error", details=f"Broker port check failed: {type(e).__name__}")
         logger.warning(f"Dashboard Status: Broker check failed: {e}")

    # 3. DICOM Listener Check (Database Status)
    try:
        listener_states: List[models.DimseListenerState] = crud.crud_dimse_listener_state.get_all_listener_states(db, limit=10) # Limit check for dashboard

        if not listener_states:
            component_statuses["dicom_listener"] = ComponentStatus(status="unknown", details="No listener status found in database.")
        else:
            # Simplified check for dashboard: Check if *any* listener is running and has recent heartbeat
            now_utc = datetime.now(timezone.utc)
            running_ok = False
            stale_found = False
            error_found = False
            listener_details = []

            for state in listener_states:
                is_stale = (now_utc - state.last_heartbeat) > timedelta(seconds=LISTENER_HEARTBEAT_TIMEOUT_SECONDS)
                listener_details.append(f"{state.listener_id}: {state.status}{' (STALE)' if is_stale else ''}")
                if state.status == 'running' and not is_stale:
                    running_ok = True
                elif state.status != 'running':
                    error_found = True
                elif is_stale:
                    stale_found = True

            if running_ok and not error_found and not stale_found:
                 component_statuses["dicom_listener"] = ComponentStatus(status="ok", details=f"{len(listener_states)} listener(s) reporting OK.")
            elif error_found:
                 component_statuses["dicom_listener"] = ComponentStatus(status="error", details=f"At least one listener in error state. Details: {'; '.join(listener_details)}")
            elif stale_found:
                 component_statuses["dicom_listener"] = ComponentStatus(status="degraded", details=f"At least one listener heartbeat stale. Details: {'; '.join(listener_details)}")
            else: # e.g., all stopped normally
                 component_statuses["dicom_listener"] = ComponentStatus(status="ok", details=f"Listener(s) reported stopped. Details: {'; '.join(listener_details)}")

    except Exception as e:
        component_statuses["dicom_listener"] = ComponentStatus(status="error", details="DB query for listener status failed.")
        logger.error(f"Dashboard Status: Error checking listener status from DB: {e}", exc_info=settings.DEBUG)


    # 4. Celery Worker Check
    if broker_reachable:
        celery_inspect_timeout = 1.5
        try:
            # Note: Using result_backend='rpc://' for inspect might be inefficient if result backend is Redis.
            # Consider creating app without explicit backend just for inspect if needed.
            temp_celery_app = Celery(broker=settings.CELERY_BROKER_URL, backend='rpc://')
            inspector = temp_celery_app.control.inspect(timeout=celery_inspect_timeout)
            active_workers = inspector.ping()

            if active_workers:
                worker_count = len(active_workers)
                component_statuses["celery_workers"] = ComponentStatus(status="ok", details=f"{worker_count} worker(s) responded.")
            else:
                component_statuses["celery_workers"] = ComponentStatus(status="error", details=f"No workers responded within {celery_inspect_timeout}s.")
                logger.warning(f"Dashboard Status: No Celery workers responded to ping.")

        except Exception as e:
            component_statuses["celery_workers"] = ComponentStatus(status="error", details="Worker inspection failed.")
            logger.error(f"Dashboard Status: Celery worker inspection failed: {e}", exc_info=settings.DEBUG)
    else:
        component_statuses["celery_workers"] = ComponentStatus(status="unknown", details="Broker unreachable.")
        logger.warning("Dashboard Status: Broker unreachable, skipping worker check.")

    # Determine overall system status
    overall_status: Literal["ok", "error", "degraded", "unknown"] = "ok"
    if any(comp.status == "error" for comp in component_statuses.values()):
        overall_status = "error"
    elif any(comp.status == "degraded" for comp in component_statuses.values()):
        overall_status = "degraded"
    elif any(comp.status == "unknown" for comp in component_statuses.values()):
        overall_status = "unknown"

    logger.debug(f"Dashboard status compiled: overall={overall_status}")
    return HealthCheckResponse(
        status=overall_status,
        components=component_statuses
    )
