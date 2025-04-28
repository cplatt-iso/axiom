# app/api/api_v1/endpoints/system.py

import logging
import socket
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
        # --- FIXED: Use source_name ---
        web_names = {source.source_name for source in dicomweb_sources if source.source_name}
        # --- END FIXED ---
        logger.debug(f"Found {len(web_names)} configured DICOMweb source names: {web_names}")
        known_sources.update(web_names)
    except Exception as e:
        logger.error(f"Failed to fetch DICOMweb source names for input list: {e}", exc_info=True)

    # 3. Add configured DIMSE Listener names
    try:
        listener_configs = crud.crud_dimse_listener_config.get_multi(db, limit=1000)
        # --- CORRECT: Uses name ---
        listener_names = {config.name for config in listener_configs if config.name}
        # --- END CORRECT ---
        logger.debug(f"Found {len(listener_names)} configured DIMSE Listener names: {listener_names}")
        known_sources.update(listener_names)
    except Exception as e:
        logger.error(f"Failed to fetch DIMSE Listener config names for input list: {e}", exc_info=True)

    # 4. Add configured DIMSE Q/R source names
    try:
        qr_sources = crud.crud_dimse_qr_source.get_multi(db, limit=1000)
        # --- CORRECT: Uses name ---
        qr_names = {source.name for source in qr_sources if source.name}
        # --- END CORRECT ---
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
        poller_states_db: List[models.DicomWebSourceState] = crud.dicomweb_source.get_multi(db=db, skip=skip, limit=limit)
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
    dependencies=[Depends(deps.get_current_active_user)],
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
        listener_states_db: List[models.DimseListenerState] = crud.crud_dimse_listener_state.get_all_listener_states(db=db, skip=skip, limit=limit)
        response_listeners = [DimseListenerStatus.model_validate(ls) for ls in listener_states_db]
        return DimseListenersStatusResponse(listeners=response_listeners)
    except AttributeError as ae:
        logger.error(f"AttributeError accessing CRUD method for DIMSE listener status: {ae}. Verify 'get_all_listener_states' exists.", exc_info=True)
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
        return schemas.system.DimseQrSourcesStatusResponse(sources=qr_sources_db)
    except Exception as e:
        logger.error(f"Error retrieving DIMSE Q/R source status from DB: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error retrieving DIMSE Q/R source status.")

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

    # 2. Message Broker Check
    broker_reachable = False
    try:
        with socket.create_connection((settings.RABBITMQ_HOST, settings.RABBITMQ_PORT), timeout=1):
             component_statuses["message_broker"] = ComponentStatus(status="ok", details="Broker port reachable")
             broker_reachable = True
    except Exception as e:
         component_statuses["message_broker"] = ComponentStatus(status="error", details=f"Broker port check failed: {type(e).__name__}")
         logger.warning(f"Dashboard Status: Broker check failed: {e}")

    # 3. DICOM Listener Check
    try:
        listener_states: List[models.DimseListenerState] = crud.crud_dimse_listener_state.get_all_listener_states(db, limit=10)
        if not listener_states:
            component_statuses["dicom_listener"] = ComponentStatus(status="unknown", details="No listener status found in database.")
        else:
            now_utc = datetime.now(timezone.utc)
            running_ok, stale_found, error_found = False, False, False
            listener_details = []
            for state in listener_states:
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
            else:
                 component_statuses["dicom_listener"] = ComponentStatus(status="ok", details=f"Listeners stopped. Details: {'; '.join(listener_details)}")
    except Exception as e:
        component_statuses["dicom_listener"] = ComponentStatus(status="error", details="DB query failed.")
        logger.error(f"Dashboard Status: Error checking listener DB status: {e}", exc_info=settings.DEBUG)

    # 4. Celery Worker Check
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
