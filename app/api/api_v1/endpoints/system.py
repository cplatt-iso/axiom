# app/api/api_v1/endpoints/system.py

import logging
from typing import List, Dict, Any, Optional, Literal # Ensure Literal is imported
# import socket # No longer needed for single listener ID lookup

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import settings
from app.api import deps
from app.db import models
from app import crud, schemas # Import top-level crud

# Import necessary schemas directly
from app.schemas.health import HealthCheckResponse, ComponentStatus
from app.schemas.system import (
    DicomWebPollersStatusResponse,
    DicomWebSourceStatus,
    # DimseListenerStatus, # Individual schema used within the list response
    DimseListenersStatusResponse # Import the list response schema
)


logger = logging.getLogger(__name__)
router = APIRouter()

# --- Input Sources Endpoint ---
@router.get(
    "/input-sources",
    response_model=List[str],
    summary="List Known Input Sources",
    description="Retrieve the list of configured identifiers for known system input sources.",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["System Info"],
)
def list_input_sources() -> List[str]:
    """ Returns known input source identifiers from settings. """
    return settings.KNOWN_INPUT_SOURCES

# --- Health Check Endpoint ---
@router.get(
    "/health",
    summary="Health Check",
    description="Performs a basic health check of the API and database connection.",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheckResponse,
    tags=["System Status"],
    # No authentication needed for basic health check
)
async def health_check(db: Session = Depends(deps.get_db)):
    """ Performs health check on database connection. """
    components: Dict[str, ComponentStatus] = {}

    # Check Database
    db_status: Literal["ok", "error"] = "ok"
    db_details = "Connection successful."
    try:
        # Use text() for executing raw SQL safely
        db.execute(text("SELECT 1"))
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}", exc_info=False)
        db_status = "error"
        db_details = f"Database connection error: Cannot execute simple query."

    components["database"] = ComponentStatus(status=db_status, details=db_details)

    # Add other component checks here later (e.g., Redis, RabbitMQ ping)

    # Determine overall status
    overall_status: Literal["ok", "error", "degraded", "unknown"] = "ok"
    if any(comp.status != "ok" for comp in components.values()):
        overall_status = "error"

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
    skip: int = 0, # Added pagination params
    limit: int = 100
) -> Any: # Return type is handled by response_model
    """
    Retrieves the current status of all configured DICOMweb source pollers
    from the database state table.
    """
    logger.debug("Request received for DICOMweb poller status.")
    try:
        # Verify 'dicomweb_state' is the correct name imported in crud/__init__.py
        # Assuming the get_all method now exists and supports pagination
        poller_states = crud.dicomweb_state.get_all(db=db, skip=skip, limit=limit)

        if poller_states is None:
             poller_states = []
             logger.warning("crud.dicomweb_state.get_all returned None, responding with empty list.")
        # Wrap the list of DB objects in the Pydantic response model
        return DicomWebPollersStatusResponse(pollers=poller_states)
    except AttributeError as ae:
        logger.error(f"AttributeError accessing CRUD for DICOMweb poller status: {ae}. Check crud/__init__.py imports.", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error accessing poller status."
        )
    except Exception as e:
        logger.error(f"Error retrieving DICOMweb poller status from DB: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve poller status from database."
        )


# --- Listener Status Endpoint (UPDATED for List) ---
@router.get(
    "/dimse-listeners/status", # Plural path
    response_model=DimseListenersStatusResponse, # Use the list response schema
    summary="Get Status of All DIMSE Listeners", # Updated summary
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["System Status"],
)
# Rename function to reflect list return
def get_dimse_listeners_status(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = 0, # Optional pagination
    limit: int = 100 # Optional pagination
) -> Any: # Return type is handled by response_model
    """
    Retrieves the current status of all running DIMSE C-STORE listeners
    from the database state table.
    """
    logger.debug("Request received for all DIMSE Listeners status.")
    try:
        # Use the get_all_states CRUD function we added
        listener_states = crud.crud_dimse_listener_state.get_all_states(db=db, skip=skip, limit=limit)

        if listener_states is None: # Should return list, but check just in case
             listener_states = []
             logger.warning("crud_dimse_listener_state.get_all_states returned None, responding with empty list.")

        # Return the data wrapped in the response schema
        # FastAPI handles converting the list of DB models using the schema's from_attributes config
        return DimseListenersStatusResponse(listeners=listener_states)
    except AttributeError as ae:
        # Check if the error is specifically about 'get_all_states'
        logger.error(f"AttributeError accessing CRUD for DIMSE listener status: {ae}. Check crud/__init__.py imports and crud_dimse_listener_state.py methods.", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error accessing listener status."
        )
    except Exception as e:
        logger.error(f"Error retrieving DIMSE Listeners status from DB: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve listener statuses from database."
        )
# --- End Listener Status Endpoint ---
