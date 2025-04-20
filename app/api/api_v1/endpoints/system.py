# app/api/api_v1/endpoints/system.py

import logging
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.config import settings
from app.api import deps
from app.db import models # Import models for user dependency
from app import crud, schemas # Import top-level crud and schemas
# Import specific schemas needed
from app.schemas import HealthCheckResponse, ComponentStatus, DicomWebPollersStatusResponse, DicomWebSourceStatus

logger = logging.getLogger(__name__)
router = APIRouter()

# --- Input Sources Endpoint ---
@router.get(
    "/input-sources",
    response_model=List[str],
    summary="List Known Input Sources",
    description="Retrieve the list of configured identifiers for known system input sources.",
    dependencies=[Depends(deps.get_current_active_user)], # Requires user to be logged in
    tags=["System Info"], # Changed tag slightly
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
    tags=["System Status"], # Consolidated tags
    # No authentication needed for basic health check
)
async def health_check(db: Session = Depends(deps.get_db)):
    """ Performs health check on database connection. """
    components: Dict[str, ComponentStatus] = {}

    # Check Database
    db_status = "ok"
    db_details = "Connection successful."
    try:
        db.execute(text("SELECT 1"))
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}", exc_info=False)
        db_status = "error"
        db_details = f"Database connection error: Cannot execute simple query."

    components["database"] = ComponentStatus(status=db_status, details=db_details)

    # Add other component checks here later (e.g., Celery ping, Redis ping)

    # Determine overall status
    overall_status = "ok"
    if any(comp.status != "ok" for comp in components.values()):
        overall_status = "error" # Or maybe 'degraded' depending on checks

    return HealthCheckResponse(
        status=overall_status,
        components=components
    )


# --- NEW DICOMweb Poller Status Endpoint ---
@router.get(
    "/dicomweb-pollers/status",
    response_model=schemas.DicomWebPollersStatusResponse,
    summary="Get Status of Configured DICOMweb Pollers",
    dependencies=[Depends(deps.get_current_active_user)], # Requires user to be logged in
    tags=["System Status"], # Consolidated tags
)
def get_dicomweb_pollers_status(
    *,
    db: Session = Depends(deps.get_db),
    # No need for current_user arg if only used for dependency check
    # current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Retrieves the current status of all configured DICOMweb source pollers
    from the database state table.
    """
    # logger.debug(f"User requesting DICOMweb poller status.") # Logging user might require passing current_user
    logger.debug("Request received for DICOMweb poller status.")
    try:
        # Use the CRUD object to get all states
        poller_states = crud.dicomweb_state.get_all(db=db)
        if poller_states is None:
             # Handle case where CRUD might return None on error (though get_all usually returns list)
             poller_states = []
             logger.warning("dicomweb_state.get_all returned None, responding with empty list.")

        # Pydantic handles conversion via response_model and from_attributes=True in schema
        return schemas.DicomWebPollersStatusResponse(pollers=poller_states)
    except Exception as e:
        logger.error(f"Error retrieving DICOMweb poller status from DB: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve poller status from database."
        )

# --- End DICOMweb Poller Status Endpoint ---
