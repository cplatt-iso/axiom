# app/api/api_v1/endpoints/system.py
import logging
from typing import List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status
# --- Add import for text ---
from sqlalchemy.orm import Session
from sqlalchemy import text # Import text construct
# --- End import ---

from app.core.config import settings
from app.api import deps
from app.schemas import HealthCheckResponse, ComponentStatus

logger = logging.getLogger(__name__)
router = APIRouter()

# --- Input Sources Endpoint ---
# (remains the same)
@router.get(
    "/input-sources",
    response_model=List[str],
    summary="List Known Input Sources",
    description="Retrieve the list of configured identifiers for known system input sources.",
    dependencies=[Depends(deps.get_current_active_user)]
)
def list_input_sources() -> List[str]:
    """ Returns known input source identifiers. """
    return settings.KNOWN_INPUT_SOURCES


# --- Health Check Endpoint ---
@router.get(
    "/health",
    tags=["Health"],
    summary="Health Check",
    description="Performs a basic health check of API subsystems.",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheckResponse,
    # No authentication needed
)
async def health_check(db: Session = Depends(deps.get_db)):
    """ Performs health check on database connection. """
    components: Dict[str, ComponentStatus] = {}

    # Check Database
    db_status = "ok"
    db_details = "Connection successful."
    try:
        # --- Use text() construct for raw SQL ---
        db.execute(text("SELECT 1"))
        # --- End change ---
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}", exc_info=False)
        db_status = "error"
        db_details = f"Database connection error: Cannot execute simple query."

    components["database"] = ComponentStatus(status=db_status, details=db_details)

    # Determine overall status
    overall_status = "ok"
    if any(comp.status != "ok" for comp in components.values()):
        overall_status = "error"

    return HealthCheckResponse(
        status=overall_status,
        components=components
    )
