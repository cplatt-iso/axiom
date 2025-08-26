# app/api/api_v1/endpoints/config_schedules.py
import logging
from typing import List, Any

from fastapi import APIRouter, Depends, HTTPException, status, Query
import structlog
from sqlalchemy.orm import Session

from app import crud, schemas # Import top-level packages
from app.db import models # Import DB models
from app.api import deps # Import API dependencies

logger = structlog.get_logger(__name__)
router = APIRouter()

# --- Dependency to get schedule config by ID ---
def get_schedule_by_id_from_path(
    schedule_id: int,
    db: Session = Depends(deps.get_db)
) -> models.Schedule:
    """
    Dependency that retrieves a schedule by ID from the path parameter.
    Raises 404 if not found.
    """
    db_schedule = crud.crud_schedule.get(db, id=schedule_id)
    if not db_schedule:
        logger.warning(f"Schedule with ID {schedule_id} not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Schedule with ID {schedule_id} not found",
        )
    return db_schedule

# --- API Routes ---

@router.post(
    "", # Relative path within this router
    response_model=schemas.ScheduleRead, # Use Read schema for response
    status_code=status.HTTP_201_CREATED,
    summary="Create Schedule",
    description="Adds a new reusable schedule definition.",
    dependencies=[Depends(deps.require_role("Admin"))], # Require Admin role
    responses={ # Add specific responses for docs
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data (e.g., time format, missing days)."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Admin role required."},
        status.HTTP_409_CONFLICT: {"description": "A schedule with the same name already exists."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def create_schedule(
    *,
    db: Session = Depends(deps.get_db),
    schedule_in: schemas.ScheduleCreate, # Use Create schema for input
    # current_user is available via dependency if needed for logging, but not used directly here
) -> models.Schedule: # Return type is DB model for ORM mode
    """Creates a new schedule definition."""
    # CRUD method handles potential name conflicts (raises 409)
    # Pydantic schema handles input validation (raises 400/422)
    try:
        return crud.crud_schedule.create(db=db, obj_in=schedule_in)
    except HTTPException as http_exc:
        raise http_exc # Re-raise known HTTP exceptions
    except Exception as e:
        logger.error(f"Unexpected error creating schedule '{schedule_in.name}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error creating schedule.")


@router.get(
    "",
    response_model=List[schemas.ScheduleRead],
    summary="List Schedules",
    description="Retrieves a list of all configured schedule definitions.",
    dependencies=[Depends(deps.get_current_active_user)], # Require login
)
def read_schedules(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records."),
) -> List[models.Schedule]:
    """Retrieves a list of schedule definitions with pagination."""
    return crud.crud_schedule.get_multi(db, skip=skip, limit=limit)


@router.get(
    "/{schedule_id}",
    response_model=schemas.ScheduleRead,
    summary="Get Schedule by ID",
    description="Retrieves details of a specific schedule definition.",
    dependencies=[Depends(deps.get_current_active_user)], # Require login
    responses={ # Add specific responses for docs
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_404_NOT_FOUND: {"description": "Schedule not found."},
    }
)
def read_schedule(
    *,
    # Use the dependency to get the schedule or raise 404
    db_schedule: models.Schedule = Depends(get_schedule_by_id_from_path),
    # current_user is available via dependency if needed
) -> models.Schedule:
    """Retrieves details for a single schedule definition by its ID."""
    return db_schedule


@router.put(
    "/{schedule_id}",
    response_model=schemas.ScheduleRead,
    summary="Update Schedule",
    description="Updates an existing schedule definition.",
    dependencies=[Depends(deps.require_role("Admin"))], # Require Admin role
    responses={ # Add specific responses for docs
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Admin role required."},
        status.HTTP_404_NOT_FOUND: {"description": "Schedule not found."},
        status.HTTP_409_CONFLICT: {"description": "Update would cause a name conflict."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def update_schedule(
    *,
    schedule_id: int, # Keep path param explicit
    db: Session = Depends(deps.get_db),
    schedule_in: schemas.ScheduleUpdate, # Use Update schema
    db_schedule: models.Schedule = Depends(get_schedule_by_id_from_path), # Fetch existing
    # current_user available via dependency
) -> models.Schedule:
    """Updates an existing schedule definition."""
    # CRUD method handles validation and conflicts
    try:
        return crud.crud_schedule.update(db=db, db_obj=db_schedule, obj_in=schedule_in)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error updating schedule ID {schedule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error updating schedule.")


@router.delete(
    "/{schedule_id}",
    response_model=schemas.ScheduleRead, # Return deleted object
    summary="Delete Schedule",
    description="Removes a schedule definition.",
    dependencies=[Depends(deps.require_role("Admin"))], # Require Admin role
    responses={ # Add specific responses for docs
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Admin role required."},
        status.HTTP_404_NOT_FOUND: {"description": "Schedule not found."},
        status.HTTP_409_CONFLICT: {"description": "Schedule is still in use by rules."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def delete_schedule(
    *,
    schedule_id: int,
    db: Session = Depends(deps.get_db),
    # No need to fetch via dependency, CRUD handles check
    # current_user available via dependency
) -> models.Schedule: # Return deleted object (transient)
    """
    Deletes a schedule definition by its ID.
    Fails if the schedule is still referenced by rules (due to FK constraint
    with ondelete='SET NULL', the DB delete itself wouldn't fail, but the CRUD
    layer catches potential FK issues if needed, or we rely on SET NULL).
    The CRUD `remove` method shown previously includes a check for FK errors.
    """
    # CRUD method handles not found and potential FK constraint errors
    try:
        return crud.crud_schedule.remove(db=db, id=schedule_id)
    except HTTPException as http_exc:
        raise http_exc # Re-raise 404, 409 (if rule check implemented/needed)
    except Exception as e:
        logger.error(f"Unexpected error deleting schedule ID {schedule_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error deleting schedule.")
