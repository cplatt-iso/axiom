# app/api/api_v1/endpoints/config_dimse_listeners.py
import logging
from typing import List, Any

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app import crud, schemas # Import top-level packages
from app.db import models # Import DB models
from app.api import deps # Import API dependencies (like get_db, get_current_active_user)

logger = logging.getLogger(__name__)
router = APIRouter()

# --- Dependency to get listener config by ID ---
def get_dimse_listener_config_by_id_from_path(
    config_id: int,
    db: Session = Depends(deps.get_db)
) -> models.DimseListenerConfig:
    """
    Dependency that retrieves a DIMSE listener config by ID from the path parameter.
    Raises 404 if not found.
    """
    db_config = crud.crud_dimse_listener_config.get(db, id=config_id)
    if not db_config:
        logger.warning(f"DIMSE listener config with ID {config_id} not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DIMSE listener config with ID {config_id} not found",
        )
    return db_config

# --- API Routes ---

@router.post(
    "",
    response_model=schemas.DimseListenerConfigRead, # Use the Read schema for response
    status_code=status.HTTP_201_CREATED,
    summary="Create DIMSE Listener Configuration",
    description="Adds a new DIMSE C-STORE SCP listener configuration.",
    # Add specific responses for documentation
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_409_CONFLICT: {"description": "Conflict (e.g., name, instance_id, or AE/port already exists)."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def create_dimse_listener_config(
    *,
    db: Session = Depends(deps.get_db),
    config_in: schemas.DimseListenerConfigCreate,
    current_user: models.User = Depends(deps.get_current_active_user), # Require login
    # Add role check if needed: current_user: models.User = Depends(deps.require_role("Admin")),
) -> models.DimseListenerConfig: # Return type is the DB model for ORM mode
    """
    Creates a new DIMSE listener configuration.
    Requires authentication.
    """
    logger.info(f"User {current_user.email} attempting to create DIMSE listener config: {config_in.name}")
    # The CRUD create method now handles uniqueness checks and raises HTTPException
    try:
        db_config = crud.crud_dimse_listener_config.create(db=db, obj_in=config_in)
        logger.info(f"Successfully created DIMSE listener config '{db_config.name}' with ID {db_config.id}")
        return db_config
    except HTTPException as http_exc:
        # Re-raise HTTPExceptions raised by CRUD layer (like 409 Conflict)
        raise http_exc
    except Exception as e:
        # Catch unexpected errors during creation
        logger.error(f"Unexpected error creating DIMSE listener config '{config_in.name}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while creating the listener config."
        )

@router.get(
    "",
    response_model=List[schemas.DimseListenerConfigRead], # Use Read schema
    summary="List DIMSE Listener Configurations",
    description="Retrieves a list of configured DIMSE C-STORE SCP listeners.",
)
def read_dimse_listener_configs(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records."),
    current_user: models.User = Depends(deps.get_current_active_user),
    # Add role check if needed: current_user: models.User = Depends(deps.require_role("Admin")),
) -> List[models.DimseListenerConfig]:
    """
    Retrieves a list of DIMSE listener configurations with pagination.
    Requires authentication.
    """
    logger.debug(f"User {current_user.email} listing DIMSE listener configs (skip={skip}, limit={limit}).")
    configs = crud.crud_dimse_listener_config.get_multi(db, skip=skip, limit=limit)
    return configs

@router.get(
    "/{config_id}",
    response_model=schemas.DimseListenerConfigRead, # Use Read schema
    summary="Get DIMSE Listener Configuration by ID",
    description="Retrieves details of a specific DIMSE listener configuration.",
     responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Configuration not found."},
    }
)
def read_dimse_listener_config(
    *,
    # Use the dependency to get the config or raise 404
    db_config: models.DimseListenerConfig = Depends(get_dimse_listener_config_by_id_from_path),
    current_user: models.User = Depends(deps.get_current_active_user),
    # Add role check if needed: current_user: models.User = Depends(deps.require_role("Admin")),
) -> models.DimseListenerConfig:
    """
    Retrieves details for a single DIMSE listener configuration by its ID.
    Requires authentication.
    """
    logger.debug(f"User {current_user.email} retrieving DIMSE listener config ID {db_config.id} ('{db_config.name}').")
    # The dependency already fetched the object
    return db_config

@router.put(
    "/{config_id}",
    response_model=schemas.DimseListenerConfigRead, # Use Read schema
    summary="Update DIMSE Listener Configuration",
    description="Updates an existing DIMSE listener configuration.",
     responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Configuration not found."},
        status.HTTP_409_CONFLICT: {"description": "Update would cause a conflict (name, instance_id, etc.)."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def update_dimse_listener_config(
    *,
    config_id: int, # Keep path param explicit for logging/context
    db: Session = Depends(deps.get_db),
    config_in: schemas.DimseListenerConfigUpdate,
    db_config: models.DimseListenerConfig = Depends(get_dimse_listener_config_by_id_from_path), # Fetch existing
    current_user: models.User = Depends(deps.get_current_active_user),
    # Add role check if needed: current_user: models.User = Depends(deps.require_role("Admin")),
) -> models.DimseListenerConfig:
    """
    Updates an existing DIMSE listener configuration.
    Requires authentication. Only fields provided in the request body are updated.
    """
    logger.info(f"User {current_user.email} attempting to update DIMSE listener config ID {config_id} ('{db_config.name}').")
    # The CRUD update method handles conflict checks and raises HTTPException
    try:
        updated_config = crud.crud_dimse_listener_config.update(
            db=db, db_obj=db_config, obj_in=config_in
        )
        logger.info(f"Successfully updated DIMSE listener config ID {updated_config.id} ('{updated_config.name}')")
        return updated_config
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error updating DIMSE listener config ID {config_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while updating listener config ID {config_id}."
        )

@router.delete(
    "/{config_id}",
    response_model=schemas.DimseListenerConfigRead, # Return deleted object as confirmation
    summary="Delete DIMSE Listener Configuration",
    description="Removes a DIMSE listener configuration.",
     responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Configuration not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def delete_dimse_listener_config(
    *,
    config_id: int,
    db: Session = Depends(deps.get_db),
    # We don't need to fetch via dependency here, CRUD does the check
    current_user: models.User = Depends(deps.get_current_active_user),
    # Add role check if needed: current_user: models.User = Depends(deps.require_role("Admin")),
) -> models.DimseListenerConfig: # Return type is DB model (transient state)
    """
    Deletes a DIMSE listener configuration by its ID.
    Requires authentication. Returns the deleted configuration data.
    """
    logger.info(f"User {current_user.email} attempting to delete DIMSE listener config ID {config_id}.")
    # CRUD remove method handles not found and raises HTTPException
    try:
        deleted_config = crud.crud_dimse_listener_config.remove(db=db, id=config_id)
        logger.info(f"Successfully deleted DIMSE listener config ID {config_id} (Name: '{deleted_config.name}')")
        return deleted_config # Return the object that was deleted
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error deleting DIMSE listener config ID {config_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while deleting listener config ID {config_id}."
        )

# --- End API Routes ---
