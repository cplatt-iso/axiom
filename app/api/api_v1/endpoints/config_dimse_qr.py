# app/api/api_v1/endpoints/config_dimse_qr.py
import logging
from typing import List, Any, Dict
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

# Corrected Imports using top-level packages/modules
from app import crud, schemas
from app.db import models
from app.api import deps
from app.schemas.enums import HealthStatus

logger = logging.getLogger(__name__)
router = APIRouter()

# --- Dependency to get DIMSE QR source by ID ---
def get_dimse_qr_source_by_id_from_path(
    source_id: int,
    db: Session = Depends(deps.get_db)
) -> models.DimseQueryRetrieveSource:
    """
    Dependency that retrieves a DIMSE Q/R source by ID from the path parameter.
    Raises 404 if not found.
    """
    db_source = crud.crud_dimse_qr_source.get(db, id=source_id)
    if not db_source:
        logger.warning(f"DIMSE Q/R source with ID {source_id} not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DIMSE Query/Retrieve source with ID {source_id} not found",
        )
    return db_source

# --- API Routes ---

@router.post(
    "", # Relative path within this router
    response_model=schemas.DimseQueryRetrieveSourceRead, # Use the Read schema
    status_code=status.HTTP_201_CREATED,
    summary="Create DIMSE Q/R Source Configuration",
    description="Adds a new DIMSE Query/Retrieve source configuration to the system.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data (e.g., validation error)."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to create configurations."},
        status.HTTP_409_CONFLICT: {"description": "A source with the same name already exists."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error during creation."},
    }
)
def create_dimse_qr_source(
    *,
    db: Session = Depends(deps.get_db),
    source_in: schemas.DimseQueryRetrieveSourceCreate, # Use the Create schema
    current_user: models.User = Depends(deps.get_current_active_user), # Require login
) -> models.DimseQueryRetrieveSource: # Return type is the DB model for ORM mode
    """
    Creates a new DIMSE Query/Retrieve source configuration. Requires authentication.
    """
    logger.info(f"User {current_user.email} attempting to create DIMSE Q/R source: {source_in.name}")
    # The CRUD create method handles uniqueness checks and raises HTTPException
    try:
        db_source = crud.crud_dimse_qr_source.create(db=db, obj_in=source_in)
        logger.info(f"Successfully created DIMSE Q/R source '{db_source.name}' with ID {db_source.id}")
        return db_source
    except HTTPException as http_exc:
        # Re-raise HTTPExceptions raised by CRUD layer (like 409 Conflict)
        raise http_exc
    except Exception as e:
        # Catch unexpected errors during creation
        logger.error(f"Unexpected error creating DIMSE Q/R source '{source_in.name}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while creating the DIMSE Q/R source."
        )

@router.get(
    "", # Relative path
    response_model=List[schemas.DimseQueryRetrieveSourceRead], # Use the Read schema
    summary="List DIMSE Q/R Source Configurations",
    description="Retrieves a list of configured DIMSE Query/Retrieve sources.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
    }
)
def read_dimse_qr_sources(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records."),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> List[models.DimseQueryRetrieveSource]: # Return type is List[DB model]
    """
    Retrieves a list of DIMSE Q/R source configurations with pagination. Requires authentication.
    """
    logger.debug(f"User {current_user.email} listing DIMSE Q/R sources (skip={skip}, limit={limit}).")
    sources = crud.crud_dimse_qr_source.get_multi(db, skip=skip, limit=limit)
    return sources

@router.get(
    "/{source_id}", # Relative path
    response_model=schemas.DimseQueryRetrieveSourceRead, # Use the Read schema
    summary="Get DIMSE Q/R Source Configuration by ID",
    description="Retrieves the details of a specific DIMSE Q/R source configuration.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "DIMSE Q/R source not found."},
    }
)
def read_dimse_qr_source(
    *,
    # Use the dependency to get the source or raise 404
    db_source: models.DimseQueryRetrieveSource = Depends(get_dimse_qr_source_by_id_from_path),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.DimseQueryRetrieveSource:
    """
    Retrieves details for a single DIMSE Q/R source configuration by its ID. Requires authentication.
    """
    logger.debug(f"User {current_user.email} retrieving DIMSE Q/R source ID {db_source.id} ('{db_source.name}').")
    # The dependency already fetched the object
    return db_source

@router.put(
    "/{source_id}", # Relative path
    response_model=schemas.DimseQueryRetrieveSourceRead, # Use Read schema
    summary="Update DIMSE Q/R Source Configuration",
    description="Updates the configuration of an existing DIMSE Query/Retrieve source.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "DIMSE Q/R source not found."},
        status.HTTP_409_CONFLICT: {"description": "Update would cause a name conflict."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def update_dimse_qr_source(
    *,
    source_id: int, # Keep path param explicit for context
    db: Session = Depends(deps.get_db),
    source_in: schemas.DimseQueryRetrieveSourceUpdate, # Use Update schema
    db_source: models.DimseQueryRetrieveSource = Depends(get_dimse_qr_source_by_id_from_path), # Fetch existing
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.DimseQueryRetrieveSource:
    """
    Updates an existing DIMSE Q/R source configuration. Requires authentication.
    Only fields provided in the request body will be updated.
    """
    logger.info(f"User {current_user.email} attempting to update DIMSE Q/R source ID {source_id} ('{db_source.name}').")
    # CRUD update method handles validation and conflict checks
    try:
        updated_source = crud.crud_dimse_qr_source.update(
            db=db, db_obj=db_source, obj_in=source_in
        )
        logger.info(f"Successfully updated DIMSE Q/R source ID {updated_source.id} ('{updated_source.name}')")
        return updated_source
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error updating DIMSE Q/R source ID {source_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while updating source ID {source_id}."
        )

@router.delete(
    "/{source_id}", # Relative path
    response_model=schemas.DimseQueryRetrieveSourceRead, # Return deleted object
    summary="Delete DIMSE Q/R Source Configuration",
    description="Removes a DIMSE Query/Retrieve source configuration.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "DIMSE Q/R source not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def delete_dimse_qr_source(
    *,
    source_id: int,
    db: Session = Depends(deps.get_db),
    # No need to use the dependency here, CRUD handles the check
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.DimseQueryRetrieveSource: # Return type is the DB model (transient state)
    """
    Deletes a DIMSE Q/R source configuration by its ID. Requires authentication.
    Returns the deleted configuration data.
    """
    logger.info(f"User {current_user.email} attempting to delete DIMSE Q/R source ID {source_id}.")
    # CRUD remove method handles not found and raises HTTPException
    try:
        deleted_source = crud.crud_dimse_qr_source.remove(db=db, id=source_id)
        logger.info(f"Successfully deleted DIMSE Q/R source ID {source_id} (Name: '{deleted_source.name}')")
        return deleted_source # Return the object that was deleted
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error deleting DIMSE Q/R source ID {source_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while deleting source ID {source_id}."
        )

# --- End API Routes ---

# --- Connection Test Endpoint ---
@router.post(
    "/{source_id}/test-connection",
    response_model=Dict[str, Any],
    summary="Test DIMSE Q/R Source Connection",
    description="Tests the connection to a DIMSE Q/R source using C-ECHO and updates its health status.",
    responses={
        status.HTTP_200_OK: {"description": "Connection test completed (check response for results)."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to test connections."},
        status.HTTP_404_NOT_FOUND: {"description": "DIMSE Q/R source with the specified ID not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error during connection test."},
    }
)
async def test_dimse_qr_connection(
    *,
    source_id: int,
    db: Session = Depends(deps.get_db),
    db_source: models.DimseQueryRetrieveSource = Depends(get_dimse_qr_source_by_id_from_path),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """
    Tests the connection to a DIMSE Q/R source using C-ECHO and updates its health status.
    
    This endpoint performs a connection test by sending a C-ECHO request to the source
    and updates the source's health status based on the results.
    """
    logger.info(f"User {current_user.email} testing connection for DIMSE Q/R source ID {source_id} ('{db_source.remote_ae_title}@{db_source.remote_host}:{db_source.remote_port}').")
    
    try:
        from app.services.connection_test_service import ConnectionTestService
        
        # Perform the connection test
        health_status, error_message = await ConnectionTestService.test_dimse_qr_connection(db_source)
        
        # Update the health status in the database
        await ConnectionTestService.update_source_health_status(
            db_session=db,
            source_type="dimse_qr",
            source_id=source_id,
            health_status=health_status,
            error_message=error_message
        )
        
        # Prepare response
        response = {
            "source_id": source_id,
            "remote_ae_title": db_source.remote_ae_title,
            "remote_endpoint": f"{db_source.remote_host}:{db_source.remote_port}",
            "health_status": health_status.value,
            "test_timestamp": datetime.now(timezone.utc).isoformat(),
            "success": health_status == HealthStatus.OK,
        }
        
        if error_message:
            response["error_message"] = error_message
        
        logger.info(f"Connection test completed for DIMSE Q/R source {source_id}: {health_status.value}")
        return response
        
    except Exception as e:
        logger.error(f"Error testing DIMSE Q/R connection for source {source_id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test connection: {str(e)}"
        )
