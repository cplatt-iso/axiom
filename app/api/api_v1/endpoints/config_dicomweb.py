# app/api/api_v1/endpoints/config_dicomweb.py
import logging
from typing import List, Any

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

# --- Corrected Imports ---
from app import crud, schemas
from app.db import models
# --- End Corrected Imports ---
from app.api import deps

logger = logging.getLogger(__name__)

router = APIRouter() # Internal router variable name can stay 'router'

# --- Dependency to get DICOMweb source by ID ---
def get_dicomweb_source_by_id_from_path(
    source_id: int,
    db: Session = Depends(deps.get_db)
) -> models.DicomWebSourceState:
    """
    Dependency that retrieves a DICOMweb source by ID from the path parameter.
    Raises 404 if not found.
    """
    db_source = crud.dicomweb_source.get(db, id=source_id)
    if not db_source:
        logger.warning(f"DICOMweb source with ID {source_id} not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"DICOMweb source with ID {source_id} not found",
        )
    return db_source

# --- API Routes ---

@router.post(
    "", # Relative path within this router
    response_model=schemas.dicomweb.DicomWebSourceConfigRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create DICOMweb Source Configuration",
    description="Adds a new DICOMweb source configuration to the system.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data (e.g., validation error)."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to create configurations."},
        status.HTTP_409_CONFLICT: {"description": "A source with the same name already exists."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error during creation."},
    }
)
def create_dicomweb_source(
    *,
    db: Session = Depends(deps.get_db),
    source_in: schemas.dicomweb.DicomWebSourceConfigCreate,
    current_user: models.User = Depends(deps.get_current_active_user), # Requires models.User
) -> models.DicomWebSourceState: # Return type is the DB model for ORM mode
    """
    Creates a new DICOMweb source configuration.

    Requires authentication. The endpoint path will be mounted under '/config/dicomweb-sources/'.
    """
    logger.info(f"User {current_user.email} attempting to create DICOMweb source: {source_in.name}")
    try:
        # Ensure crud.dicomweb_source exists via app.crud.__init__
        db_source = crud.dicomweb_source.create(db=db, obj_in=source_in)
        logger.info(f"Successfully created DICOMweb source '{db_source.source_name}' with ID {db_source.id} by user {current_user.email}")
        return db_source
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error creating DICOMweb source '{source_in.name}' by user {current_user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while creating the DICOMweb source."
        )


@router.get(
    "", # Relative path within this router
    response_model=List[schemas.dicomweb.DicomWebSourceConfigRead],
    summary="List DICOMweb Source Configurations",
    description="Retrieves a list of configured DICOMweb sources.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to view configurations."},
    }
)
def read_dicomweb_sources(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip for pagination."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records to return."),
    current_user: models.User = Depends(deps.get_current_active_user), # Requires models.User
) -> List[models.DicomWebSourceState]: # Return type is List[DB model] for ORM mode
    """
    Retrieves a list of DICOMweb source configurations with pagination.

    Requires authentication. The endpoint path will be mounted under '/config/dicomweb-sources/'.
    """
    logger.debug(f"User {current_user.email} listing DICOMweb sources (skip={skip}, limit={limit}).")
    sources = crud.dicomweb_source.get_multi(db, skip=skip, limit=limit)
    return sources


@router.get(
    "/{source_id}", # Relative path within this router
    response_model=schemas.dicomweb.DicomWebSourceConfigRead,
    summary="Get DICOMweb Source Configuration by ID",
    description="Retrieves the details of a specific DICOMweb source configuration using its ID.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to view this configuration."},
        status.HTTP_404_NOT_FOUND: {"description": "DICOMweb source with the specified ID not found."},
    }
)
def read_dicomweb_source(
    *,
    # Use the dependency to get the source or raise 404
    db_source: models.DicomWebSourceState = Depends(get_dicomweb_source_by_id_from_path), # Requires models.DicomWebSourceState
    current_user: models.User = Depends(deps.get_current_active_user), # Requires models.User
) -> models.DicomWebSourceState:
    """
    Retrieves details for a single DICOMweb source configuration by its database ID.

    Requires authentication. The endpoint path will be mounted under '/config/dicomweb-sources/{source_id}'.
    """
    logger.debug(f"User {current_user.email} retrieving DICOMweb source ID {db_source.id} ('{db_source.source_name}').")
    return db_source


@router.put(
    "/{source_id}", # Relative path within this router
    response_model=schemas.dicomweb.DicomWebSourceConfigRead,
    summary="Update DICOMweb Source Configuration",
    description="Updates the configuration of an existing DICOMweb source.",
     responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data (e.g., validation error, auth config mismatch)."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to update configurations."},
        status.HTTP_404_NOT_FOUND: {"description": "DICOMweb source with the specified ID not found."},
        status.HTTP_409_CONFLICT: {"description": "Update would cause a name conflict with another source."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error during update."},
    }
)
def update_dicomweb_source(
    *,
    source_id: int,
    db: Session = Depends(deps.get_db),
    source_in: schemas.dicomweb.DicomWebSourceConfigUpdate,
    db_source: models.DicomWebSourceState = Depends(get_dicomweb_source_by_id_from_path), # Requires models.DicomWebSourceState
    current_user: models.User = Depends(deps.get_current_active_user), # Requires models.User
) -> models.DicomWebSourceState:
    """
    Updates an existing DICOMweb source configuration.

    Requires authentication. Only fields provided in the request body will be updated.
    The endpoint path will be mounted under '/config/dicomweb-sources/{source_id}'.
    """
    logger.info(f"User {current_user.email} attempting to update DICOMweb source ID {source_id} ('{db_source.source_name}').")
    try:
        updated_source = crud.dicomweb_source.update(db=db, db_obj=db_source, obj_in=source_in)
        logger.info(f"Successfully updated DICOMweb source ID {updated_source.id} ('{updated_source.source_name}') by user {current_user.email}")
        return updated_source
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error updating DICOMweb source ID {source_id} by user {current_user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while updating DICOMweb source ID {source_id}."
        )


@router.delete(
    "/{source_id}", # Relative path within this router
    response_model=schemas.dicomweb.DicomWebSourceConfigRead, # Return deleted object as confirmation
    summary="Delete DICOMweb Source Configuration",
    description="Removes a DICOMweb source configuration from the system.",
     responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to delete configurations."},
        status.HTTP_404_NOT_FOUND: {"description": "DICOMweb source with the specified ID not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error during deletion."},
    }
)
def delete_dicomweb_source(
    *,
    source_id: int,
    db: Session = Depends(deps.get_db),
    db_source: models.DicomWebSourceState = Depends(get_dicomweb_source_by_id_from_path), # Requires models.DicomWebSourceState
    current_user: models.User = Depends(deps.get_current_active_user), # Requires models.User
) -> models.DicomWebSourceState: # Return type is the DB model (transient state)
    """
    Deletes a DICOMweb source configuration by its database ID.

    Requires authentication. Returns the deleted configuration data.
    The endpoint path will be mounted under '/config/dicomweb-sources/{source_id}'.
    """
    logger.info(f"User {current_user.email} attempting to delete DICOMweb source ID {source_id} ('{db_source.source_name}').")
    try:
        deleted_source = crud.dicomweb_source.remove(db=db, id=source_id)
        logger.info(f"Successfully deleted DICOMweb source ID {source_id} (Name: '{deleted_source.source_name}') by user {current_user.email}")
        return deleted_source
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error deleting DICOMweb source ID {source_id} by user {current_user.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while deleting DICOMweb source ID {source_id}."
        )

# --- End API Routes ---
