# app/api/api_v1/endpoints/config_storage_backends.py
import logging
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query, Response
from sqlalchemy.orm import Session
import structlog

from app import crud, schemas
from app.db import models
from app.api import deps

logger = structlog.get_logger(__name__)
router = APIRouter()


def get_storage_backend_config_by_id_from_path(
    config_id: int,
    db: Session = Depends(deps.get_db)
) -> models.StorageBackendConfig:
    db_config = crud.crud_storage_backend_config.get(db, id=config_id)
    if not db_config:
        logger.warning(f"Storage backend config with ID {config_id} not found.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Storage backend config with ID {config_id} not found",
        )
    return db_config


@router.post(
    "",
    response_model=schemas.StorageBackendConfigRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create Storage Backend Configuration",
    description="Adds a new storage backend configuration.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_409_CONFLICT: {"description": "Conflict (e.g., name already exists)."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def create_storage_backend_config(
    *,
    db: Session = Depends(deps.get_db),
    config_in: schemas.StorageBackendConfigCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.StorageBackendConfig:
    logger.info(f"User {current_user.email} attempting to create storage backend config: {config_in.name}")
    try:
        db_config = crud.crud_storage_backend_config.create(db=db, obj_in=config_in)
        logger.info(f"Successfully created storage backend config '{db_config.name}' with ID {db_config.id}")
        return db_config
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error creating storage backend config '{config_in.name}': {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while creating the storage backend config."
        )


@router.get(
    "",
    response_model=List[schemas.StorageBackendConfigRead],
    summary="List Storage Backend Configurations",
    description="Retrieves a list of configured storage backends.",
)
def read_storage_backend_configs(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> List[models.StorageBackendConfig]:
    logger.debug(f"User {current_user.email} listing storage backend configs (skip={skip}, limit={limit}).")
    configs = crud.crud_storage_backend_config.get_multi(db, skip=skip, limit=limit)
    return configs


@router.get(
    "/{config_id}",
    response_model=schemas.StorageBackendConfigRead,
    summary="Get Storage Backend Configuration by ID",
    description="Retrieves details of a specific storage backend configuration.",
     responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Configuration not found."},
    }
)
def read_storage_backend_config(
    *,
    db_config: models.StorageBackendConfig = Depends(get_storage_backend_config_by_id_from_path),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.StorageBackendConfig:
    logger.debug(f"User {current_user.email} retrieving storage backend config ID {db_config.id} ('{db_config.name}').")
    return db_config


@router.put(
    "/{config_id}",
    response_model=schemas.StorageBackendConfigRead,
    summary="Update Storage Backend Configuration",
    description="Updates an existing storage backend configuration.",
     responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Configuration not found."},
        status.HTTP_409_CONFLICT: {"description": "Update would cause a name conflict."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def update_storage_backend_config(
    *,
    config_id: int,
    db: Session = Depends(deps.get_db),
    config_in: schemas.StorageBackendConfigUpdate,
    db_config: models.StorageBackendConfig = Depends(get_storage_backend_config_by_id_from_path),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.StorageBackendConfig:
    logger.info(f"User {current_user.email} attempting to update storage backend config ID {config_id} ('{db_config.name}').")
    try:
        updated_config = crud.crud_storage_backend_config.update(
            db=db, db_obj=db_config, obj_in=config_in
        )
        logger.info(f"Successfully updated storage backend config ID {updated_config.id} ('{updated_config.name}')")
        return updated_config
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error updating storage backend config ID {config_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while updating backend config ID {config_id}."
        )


@router.delete(
    "/{config_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete Storage Backend Configuration",
    description="Removes a storage backend configuration.",
     responses={
        status.HTTP_204_NO_CONTENT: {"description": "Backend successfully deleted."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Configuration not found."},
        status.HTTP_409_CONFLICT: {"description": "Cannot delete, backend is in use (e.g., by rules)."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def delete_storage_backend_config(
    *,
    config_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Response:
    logger.info(f"User {current_user.email} attempting to delete storage backend config ID {config_id}.")
    try:
        deleted_config = crud.crud_storage_backend_config.remove(db=db, id=config_id)
        logger.info(f"Successfully deleted storage backend config ID {config_id} (Name was: '{deleted_config.name}')")
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error deleting storage backend config ID {config_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while deleting backend config ID {config_id}."
        )
