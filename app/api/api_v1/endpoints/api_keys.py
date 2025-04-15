# app/api/api_v1/endpoints/api_keys.py
import logging
from typing import List, Any

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.db import models
from app.api import deps
from app import crud, schemas # Make sure schemas.__init__ exports ApiKey schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post(
    "/",
    response_model=schemas.ApiKeyCreateResponse, # Returns the FULL key once
    status_code=status.HTTP_201_CREATED,
    summary="Create a new API Key for the current user",
)
def create_api_key(
    *,
    db: Session = Depends(deps.get_db),
    key_in: schemas.ApiKeyCreate, # User provides name, maybe expiry
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Generates a new API key associated with the currently authenticated user.

    **Important:** The returned `full_key` is shown **only once**.
    Store it securely immediately. It cannot be retrieved again.
    """
    try:
        db_key, full_key = crud.crud_api_key.create_api_key(db=db, obj_in=key_in, user_id=current_user.id)
        # Manually construct the response to include the full_key
        response_data = schemas.ApiKey.model_validate(db_key).model_dump()
        response_data["full_key"] = full_key
        return schemas.ApiKeyCreateResponse(**response_data)
    except Exception as e:
        # Catch potential DB errors during creation
        logger.error(f"Failed to create API key for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create API key.")


@router.get(
    "/",
    response_model=List[schemas.ApiKey], # Returns list WITHOUT full key
    summary="List API Keys for the current user",
)
def list_api_keys(
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
) -> Any:
    """
    Retrieve all API keys belonging to the currently authenticated user.
    Does not show the full API key value for security.
    """
    keys = crud.crud_api_key.get_multi_by_user(db=db, user_id=current_user.id, skip=skip, limit=limit)
    return keys


@router.get(
    "/{key_id}",
    response_model=schemas.ApiKey, # Returns details WITHOUT full key
    summary="Get details of a specific API Key owned by the current user",
)
def get_api_key(
    key_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Retrieve details for a specific API key owned by the current user.
    """
    db_key = crud.crud_api_key.get(db=db, key_id=key_id, user_id=current_user.id)
    if not db_key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API Key not found or not owned by user.")
    return db_key


@router.put(
    "/{key_id}",
    response_model=schemas.ApiKey, # Returns updated details WITHOUT full key
    summary="Update an API Key owned by the current user",
)
def update_api_key(
    key_id: int,
    *,
    db: Session = Depends(deps.get_db),
    key_in: schemas.ApiKeyUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Update properties of an API Key (e.g., name, is_active).
    """
    db_key = crud.crud_api_key.get(db=db, key_id=key_id, user_id=current_user.id)
    if not db_key:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API Key not found or not owned by user.")
    if key_in.model_dump(exclude_unset=True) == {}:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields provided for update.")

    updated_key = crud.crud_api_key.update(db=db, db_obj=db_key, obj_in=key_in)
    return updated_key


@router.delete(
    "/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an API Key owned by the current user",
)
def delete_api_key(
    key_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> None:
    """
    Delete an API key owned by the current user.
    """
    deleted = crud.crud_api_key.delete(db=db, key_id=key_id, user_id=current_user.id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="API Key not found or not owned by user.")
    return None
