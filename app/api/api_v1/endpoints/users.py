# app/api/api_v1/endpoints/users.py
import logging
from typing import List, Any

from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlalchemy.orm import Session

from app.db import models
from app.api import deps
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get(
    "/me",
    response_model=schemas.User,
    summary="Get current logged-in user details"
)
def read_users_me(
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Fetch details for the currently authenticated user (based on token).
    Includes user roles.
    """
    # The dependency already fetches the user with roles (due to joinedload)
    return current_user


# --- Admin Protected Routes ---

@router.get(
    "/",
    response_model=List[schemas.User],
    summary="List users (Admin only)",
    dependencies=[Depends(deps.require_role("Admin"))] # Require Admin role
)
def read_users(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
    # current_admin: models.User = Depends(deps.get_current_active_superuser) # Alternative if using superuser check
) -> Any:
    """
    Retrieve a list of users. Requires Admin privileges.
    """
    users = crud.crud_user.get_multi(db=db, skip=skip, limit=limit)
    return users


@router.get(
    "/{user_id}",
    response_model=schemas.User,
    summary="Get specific user by ID (Admin only)",
    dependencies=[Depends(deps.require_role("Admin"))]
)
def read_user_by_id(
    user_id: int,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Get details for a specific user by their ID. Requires Admin privileges.
    """
    db_user = crud.crud_user.get(db=db, user_id=user_id)
    if db_user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return db_user


@router.put(
    "/{user_id}",
    response_model=schemas.User,
    summary="Update user (Admin only)",
    dependencies=[Depends(deps.require_role("Admin"))]
)
def update_user(
    user_id: int,
    *,
    db: Session = Depends(deps.get_db),
    user_in: schemas.UserUpdate,
) -> Any:
    """
    Update user details (e.g., is_active). Requires Admin privileges.
    Cannot update roles via this endpoint.
    """
    db_user = crud.crud_user.get(db=db, user_id=user_id)
    if not db_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    # Ensure certain fields are not updated here if needed
    if user_in.email is not None and user_in.email != db_user.email:
         logger.warning(f"Attempt by admin to change email for user {user_id} ignored.")
         user_in.email = None # Or raise 400 Bad Request

    updated_user = crud.crud_user.update(db=db, db_user=db_user, user_in=user_in)
    return updated_user


@router.post(
    "/{user_id}/roles",
    response_model=schemas.User,
    summary="Assign a role to a user (Admin only)",
    dependencies=[Depends(deps.require_role("Admin"))]
)
def assign_role_to_user_endpoint(
    user_id: int,
    role_info: dict = Body(..., example={"role_id": 1}), # Expect {"role_id": ID}
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Assigns a specific role to a user by their IDs. Requires Admin privileges.
    """
    role_id = role_info.get("role_id")
    if role_id is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing 'role_id' in request body.")

    db_user = crud.crud_user.get(db=db, user_id=user_id)
    if not db_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    db_role = crud.crud_role.get_role_by_id(db=db, role_id=role_id)
    if not db_role:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Role with id {role_id} not found")

    updated_user = crud.crud_user.assign_role(db=db, db_user=db_user, db_role=db_role)
    return updated_user


@router.delete(
    "/{user_id}/roles/{role_id}",
    response_model=schemas.User,
    summary="Remove a role from a user (Admin only)",
    dependencies=[Depends(deps.require_role("Admin"))]
)
def remove_role_from_user_endpoint(
    user_id: int,
    role_id: int,
    *,
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Removes a specific role from a user by their IDs. Requires Admin privileges.
    """
    db_user = crud.crud_user.get(db=db, user_id=user_id)
    if not db_user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    db_role = crud.crud_role.get_role_by_id(db=db, role_id=role_id)
    if not db_role:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Role with id {role_id} not found")

    updated_user = crud.crud_user.remove_role(db=db, db_user=db_user, db_role=db_role)
    return updated_user


# Optional: Add DELETE /users/{user_id} endpoint if needed (Admin only)
