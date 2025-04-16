# app/api/api_v1/endpoints/roles.py
import logging
from typing import List, Any

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.db import models
from app.api import deps
from app import crud, schemas

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get(
    "/",
    response_model=List[schemas.Role],
    summary="List all available roles",
    dependencies=[Depends(deps.get_current_active_user)] # Require login to see roles
)
def list_available_roles(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=200),
) -> Any:
    """
    Retrieve a list of all available roles in the system.
    Requires user to be authenticated.
    """
    roles = crud.crud_role.get_roles_multi(db=db, skip=skip, limit=limit)
    return roles

# Optional: Add POST/PUT/DELETE for roles later, protected by Admin role
# @router.post("/", response_model=schemas.Role, status_code=201, dependencies=[Depends(deps.require_role("Admin"))])
# def create_role(...): ...
