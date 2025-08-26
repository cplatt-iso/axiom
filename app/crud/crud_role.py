# app/crud/crud_role.py
import logging
import structlog
from typing import List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.db import models # Use models from app.db
from app.schemas import RoleCreate # Import needed schemas

logger = structlog.get_logger(__name__)

def get_role_by_id(db: Session, role_id: int) -> Optional[models.Role]:
    """Gets a role by its primary key ID."""
    return db.get(models.Role, role_id)

def get_role_by_name(db: Session, name: str) -> Optional[models.Role]:
    """Gets a role by its unique name."""
    statement = select(models.Role).where(models.Role.name == name)
    return db.execute(statement).scalar_one_or_none()

def get_roles_multi(db: Session, skip: int = 0, limit: int = 100) -> List[models.Role]:
    """Gets multiple roles, ordered by name."""
    statement = select(models.Role).order_by(models.Role.name).offset(skip).limit(limit)
    return list(db.execute(statement).scalars().all())

# Optional: Add create/update/delete for roles if needed via API later
# def create_role(db: Session, *, obj_in: RoleCreate) -> models.Role: ...
# def update_role(db: Session, *, db_obj: models.Role, obj_in: RoleUpdate) -> models.Role: ...
# def delete_role(db: Session, *, role_id: int) -> bool: ...
