# app/crud/crud_api_key.py
import logging
from typing import List, Optional
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import select, update as sql_update

from app.db import models # Use models from app.db
from app.schemas import api_key as schemas_api_key # Use specific alias
from app.core.security import generate_api_key_string, get_api_key_hash, verify_api_key

logger = logging.getLogger(__name__)

def create_api_key(db: Session, *, obj_in: schemas_api_key.ApiKeyCreate, user_id: int) -> tuple[models.ApiKey, str]:
    """
    Creates a new API Key for a user.
    Returns the DB object AND the full unhashed key (only once).
    """
    prefix, full_key = generate_api_key_string()
    hashed_key = get_api_key_hash(full_key)

    # Check for extremely unlikely prefix collision
    existing_prefix = db.scalar(select(models.ApiKey).filter(models.ApiKey.prefix == prefix).limit(1))
    if existing_prefix:
         logger.error(f"API Key prefix collision detected for prefix {prefix}. Retrying generation.")
         # Simple retry strategy (could be more robust)
         return create_api_key(db=db, obj_in=obj_in, user_id=user_id)

    db_obj = models.ApiKey(
        **obj_in.model_dump(exclude_unset=True), # Use provided name, expires_at
        prefix=prefix,
        hashed_key=hashed_key,
        user_id=user_id,
        is_active=True # Default to active
    )
    db.add(db_obj)
    try:
        db.commit()
        db.refresh(db_obj)
        logger.info(f"Created API Key ID {db_obj.id} for User ID {user_id}")
        return db_obj, full_key # Return DB object and the raw key
    except Exception as e:
        logger.error(f"Error creating API Key for user {user_id}: {e}", exc_info=True)
        db.rollback()
        raise

def get(db: Session, *, key_id: int, user_id: int) -> Optional[models.ApiKey]:
    """Gets an API key by its ID, ensuring it belongs to the specified user."""
    statement = select(models.ApiKey).where(models.ApiKey.id == key_id, models.ApiKey.user_id == user_id)
    return db.execute(statement).scalar_one_or_none()

def get_multi_by_user(db: Session, *, user_id: int, skip: int = 0, limit: int = 100) -> List[models.ApiKey]:
    """Gets all API keys belonging to a specific user."""
    statement = (
        select(models.ApiKey)
        .where(models.ApiKey.user_id == user_id)
        .order_by(models.ApiKey.created_at.desc())
        .offset(skip)
        .limit(limit)
    )
    return list(db.execute(statement).scalars().all())

def get_active_key_by_prefix(db: Session, *, prefix: str) -> Optional[models.ApiKey]:
     """Gets an active API key based on its prefix. Used during authentication."""
     statement = select(models.ApiKey).where(
         models.ApiKey.prefix == prefix,
         models.ApiKey.is_active == True,
         # Optional: Check expiration
         (models.ApiKey.expires_at == None) | (models.ApiKey.expires_at > datetime.now(timezone.utc)) # noqa E711
     )
     return db.execute(statement).scalar_one_or_none()


def update_last_used(db: Session, *, api_key: models.ApiKey) -> None:
    """Updates the last_used_at timestamp for a key."""
    try:
        api_key.last_used_at = datetime.now(timezone.utc)
        db.add(api_key)
        db.commit()
        # No need to refresh if just updating timestamp
    except Exception as e:
         logger.warning(f"Failed to update last_used for API Key ID {api_key.id}: {e}")
         db.rollback() # Rollback this minor update, main auth likely succeeded already


def update(db: Session, *, db_obj: models.ApiKey, obj_in: schemas_api_key.ApiKeyUpdate) -> models.ApiKey:
    """Updates an API key (name, is_active, expires_at)."""
    update_data = obj_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_obj, field, value)
    db.add(db_obj)
    try:
        db.commit()
        db.refresh(db_obj)
        logger.info(f"Updated API Key ID {db_obj.id}")
        return db_obj
    except Exception as e:
        logger.error(f"Error updating API Key ID {db_obj.id}: {e}", exc_info=True)
        db.rollback()
        raise

def delete(db: Session, *, key_id: int, user_id: int) -> bool:
    """Deletes an API key by ID, ensuring it belongs to the user."""
    db_obj = get(db, key_id=key_id, user_id=user_id)
    if not db_obj:
        return False # Not found or not owned by user
    db.delete(db_obj)
    try:
        db.commit()
        logger.info(f"Deleted API Key ID {key_id} for User ID {user_id}")
        return True
    except Exception as e:
        logger.error(f"Error deleting API Key ID {key_id}: {e}", exc_info=True)
        db.rollback()
        raise

