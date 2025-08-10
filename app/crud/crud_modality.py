# app/crud/crud_modality.py
import logging
from typing import List, Optional
from datetime import datetime

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import select, update as sql_update, and_

from app.db import models
from app.schemas import modality as schemas_modality

logger = logging.getLogger(__name__)

def create(db: Session, *, obj_in: schemas_modality.ModalityCreate) -> models.Modality:
    """Create a new modality."""
    db_obj = models.Modality(**obj_in.model_dump(exclude_unset=True))
    db.add(db_obj)
    try:
        db.commit()
        db.refresh(db_obj)
        logger.info(f"Created modality ID {db_obj.id}: {db_obj.ae_title} ({db_obj.modality_type})")
        return db_obj
    except Exception as e:
        logger.error(f"Error creating modality: {e}", exc_info=True)
        db.rollback()
        raise

def get(db: Session, *, modality_id: int) -> Optional[models.Modality]:
    """Get a modality by ID."""
    statement = select(models.Modality).where(models.Modality.id == modality_id)
    return db.scalar(statement)

def get_by_ae_title(db: Session, *, ae_title: str) -> Optional[models.Modality]:
    """Get a modality by AE Title."""
    statement = select(models.Modality).where(models.Modality.ae_title == ae_title)
    return db.scalar(statement)

def get_by_ip_address(db: Session, *, ip_address: str) -> List[models.Modality]:
    """Get modalities by IP address (there could be multiple on different ports)."""
    statement = select(models.Modality).where(models.Modality.ip_address == ip_address)
    return list(db.scalars(statement))

def get_multi(
    db: Session, *, 
    skip: int = 0, 
    limit: int = 100, 
    facility_id: Optional[int] = None,
    modality_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    is_dmwl_enabled: Optional[bool] = None,
    department: Optional[str] = None
) -> List[models.Modality]:
    """Get multiple modalities with optional filtering."""
    statement = select(models.Modality)
    
    if facility_id is not None:
        statement = statement.where(models.Modality.facility_id == facility_id)
    
    if modality_type is not None:
        statement = statement.where(models.Modality.modality_type == modality_type.upper())
    
    if is_active is not None:
        statement = statement.where(models.Modality.is_active == is_active)
    
    if is_dmwl_enabled is not None:
        statement = statement.where(models.Modality.is_dmwl_enabled == is_dmwl_enabled)
    
    if department is not None:
        statement = statement.where(models.Modality.department == department)
    
    statement = statement.offset(skip).limit(limit).order_by(models.Modality.ae_title)
    return list(db.scalars(statement))

def get_dmwl_enabled(db: Session, *, facility_id: Optional[int] = None) -> List[models.Modality]:
    """Get all modalities that are enabled for DMWL queries."""
    statement = (
        select(models.Modality)
        .join(models.Facility)
        .where(
            and_(
                models.Modality.is_active == True,
                models.Modality.is_dmwl_enabled == True,
                models.Facility.is_active == True
            )
        )
    )
    
    if facility_id is not None:
        statement = statement.where(models.Modality.facility_id == facility_id)
    
    statement = statement.order_by(models.Modality.ae_title)
    return list(db.scalars(statement))

def update(
    db: Session, *, 
    db_obj: models.Modality, 
    obj_in: schemas_modality.ModalityUpdate
) -> models.Modality:
    """Update an existing modality."""
    update_data = obj_in.model_dump(exclude_unset=True)
    
    if update_data:
        statement = (
            sql_update(models.Modality)
            .where(models.Modality.id == db_obj.id)
            .values(**update_data)
        )
        db.execute(statement)
        
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Updated modality ID {db_obj.id}: {db_obj.ae_title}")
            return db_obj
        except Exception as e:
            logger.error(f"Error updating modality ID {db_obj.id}: {e}", exc_info=True)
            db.rollback()
            raise
    
    return db_obj

def delete(db: Session, *, modality_id: int) -> bool:
    """Delete a modality."""
    db_obj = get(db, modality_id=modality_id)
    if db_obj:
        try:
            db.delete(db_obj)
            db.commit()
            logger.info(f"Deleted modality ID {modality_id}: {db_obj.ae_title}")
            return True
        except Exception as e:
            logger.error(f"Error deleting modality ID {modality_id}: {e}", exc_info=True)
            db.rollback()
            raise
    return False

def count(
    db: Session, *, 
    facility_id: Optional[int] = None,
    modality_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    is_dmwl_enabled: Optional[bool] = None
) -> int:
    """Count modalities with optional filtering."""
    statement = select(models.Modality)
    
    if facility_id is not None:
        statement = statement.where(models.Modality.facility_id == facility_id)
    
    if modality_type is not None:
        statement = statement.where(models.Modality.modality_type == modality_type.upper())
    
    if is_active is not None:
        statement = statement.where(models.Modality.is_active == is_active)
    
    if is_dmwl_enabled is not None:
        statement = statement.where(models.Modality.is_dmwl_enabled == is_dmwl_enabled)
    
    return len(list(db.scalars(statement)))

def get_with_facility(db: Session, *, modality_id: int) -> Optional[models.Modality]:
    """Get a modality with its facility loaded."""
    statement = (
        select(models.Modality)
        .options(selectinload(models.Modality.facility))
        .where(models.Modality.id == modality_id)
    )
    return db.scalar(statement)

def is_ae_title_available(db: Session, *, ae_title: str, exclude_id: Optional[int] = None) -> bool:
    """Check if an AE Title is available (not used by another modality)."""
    statement = select(models.Modality).where(models.Modality.ae_title == ae_title)
    
    if exclude_id is not None:
        statement = statement.where(models.Modality.id != exclude_id)
    
    existing = db.scalar(statement)
    return existing is None

def can_query_dmwl(db: Session, *, ae_title: str, ip_address: Optional[str] = None) -> tuple[bool, Optional[models.Modality]]:
    """
    Check if a modality (identified by AE Title and optionally IP) is allowed to query DMWL.
    Returns (allowed, modality_object) tuple.
    """
    statement = (
        select(models.Modality)
        .join(models.Facility)
        .where(
            and_(
                models.Modality.ae_title == ae_title,
                models.Modality.is_active == True,
                models.Modality.is_dmwl_enabled == True,
                models.Facility.is_active == True
            )
        )
    )
    
    if ip_address:
        statement = statement.where(models.Modality.ip_address == ip_address)
    
    modality = db.scalar(statement)
    return modality is not None, modality
