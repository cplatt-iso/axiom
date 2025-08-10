# app/crud/crud_facility.py
import logging
from typing import List, Optional
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import select, update as sql_update, and_

from app.db import models
from app.schemas import facility as schemas_facility

logger = logging.getLogger(__name__)

def create(db: Session, *, obj_in: schemas_facility.FacilityCreate) -> models.Facility:
    """Create a new facility."""
    db_obj = models.Facility(**obj_in.model_dump(exclude_unset=True))
    db.add(db_obj)
    try:
        db.commit()
        db.refresh(db_obj)
        logger.info(f"Created facility ID {db_obj.id}: {db_obj.name}")
        return db_obj
    except Exception as e:
        logger.error(f"Error creating facility: {e}", exc_info=True)
        db.rollback()
        raise

def get(db: Session, *, facility_id: int) -> Optional[models.Facility]:
    """Get a facility by ID."""
    statement = select(models.Facility).where(models.Facility.id == facility_id)
    return db.scalar(statement)

def get_by_name(db: Session, *, name: str) -> Optional[models.Facility]:
    """Get a facility by name."""
    statement = select(models.Facility).where(models.Facility.name == name)
    return db.scalar(statement)

def get_by_facility_id(db: Session, *, facility_id: str) -> Optional[models.Facility]:
    """Get a facility by external facility_id."""
    statement = select(models.Facility).where(models.Facility.facility_id == facility_id)
    return db.scalar(statement)

def get_multi(
    db: Session, *, 
    skip: int = 0, 
    limit: int = 100, 
    is_active: Optional[bool] = None
) -> List[models.Facility]:
    """Get multiple facilities with optional filtering."""
    statement = select(models.Facility)
    
    if is_active is not None:
        statement = statement.where(models.Facility.is_active == is_active)
    
    statement = statement.offset(skip).limit(limit).order_by(models.Facility.name)
    return list(db.scalars(statement))

def update(
    db: Session, *, 
    db_obj: models.Facility, 
    obj_in: schemas_facility.FacilityUpdate
) -> models.Facility:
    """Update an existing facility."""
    update_data = obj_in.model_dump(exclude_unset=True)
    
    if update_data:
        statement = (
            sql_update(models.Facility)
            .where(models.Facility.id == db_obj.id)
            .values(**update_data)
        )
        db.execute(statement)
        
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Updated facility ID {db_obj.id}: {db_obj.name}")
            return db_obj
        except Exception as e:
            logger.error(f"Error updating facility ID {db_obj.id}: {e}", exc_info=True)
            db.rollback()
            raise
    
    return db_obj

def delete(db: Session, *, facility_id: int) -> bool:
    """Delete a facility (and cascade to modalities)."""
    db_obj = get(db, facility_id=facility_id)
    if db_obj:
        try:
            db.delete(db_obj)
            db.commit()
            logger.info(f"Deleted facility ID {facility_id}: {db_obj.name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting facility ID {facility_id}: {e}", exc_info=True)
            db.rollback()
            raise
    return False

def count(db: Session, *, is_active: Optional[bool] = None) -> int:
    """Count facilities with optional filtering."""
    statement = select(models.Facility)
    
    if is_active is not None:
        statement = statement.where(models.Facility.is_active == is_active)
    
    return len(list(db.scalars(statement)))

def get_with_modalities(db: Session, *, facility_id: int) -> Optional[models.Facility]:
    """Get a facility with its modalities loaded."""
    from sqlalchemy.orm import selectinload
    
    statement = (
        select(models.Facility)
        .options(selectinload(models.Facility.modalities))
        .where(models.Facility.id == facility_id)
    )
    return db.scalar(statement)

def get_modalities(
    db: Session, *, 
    facility_id: int,
    is_active: Optional[bool] = None,
    is_dmwl_enabled: Optional[bool] = None,
    modality_type: Optional[str] = None
) -> List[models.Modality]:
    """Get modalities for a specific facility with optional filtering."""
    statement = select(models.Modality).where(models.Modality.facility_id == facility_id)
    
    if is_active is not None:
        statement = statement.where(models.Modality.is_active == is_active)
    
    if is_dmwl_enabled is not None:
        statement = statement.where(models.Modality.is_dmwl_enabled == is_dmwl_enabled)
    
    if modality_type is not None:
        statement = statement.where(models.Modality.modality_type == modality_type.upper())
    
    statement = statement.order_by(models.Modality.ae_title)
    return list(db.scalars(statement))

def count_modalities(
    db: Session, *, 
    facility_id: int,
    is_active: Optional[bool] = None,
    is_dmwl_enabled: Optional[bool] = None,
    modality_type: Optional[str] = None
) -> int:
    """Count modalities for a specific facility with optional filtering."""
    statement = select(models.Modality).where(models.Modality.facility_id == facility_id)
    
    if is_active is not None:
        statement = statement.where(models.Modality.is_active == is_active)
    
    if is_dmwl_enabled is not None:
        statement = statement.where(models.Modality.is_dmwl_enabled == is_dmwl_enabled)
    
    if modality_type is not None:
        statement = statement.where(models.Modality.modality_type == modality_type.upper())
    
    return len(list(db.scalars(statement)))
