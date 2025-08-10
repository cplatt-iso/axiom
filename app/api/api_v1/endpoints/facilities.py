# app/api/api_v1/endpoints/facilities.py
from typing import Any, List, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api import deps
from app.crud.crud_facility import (
    create, get, get_by_name, get_multi, update, delete, count,
    get_with_modalities, get_modalities, count_modalities
)
from app.db import models
from app.schemas.facility import (
    Facility, FacilityCreate, FacilityUpdate, FacilityModalityInfo
)

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/", response_model=Facility, status_code=status.HTTP_201_CREATED)
def create_facility(
    *,
    db: Session = Depends(deps.get_db),
    facility_in: FacilityCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Create new facility."""
    # Check if facility name already exists
    if get_by_name(db, name=facility_in.name):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A facility with this name already exists"
        )
    
    facility = create(db=db, obj_in=facility_in)
    return facility

@router.get("/", response_model=List[Facility])
def read_facilities(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    is_active: Optional[bool] = None,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Retrieve facilities."""
    facilities = get_multi(
        db=db, 
        skip=skip, 
        limit=limit, 
        is_active=is_active
    )
    return facilities

@router.get("/{facility_id}", response_model=Facility)
def read_facility(
    *,
    db: Session = Depends(deps.get_db),
    facility_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get facility by ID."""
    facility = get(db=db, facility_id=facility_id)
    if not facility:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Facility not found"
        )
    return facility

@router.get("/{facility_id}/with-modalities")
def read_facility_with_modalities(
    *,
    db: Session = Depends(deps.get_db),
    facility_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get facility with its modalities."""
    facility = get(db=db, facility_id=facility_id)
    if not facility:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Facility not found"
        )
    
    # Get modalities separately to avoid forward reference issues
    modalities = get_modalities(db=db, facility_id=facility_id)
    
    # Return a dictionary with facility and modalities
    return {
        "facility": facility,
        "modalities": modalities
    }

@router.put("/{facility_id}", response_model=Facility)
def update_facility(
    *,
    db: Session = Depends(deps.get_db),
    facility_id: int,
    facility_in: FacilityUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Update a facility."""
    facility = get(db=db, facility_id=facility_id)
    if not facility:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Facility not found"
        )
    
    # Check if name is being changed and new name already exists
    if facility_in.name and facility_in.name != facility.name:
        existing = get_by_name(db, name=facility_in.name)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A facility with this name already exists"
            )
    
    facility = update(db=db, db_obj=facility, obj_in=facility_in)
    return facility

@router.delete("/{facility_id}")
def delete_facility(
    *,
    db: Session = Depends(deps.get_db),
    facility_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> None:
    """Delete a facility."""
    facility = get(db=db, facility_id=facility_id)
    if not facility:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Facility not found"
        )
    
    # Check if facility has modalities
    modality_count = count_modalities(db=db, facility_id=facility_id)
    if modality_count > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot delete facility with {modality_count} modalities. Remove or reassign modalities first."
        )
    
    if not delete(db=db, facility_id=facility_id):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete facility"
        )

@router.get("/{facility_id}/modalities", response_model=List[FacilityModalityInfo])
def read_facility_modalities(
    *,
    db: Session = Depends(deps.get_db),
    facility_id: int,
    is_active: Optional[bool] = None,
    is_dmwl_enabled: Optional[bool] = None,
    modality_type: Optional[str] = None,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get modalities for a specific facility."""
    facility = get(db=db, facility_id=facility_id)
    if not facility:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Facility not found"
        )
    
    modalities = get_modalities(
        db=db,
        facility_id=facility_id,
        is_active=is_active,
        is_dmwl_enabled=is_dmwl_enabled,
        modality_type=modality_type
    )
    return modalities

@router.get("/{facility_id}/stats")
def read_facility_stats(
    *,
    db: Session = Depends(deps.get_db),
    facility_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get statistics for a facility."""
    facility = get(db=db, facility_id=facility_id)
    if not facility:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Facility not found"
        )
    
    stats = {
        "total_modalities": count_modalities(db=db, facility_id=facility_id),
        "active_modalities": count_modalities(db=db, facility_id=facility_id, is_active=True),
        "dmwl_enabled_modalities": count_modalities(db=db, facility_id=facility_id, is_dmwl_enabled=True),
        "facility": facility
    }
    return stats
