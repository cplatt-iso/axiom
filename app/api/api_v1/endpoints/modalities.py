# app/api/api_v1/endpoints/modalities.py
from typing import Any, List, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api import deps
from app.crud.crud_modality import (
    create, get, get_by_ae_title, get_by_ip_address, get_multi, 
    get_dmwl_enabled, update, delete, get_with_facility,
    is_ae_title_available, can_query_dmwl
)
from app.crud.crud_facility import get as get_facility
from app.db import models
from app.schemas.modality import (
    Modality, ModalityCreate, ModalityUpdate
)

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/", response_model=Modality, status_code=status.HTTP_201_CREATED)
def create_modality(
    *,
    db: Session = Depends(deps.get_db),
    modality_in: ModalityCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Create new modality."""
    # Verify facility exists
    facility = get_facility(db, facility_id=modality_in.facility_id)
    if not facility:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Facility not found"
        )
    
    # Check if AE Title already exists
    if not is_ae_title_available(db, ae_title=modality_in.ae_title):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A modality with this AE Title already exists"
        )
    
    modality = create(db=db, obj_in=modality_in)
    return modality

@router.get("/", response_model=List[Modality])
def read_modalities(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    facility_id: Optional[int] = None,
    modality_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    is_dmwl_enabled: Optional[bool] = None,
    department: Optional[str] = None,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Retrieve modalities."""
    modalities = get_multi(
        db=db,
        skip=skip,
        limit=limit,
        facility_id=facility_id,
        modality_type=modality_type,
        is_active=is_active,
        is_dmwl_enabled=is_dmwl_enabled,
        department=department
    )
    return modalities

@router.get("/dmwl-enabled", response_model=List[Modality])
def read_dmwl_enabled_modalities(
    *,
    db: Session = Depends(deps.get_db),
    facility_id: Optional[int] = None,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get all modalities enabled for DMWL queries."""
    modalities = get_dmwl_enabled(db=db, facility_id=facility_id)
    return modalities

@router.get("/{modality_id}", response_model=Modality)
def read_modality(
    *,
    db: Session = Depends(deps.get_db),
    modality_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get modality by ID."""
    modality = get(db=db, modality_id=modality_id)
    if not modality:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Modality not found"
        )
    return modality

@router.get("/{modality_id}/with-facility")
def read_modality_with_facility(
    *,
    db: Session = Depends(deps.get_db),
    modality_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get modality with its facility."""
    modality = get(db=db, modality_id=modality_id)
    if not modality:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Modality not found"
        )
    
    # Get facility separately to avoid forward reference issues
    from app.crud.crud_facility import get as get_facility
    facility = get_facility(db=db, facility_id=modality.facility_id)
    
    # Return a dictionary with modality and facility
    return {
        "modality": modality,
        "facility": facility
    }

@router.put("/{modality_id}", response_model=Modality)
def update_modality(
    *,
    db: Session = Depends(deps.get_db),
    modality_id: int,
    modality_in: ModalityUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Update a modality."""
    modality = get(db=db, modality_id=modality_id)
    if not modality:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Modality not found"
        )
    
    # Check if AE Title is being changed and new AE Title already exists
    if modality_in.ae_title and modality_in.ae_title != modality.ae_title:
        if not is_ae_title_available(db, ae_title=modality_in.ae_title, exclude_id=modality_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A modality with this AE Title already exists"
            )
    
    # Check if facility_id is being changed and new facility exists
    if modality_in.facility_id and modality_in.facility_id != modality.facility_id:
        facility = get_facility(db, facility_id=modality_in.facility_id)
        if not facility:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Facility not found"
            )
    
    modality = update(db=db, db_obj=modality, obj_in=modality_in)
    return modality

@router.delete("/{modality_id}")
def delete_modality(
    *,
    db: Session = Depends(deps.get_db),
    modality_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> None:
    """Delete a modality."""
    modality = get(db=db, modality_id=modality_id)
    if not modality:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Modality not found"
        )
    
    if not delete(db=db, modality_id=modality_id):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete modality"
        )

@router.get("/by-ae-title/{ae_title}", response_model=Modality)
def read_modality_by_ae_title(
    *,
    db: Session = Depends(deps.get_db),
    ae_title: str,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get modality by AE Title."""
    modality = get_by_ae_title(db=db, ae_title=ae_title)
    if not modality:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Modality not found"
        )
    return modality

@router.get("/by-ip/{ip_address}", response_model=List[Modality])
def read_modalities_by_ip(
    *,
    db: Session = Depends(deps.get_db),
    ip_address: str,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Get modalities by IP address."""
    modalities = get_by_ip_address(db=db, ip_address=ip_address)
    return modalities

@router.post("/check-ae-title/{ae_title}")
def check_ae_title_availability(
    *,
    db: Session = Depends(deps.get_db),
    ae_title: str,
    exclude_id: Optional[int] = None,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Check if an AE Title is available."""
    available = is_ae_title_available(db=db, ae_title=ae_title, exclude_id=exclude_id)
    return {"ae_title": ae_title, "available": available}

@router.post("/check-dmwl-access/{ae_title}")
def check_dmwl_access(
    *,
    db: Session = Depends(deps.get_db),
    ae_title: str,
    ip_address: Optional[str] = None,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """Check if a modality can access DMWL."""
    allowed, modality = can_query_dmwl(db=db, ae_title=ae_title, ip_address=ip_address)
    
    response = {
        "ae_title": ae_title,
        "ip_address": ip_address,
        "dmwl_access_allowed": allowed
    }
    
    if modality:
        response["modality"] = {
            "id": modality.id,
            "name": modality.name,
            "modality_type": modality.modality_type,
            "facility_id": modality.facility_id,
            "department": modality.department
        }
    
    return response
