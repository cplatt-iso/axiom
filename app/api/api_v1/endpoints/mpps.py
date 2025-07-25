# app/api/api_v1/endpoints/mpps.py
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app.db import models
from app import crud
import app.schemas as schemas
from app.api import deps

router = APIRouter()

@router.post("/", response_model=schemas.MppsRead, status_code=status.HTTP_201_CREATED)
def create_mpps(
    *,
    db: Session = Depends(deps.get_db),
    mpps_in: schemas.MppsCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Create a new MPPS entry (SCP/SCU event from a modality).
    """
    db_mpps = crud.mpps.create(db=db, obj_in=mpps_in)
    if not db_mpps:
        raise HTTPException(status_code=400, detail="Failed to create MPPS entry.")
    return db_mpps

@router.get("/", response_model=List[schemas.MppsRead])
def list_mpps(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    imaging_order_id: Optional[int] = Query(None, description="Filter by Imaging Order ID."),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    List MPPS entries, optionally filtered by Imaging Order.
    """
    query = db.query(models.Mpps)
    if imaging_order_id:
        query = query.filter(models.Mpps.imaging_order_id == imaging_order_id)
    return query.offset(skip).limit(limit).all()

@router.get("/{mpps_id}", response_model=schemas.MppsRead)
def get_mpps( 
    mpps_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Get a specific MPPS entry by ID.
    """
    db_mpps = db.query(models.Mpps).filter(models.Mpps.id == mpps_id).first()
    if not db_mpps:
        raise HTTPException(status_code=404, detail="MPPS entry not found.")
    return db_mpps

@router.put("/{mpps_id}", response_model=schemas.MppsRead)
def update_mpps(
    mpps_id: int,
    mpps_in: schemas.MppsUpdate,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Update an MPPS entry.
    """
    db_mpps = db.query(models.Mpps).filter(models.Mpps.id == mpps_id).first()
    if not db_mpps:
        raise HTTPException(status_code=404, detail="MPPS entry not found.")
    for field, value in mpps_in.model_dump(exclude_unset=True).items():
        setattr(db_mpps, field, value)
    db.commit()
    db.refresh(db_mpps)
    return db_mpps

@router.delete("/{mpps_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_mpps(
    mpps_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Delete an MPPS entry.
    """
    db_mpps = db.query(models.Mpps).filter(models.Mpps.id == mpps_id).first()
    if not db_mpps:
        raise HTTPException(status_code=404, detail="MPPS entry not found.")
    db.delete(db_mpps)
    db.commit()
    return None
