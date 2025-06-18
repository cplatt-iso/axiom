# backend/app/api/api_v1/endpoints/orders.py

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import models
from app import crud, schemas
from app.api import deps

router = APIRouter()

@router.get("/", response_model=List[schemas.ImagingOrderRead])
def read_imaging_orders(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    accession_number: Optional[str] = None,
    # TODO: Add more filters like patient_id, modality, date range etc.
    current_user: models.User = Depends(deps.get_current_active_user), # Secure the endpoint
):
    """
    Retrieve imaging orders.
    """
    # This is a shit implementation of filtering, but it's a start.
    if accession_number:
        order = crud.imaging_order.get_by_accession_number(db, accession_number=accession_number)
        return [order] if order else []
    
    orders = crud.imaging_order.get_multi(db, skip=skip, limit=limit)
    return orders


@router.get("/{order_id}", response_model=schemas.ImagingOrderRead)
def read_imaging_order(
    order_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Get a specific imaging order by its database ID.
    """
    db_order = crud.imaging_order.get(db, id=order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Imaging order not found")
    return db_order