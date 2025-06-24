# backend/app/api/api_v1/endpoints/orders.py

from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db import models
from app import crud, schemas
from app.api import deps

router = APIRouter()

@router.get("/", response_model=schemas.ImagingOrderReadResponse)
def read_imaging_orders(
    db: Session = Depends(deps.get_db),
    # --- Look at these beautiful, functional parameters! ---
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None,
    modalities: Optional[List[str]] = Query(None, description="List of modalities to filter by."),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Retrieve imaging orders with robust filtering, searching, and pagination.
    This is the primary endpoint for the Orders/Worklist UI.
    """
    orders, total_count = crud.imaging_order.get_orders_paginated(
        db,
        skip=skip,
        limit=limit,
        search=search,
        modalities=modalities,
        start_date=start_date,
        end_date=end_date,
    )
    
    return {"items": orders, "total": total_count}


@router.get("/{order_id}", response_model=schemas.ImagingOrderRead)
def read_imaging_order(
    order_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Get a specific imaging order by its database ID.
    (This was already fine, so I'm leaving it the fuck alone.)
    """
    db_order = crud.imaging_order.get(db, id=order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Imaging order not found")
    return db_order