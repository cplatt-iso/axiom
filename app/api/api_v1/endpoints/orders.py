# backend/app/api/api_v1/endpoints/orders.py

from typing import List, Optional
from datetime import date
import asyncio
import json
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.db import models
from app import crud, schemas
from app.api import deps
from app.events import publish_order_event
from aio_pika.abc import AbstractRobustConnection

router = APIRouter()

@router.get("/", response_model=schemas.ImagingOrderReadResponse)
def read_imaging_orders(
    db: Session = Depends(deps.get_db),
    # --- Look at these beautiful, functional parameters! ---
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None,
    modalities: Optional[List[str]] = Query(None, description="List of modalities to filter by."),
    statuses: Optional[List[str]] = Query(None, description="List of order statuses to filter by (e.g., SCHEDULED, IN_PROGRESS, CANCELED). Repeat for multiple."),
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
        statuses=statuses,
        start_date=start_date,
        end_date=end_date,
    )
    return {"items": orders, "total": total_count}


@router.get("/events", summary="Subscribe to real-time order updates")
async def sse_endpoint(
    request: Request,
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Server-Sent Events endpoint to stream order updates.
    """
    from app.events import sse_event_stream # Import locally to avoid circular dependency issues
    return StreamingResponse(sse_event_stream(request), media_type="text/event-stream")


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


@router.post("/", response_model=schemas.ImagingOrderRead, status_code=201)
async def create_imaging_order(
    *,
    db: Session = Depends(deps.get_db),
    order_in: schemas.ImagingOrderCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
    rabbitmq_connection: AbstractRobustConnection = Depends(deps.get_rabbitmq_connection),
):
    """
    Create new imaging order.
    """
    order = crud.imaging_order.create(db, obj_in=order_in)
    # Publish event to RabbitMQ
    await publish_order_event(
        event_type="order_created",
        payload=schemas.ImagingOrderRead.model_validate(order).model_dump(mode='json'),
        connection=rabbitmq_connection
    )
    return order


@router.put("/{order_id}", response_model=schemas.ImagingOrderRead)
async def update_imaging_order(
    *,
    db: Session = Depends(deps.get_db),
    order_id: int,
    order_in: schemas.ImagingOrderUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
    rabbitmq_connection: AbstractRobustConnection = Depends(deps.get_rabbitmq_connection),
):
    """
    Update an imaging order.
    """
    db_order = crud.imaging_order.get(db, id=order_id)
    if not db_order:
        raise HTTPException(status_code=404, detail="Imaging order not found")
    order = crud.imaging_order.update(db, db_obj=db_order, obj_in=order_in)
    # Publish event to RabbitMQ
    await publish_order_event(
        event_type="order_updated",
        payload=schemas.ImagingOrderRead.model_validate(order).model_dump(mode='json'),
        connection=rabbitmq_connection
    )
    return order


@router.delete("/{order_id}", status_code=204)
async def delete_imaging_order(
    *,
    db: Session = Depends(deps.get_db),
    order_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
    rabbitmq_connection: AbstractRobustConnection = Depends(deps.get_rabbitmq_connection),
):
    """
    Delete an imaging order by ID.
    """
    db_order = crud.imaging_order.get(db, id=order_id)
    if not db_order:
        raise HTTPException(status_code=404, detail="Imaging order not found")
    
    # Store order data for the event before deletion
    order_data = schemas.ImagingOrderRead.from_orm(db_order).dict()
    
    # Delete the order
    try:
        crud.imaging_order.remove(db, id=order_id)
    except ValueError as e:
        # Handle database constraints (e.g., foreign key constraints)
        raise HTTPException(
            status_code=400, 
            detail=f"Cannot delete order: {str(e)}"
        )
    
    # Publish event to RabbitMQ
    await publish_order_event(
        event_type="order_deleted",
        payload=order_data,
        connection=rabbitmq_connection
    )
    
    # Return 204 No Content (no response body for successful deletion)