# app/api/api_v1/endpoints/senders.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Any

from app.crud.crud_sender_config import crud_sender_config
from app.schemas import sender_config as sender_schemas
from app.api import deps

router = APIRouter()

@router.post("/", response_model=sender_schemas.SenderConfigRead)
def create_sender_config(
    *,
    db: Session = Depends(deps.get_db),
    sender_in: sender_schemas.SenderConfigCreate,
    current_user: Any = Depends(deps.get_current_active_user),
) -> Any:
    """
    Create a new sender configuration.
    """
    sender = crud_sender_config.create(db, obj_in=sender_in)
    return sender

@router.get("/", response_model=List[sender_schemas.SenderConfigRead])
def read_senders(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    current_user: Any = Depends(deps.get_current_active_user),
) -> Any:
    """
    Retrieve sender configurations.
    """
    senders = crud_sender_config.get_multi(db, skip=skip, limit=limit)
    return senders

@router.get("/{id}", response_model=sender_schemas.SenderConfigRead)
def read_sender(
    *,
    db: Session = Depends(deps.get_db),
    id: int,
    current_user: Any = Depends(deps.get_current_active_user),
) -> Any:
    """
    Get sender configuration by ID.
    """
    sender = crud_sender_config.get(db, id=id)
    if not sender:
        raise HTTPException(status_code=404, detail="Sender not found")
    return sender

@router.put("/{id}", response_model=sender_schemas.SenderConfigRead)
def update_sender(
    *,
    db: Session = Depends(deps.get_db),
    id: int,
    sender_in: sender_schemas.SenderConfigUpdate,
    current_user: Any = Depends(deps.get_current_active_user),
) -> Any:
    """
    Update a sender configuration.
    """
    sender = crud_sender_config.get(db, id=id)
    if not sender:
        raise HTTPException(status_code=404, detail="Sender not found")
    sender = crud_sender_config.update(db, db_obj=sender, obj_in=sender_in)
    return sender

@router.delete("/{id}")
def delete_sender(
    *,
    db: Session = Depends(deps.get_db),
    id: int,
    current_user: Any = Depends(deps.get_current_active_user),
) -> Any:
    """
    Delete a sender configuration.
    """
    sender = crud_sender_config.get(db, id=id)
    if not sender:
        raise HTTPException(status_code=404, detail="Sender not found")
    crud_sender_config.remove(db, id=id)
    return {"message": "Sender deleted successfully"}
