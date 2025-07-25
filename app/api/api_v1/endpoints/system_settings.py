from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from app import crud, schemas
from app.api import deps
from app.db import models

router = APIRouter()

@router.get("/", response_model=List[schemas.SystemSettingRead])
def read_system_settings(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    current_user: models.User = Depends(deps.get_current_active_superuser),
):
    """
    Retrieve system settings.
    """
    settings = crud.system_setting.get_multi(db, skip=skip, limit=limit)
    return settings

@router.put("/{key}", response_model=schemas.SystemSettingRead)
def update_system_setting(
    *,
    db: Session = Depends(deps.get_db),
    key: str,
    setting_in: schemas.SystemSettingUpdate,
    current_user: models.User = Depends(deps.get_current_active_superuser),
):
    """
    Update a system setting.
    """
    db_setting = crud.system_setting.get_by_key(db, key=key)
    if not db_setting:
        raise HTTPException(status_code=404, detail="Setting not found")
    setting = crud.system_setting.update(db, db_obj=db_setting, obj_in=setting_in)
    return setting
