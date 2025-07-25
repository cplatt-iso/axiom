# /app/schemas/mpps.py
from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime
import uuid

from .enums import MppsStatus

# --- Base Schema: Core MPPS attributes ---
class MppsBase(BaseModel):
    sop_instance_uid: str
    status: MppsStatus
    modality: Optional[str] = None
    performed_station_ae_title: Optional[str] = None
    performed_procedure_step_start_datetime: Optional[datetime] = None
    performed_procedure_step_end_datetime: Optional[datetime] = None
    raw_mpps_message: Optional[dict] = None
    imaging_order_id: Optional[int] = None

# --- Create Schema: Data required to create a new MPPS entry ---
class MppsCreate(MppsBase):
    pass

# --- Update Schema: For PATCH operations ---
class MppsUpdate(BaseModel):
    status: Optional[MppsStatus] = None
    performed_procedure_step_end_datetime: Optional[datetime] = None
    raw_mpps_message: Optional[dict] = None
    
    model_config = ConfigDict(extra='ignore')

# --- Read Schema: Data returned from the API (includes system-generated fields) ---
class MppsRead(MppsBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
