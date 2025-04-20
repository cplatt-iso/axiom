# app/schemas/system.py

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class DicomWebSourceStatus(BaseModel):
    source_name: str
    is_enabled: bool
    last_processed_timestamp: Optional[datetime] = None
    last_successful_run: Optional[datetime] = None
    last_error_run: Optional[datetime] = None
    last_error_message: Optional[str] = None

    model_config = { # Pydantic v2 config
        "from_attributes": True
    }

class DicomWebPollersStatusResponse(BaseModel):
    pollers: List[DicomWebSourceStatus]
