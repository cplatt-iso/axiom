# app/schemas/system.py
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any
from datetime import datetime

# --- DICOMweb Poller Schemas ---

class DicomWebSourceStatus(BaseModel):
    """Schema representing the status of a DICOMweb source poller, derived from the DB state."""
    id: int
    created_at: datetime
    updated_at: datetime # This naturally corresponds to the last heartbeat/update time

    source_name: str
    is_enabled: bool
    last_processed_timestamp: Optional[datetime] = None
    last_successful_run: Optional[datetime] = None
    last_error_run: Optional[datetime] = None
    last_error_message: Optional[str] = None
    found_instance_count: int = Field(0, description="Total instances found by QIDO.")
    queued_instance_count: int = Field(0, description="Total instances queued for processing.")
    processed_instance_count: int = Field(0, description="Total instances successfully processed.")

    model_config = {
        "from_attributes": True, # Enable ORM mode (reads attributes matching field names)
        # "populate_by_name": True, # Not needed if names match and no aliases used for input
    }

class DicomWebPollersStatusResponse(BaseModel):
    """Schema for the API response containing all poller statuses."""
    pollers: List[DicomWebSourceStatus] = []

class DimseListenerStatus(BaseModel):
    id: int
    listener_id: str
    status: str
    status_message: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    ae_title: Optional[str] = None
    last_heartbeat: datetime # This likely maps to 'updated_at' from Base model for ListenerState
    created_at: datetime
    received_instance_count: int = Field(0, description="Total instances received.")
    processed_instance_count: int = Field(0, description="Total instances processed.")
    model_config = {"from_attributes": True}


class DimseListenersStatusResponse(BaseModel):
    """Schema for the API response containing all DIMSE listener statuses."""
    listeners: List[DimseListenerStatus] = []

class DimseQrSourceStatus(BaseModel):
    """Schema representing the status of a DIMSE Q/R source poller."""
    id: int
    created_at: datetime
    updated_at: datetime # Maps to last general update time

    name: str
    is_enabled: bool
    last_successful_query: Optional[datetime] = None
    last_successful_move: Optional[datetime] = None
    last_error_time: Optional[datetime] = None
    last_error_message: Optional[str] = None
    # Metrics
    found_study_count: int = Field(0, description="Total studies found by C-FIND.")
    move_queued_study_count: int = Field(0, description="Total studies queued for C-MOVE.")
    processed_instance_count: int = Field(0, description="Total instances processed after C-MOVE.")

    model_config = ConfigDict(from_attributes=True) # Use ConfigDict for Pydantic v2

class DimseQrSourcesStatusResponse(BaseModel):
    """Schema for the API response containing all DIMSE Q/R source statuses."""
    sources: List[DimseQrSourceStatus] = []
