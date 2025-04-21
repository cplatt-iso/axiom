# app/schemas/system.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

# --- DICOMweb Poller Schemas ---

class DicomWebSourceStatus(BaseModel):
    """Schema representing the status of a DICOMweb source poller, derived from the DB state."""
    # Inherited from Base via DicomWebSourceState
    id: int
    created_at: datetime
    # Use the actual model attribute name 'updated_at'
    updated_at: datetime # This naturally corresponds to the last heartbeat/update time

    # Direct mapping from DicomWebSourceState - Names match model attributes
    source_name: str
    is_enabled: bool
    last_processed_timestamp: Optional[datetime] = None
    # Use the actual model attribute name 'last_successful_run'
    last_successful_run: Optional[datetime] = None
    last_error_run: Optional[datetime] = None
    # Use the actual model attribute name 'last_error_message'
    last_error_message: Optional[str] = None

    model_config = {
        "from_attributes": True, # Enable ORM mode (reads attributes matching field names)
        # "populate_by_name": True, # Not needed if names match and no aliases used for input
    }

class DicomWebPollersStatusResponse(BaseModel):
    """Schema for the API response containing all poller statuses."""
    # This list will contain DicomWebSourceStatus objects populated from the DB model
    pollers: List[DicomWebSourceStatus] = []


# --- Listener Status Schemas ---
# (Keep DimseListenerStatus as is)
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
    model_config = {"from_attributes": True}
    # Note: If ListenerState model uses 'updated_at' but you want 'last_heartbeat' in JSON,
    # you *would* use an alias here, but only if the frontend expects 'last_heartbeat'.
    # updated_at: datetime = Field(..., alias="last_heartbeat") # Example if frontend needed alias

class DimseListenersStatusResponse(BaseModel):
    """Schema for the API response containing all DIMSE listener statuses."""
    listeners: List[DimseListenerStatus] = []
