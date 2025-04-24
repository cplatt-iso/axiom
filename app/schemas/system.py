# app/schemas/system.py
from pydantic import BaseModel, Field, ConfigDict # Import ConfigDict
from typing import List, Optional, Dict, Any
from datetime import datetime

# --- DICOMweb Poller Schemas ---
# ... (DicomWebSourceStatus and DicomWebPollersStatusResponse remain the same) ...
class DicomWebSourceStatus(BaseModel):
    """Schema representing the status of a DICOMweb source poller, derived from the DB state."""
    id: int
    created_at: datetime
    updated_at: datetime # Maps to last heartbeat/update time

    source_name: str
    is_enabled: bool
    last_processed_timestamp: Optional[datetime] = None
    last_successful_run: Optional[datetime] = None
    last_error_run: Optional[datetime] = None
    last_error_message: Optional[str] = None
    # --- ADDED Metrics ---
    found_instance_count: int = Field(0, description="Total instances found by QIDO.")
    queued_instance_count: int = Field(0, description="Total instances queued for processing.")
    processed_instance_count: int = Field(0, description="Total instances successfully processed.")
    # --- END ADDED ---

    model_config = ConfigDict(from_attributes=True) # Use ConfigDict for Pydantic v2

class DicomWebPollersStatusResponse(BaseModel):
    """Schema for the API response containing all poller statuses."""
    pollers: List[DicomWebSourceStatus] = []


# --- Listener Status Schemas ---
# ... (DimseListenerStatus and DimseListenersStatusResponse remain the same) ...
class DimseListenerStatus(BaseModel):
    """Schema representing the status of a DIMSE listener instance."""
    id: int
    listener_id: str
    status: str
    status_message: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    ae_title: Optional[str] = None
    last_heartbeat: datetime # Maps to updated_at
    created_at: datetime
    # --- ADDED Metrics ---
    received_instance_count: int = Field(0, description="Total instances received.")
    processed_instance_count: int = Field(0, description="Total instances processed.")
    # --- END ADDED ---

    model_config = ConfigDict(from_attributes=True) # Use ConfigDict for Pydantic v2

class DimseListenersStatusResponse(BaseModel):
    """Schema for the API response containing all DIMSE listener statuses."""
    listeners: List[DimseListenerStatus] = []

# --- DIMSE Q/R Source Status Schemas ---
class DimseQrSourceStatus(BaseModel):
    """Schema representing the status of a DIMSE Q/R source poller."""
    id: int
    created_at: datetime
    updated_at: datetime # Maps to last general update time

    name: str
    is_enabled: bool
    # --- ADDED Remote Peer Details ---
    remote_ae_title: str = Field(..., description="AE Title of the remote peer.")
    remote_host: str = Field(..., description="Hostname or IP address of the remote peer.")
    remote_port: int = Field(..., description="Network port of the remote peer.")
    # --- END ADDED ---
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
# --- END DIMSE Q/R Source Status Schemas ---
