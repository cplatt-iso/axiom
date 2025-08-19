# app/schemas/system.py
from pydantic import BaseModel, Field, ConfigDict # Import ConfigDict
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path

from app.schemas.enums import HealthStatus

class DirectoryUsageStats(BaseModel):
    path: str = Field(..., description="The directory path checked.")
    content_bytes: int = Field(..., description="Total size of files within this directory in bytes.")

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
    # --- ADDED Health Status ---
    health_status: HealthStatus = Field(HealthStatus.UNKNOWN, description="Connection health status")
    last_health_check: Optional[datetime] = Field(None, description="Last health check timestamp")
    last_health_error: Optional[str] = Field(None, description="Last health check error message")
    # --- END ADDED ---

    model_config = ConfigDict(from_attributes=True) # Use ConfigDict for Pydantic v2

class DicomWebPollersStatusResponse(BaseModel):
    """Schema for the API response containing all poller statuses."""
    pollers: List[DicomWebSourceStatus] = []


# --- Listener Status Schemas ---
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

class DimseListenerFullStatus(BaseModel):
    """Schema representing the full status of a DIMSE listener including configuration."""
    # Configuration fields
    config_id: int = Field(..., description="Configuration ID")
    name: str = Field(..., description="Listener configuration name")
    description: Optional[str] = Field(None, description="Configuration description")
    listener_type: str = Field(..., description="Type of listener (pynetdicom, dcm4che)")
    ae_title: str = Field(..., description="Application Entity Title")
    port: int = Field(..., description="Network port")
    is_enabled: bool = Field(..., description="Whether the configuration is enabled")
    instance_id: Optional[str] = Field(None, description="Instance ID for runtime identification")
    tls_enabled: bool = Field(False, description="Whether TLS is enabled")
    
    # Runtime status fields (optional if listener is not reporting)
    runtime_status: Optional[str] = Field(None, description="Current runtime status")
    status_message: Optional[str] = Field(None, description="Status message")
    runtime_host: Optional[str] = Field(None, description="Runtime host")
    last_heartbeat: Optional[datetime] = Field(None, description="Last heartbeat timestamp")
    received_instance_count: int = Field(0, description="Total instances received")
    processed_instance_count: int = Field(0, description="Total instances processed")
    
    model_config = ConfigDict(from_attributes=True)

class DimseListenersStatusResponse(BaseModel):
    """Schema for the API response containing all DIMSE listener statuses."""
    listeners: List[DimseListenerStatus] = []

class DimseListenersFullStatusResponse(BaseModel):
    """Schema for the API response containing all DIMSE listener configurations with status."""
    listeners: List[DimseListenerFullStatus] = []

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
    # --- ADDED Health Status ---
    health_status: HealthStatus = Field(HealthStatus.UNKNOWN, description="Connection health status")
    last_health_check: Optional[datetime] = Field(None, description="Last health check timestamp")
    last_health_error: Optional[str] = Field(None, description="Last health check error message")
    # --- END ADDED ---

    model_config = ConfigDict(from_attributes=True) # Use ConfigDict for Pydantic v2

class DimseQrSourcesStatusResponse(BaseModel):
    """Schema for the API response containing all DIMSE Q/R source statuses."""
    sources: List[DimseQrSourceStatus] = []
# --- END DIMSE Q/R Source Status Schemas ---

# --- Google Healthcare Source Status Schemas ---
class GoogleHealthcareSourceStatus(BaseModel):
    """Schema representing the status of a Google Healthcare source."""
    id: int
    created_at: datetime
    updated_at: datetime

    name: str
    is_enabled: bool
    gcp_project_id: str = Field(..., description="Google Cloud project ID")
    gcp_dataset_id: str = Field(..., description="Healthcare dataset ID")
    gcp_dicom_store_id: str = Field(..., description="DICOM store ID")
    gcp_location: str = Field(..., description="Google Cloud location/region")
    # --- Health Status ---
    health_status: HealthStatus = Field(HealthStatus.UNKNOWN, description="Connection health status")
    last_health_check: Optional[datetime] = Field(None, description="Last health check timestamp")
    last_health_error: Optional[str] = Field(None, description="Last health check error message")
    # --- END Health Status ---

    model_config = ConfigDict(from_attributes=True)

class GoogleHealthcareSourcesStatusResponse(BaseModel):
    """Schema for the API response containing all Google Healthcare source statuses."""
    sources: List[GoogleHealthcareSourceStatus] = []
# --- END Google Healthcare Source Status Schemas ---

class DiskUsageStats(BaseModel):
    # Keep overall filesystem stats for context
    filesystem_total_bytes: int = Field(..., description="Total disk space on the underlying filesystem in bytes.")
    filesystem_free_bytes: int = Field(..., description="Free disk space on the underlying filesystem in bytes.")
    # List of stats for specific relevant directories
    directories: List[DirectoryUsageStats] = Field(..., description="Usage statistics for specific monitored directories.")

class SystemInfo(BaseModel):
    project_name: str
    project_version: str
    environment: str
    debug_mode: bool
    log_original_attributes: bool
    delete_on_success: bool
    delete_unmatched_files: bool
    delete_on_no_destination: bool
    move_to_error_on_partial_failure: bool
    dicom_storage_path: str
    dicom_error_path: str
    filesystem_storage_path: str
    temp_dir: Optional[str] = None # TEMP_DIR might be None
    openai_configured: bool # Just indicate if key is set

    model_config = ConfigDict(from_attributes=True)
