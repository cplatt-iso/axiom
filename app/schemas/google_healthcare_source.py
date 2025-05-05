# backend/app/schemas/google_healthcare_source.py
from pydantic import BaseModel, Field, Json, field_validator, model_validator
from typing import Optional, Dict, Any

# Shared properties
class GoogleHealthcareSourceBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255, description="Unique name for the Google Healthcare DICOM Store source.")
    description: Optional[str] = Field(None, max_length=512, description="Optional description.")
    gcp_project_id: str = Field(..., min_length=1, description="Google Cloud Project ID.")
    gcp_location: str = Field(..., min_length=1, description="Google Cloud Location (e.g., 'us-central1').")
    gcp_dataset_id: str = Field(..., min_length=1, description="Google Healthcare Dataset ID.")
    gcp_dicom_store_id: str = Field(..., min_length=1, description="Google Healthcare DICOM Store ID.")
    polling_interval_seconds: int = Field(default=300, gt=0, description="How often to poll for new studies (in seconds).")
    query_filters: Optional[Dict[str, Any]] = Field(None, description="Optional key-value pairs for filtering queries (e.g., '{\"StudyDate\": \"-1d\"}'). Interpreted by the poller/querier.")
    is_enabled: bool = Field(default=True, description="Whether this source is generally usable (e.g., in Data Browser, Rules).")
    is_active: bool = Field(default=True, description="Whether the automatic poller should query this source.")

    @field_validator('query_filters', mode='before')
    @classmethod
    def empty_str_to_none(cls, v):
        if isinstance(v, str) and v.strip() == "":
            return None
        # Let Pydantic handle JSON parsing if it's a string, or pass through if already dict/None
        return v

    @model_validator(mode='after')
    def check_active_requires_enabled(self):
        if self.is_active and not self.is_enabled:
            raise ValueError("Source cannot be active if it is not enabled.")
        return self

# Properties to receive via API on creation
class GoogleHealthcareSourceCreate(GoogleHealthcareSourceBase):
    pass # All base fields are needed for creation

# Properties to receive via API on update
class GoogleHealthcareSourceUpdate(GoogleHealthcareSourceBase):
    # Allow partial updates by making fields optional, but keep identifier logic
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=512)
    gcp_project_id: Optional[str] = Field(None, min_length=1)
    gcp_location: Optional[str] = Field(None, min_length=1)
    gcp_dataset_id: Optional[str] = Field(None, min_length=1)
    gcp_dicom_store_id: Optional[str] = Field(None, min_length=1)
    polling_interval_seconds: Optional[int] = Field(None, gt=0)
    query_filters: Optional[Dict[str, Any]] = Field(None) # Allow setting to null or new dict
    is_enabled: Optional[bool] = None
    is_active: Optional[bool] = None

    # Need model validator again for update case
    @model_validator(mode='after')
    def check_active_requires_enabled_update(self):
        # Need to consider existing state if not provided in update
        # This validation might be better handled in CRUD/API layer where existing obj is known
        # For now, just validate if both are provided in the update payload
        is_active = self.is_active
        is_enabled = self.is_enabled
        if is_active is not None and is_enabled is not None:
             if is_active and not is_enabled:
                  raise ValueError("Source cannot be active if it is not enabled.")
        # If only one is provided, the check needs the current DB state, handle in CRUD/API.
        return self


# Properties shared by models stored in DB
class GoogleHealthcareSourceInDBBase(GoogleHealthcareSourceBase):
    id: int

    class Config:
        from_attributes = True # Pydantic V2 compatibility with SQLAlchemy models

# Properties to return to client
class GoogleHealthcareSourceRead(GoogleHealthcareSourceInDBBase):
    pass
