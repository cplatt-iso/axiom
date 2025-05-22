# backend/app/schemas/google_healthcare_source.py
from pydantic import BaseModel, Field, Json, field_validator, model_validator, ConfigDict # MODIFIED: Added ConfigDict
from typing import Optional, Dict, Any
from datetime import datetime

# Shared properties
class GoogleHealthcareSourceBase(BaseModel):
    # 'name' is fundamental, but make Optional in Base to allow Update to omit it. Create will require it.
    name: Optional[str] = Field(None, min_length=1, max_length=255, description="Unique name for the Google Healthcare DICOM Store source.") # MODIFIED
    description: Optional[str] = Field(None, max_length=512, description="Optional description.")
    
    # Fields that can be optional in Update should be Optional in Base.
    # Defaults can still be provided.
    gcp_project_id: Optional[str] = Field(None, min_length=1, description="Google Cloud Project ID.")
    gcp_location: Optional[str] = Field(None, min_length=1, description="Google Cloud Location (e.g., 'us-central1').")
    gcp_dataset_id: Optional[str] = Field(None, min_length=1, description="Google Healthcare Dataset ID.")
    gcp_dicom_store_id: Optional[str] = Field(None, min_length=1, description="Google Healthcare DICOM Store ID.")
    polling_interval_seconds: Optional[int] = Field(default=300, gt=0, description="How often to poll for new studies (in seconds).")
    query_filters: Optional[Dict[str, Any]] = Field(None, description="Optional key-value pairs for filtering queries (e.g., '{\"StudyDate\": \"-1d\"}'). Interpreted by the poller/querier.")
    is_enabled: Optional[bool] = Field(default=True, description="Whether this source is generally usable (e.g., in Data Browser, Rules).")
    is_active: Optional[bool] = Field(default=True, description="Whether the automatic poller should query this source.")

    @field_validator('query_filters', mode='before')
    @classmethod
    def empty_str_to_none(cls, v):
        if isinstance(v, str) and v.strip() == "":
            return None
        # Let Pydantic handle JSON parsing if it's a string, or pass through if already dict/None
        return v

    @model_validator(mode='after')
    def check_active_requires_enabled(self):
        # Adjust validator to handle Optional bools
        if self.is_active is True and self.is_enabled is False:
            raise ValueError("Source cannot be active if it is not enabled.")
        return self

# Properties to receive via API on creation
class GoogleHealthcareSourceCreate(GoogleHealthcareSourceBase):
    # Explicitly require fields that were made Optional in Base but are mandatory for creation.
    # Pylance may flag these as incompatible overrides due to strict invariance.
    # This is a common Pydantic pattern.
    name: str = Field(..., min_length=1, max_length=255, description="Unique name for the Google Healthcare DICOM Store source.") # type: ignore
    gcp_project_id: str = Field(..., min_length=1, description="Google Cloud Project ID.") # type: ignore
    gcp_location: str = Field(..., min_length=1, description="Google Cloud Location (e.g., 'us-central1').") # type: ignore
    gcp_dataset_id: str = Field(..., min_length=1, description="Google Healthcare Dataset ID.") # type: ignore
    gcp_dicom_store_id: str = Field(..., min_length=1, description="Google Healthcare DICOM Store ID.") # type: ignore
    
    # polling_interval_seconds has a default in Base, but ensure it's an int for Create.
    polling_interval_seconds: int = Field(default=300, gt=0) # type: ignore
    # is_enabled and is_active have defaults in Base, but ensure they are bools for Create.
    is_enabled: bool = Field(default=True) # type: ignore
    is_active: bool = Field(default=True) # type: ignore
    # The model_validator from Base will apply and work correctly due to non-Optional types here.

# Properties to receive via API on update
class GoogleHealthcareSourceUpdate(GoogleHealthcareSourceBase):
    # Allow partial updates by making fields optional.
    # These are now compatible with the Optional types in the modified GoogleHealthcareSourceBase.
    name: Optional[str] = Field(None, min_length=1, max_length=255) 
    description: Optional[str] = Field(None, max_length=512)
    gcp_project_id: Optional[str] = Field(None, min_length=1)
    gcp_location: Optional[str] = Field(None, min_length=1)
    gcp_dataset_id: Optional[str] = Field(None, min_length=1)
    gcp_dicom_store_id: Optional[str] = Field(None, min_length=1)
    polling_interval_seconds: Optional[int] = Field(default=None, gt=0) # MODIFIED: explicit default=None
    query_filters: Optional[Dict[str, Any]] = Field(None) 
    is_enabled: Optional[bool] = Field(default=None) # Use Field(default=None) for explicit optionality
    is_active: Optional[bool] = Field(default=None)  # Use Field(default=None) for explicit optionality

    # Need model validator again for update case
    @model_validator(mode='after')
    def check_active_requires_enabled_update(self) -> 'GoogleHealthcareSourceUpdate': # MODIFIED: Added return type and will add return self
        # This validation needs to be more careful for updates, as fields might not be present.
        # It should ideally consider the existing DB state if a field is not in the update payload.
        # For simplicity here, we'll validate if both are present in the update.
        # A more robust validation would be in the CRUD update method, accessing db_obj.
        
        # Determine effective values (current or updated)
        # This is tricky without the original object. For a pure Pydantic model,
        # we only know what's passed in the update.
        # If is_active is being set to True, and is_enabled is explicitly False in this update, or not set (and was False)
        # This logic is better in CRUD where db_obj is available.
        # For now, a simple check if both are provided in the update:
        if self.is_active is True and self.is_enabled is False:
             raise ValueError("Source cannot be active if it is not enabled (based on update values).")
        return self # MODIFIED: Added return self

# Properties to return to client (includes DB-generated fields)
class GoogleHealthcareSourceRead(GoogleHealthcareSourceBase):
    id: int
    # Inherits all fields from GoogleHealthcareSourceBase
    # Ensure that the fields in GoogleHealthcareSourceBase are appropriate for a read model
    # (e.g., sensitive data might be handled differently or excluded if necessary)
    
    # Add any additional fields that are part of the DB model and should be returned,
    # but are not in GoogleHealthcareSourceBase. For example:
    created_at: datetime
    updated_at: datetime
    
    # If there are state fields specific to the GoogleHealthcareSource model in the DB
    # that should be exposed on read, add them here. For example:
    # last_successful_poll: Optional[datetime] = None
    # last_error_message: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
