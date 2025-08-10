# app/schemas/facility.py
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, List
from datetime import datetime

class FacilityBase(BaseModel):
    """Base schema for Facility with common fields."""
    name: str = Field(..., min_length=1, max_length=255, description="Unique name of the healthcare facility.")
    description: Optional[str] = Field(None, description="Optional description of the facility.")
    
    # Address Information
    address_line_1: Optional[str] = Field(None, max_length=255, description="Primary address line (street number and name).")
    address_line_2: Optional[str] = Field(None, max_length=255, description="Secondary address line (apartment, suite, building, etc.).")
    city: Optional[str] = Field(None, max_length=100, description="City name.")
    state: Optional[str] = Field(None, max_length=100, description="State or province.")
    postal_code: Optional[str] = Field(None, max_length=20, description="Postal or ZIP code.")
    country: Optional[str] = Field(None, max_length=100, description="Country name.")
    
    # Contact Information
    phone: Optional[str] = Field(None, max_length=50, description="Primary phone number for the facility.")
    email: Optional[str] = Field(None, max_length=255, description="Primary email contact for the facility.")
    
    # Administrative
    is_active: bool = Field(True, description="Whether this facility is currently active.")
    facility_id: Optional[str] = Field(None, max_length=50, description="External facility identifier (e.g., from HIS/RIS).")

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = v.strip()
            if not v:
                return None
            # Basic email validation
            if '@' not in v or '.' not in v.split('@')[-1]:
                raise ValueError('Invalid email format')
        return v

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError('Facility name cannot be empty')
        return v

class FacilityCreate(FacilityBase):
    """Schema for creating a new facility."""
    pass

class FacilityUpdate(BaseModel):
    """Schema for updating an existing facility."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    address_line_1: Optional[str] = Field(None, max_length=255)
    address_line_2: Optional[str] = Field(None, max_length=255)
    city: Optional[str] = Field(None, max_length=100)
    state: Optional[str] = Field(None, max_length=100)
    postal_code: Optional[str] = Field(None, max_length=20)
    country: Optional[str] = Field(None, max_length=100)
    phone: Optional[str] = Field(None, max_length=50)
    email: Optional[str] = Field(None, max_length=255)
    is_active: Optional[bool] = None
    facility_id: Optional[str] = Field(None, max_length=50)

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = v.strip()
            if not v:
                return None
            if '@' not in v or '.' not in v.split('@')[-1]:
                raise ValueError('Invalid email format')
        return v

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = v.strip()
            if not v:
                raise ValueError('Facility name cannot be empty')
        return v

class FacilityInDBBase(FacilityBase):
    """Base schema for Facility data from database."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

class Facility(FacilityInDBBase):
    """Schema for Facility responses (without modalities)."""
    pass

# Note: FacilityWithModalities removed to avoid forward reference issues
# Use the FacilityModalityInfo schema and separate API calls instead

class FacilityModalityInfo(BaseModel):
    """Basic modality information for facility endpoints."""
    id: int
    name: str
    ae_title: str
    ip_address: str
    port: int
    modality_type: str
    department: Optional[str] = None
    is_active: bool
    is_dmwl_enabled: bool

    model_config = ConfigDict(from_attributes=True)
