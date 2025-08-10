# app/schemas/modality.py
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, TYPE_CHECKING
from datetime import datetime
import re
import ipaddress

if TYPE_CHECKING:
    from .facility import Facility

# DICOM AE Title validation pattern
AE_TITLE_PATTERN = re.compile(r"^[ A-Za-z0-9._-]{1,16}$")

def _validate_ae_title(v: str) -> str:
    """Validate DICOM AE Title format."""
    v_stripped = v.strip()
    if not v_stripped:
        raise ValueError('AE Title cannot be empty')
    if not AE_TITLE_PATTERN.match(v_stripped):
        raise ValueError('AE Title contains invalid characters or is too long (max 16 chars)')
    if v != v_stripped:
        raise ValueError('AE Title cannot have leading or trailing whitespace')
    return v_stripped

# Valid DICOM modality types (commonly used ones)
VALID_MODALITY_TYPES = [
    'CR', 'CT', 'DX', 'ES', 'MG', 'MR', 'NM', 'OT', 'PT', 'RF', 'RG', 'SC', 'US', 'XA',
    'DR', 'IO', 'PX', 'GM', 'SM', 'XC', 'OP', 'AR', 'BDUS', 'EPS', 'HD', 'IVOCT', 'IVUS',
    'KER', 'LEN', 'LS', 'OAM', 'OCT', 'OPM', 'OPT', 'OPV', 'OSS', 'PLAN', 'POS', 'PR',
    'REG', 'RESP', 'RWV', 'SEG', 'SRF', 'TG', 'VA', 'XCV'
]

class ModalityBase(BaseModel):
    """Base schema for Modality with common fields."""
    name: str = Field(..., min_length=1, max_length=255, description="Descriptive name of the modality.")
    description: Optional[str] = Field(None, description="Optional description of the modality.")
    
    # DICOM Network Configuration
    ae_title: str = Field(..., min_length=1, max_length=16, description="DICOM Application Entity Title for this modality.")
    ip_address: str = Field(..., description="IP address of the modality (supports IPv4 and IPv6).")
    port: Optional[int] = Field(104, ge=1, le=65535, description="DICOM port number (default 104).")
    
    # Modality Type
    modality_type: str = Field(..., min_length=1, max_length=16, description="DICOM modality type (CT, MR, DR, CR, US, etc.).")
    
    # Security and Access Control
    is_active: bool = Field(True, description="Whether this modality is currently active.")
    is_dmwl_enabled: bool = Field(True, description="Whether this modality is allowed to query the modality worklist.")
    
    # Facility Relationship
    facility_id: int = Field(..., description="ID of the facility this modality belongs to.")
    
    # Equipment Details
    manufacturer: Optional[str] = Field(None, max_length=255, description="Equipment manufacturer.")
    model: Optional[str] = Field(None, max_length=255, description="Equipment model name/number.")
    software_version: Optional[str] = Field(None, max_length=100, description="Software version of the modality.")
    station_name: Optional[str] = Field(None, max_length=16, description="DICOM Station Name.")
    
    # Access Control Settings
    department: Optional[str] = Field(None, max_length=100, description="Department this modality belongs to.")
    location: Optional[str] = Field(None, max_length=255, description="Physical location of the modality.")

    @field_validator('ae_title')
    @classmethod
    def validate_ae_title(cls, v: str) -> str:
        return _validate_ae_title(v)

    @field_validator('ip_address')
    @classmethod
    def validate_ip_address(cls, v: str) -> str:
        try:
            ipaddress.ip_address(v.strip())
            return v.strip()
        except ValueError:
            raise ValueError('Invalid IP address format')

    @field_validator('modality_type')
    @classmethod
    def validate_modality_type(cls, v: str) -> str:
        v_upper = v.upper().strip()
        if v_upper not in VALID_MODALITY_TYPES:
            raise ValueError(f'Invalid modality type. Must be one of: {", ".join(VALID_MODALITY_TYPES)}')
        return v_upper

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError('Modality name cannot be empty')
        return v

class ModalityCreate(ModalityBase):
    """Schema for creating a new modality."""
    pass

class ModalityUpdate(BaseModel):
    """Schema for updating an existing modality."""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    ae_title: Optional[str] = Field(None, min_length=1, max_length=16)
    ip_address: Optional[str] = None
    port: Optional[int] = Field(None, ge=1, le=65535)
    modality_type: Optional[str] = Field(None, min_length=1, max_length=16)
    is_active: Optional[bool] = None
    is_dmwl_enabled: Optional[bool] = None
    facility_id: Optional[int] = None
    manufacturer: Optional[str] = Field(None, max_length=255)
    model: Optional[str] = Field(None, max_length=255)
    software_version: Optional[str] = Field(None, max_length=100)
    station_name: Optional[str] = Field(None, max_length=16)
    department: Optional[str] = Field(None, max_length=100)
    location: Optional[str] = Field(None, max_length=255)

    @field_validator('ae_title')
    @classmethod
    def validate_ae_title(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            return _validate_ae_title(v)
        return v

    @field_validator('ip_address')
    @classmethod
    def validate_ip_address(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                ipaddress.ip_address(v.strip())
                return v.strip()
            except ValueError:
                raise ValueError('Invalid IP address format')
        return v

    @field_validator('modality_type')
    @classmethod
    def validate_modality_type(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v_upper = v.upper().strip()
            if v_upper not in VALID_MODALITY_TYPES:
                raise ValueError(f'Invalid modality type. Must be one of: {", ".join(VALID_MODALITY_TYPES)}')
            return v_upper
        return v

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = v.strip()
            if not v:
                raise ValueError('Modality name cannot be empty')
        return v

class ModalityInDBBase(ModalityBase):
    """Base schema for Modality data from database."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

class Modality(ModalityInDBBase):
    """Schema for Modality responses (without facility)."""
    pass

# Note: ModalityWithFacility removed to avoid forward reference issues
# Use separate API calls to get facility information when needed
