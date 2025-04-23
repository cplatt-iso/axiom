# app/schemas/dimse_listener_config.py
from pydantic import BaseModel, Field, field_validator, ConfigDict, ValidationInfo # <-- Import ValidationInfo
from typing import Optional
from datetime import datetime
import re

AE_TITLE_PATTERN = re.compile(r"^[ A-Za-z0-9._-]{1,16}$")

class DimseListenerConfigBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique, user-friendly name for this listener configuration.")
    description: Optional[str] = Field(None, description="Optional description of the listener's purpose.")
    ae_title: str = Field(..., min_length=1, max_length=16, description="The Application Entity Title the listener will use (1-16 chars, no backslash/control chars).")
    port: int = Field(..., gt=0, lt=65536, description="The network port the listener will bind to (1-65535).")
    is_enabled: bool = Field(True, description="Whether this listener configuration is active and should be started.")
    instance_id: Optional[str] = Field(None, min_length=1, max_length=255, description="Optional: Unique ID matching AXIOM_INSTANCE_ID env var of the listener process.")

    @field_validator('ae_title')
    @classmethod
    def validate_ae_title(cls, value: str, info: ValidationInfo) -> str: # <-- Added 'info' parameter
        # Now 'value' correctly holds the ae_title string
        if not isinstance(value, str): # Basic type check just in case
             raise TypeError("AE Title must be a string")
        v = value.strip() # Trim leading/trailing whitespace
        if not v:
            raise ValueError('AE Title cannot be empty or just whitespace.')
        if not AE_TITLE_PATTERN.match(v):
            raise ValueError('AE Title contains invalid characters or is too long (max 16). Allowed: A-Z a-z 0-9 . _ - SPACE')
        if len(v) > 16:
             raise ValueError('AE Title cannot exceed 16 characters after trimming whitespace.')
        # No need to check strip again, pattern ensures no leading/trailing space remain implicitly
        # if v != v.strip():
        #     raise ValueError('AE Title cannot have leading or trailing whitespace.')
        return v # Return the validated (and stripped) value

    @field_validator('instance_id')
    @classmethod
    def validate_instance_id_format(cls, value: Optional[str], info: ValidationInfo) -> Optional[str]: # <-- Added 'info' parameter
        # Now 'value' correctly holds the instance_id string or None
        if value is not None:
            if not isinstance(value, str): # Basic type check
                 raise TypeError("Instance ID must be a string or null")
            v = value.strip()
            if not v:
                return None # Treat empty string as None
            if not re.match(r"^[a-zA-Z0-9_.-]+$", v):
                 raise ValueError('Instance ID can only contain letters, numbers, underscores, periods, and hyphens.')
            return v # Return the stripped value
        return None


class DimseListenerConfigCreate(DimseListenerConfigBase):
    pass

class DimseListenerConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    ae_title: Optional[str] = Field(None, min_length=1, max_length=16)
    port: Optional[int] = Field(None, gt=0, lt=65535)
    is_enabled: Optional[bool] = None
    instance_id: Optional[str] = Field(None, min_length=1, max_length=255)

    # Re-apply validators for update fields, ensuring correct signature
    _validate_ae_title = field_validator('ae_title', mode='before')( # Use mode='before' for optional fields
        lambda v, info: DimseListenerConfigBase.validate_ae_title(v, info) if v is not None else None
    )
    _validate_instance_id = field_validator('instance_id', mode='before')( # Use mode='before' for optional fields
        lambda v, info: DimseListenerConfigBase.validate_instance_id_format(v, info) if v is not None else None
    )


class DimseListenerConfigRead(DimseListenerConfigBase):
    id: int
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)
