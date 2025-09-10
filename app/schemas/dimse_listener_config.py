# app/schemas/dimse_listener_config.py
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator, # <-- Import model_validator
    ConfigDict,
    ValidationInfo
)
from typing import Optional, Any # <-- Added Any
from datetime import datetime
import re

from app.schemas.enums import DicomImplementationType

AE_TITLE_PATTERN = re.compile(r"^[ A-Za-z0-9._-]{1,16}$")

class DimseListenerConfigBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique, user-friendly name for this listener configuration.")
    description: Optional[str] = Field(None, description="Optional description of the listener's purpose.")
    ae_title: str = Field(..., min_length=1, max_length=16, description="The Application Entity Title the listener will use (1-16 chars, no backslash/control chars).")
    port: int = Field(..., gt=0, lt=65536, description="The network port the listener will bind to (1-65535).")
    is_enabled: bool = Field(True, description="Whether this listener configuration is active and should be started.")
    instance_id: Optional[str] = Field(None, min_length=1, max_length=255, description="Optional: Unique ID matching AXIOM_INSTANCE_ID env var of the listener process.")

    # --- TLS Configuration Fields ---
    tls_enabled: bool = Field(False, description="Enable TLS for incoming connections.")
    tls_cert_secret_name: Optional[str] = Field(None, description="Secret Manager resource name for the listener's server certificate (PEM). REQUIRED if TLS enabled.")
    tls_key_secret_name: Optional[str] = Field(None, description="Secret Manager resource name for the listener's private key (PEM). REQUIRED if TLS enabled.")
    tls_ca_cert_secret_name: Optional[str] = Field(None, description="Optional: Secret Manager resource name for the CA certificate (PEM) to verify client certificates (for mTLS).")
    # --- End TLS Fields ---

    @field_validator('ae_title')
    @classmethod
    def validate_ae_title(cls, value: str, info: ValidationInfo) -> str:
        if not isinstance(value, str):
             raise TypeError("AE Title must be a string")
        v = value.strip()
        if not v:
            raise ValueError('AE Title cannot be empty or just whitespace.')
        if not AE_TITLE_PATTERN.match(v):
            raise ValueError('AE Title contains invalid characters or is too long (max 16). Allowed: A-Z a-z 0-9 . _ - SPACE')
        # Simplified length check
        # if len(v) > 16:
        #      raise ValueError('AE Title cannot exceed 16 characters after trimming whitespace.')
        return v

    @field_validator('instance_id')
    @classmethod
    def validate_instance_id_format(cls, value: Optional[str], info: ValidationInfo) -> Optional[str]:
        if value is not None:
            if not isinstance(value, str):
                 raise TypeError("Instance ID must be a string or null")
            v = value.strip()
            if not v:
                return None
            if not re.match(r"^[a-zA-Z0-9_.-]+$", v):
                 raise ValueError('Instance ID can only contain letters, numbers, underscores, periods, and hyphens.')
            return v
        return None

    # --- Model Validator for TLS fields ---
    @model_validator(mode='after') # Check fields after individual validation
    def check_tls_secrets_if_enabled(self) -> 'DimseListenerConfigBase':
        # Check 'self' which holds the validated model instance
        if self.tls_enabled:
            if not self.tls_cert_secret_name:
                raise ValueError("tls_cert_secret_name is required when tls_enabled is True")
            if not self.tls_key_secret_name:
                raise ValueError("tls_key_secret_name is required when tls_enabled is True")
        # No need to raise error if False, but ensure secrets aren't mandatory then
        return self
    # --- End Model Validator ---


class DimseListenerConfigCreate(DimseListenerConfigBase):
    # Inherits TLS fields and validation from Base
    listener_type: DicomImplementationType = Field(DicomImplementationType.PYNETDICOM, description="The type of listener implementation.")

    @field_validator('listener_type')
    @classmethod
    def validate_listener_type(cls, value: DicomImplementationType) -> DicomImplementationType:
        # Enum validation is automatic, but we can add extra logic here if needed
        return value


class DimseListenerConfigUpdate(BaseModel):
    # All fields are optional for update
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    ae_title: Optional[str] = Field(None, min_length=1, max_length=16)
    port: Optional[int] = Field(None, gt=0, lt=65535)
    is_enabled: Optional[bool] = None
    instance_id: Optional[str] = Field(None, min_length=1, max_length=255)
    listener_type: Optional[DicomImplementationType] = None

    # --- TLS Fields Added to Update ---
    tls_enabled: Optional[bool] = None
    tls_cert_secret_name: Optional[str] = None
    tls_key_secret_name: Optional[str] = None
    tls_ca_cert_secret_name: Optional[str] = None
    # --- End TLS Fields ---

    # Re-apply field validators using the Base methods
    @field_validator('ae_title', mode='before')
    @classmethod
    def validate_update_ae_title(cls, v: Any, info: ValidationInfo) -> Optional[str]:
        if v is None: return None
        # Call the validator from the Base class
        return DimseListenerConfigBase.validate_ae_title(v, info)

    @field_validator('instance_id', mode='before')
    @classmethod
    def validate_update_instance_id(cls, v: Any, info: ValidationInfo) -> Optional[str]:
        if v is None: return None
        # Call the validator from the Base class
        return DimseListenerConfigBase.validate_instance_id_format(v, info)

    @field_validator('listener_type')
    @classmethod
    def validate_update_listener_type(cls, value: Optional[DicomImplementationType]) -> Optional[DicomImplementationType]:
        # Enum validation is automatic, but we can add extra logic here if needed
        return value

    # --- Model Validator for Update Schema ---
    @model_validator(mode='after')
    def check_update_tls_consistency(self) -> 'DimseListenerConfigUpdate':
        # This needs to check the combination of provided values.
        # If tls_enabled is explicitly set to True, then cert/key must be provided (or already exist).
        # This is tricky because we don't know the existing state here easily.
        # The Base validator applied via API logic might be sufficient,
        # but adding a check here for explicit True setting.
        if self.tls_enabled is True: # Only check if it's explicitly being set to True
             if self.tls_cert_secret_name is None or self.tls_key_secret_name is None:
                 # If user sets tls_enabled=True but doesn't provide secrets *in the same update*,
                 # assume they should already exist or raise error. Let's raise error for clarity.
                 # Note: This logic might need refinement based on desired PUT behavior
                 # (e.g., partial updates vs full replacement for TLS config).
                 # For now, require secrets if setting tls_enabled=True in the PUT.
                 raise ValueError("If setting tls_enabled=True, both tls_cert_secret_name and tls_key_secret_name must be provided in the update.")
        # If tls_enabled is set to False, we might want to nullify secret names? Not doing that automatically here.
        # If tls_enabled is None, we don't enforce anything about secrets in this specific update.
        return self
    # --- End Model Validator ---


class DimseListenerConfigRead(DimseListenerConfigBase):
    # Inherits TLS fields and validation from Base
    id: int
    created_at: datetime
    updated_at: datetime
    listener_type: str

    # Ensure the model can be created from ORM attributes
    model_config = ConfigDict(from_attributes=True)
