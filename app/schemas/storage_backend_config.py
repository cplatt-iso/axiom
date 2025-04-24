# app/schemas/storage_backend_config.py
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, Dict, Any, Literal
from datetime import datetime
import json as pyjson

# Define allowed backend types explicitly for validation
AllowedBackendType = Literal[
    "filesystem",
    "cstore",
    "gcs",
    "google_healthcare",
    "stow_rs"
]

class StorageBackendConfigBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique, user-friendly name for this storage backend configuration.")
    description: Optional[str] = Field(None, description="Optional description of the backend's purpose or location.")
    backend_type: AllowedBackendType = Field(..., description="Type of the storage backend.")
    config: Dict[str, Any] = Field(default_factory=dict, description="JSON object containing backend-specific settings.")
    is_enabled: bool = Field(True, description="Whether this storage backend configuration is active.")

    # Add validation specific to the config structure based on backend_type if desired,
    # although this can become complex. Basic validation might be sufficient here,
    # relying on the backend service itself to validate during use.
    # Example (optional - can be complex to maintain):
    # @model_validator(mode='after')
    # def check_config_keys(self) -> 'StorageBackendConfigBase':
    #     config = self.config
    #     b_type = self.backend_type
    #     if b_type == "filesystem" and "path" not in config:
    #         raise ValueError("Filesystem config requires 'path'.")
    #     if b_type == "cstore" and not all(k in config for k in ["ae_title", "host", "port"]):
    #         raise ValueError("CStore config requires 'ae_title', 'host', 'port'.")
    #     # Add checks for gcs, google_healthcare, stow_rs...
    #     return self

class StorageBackendConfigCreate(StorageBackendConfigBase):
    pass

class StorageBackendConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    backend_type: Optional[AllowedBackendType] = None
    config: Optional[Dict[str, Any]] = None
    is_enabled: Optional[bool] = None

    # Add the same optional validator if implemented in Base
    # @model_validator(mode='after')
    # def check_config_keys_update(self) -> 'StorageBackendConfigUpdate':
    #     # Need to consider existing values if only type or config is updated
    #     # This logic becomes more complex for partial updates.
    #     pass

class StorageBackendConfigRead(StorageBackendConfigBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True) # Enable ORM mode
