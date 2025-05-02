# app/schemas/storage_backend_config.py
from pydantic import BaseModel, Field, field_validator, ConfigDict, model_validator, Discriminator, SkipValidation
from typing import Optional, Dict, Any, Literal, Union, Annotated, List # Added Union, Annotated, List
from datetime import datetime
import json as pyjson

# --- Base schema with common fields ---
class StorageBackendConfigBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique, user-friendly name for this storage backend configuration.")
    description: Optional[str] = Field(None, description="Optional description of the backend's purpose or location.")
    is_enabled: bool = Field(True, description="Whether this storage backend configuration is active.")
    # 'backend_type' will be added in subclasses with Literal type

# --- Type-Specific Configuration Schemas ---
# Define the structure for each backend type without the common fields yet

class FileSystemConfig(BaseModel):
    path: str = Field(..., description="Path to the directory for storing files.")

class GcsConfig(BaseModel):
    bucket: str = Field(..., description="Name of the GCS bucket.")
    prefix: Optional[str] = Field(None, description="Optional prefix (folder path) within the bucket.")
    # credentials_secret_name: Optional[str] = None # Future

class CStoreConfig(BaseModel):
    remote_ae_title: str = Field(..., max_length=16, description="AE Title of the remote C-STORE SCP.")
    remote_host: str = Field(..., description="Hostname or IP address of the remote SCP.")
    remote_port: int = Field(..., gt=0, le=65535, description="Network port of the remote SCP.")
    local_ae_title: Optional[str] = Field("AXIOM_STORE_SCU", max_length=16, description="AE Title OUR SCU will use when associating.")
    # TLS fields
    tls_enabled: bool = Field(False, description="Enable TLS for outgoing connections.")
    tls_ca_cert_secret_name: Optional[str] = Field(None, description="REQUIRED if TLS enabled: Secret Manager name for CA cert to verify remote server.")
    tls_client_cert_secret_name: Optional[str] = Field(None, description="Optional (for mTLS): Secret Manager name for OUR client certificate.")
    tls_client_key_secret_name: Optional[str] = Field(None, description="Optional (for mTLS): Secret Manager name for OUR client private key.")

    @model_validator(mode='after')
    def check_tls_config(self) -> 'CStoreConfig':
        if self.tls_enabled and not self.tls_ca_cert_secret_name:
            raise ValueError("`tls_ca_cert_secret_name` is required when TLS is enabled for C-STORE.")
        if (self.tls_client_cert_secret_name and not self.tls_client_key_secret_name) or \
           (not self.tls_client_cert_secret_name and self.tls_client_key_secret_name):
            raise ValueError("Both client certificate and key secret names must be provided together for mTLS, or neither.")
        return self

class GoogleHealthcareConfig(BaseModel):
    gcp_project_id: str = Field(...)
    gcp_location: str = Field(...)
    gcp_dataset_id: str = Field(...)
    gcp_dicom_store_id: str = Field(...)
    # credentials_secret_name: Optional[str] = None # Future

class StowRsConfig(BaseModel):
    base_url: str = Field(..., description="Base URL of the STOW-RS service (e.g., https://dicom.server.com/dicomweb).")
    # auth fields... TBD


# --- Create Schemas (Combining Base + Type-Specific) ---
# Each 'Create' schema inherits common fields and adds the type-specific fields

class StorageBackendConfigCreate_Filesystem(StorageBackendConfigBase, FileSystemConfig):
    backend_type: Literal["filesystem"] = "filesystem"

class StorageBackendConfigCreate_GCS(StorageBackendConfigBase, GcsConfig):
    backend_type: Literal["gcs"] = "gcs"

class StorageBackendConfigCreate_CStore(StorageBackendConfigBase, CStoreConfig):
    backend_type: Literal["cstore"] = "cstore"

class StorageBackendConfigCreate_GoogleHealthcare(StorageBackendConfigBase, GoogleHealthcareConfig):
    backend_type: Literal["google_healthcare"] = "google_healthcare"

class StorageBackendConfigCreate_StowRs(StorageBackendConfigBase, StowRsConfig):
    backend_type: Literal["stow_rs"] = "stow_rs"

# --- Discriminated Union for Create ---
# Use Annotated and Discriminator to tell Pydantic how to choose the right Create model
# Based on the 'backend_type' field. This is what the API endpoint will accept.
StorageBackendConfigCreate = Annotated[
    Union[
        StorageBackendConfigCreate_Filesystem,
        StorageBackendConfigCreate_GCS,
        StorageBackendConfigCreate_CStore,
        StorageBackendConfigCreate_GoogleHealthcare,
        StorageBackendConfigCreate_StowRs,
    ],
    Discriminator("backend_type"),
]

# --- Update Schemas (All fields optional) ---
# Similar structure, but all fields are Optional

class StorageBackendConfigUpdate_Filesystem(BaseModel):
    path: Optional[str] = None

class StorageBackendConfigUpdate_GCS(BaseModel):
    bucket: Optional[str] = None
    prefix: Optional[str] = None

class StorageBackendConfigUpdate_CStore(BaseModel):
    remote_ae_title: Optional[str] = Field(None, max_length=16)
    remote_host: Optional[str] = None
    remote_port: Optional[int] = Field(None, gt=0, le=65535)
    local_ae_title: Optional[str] = Field(None, max_length=16)
    tls_enabled: Optional[bool] = None
    tls_ca_cert_secret_name: Optional[str] = None
    tls_client_cert_secret_name: Optional[str] = None
    tls_client_key_secret_name: Optional[str] = None

    # Add validator similar to CStoreConfig if strict checking on partial updates is needed

class StorageBackendConfigUpdate_GoogleHealthcare(BaseModel):
    gcp_project_id: Optional[str] = None
    gcp_location: Optional[str] = None
    gcp_dataset_id: Optional[str] = None
    gcp_dicom_store_id: Optional[str] = None

class StorageBackendConfigUpdate_StowRs(BaseModel):
    base_url: Optional[str] = None

# --- Combined Update Schema (Common fields + placeholder for specific) ---
# This is tricky. A simple union doesn't work well for PATCH semantics.
# A common approach is to have a single Update schema with all possible fields
# optional, and potentially validate based on backend_type if provided.
# Alternatively, the API endpoint logic handles extracting common vs specific fields.
# Let's try a single schema approach for now, though it's less strictly typed at this level.

class StorageBackendConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    is_enabled: Optional[bool] = None

    # Include ALL possible specific fields as optional
    # Filesystem
    path: Optional[str] = None
    # GCS
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    # CStore
    remote_ae_title: Optional[str] = Field(None, max_length=16)
    remote_host: Optional[str] = None
    remote_port: Optional[int] = Field(None, gt=0, le=65535)
    local_ae_title: Optional[str] = Field(None, max_length=16)
    tls_enabled: Optional[bool] = None
    tls_ca_cert_secret_name: Optional[str] = None
    tls_client_cert_secret_name: Optional[str] = None
    tls_client_key_secret_name: Optional[str] = None
    # Google Healthcare
    gcp_project_id: Optional[str] = None
    gcp_location: Optional[str] = None
    gcp_dataset_id: Optional[str] = None
    gcp_dicom_store_id: Optional[str] = None
    # StowRs
    base_url: Optional[str] = None

    # Note: We CANNOT include 'backend_type' here easily, as changing the type
    # would require changing which other fields are valid. Type changes should
    # likely be handled by deleting and recreating, or complex logic in the API.
    # This single Update model approach has limitations but is common for PATCH.

    # Add validation logic if needed, e.g., ensure CStore TLS fields make sense if provided
    @model_validator(mode='after')
    def check_update_tls_config(self) -> 'StorageBackendConfigUpdate':
        if self.tls_enabled is True and self.tls_ca_cert_secret_name is None:
            # This validation assumes we know the original object's state or
            # that if tls_enabled is being set to True, the CA must be provided *in the same request*.
            # This might be too strict for PATCH. Removing for now. Consider validation in CRUD.
            pass
        if (self.tls_client_cert_secret_name and not self.tls_client_key_secret_name) or \
           (not self.tls_client_cert_secret_name and self.tls_client_key_secret_name):
            raise ValueError("Both client certificate and key secret names must be provided together for mTLS update, or neither.")
        return self

# --- Read Schemas (Similar structure to Create, includes common + specific + ID/timestamps) ---

class StorageBackendConfigRead_Filesystem(StorageBackendConfigBase, FileSystemConfig):
    id: int
    backend_type: Literal["filesystem"] = "filesystem"
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)

class StorageBackendConfigRead_GCS(StorageBackendConfigBase, GcsConfig):
    id: int
    backend_type: Literal["gcs"] = "gcs"
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)

class StorageBackendConfigRead_CStore(StorageBackendConfigBase, CStoreConfig):
    id: int
    backend_type: Literal["cstore"] = "cstore"
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)

class StorageBackendConfigRead_GoogleHealthcare(StorageBackendConfigBase, GoogleHealthcareConfig):
    id: int
    backend_type: Literal["google_healthcare"] = "google_healthcare"
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)

class StorageBackendConfigRead_StowRs(StorageBackendConfigBase, StowRsConfig):
    id: int
    backend_type: Literal["stow_rs"] = "stow_rs"
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)


# --- Discriminated Union for Read ---
# This is what the API endpoint will return in lists or single gets.
StorageBackendConfigRead = Annotated[
    Union[
        StorageBackendConfigRead_Filesystem,
        StorageBackendConfigRead_GCS,
        StorageBackendConfigRead_CStore,
        StorageBackendConfigRead_GoogleHealthcare,
        StorageBackendConfigRead_StowRs,
    ],
    Discriminator("backend_type"),
]

# --- Optional: Schema for listing (maybe simpler) ---
# Sometimes you only need common fields for lists
class StorageBackendConfigListItem(BaseModel):
     id: int
     name: str
     description: Optional[str] = None
     backend_type: str # Keep as string here for simplicity maybe? Or use Literal
     is_enabled: bool
     created_at: datetime
     updated_at: datetime
     model_config = ConfigDict(from_attributes=True)
