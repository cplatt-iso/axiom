# app/schemas/storage_backend_config.py
from pydantic import BaseModel, Field, field_validator, ConfigDict, model_validator, Discriminator, SkipValidation
from typing import Optional, Dict, Any, Literal, Union, Annotated, List
from datetime import datetime
import enum # You'll need this for the shiny new enum!
import json as pyjson

# --- NEW: Enum for STOW-RS Authentication Types ---
# Because magic strings are for amateurs and debugging nightmares.
class StowRsAuthType(str, enum.Enum):
    NONE = "none"
    BASIC = "basic"
    BEARER = "bearer"
    APIKEY = "apikey"

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
    
    # --- NEW: STOW-RS Authentication Fields ---
    auth_type: Optional[StowRsAuthType] = Field(
        default=StowRsAuthType.NONE, 
        description="Authentication type for the STOW-RS endpoint."
    )
    basic_auth_username_secret_name: Optional[str] = Field(
        None, 
        description="GCP Secret Manager name for Basic Auth username. Required if auth_type is 'basic'."
    )
    basic_auth_password_secret_name: Optional[str] = Field(
        None, 
        description="GCP Secret Manager name for Basic Auth password. Required if auth_type is 'basic'."
    )
    bearer_token_secret_name: Optional[str] = Field(
        None, 
        description="GCP Secret Manager name for Bearer token. Required if auth_type is 'bearer'."
    )
    api_key_secret_name: Optional[str] = Field(
        None, 
        description="GCP Secret Manager name for the API key. Required if auth_type is 'apikey'."
    )
    api_key_header_name_override: Optional[str] = Field(
        None, 
        description="Optional: Header name for the API key (e.g., 'X-API-Key'). Defaults to 'Authorization' with 'ApiKey' prefix if not 'apikey' type, or a common default like 'X-Api-Key'. If auth_type is 'apikey', this will be the specific header name to use."
    )
    tls_ca_cert_secret_name: Optional[str] = Field(
        None,
        description="Optional: GCP Secret Manager name for a custom CA certificate (PEM) to verify the STOW-RS server's TLS certificate."
    )

    @model_validator(mode='after')
    def check_stow_rs_auth_config(self) -> 'StowRsConfig':
        if self.auth_type == StowRsAuthType.BASIC:
            if not self.basic_auth_username_secret_name or not self.basic_auth_password_secret_name:
                raise ValueError(
                    "For 'basic' auth, 'basic_auth_username_secret_name' and 'basic_auth_password_secret_name' are required."
                )
        elif self.auth_type == StowRsAuthType.BEARER:
            if not self.bearer_token_secret_name:
                raise ValueError("For 'bearer' auth, 'bearer_token_secret_name' is required.")
        elif self.auth_type == StowRsAuthType.APIKEY:
            if not self.api_key_secret_name: # api_key_header_name_override can be optional with a default in the service
                raise ValueError("For 'apikey' auth, 'api_key_secret_name' is required.")
            if not self.api_key_header_name_override:
                 # You could also raise here or let the service use a default.
                 # Let's make it required by the schema if type is 'apikey' for clarity for now.
                 raise ValueError("For 'apikey' auth, 'api_key_header_name_override' is also required (e.g., 'X-Api-Key').")


        # If auth_type is NONE, ensure other fields are not misconfigured (optional cleanup)
        # This is more of a "best practice" than strict validation, as they'd just be ignored.
        if self.auth_type == StowRsAuthType.NONE:
            if self.basic_auth_username_secret_name or \
               self.basic_auth_password_secret_name or \
               self.bearer_token_secret_name or \
               self.api_key_secret_name:
                # This could be a warning or just silently ignored by the service.
                # For schema, it's not strictly an error if they are provided but auth_type is "none".
                pass
        return self

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
# ... (other update schemas remain unchanged) ...

class StorageBackendConfigUpdate_StowRs(BaseModel): # This is for specific part of StorageBackendConfigUpdate
    base_url: Optional[str] = None
    # --- NEW: STOW-RS Auth Fields for Update ---
    auth_type: Optional[StowRsAuthType] = None # Allow changing auth type
    basic_auth_username_secret_name: Optional[str] = None
    basic_auth_password_secret_name: Optional[str] = None
    bearer_token_secret_name: Optional[str] = None
    api_key_secret_name: Optional[str] = None
    api_key_header_name_override: Optional[str] = None
    tls_ca_cert_secret_name: Optional[str] = None # Already in CStore, shared field for StowRS too

# --- Combined Update Schema (Common fields + placeholder for specific) ---
class StorageBackendConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    is_enabled: Optional[bool] = None

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
    # tls_ca_cert_secret_name is shared by CStore and STOW-RS for their respective custom CAs
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
    # --- NEW: STOW-RS Auth Fields in Combined Update ---
    auth_type: Optional[StowRsAuthType] = Field(None, description="STOW-RS authentication type.") # Field added
    basic_auth_username_secret_name: Optional[str] = Field(None, description="STOW-RS Basic Auth username secret name.")
    basic_auth_password_secret_name: Optional[str] = Field(None, description="STOW-RS Basic Auth password secret name.")
    bearer_token_secret_name: Optional[str] = Field(None, description="STOW-RS Bearer token secret name.")
    api_key_secret_name: Optional[str] = Field(None, description="STOW-RS API key secret name.")
    api_key_header_name_override: Optional[str] = Field(None, description="STOW-RS API key header name override.")
    # tls_ca_cert_secret_name for STOW-RS is covered by the general one above

    @model_validator(mode='after')
    def check_update_stow_rs_auth_logic(self) -> 'StorageBackendConfigUpdate':
        # This validator runs on the combined update model.
        # It should only validate STOW-RS fields if auth_type for STOW-RS is being set/changed.
        # If auth_type is not in the update payload, we assume existing values are valid or unchanged.
        # This is complex because we don't know the original backend_type here easily.
        # The CRUD/service layer is a better place for such context-aware validation on update.
        # For now, a light check if auth_type is explicitly provided in the PATCH:
        if self.auth_type: # If auth_type is part of the PATCH payload for STOW-RS
            if self.auth_type == StowRsAuthType.BASIC:
                # If changing to basic, both username and password secrets should ideally be provided in the patch,
                # or already exist on the object being patched. This is hard to enforce here.
                # We'll rely on the StowRsConfig validator for *creation* and the service for *update* consistency.
                pass 
            # Similar logic for bearer and apikey if desired, but it gets tricky for PATCH.
        return self

    @model_validator(mode='after')
    def check_update_cstore_tls_config(self) -> 'StorageBackendConfigUpdate':
        # This is the existing CStore validator, ensure it still makes sense.
        # If tls_enabled is explicitly set to True for a CStore backend, ca_cert_secret_name might be needed.
        # Again, complex for PATCH, depends on original state.
        if (self.tls_client_cert_secret_name and not self.tls_client_key_secret_name) or \
           (not self.tls_client_cert_secret_name and self.tls_client_key_secret_name):
            raise ValueError("C-STORE: Both client certificate and key secret names must be provided together for mTLS update, or neither.")
        return self
        
    @model_validator(mode='after')
    def ensure_at_least_one_field_for_update(self) -> 'StorageBackendConfigUpdate':
        if not self.model_fields_set: # Ensure at least one field is provided
            raise ValueError("At least one field must be provided for update.")
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

class StorageBackendConfigRead_StowRs(StorageBackendConfigBase, StowRsConfig): # Will inherit new fields from StowRsConfig
    id: int
    backend_type: Literal["stow_rs"] = "stow_rs"
    created_at: datetime
    updated_at: datetime
    model_config = ConfigDict(from_attributes=True)


# --- Discriminated Union for Read ---
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
class StorageBackendConfigListItem(BaseModel):
     id: int
     name: str
     description: Optional[str] = None
     backend_type: str 
     is_enabled: bool
     created_at: datetime
     updated_at: datetime
     model_config = ConfigDict(from_attributes=True)