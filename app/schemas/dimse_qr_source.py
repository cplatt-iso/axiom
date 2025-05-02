# app/schemas/dimse_qr_source.py
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict, Json # Added model_validator
from typing import Optional, Dict, Any
from datetime import datetime, date, timedelta
import re
import json as pyjson

# Re-use AE Title validation logic
AE_TITLE_PATTERN = re.compile(r"^[ A-Za-z0-9._-]{1,16}$")
def _validate_ae_title(v: Optional[str]) -> Optional[str]:
    # (Keep existing validator)
    if v is not None:
        v_stripped = v.strip();
        if not v_stripped: return None
        if not AE_TITLE_PATTERN.match(v_stripped): raise ValueError('AE Title contains invalid characters or is too long (max 16).')
        if v != v_stripped: raise ValueError('AE Title cannot have leading or trailing whitespace.')
        return v_stripped
    return None

# --- Base Schema (Add TLS fields) ---
class DimseQueryRetrieveSourceBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique, user-friendly name for this remote DIMSE source configuration.")
    description: Optional[str] = Field(None, description="Optional description of the remote peer or its purpose.")
    remote_ae_title: str = Field(..., min_length=1, max_length=16, description="AE Title of the remote peer to query/retrieve from.")
    remote_host: str = Field(..., min_length=1, description="Hostname or IP address of the remote peer.")
    remote_port: int = Field(..., gt=0, lt=65536, description="Network port of the remote peer's DIMSE service (1-65535).")
    local_ae_title: str = Field("AXIOM_QR_SCU", min_length=1, max_length=16, description="AE Title our SCU will use when associating.")

    # --- ADDED TLS Fields ---
    tls_enabled: bool = Field(False, description="Enable TLS for outgoing connections to the remote peer.")
    tls_ca_cert_secret_name: Optional[str] = Field(None, min_length=1, description="REQUIRED for TLS: Secret Manager resource name for the CA certificate (PEM) used to verify the remote peer's server certificate.")
    tls_client_cert_secret_name: Optional[str] = Field(None, min_length=1, description="Optional (for mTLS): Secret Manager resource name for OUR client certificate (PEM).")
    tls_client_key_secret_name: Optional[str] = Field(None, min_length=1, description="Optional (for mTLS): Secret Manager resource name for OUR client private key (PEM).")
    # --- END ADDED TLS Fields ---

    polling_interval_seconds: int = Field(300, gt=0, description="Frequency in seconds to poll the source using C-FIND.")
    is_enabled: bool = Field(True, description="Whether this source is generally enabled (e.g., shows in Data Browser, usable by system).")
    is_active: bool = Field(True, description="Whether AUTOMATIC polling for this source is active based on its schedule.")
    query_level: str = Field("STUDY", pattern=r"^(STUDY|SERIES|PATIENT)$", description="Query Retrieve Level for C-FIND (STUDY, SERIES, PATIENT).")
    query_filters: Optional[Dict[str, Any]] = Field(None, description=( "JSON object containing key-value pairs for C-FIND query identifiers. Example: {'ModalitiesInStudy': 'CT'}. For StudyDate, use 'YYYYMMDD', 'YYYYMMDD-', 'YYYYMMDD-YYYYMMDD', 'TODAY', 'YESTERDAY', or '-<N>d'." ))
    move_destination_ae_title: Optional[str] = Field(None, min_length=1, max_length=16, description="Optional: AE Title of OUR listener where retrieved instances should be sent via C-MOVE.")

    # --- Validators ---
    _validate_remote_ae = field_validator('remote_ae_title')(_validate_ae_title)
    _validate_local_ae = field_validator('local_ae_title')(_validate_ae_title)
    _validate_move_dest_ae = field_validator('move_destination_ae_title')(_validate_ae_title)

    @field_validator('remote_host')
    @classmethod
    def validate_host(cls, v: str) -> str:
        # (Keep existing validator)
        v_stripped = v.strip();
        if not v_stripped: raise ValueError("Remote host cannot be empty.")
        if ' ' in v_stripped or '/' in v_stripped: raise ValueError("Remote host contains invalid characters.")
        return v_stripped

    @field_validator('query_filters', mode='before')
    @classmethod
    def validate_query_filters_json(cls, v):
        # (Keep existing validator)
        if v is None: return None
        if isinstance(v, dict): return v
        if isinstance(v, str):
            v_stripped = v.strip();
            if not v_stripped: return None
            try: parsed = pyjson.loads(v_stripped);
            except pyjson.JSONDecodeError: raise ValueError("Query Filters is not a valid JSON string.")
            if not isinstance(parsed, dict): raise ValueError("Query Filters must be a valid JSON object string or dictionary.")
            return parsed
        raise TypeError("Query Filters must be a dictionary, valid JSON string, or null.")

    # --- ADDED TLS Validation ---
    @model_validator(mode='after')
    def check_tls_fields(self) -> 'DimseQueryRetrieveSourceBase':
        if self.tls_enabled and not self.tls_ca_cert_secret_name:
            raise ValueError("`tls_ca_cert_secret_name` is required when TLS is enabled.")
        # Check if mTLS fields are consistent (both present or both absent)
        if bool(self.tls_client_cert_secret_name) != bool(self.tls_client_key_secret_name):
            raise ValueError("Both `tls_client_cert_secret_name` and `tls_client_key_secret_name` must be provided together for mTLS, or neither.")
        return self
    # --- END ADDED TLS Validation ---


# --- Create Schema (Payload for POST) ---
class DimseQueryRetrieveSourceCreate(DimseQueryRetrieveSourceBase):
    pass # Inherits all fields (including TLS) and validation from Base

# --- Update Schema (Payload for PUT/PATCH) ---
class DimseQueryRetrieveSourceUpdate(BaseModel):
    # All fields are optional for updates
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    remote_ae_title: Optional[str] = Field(None, min_length=1, max_length=16)
    remote_host: Optional[str] = Field(None, min_length=1)
    remote_port: Optional[int] = Field(None, gt=0, lt=65536)
    local_ae_title: Optional[str] = Field(None, min_length=1, max_length=16)

    # --- ADDED Optional TLS Fields ---
    tls_enabled: Optional[bool] = None
    tls_ca_cert_secret_name: Optional[str] = Field(None, min_length=1)
    tls_client_cert_secret_name: Optional[str] = Field(None, min_length=1)
    tls_client_key_secret_name: Optional[str] = Field(None, min_length=1)
    # --- END ADDED ---

    polling_interval_seconds: Optional[int] = Field(None, gt=0)
    is_enabled: Optional[bool] = None
    is_active: Optional[bool] = None
    query_level: Optional[str] = Field(None, pattern=r"^(STUDY|SERIES|PATIENT)$")
    query_filters: Optional[Dict[str, Any]] = Field(None, description="Update query filters (provide full new object or null to clear).")
    move_destination_ae_title: Optional[str] = Field(None, min_length=1, max_length=16)

    # Re-apply validators for optional fields
    _validate_remote_ae = field_validator('remote_ae_title', mode='before')(_validate_ae_title)
    _validate_local_ae = field_validator('local_ae_title', mode='before')(_validate_ae_title)
    _validate_move_dest_ae = field_validator('move_destination_ae_title', mode='before')(_validate_ae_title)
    _validate_host = field_validator('remote_host', mode='before')(
        lambda v: DimseQueryRetrieveSourceBase.validate_host(v) if v is not None else None
    )
    _validate_filters_json = field_validator('query_filters', mode='before')(
        lambda v: DimseQueryRetrieveSourceBase.validate_query_filters_json(v) if v is not None else None
    )

    # --- ADDED Optional TLS Validation for Update ---
    # This needs careful thought for PATCH. If only tls_enabled=True is sent,
    # we don't know if tls_ca_cert_secret_name exists on the DB object.
    # Validation should ideally happen in the CRUD layer or endpoint where
    # the existing object state is known. Pydantic can only validate the incoming data.
    # We'll keep the mTLS consistency check.
    @model_validator(mode='after')
    def check_update_tls_fields(self) -> 'DimseQueryRetrieveSourceUpdate':
        # Cannot reliably check tls_enabled vs tls_ca_cert_secret_name here for PATCH
        # Check mTLS consistency if *both* fields are included in the update payload or not None
        client_cert = self.tls_client_cert_secret_name
        client_key = self.tls_client_key_secret_name
        if (client_cert is not None and client_key is None) or \
           (client_cert is None and client_key is not None):
            # This only catches cases where one is explicitly provided and the other is missing *in the update itself*.
            # It doesn't catch setting one without the other if the other already exists in DB.
             raise ValueError("Both client certificate and key secret names must be provided together for mTLS update, or neither.")
        return self
    # --- END ADDED ---


# --- Read Schema (Data returned by GET endpoints) ---
class DimseQueryRetrieveSourceRead(DimseQueryRetrieveSourceBase):
    # Include fields inherited from DB Base model
    id: int
    created_at: datetime
    updated_at: datetime

    # Include state fields
    last_successful_query: Optional[datetime] = None
    last_successful_move: Optional[datetime] = None
    last_error_time: Optional[datetime] = None
    last_error_message: Optional[str] = None
    found_study_count: int = Field(0, description="Total studies found by C-FIND.")
    move_queued_study_count: int = Field(0, description="Total studies queued for C-MOVE.")
    processed_instance_count: int = Field(0, description="Total instances processed after C-MOVE.")

    # TLS fields are inherited from Base
    model_config = ConfigDict(from_attributes=True) # Enable ORM mode


# Type aliases for clarity (Optional)
DimseQueryRetrieveSourceCreatePayload = DimseQueryRetrieveSourceCreate
DimseQueryRetrieveSourceUpdatePayload = DimseQueryRetrieveSourceUpdate
