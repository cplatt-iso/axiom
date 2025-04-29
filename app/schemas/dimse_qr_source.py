# app/schemas/dimse_qr_source.py
from pydantic import BaseModel, Field, field_validator, ConfigDict, Json
from typing import Optional, Dict, Any
from datetime import datetime, date, timedelta # Import date/timedelta
import re
import json as pyjson # Import standard json library for parsing check

# Re-use AE Title validation logic (similar to DimseListenerConfig)
AE_TITLE_PATTERN = re.compile(r"^[ A-Za-z0-9._-]{1,16}$")
def _validate_ae_title(v: Optional[str]) -> Optional[str]:
    if v is not None:
        v_stripped = v.strip()
        if not v_stripped:
            # Allow explicitly setting to None/empty if nullable in DB (e.g., move_destination)
            return None # Or raise ValueError if AE Title should *always* be non-empty when provided
        if not AE_TITLE_PATTERN.match(v_stripped):
            raise ValueError('AE Title contains invalid characters or is too long (max 16).')
        # Ensure no leading/trailing whitespace remains after potential strip
        if v != v_stripped:
             raise ValueError('AE Title cannot have leading or trailing whitespace.')
        return v_stripped # Return stripped version
    return None # Pass through None

# --- Base Schema ---
class DimseQueryRetrieveSourceBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique, user-friendly name for this remote DIMSE source configuration.")
    description: Optional[str] = Field(None, description="Optional description of the remote peer or its purpose.")
    remote_ae_title: str = Field(..., min_length=1, max_length=16, description="AE Title of the remote peer to query/retrieve from.")
    remote_host: str = Field(..., min_length=1, description="Hostname or IP address of the remote peer.")
    remote_port: int = Field(..., gt=0, lt=65536, description="Network port of the remote peer's DIMSE service (1-65535).")
    local_ae_title: str = Field("AXIOM_QR_SCU", min_length=1, max_length=16, description="AE Title our SCU will use when associating.")
    polling_interval_seconds: int = Field(300, gt=0, description="Frequency in seconds to poll the source using C-FIND.")
    is_enabled: bool = Field(
        True,
        description="Whether this source is generally enabled (e.g., shows in Data Browser, usable by system)." # <-- Clarified
    )
    is_active: bool = Field( # <-- ADDED THIS SHIT
        True,
        description="Whether AUTOMATIC polling for this source is active based on its schedule."
    )
    query_level: str = Field("STUDY", pattern=r"^(STUDY|SERIES|PATIENT)$", description="Query Retrieve Level for C-FIND (STUDY, SERIES, PATIENT).") # Example pattern
    # Accept dict or valid JSON string for filters on input, store as dict
    query_filters: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "JSON object containing key-value pairs for C-FIND query identifiers. "
            "Example: {'ModalitiesInStudy': 'CT', 'PatientName': 'DOE^JOHN*'}. "
            "For StudyDate, use 'YYYYMMDD', 'YYYYMMDD-', 'YYYYMMDD-YYYYMMDD', "
            "'TODAY', 'YESTERDAY', or '-<N>d' (N days ago to present, e.g., '-7d')."
        )
    )
    move_destination_ae_title: Optional[str] = Field(None, min_length=1, max_length=16, description="Optional: AE Title of OUR listener where retrieved instances should be sent via C-MOVE.")

    # --- Validators ---
    _validate_remote_ae = field_validator('remote_ae_title')(_validate_ae_title)
    _validate_local_ae = field_validator('local_ae_title')(_validate_ae_title)
    _validate_move_dest_ae = field_validator('move_destination_ae_title')(_validate_ae_title)

    @field_validator('remote_host')
    @classmethod
    def validate_host(cls, v: str) -> str:
        v_stripped = v.strip()
        if not v_stripped:
            raise ValueError("Remote host cannot be empty.")
        # Basic check: avoid obviously invalid characters, but don't try full hostname/IP validation here
        if ' ' in v_stripped or '/' in v_stripped:
             raise ValueError("Remote host contains invalid characters.")
        return v_stripped

    # Pydantic v2 way to validate JSON string input for a Dict field
    @field_validator('query_filters', mode='before')
    @classmethod
    def validate_query_filters_json(cls, v):
        if v is None:
            return None
        if isinstance(v, dict):
            # Optional: Add specific validation for dynamic date strings here if desired
            # e.g., check if StudyDate value matches expected patterns
            return v # Already a dict, pass through
        if isinstance(v, str):
            v_stripped = v.strip()
            if not v_stripped:
                return None # Treat empty string as None
            try:
                parsed = pyjson.loads(v_stripped)
                if not isinstance(parsed, dict):
                    raise ValueError("Query Filters must be a valid JSON object string or dictionary.")
                # Optional: Add validation for dynamic date strings after parsing here
                return parsed
            except pyjson.JSONDecodeError:
                raise ValueError("Query Filters is not a valid JSON string.")
        raise TypeError("Query Filters must be a dictionary, valid JSON string, or null.")


# --- Create Schema (Payload for POST) ---
class DimseQueryRetrieveSourceCreate(DimseQueryRetrieveSourceBase):
    pass # Inherits all fields (including is_active) and validation from Base

# --- Update Schema (Payload for PUT/PATCH) ---
class DimseQueryRetrieveSourceUpdate(BaseModel):
    # All fields are optional for updates
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    remote_ae_title: Optional[str] = Field(None, min_length=1, max_length=16)
    remote_host: Optional[str] = Field(None, min_length=1)
    remote_port: Optional[int] = Field(None, gt=0, lt=65536)
    local_ae_title: Optional[str] = Field(None, min_length=1, max_length=16)
    polling_interval_seconds: Optional[int] = Field(None, gt=0)
    is_enabled: Optional[bool] = None
    is_active: Optional[bool] = None # <-- ADDED THIS SHIT TOO
    query_level: Optional[str] = Field(None, pattern=r"^(STUDY|SERIES|PATIENT)$")
    query_filters: Optional[Dict[str, Any]] = Field(None, description="Update query filters (provide full new object or null to clear).")
    move_destination_ae_title: Optional[str] = Field(None, min_length=1, max_length=16) # Allow null to clear

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

# --- Read Schema (Data returned by GET endpoints) ---
class DimseQueryRetrieveSourceRead(DimseQueryRetrieveSourceBase):
    # Include fields inherited from DB Base model
    id: int
    created_at: datetime
    updated_at: datetime

    # Include state fields (read-only from API perspective for config)
    last_successful_query: Optional[datetime] = None
    last_successful_move: Optional[datetime] = None
    last_error_time: Optional[datetime] = None
    last_error_message: Optional[str] = None
    found_study_count: int = Field(0, description="Total studies found by C-FIND.")
    move_queued_study_count: int = Field(0, description="Total studies queued for C-MOVE.")
    processed_instance_count: int = Field(0, description="Total instances processed after C-MOVE.")

    # No need to explicitly add `is_active` here;
    # it's inherited from Base and `from_attributes` will pull it from the DB model.

    model_config = ConfigDict(from_attributes=True) # Enable ORM mode


# Type aliases for clarity in API function signatures
DimseQueryRetrieveSourceCreatePayload = DimseQueryRetrieveSourceCreate
DimseQueryRetrieveSourceUpdatePayload = DimseQueryRetrieveSourceUpdate
