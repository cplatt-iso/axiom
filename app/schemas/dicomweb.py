# app/schemas/dicomweb.py
from typing import List, Optional, Dict, Any, Literal
# Pydantic v2 specific imports if needed, assuming v2 features are used
from pydantic import BaseModel, Field, HttpUrl, validator, root_validator, field_validator, model_validator # Added field_validator, model_validator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Based on PS3.18 Table 10.6.1-3. Attributes for Referenced SOP Sequence Item Macro
class ReferencedSOP(BaseModel):
    ReferencedSOPClassUID: str = Field(..., alias="00081150")
    ReferencedSOPInstanceUID: str = Field(..., alias="00081155")
    RetrieveURL: Optional[str] = Field(None, alias="00081190") # Optional WADO-RS Retrieve URL

# Based on PS3.18 Table 10.6.1-4. Attributes for Failed SOP Sequence Item Macro
class FailedSOP(BaseModel):
    ReferencedSOPClassUID: str = Field(..., alias="00081150")
    ReferencedSOPInstanceUID: str = Field(..., alias="00081155")
    FailureReason: int = Field(..., alias="00081197") # DICOM Failure Reason code
    ReasonDetail: Optional[str] = Field(None) # Custom field for more detail

# Based on PS3.18 Table 10.6.1-2. Attributes for STOW-RS Response Payload
class STOWResponse(BaseModel):
    TransactionUID: Optional[str] = Field(None, alias="00081195") # Optional
    ReferencedSOPSequence: Optional[List[ReferencedSOP]] = Field(None, alias="00081199")
    FailedSOPSequence: Optional[List[FailedSOP]] = Field(None, alias="00081198")
    # Include other potential response fields if needed later, e.g., warnings

    class Config:
        extra = 'allow'
        populate_by_name = True
        json_encoders = {}
        by_alias = True # Ensure aliases are used when generating JSON response


# --- Helper codes for FailureReason (0008,1197) ---
# Simplified subset based on PS3.7 Annex C - Status Codes
class FailureReasonCode:
    # Specific STOW related failure reasons (PS3.18 Section 10.6.1.3.1)
    ProcessingFailure = 0x0110  # Processing failure
    NoSuchAttribute = 0x0105 # No Such Attribute (e.g., missing required element for matching)
    InvalidAttributeValue = 0x0106 # Invalid Attribute Value
    RefusedSOPClassNotSupported = 0x0112 # SOP Class Not Supported
    RefusedOutOfResources = 0x0122 # Out of Resources
    # General Data Set or SOP Instance Error
    ErrorDataSetDoesNotMatchSOPClass = 0xA900
    ErrorCannotUnderstand = 0xC000 # Cannot understand/parse
    # Custom Axiom Flow specific codes (use ranges outside official DICOM codes if possible)
    RuleMatchingFailed = 0xF001 # Custom: Failed during rule matching phase
    DispatchFailed = 0xF002 # Custom: Failed during dispatch to destination
    StorageBackendError = 0xF003 # Custom: Storage backend reported an error
    QueuingFailed = 0xF004 # Custom: Failed to queue instance for processing
    BadRequestMultipart = 0xF100 # Custom: Request body was not valid multipart/related
    BadRequestMissingDicomPart = 0xF101 # Custom: Multipart message missing DICOM application/dicom parts
    BadRequestParsingError = 0xF102 # Custom: Error parsing a DICOM part

# --- END OF MISSING CLASSES ---


# --- DICOMweb Source Configuration Schemas ---

# Add 'apikey' to the allowed types
AuthType = Literal["none", "basic", "bearer", "apikey"] # <-- ADDED 'apikey'

class DicomWebSourceConfigBase(BaseModel):
    name: str = Field(..., description="Unique human-readable name for this DICOMweb source.")
    description: Optional[str] = Field(None, description="Optional description for the source.")
    base_url: str = Field(..., description="Base URL of the DICOMweb service (e.g., http://orthanc:8042/dicom-web).")
    qido_prefix: str = Field("qido-rs", description="Path prefix for QIDO-RS service.")
    wado_prefix: str = Field("wado-rs", description="Path prefix for WADO-RS service.")
    polling_interval_seconds: int = Field(
        default=300, gt=0,
        description="Frequency in seconds at which to poll the source for new studies."
    )
    is_enabled: bool = Field(True, description="Whether the poller for this source is active.")
    auth_type: AuthType = Field("none", description="Authentication method required by the DICOMweb source.")
    auth_config: Optional[Dict[str, Any]] = Field(
        None,
        # Updated description to include apikey example
        description="JSON object containing credentials (e.g., {'username': ..., 'password': ...}, {'token': ...}, {'header_name': 'X-Api-Key', 'key': ...}).",
    )
    search_filters: Optional[Dict[str, Any]] = Field(
        None,
        description="JSON object containing QIDO-RS query parameters (e.g., {'StudyDate': '20230101-', 'ModalitiesInStudy': 'CT'}).",
    )
    # Keep source_name private for internal population
    source_name: Optional[str] = Field(None, exclude=True)

    # --- Individual Field Validators ---
    # Using the deprecated @validator for broader Pydantic v1/v2 compatibility for simple field checks
    @validator('base_url')
    def check_base_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('Base URL must start with http:// or https://')
        return v

    # --- Model Validators (Pydantic V2 Syntax) ---
    @model_validator(mode='before')
    @classmethod
    def validate_auth_config_structure(cls, data: Any) -> Any:
        if not isinstance(data, dict):
             return data

        auth_type = data.get('auth_type')
        auth_config = data.get('auth_config')

        if auth_type == "basic":
            if not isinstance(auth_config, dict) or 'username' not in auth_config or 'password' not in auth_config:
                raise ValueError("For 'basic' auth_type, 'auth_config' must be a dict with 'username' and 'password'.")
            if not isinstance(auth_config.get('username'), str) or not auth_config.get('username'):
                 raise ValueError("Username in auth_config must be a non-empty string for basic auth.")
            if not isinstance(auth_config.get('password'), str) or not auth_config.get('password'):
                 raise ValueError("Password in auth_config must be a non-empty string for basic auth.")
        elif auth_type == "bearer":
            if not isinstance(auth_config, dict) or 'token' not in auth_config:
                raise ValueError("For 'bearer' auth_type, 'auth_config' must be a dict with 'token'.")
            if not isinstance(auth_config.get('token'), str) or not auth_config.get('token'):
                 raise ValueError("Token in auth_config must be a non-empty string for bearer auth.")
        # --- ADDED API Key Validation ---
        elif auth_type == "apikey":
             if not isinstance(auth_config, dict) or 'header_name' not in auth_config or 'key' not in auth_config:
                 raise ValueError("For 'apikey' auth_type, 'auth_config' must be a dict with 'header_name' and 'key'.")
             if not isinstance(auth_config.get('header_name'), str) or not auth_config.get('header_name'):
                 raise ValueError("Header Name in auth_config must be a non-empty string for apikey auth.")
             if not isinstance(auth_config.get('key'), str) or not auth_config.get('key'):
                 raise ValueError("Key in auth_config must be a non-empty string for apikey auth.")
        # --- END ADDED API Key Validation ---
        elif auth_type == "none":
             if auth_config is not None and isinstance(auth_config, dict) and len(auth_config) > 0 :
                 raise ValueError("For 'none' auth_type, 'auth_config' must be empty or null.")
             data['auth_config'] = None # Normalize to None

        return data

    @model_validator(mode='after')
    def populate_source_name(self) -> 'DicomWebSourceConfigBase':
        # Logic remains the same
        if self.name and not self.source_name:
             self.source_name = self.name
             logger.debug(f"Populated 'source_name' from 'name': {self.name}")
        elif not self.name and not self.source_name:
             logger.warning("Could not populate source_name as 'name' field was missing or empty after validation.")
        return self
    # --- End Model Validators ---

# --- Keep DicomWebSourceConfigCreate as is ---
class DicomWebSourceConfigCreate(DicomWebSourceConfigBase):
    pass # Inherits everything, including new 'apikey' possibility


class DicomWebSourceConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, description="Unique human-readable name for this DICOMweb source.")
    description: Optional[str] = Field(None, description="Optional description for the source.")
    base_url: Optional[str] = Field(None, description="Base URL of the DICOMweb service.")
    qido_prefix: Optional[str] = Field(None, description="Path prefix for QIDO-RS service.")
    wado_prefix: Optional[str] = Field(None, description="Path prefix for WADO-RS service.")
    polling_interval_seconds: Optional[int] = Field(None, gt=0, description="Frequency in seconds.")
    is_enabled: Optional[bool] = Field(None, description="Whether the poller for this source is active.")
    auth_type: Optional[AuthType] = Field(None, description="Authentication method.") # Now includes 'apikey'
    auth_config: Optional[Dict[str, Any]] = Field(None, description="JSON object containing credentials.")
    search_filters: Optional[Dict[str, Any]] = Field(None, description="JSON object containing QIDO-RS query parameters.")

    # Using field_validator syntax for optional base_url check
    @field_validator('base_url')
    @classmethod
    def check_optional_base_url(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.startswith(('http://', 'https://')):
            raise ValueError('Base URL must start with http:// or https://')
        return v

    # Updated root_validator for Update to include apikey check
    # Sticking with deprecated root_validator for Update as model_validator logic is more complex for partial updates
    @root_validator(pre=True)
    def validate_update_auth_config_structure(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        auth_type = values.get('auth_type')
        auth_config = values.get('auth_config')

        if 'auth_config' in values:
            effective_auth_type = auth_type
            if effective_auth_type == "basic":
                 if not isinstance(auth_config, dict) or 'username' not in auth_config or 'password' not in auth_config:
                      raise ValueError("If providing 'auth_config' with 'basic' auth_type, it must be a dict with 'username' and 'password'.")
            elif effective_auth_type == "bearer":
                 if not isinstance(auth_config, dict) or 'token' not in auth_config:
                      raise ValueError("If providing 'auth_config' with 'bearer' auth_type, it must be a dict with 'token'.")
            # --- ADDED API Key Check for Update ---
            elif effective_auth_type == "apikey":
                 if not isinstance(auth_config, dict) or 'header_name' not in auth_config or 'key' not in auth_config:
                      raise ValueError("If providing 'auth_config' with 'apikey' auth_type, it must be a dict with 'header_name' and 'key'.")
            # --- END API Key Check ---
            elif effective_auth_type == "none":
                 if auth_config is not None and isinstance(auth_config, dict) and len(auth_config) > 0 :
                      raise ValueError("If providing 'auth_config', it must be empty or null when auth_type is 'none'.")
                 values['auth_config'] = None # Normalize

        if auth_type == "none":
            values['auth_config'] = None

        return values

class DicomWebSourceConfigRead(BaseModel):
    id: int = Field(..., description="Internal database ID.")
    name: str = Field(..., validation_alias='source_name', description="Unique human-readable name.")
    description: Optional[str] = Field(None, description="Optional description.")
    base_url: str = Field(..., description="Base URL of the DICOMweb service.")
    qido_prefix: str = Field(..., description="Path prefix for QIDO-RS.")
    wado_prefix: str = Field(..., description="Path prefix for WADO-RS.")
    polling_interval_seconds: int = Field(..., description="Polling frequency in seconds.")
    is_enabled: bool = Field(..., description="Whether polling is active.")
    auth_type: AuthType = Field(..., description="Authentication method.") # Includes apikey now
    auth_config: Optional[Dict[str, Any]] = Field(None, description="Authentication credentials.")
    search_filters: Optional[Dict[str, Any]] = Field(None, description="QIDO-RS query parameters.")
    last_processed_timestamp: Optional[datetime] = Field(None, description="Timestamp of the last instance processed based on its DICOM date/time.")
    last_successful_run: Optional[datetime] = Field(None, description="Timestamp of the last successful polling cycle completion.")
    last_error_run: Optional[datetime] = Field(None, description="Timestamp of the last polling cycle that encountered an error.")
    last_error_message: Optional[str] = Field(None, description="Details of the last error encountered.")
    found_instance_count: int = Field(0, description="Total instances found by QIDO.")
    queued_instance_count: int = Field(0, description="Total instances queued for processing.")
    processed_instance_count: int = Field(0, description="Total instances successfully processed.")

    class Config:
        from_attributes = True # Enable ORM mode for Pydantic V2

