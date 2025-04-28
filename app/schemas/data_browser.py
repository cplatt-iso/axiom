# app/schemas/data_browser.py
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime, date
import re # Import re
import logging # Import logging

logger = logging.getLogger(__name__) # Setup logger

# --- Query Request Schema (Keep as is) ---
AllowedQueryParam = Literal[
    "PatientName", "PatientID", "AccessionNumber", "StudyDate", "StudyTime",
    "ModalitiesInStudy", "ReferringPhysicianName", "StudyDescription",
    "PatientBirthDate",
]
class DataBrowserQueryParam(BaseModel):
    field: AllowedQueryParam = Field(..., description="DICOM tag keyword or standard query parameter.")
    value: str = Field(..., description="Value to search for (string format). Use * for wildcard where supported.")
class DataBrowserQueryRequest(BaseModel):
    source_id: int = Field(..., description="Database ID of the configured source (DimseQueryRetrieveSource or DicomWebSourceState).")
    query_parameters: List[DataBrowserQueryParam] = Field( default_factory=list, description="List of parameters to filter the query (e.g., PatientName, StudyDate). If empty, may fetch recent studies based on source config or defaults." )
    @field_validator('query_parameters')
    @classmethod
    def check_date_format(cls, params: List[DataBrowserQueryParam]):
        for param in params:
             if param.field.lower() == "studydate":
                 if not re.match(r"^(\d{8}|\d{8}-|-\d{8}|\d{8}-\d{8}|TODAY|YESTERDAY|-\d+[dD])$", param.value.upper()):
                     raise ValueError(f"Invalid StudyDate format '{param.value}'. Use YYYYMMDD, YYYYMMDD-, etc.")
             elif param.field.lower() == "patientbirthdate":
                  if not re.match(r"^\d{8}$", param.value):
                     raise ValueError(f"Invalid PatientBirthDate format '{param.value}'. Use YYYYMMDD.")
        return params

# --- Query Response Schema ---

class StudyResultItem(BaseModel):
    """Represents a single study found in the query results."""
    PatientName: Optional[str] = Field(None, alias="00100010")
    PatientID: Optional[str] = Field(None, alias="00100020")
    StudyInstanceUID: str = Field(..., alias="0020000D") # Required
    StudyDate: Optional[str] = Field(None, alias="00080020")
    StudyTime: Optional[str] = Field(None, alias="00080030")
    AccessionNumber: Optional[str] = Field(None, alias="00080050")
    ModalitiesInStudy: Optional[List[str]] = Field(None, alias="00080061")
    ReferringPhysicianName: Optional[str] = Field(None, alias="00080090")
    PatientBirthDate: Optional[str] = Field(None, alias="00100030")
    StudyDescription: Optional[str] = Field(None, alias="00081030")
    NumberOfStudyRelatedSeries: Optional[int] = Field(None, alias="00201206")
    NumberOfStudyRelatedInstances: Optional[int] = Field(None, alias="00201208")

    # Source info fields (no alias needed as they are added manually)
    source_id: int = Field(..., description="ID of the source queried.")
    source_name: str = Field(..., description="Name of the source queried.")
    source_type: str = Field(..., description="Type of the source queried (dicomweb or dimse-qr).")

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        # --- ADDED: Ensure aliases are used during serialization ---
        by_alias=True,
        # --- END ADDED ---
    )

    # --- UPDATED: More robust validators ---
    @field_validator(
        'PatientName', 'PatientID', 'StudyInstanceUID', 'StudyDate', 'StudyTime',
        'AccessionNumber', 'ReferringPhysicianName', 'PatientBirthDate', 'StudyDescription',
        mode='before'
    )
    @classmethod
    def flatten_dicom_value(cls, v: Any):
        """Attempts to extract a single string value from various DICOM JSON representations."""
        if isinstance(v, dict) and 'Value' in v:
             value_list = v.get('Value') # Use .get for safety
             if isinstance(value_list, list) and len(value_list) > 0:
                 first_val = value_list[0]
                 # Handle Person Name structure
                 if v.get('vr') == 'PN' and isinstance(first_val, dict) and 'Alphabetic' in first_val:
                     return first_val.get('Alphabetic') # Use .get
                 # Handle other potential nested structures if needed, otherwise convert first item
                 return str(first_val) if first_val is not None else None
             else:
                 # Value key exists but list is empty or value is None
                 return None
        elif isinstance(v, dict) and 'Value' not in v:
             # Handle cases like {'vr': 'LO'} where value is implicitly null
             return None
        elif isinstance(v, list) and len(v) > 0:
             # Handle case where it's just a list ['value']
             return str(v[0]) if v[0] is not None else None
        elif isinstance(v, (str, int, float)):
             # Handle simple values directly passed
             return str(v)
        elif v is None:
             return None
        else:
             logger.warning(f"Unhandled DICOM value type in validator: {type(v)}, value: {v}")
             return None # Or raise error if strictness required

    @field_validator('ModalitiesInStudy', mode='before')
    @classmethod
    def process_modalities(cls, v: Any):
        """Extracts a list of strings for modalities."""
        value_list = None
        if isinstance(v, dict) and 'Value' in v:
             value_list = v.get('Value')
        elif isinstance(v, list):
             value_list = v
        elif isinstance(v, str):
             # Handle potential backslash-separated values from C-FIND
             return [item.strip() for item in v.split('\\') if item.strip()]

        if isinstance(value_list, list):
             return [str(item) for item in value_list if item is not None]

        logger.warning(f"Could not process ModalitiesInStudy from: {v}")
        return None

    @field_validator('NumberOfStudyRelatedSeries', 'NumberOfStudyRelatedInstances', mode='before')
    @classmethod
    def process_counts(cls, v: Any):
        """Extracts an integer count."""
        value_to_parse = None
        if isinstance(v, dict) and 'Value' in v:
             value_list = v.get('Value')
             if isinstance(value_list, list) and len(value_list) > 0:
                 value_to_parse = value_list[0]
        elif isinstance(v, (int, str, float)): # Allow float conversion?
             value_to_parse = v

        if value_to_parse is not None:
             try:
                 # Handle potential float strings before int conversion
                 if isinstance(value_to_parse, str) and '.' in value_to_parse:
                      return int(float(value_to_parse))
                 return int(value_to_parse)
             except (ValueError, TypeError):
                 logger.warning(f"Could not convert count value to int: {value_to_parse}")
                 return None
        return None
    # --- END UPDATED Validators ---

class DataBrowserQueryResponse(BaseModel):
    """Response containing the list of studies found."""
    query_status: Literal["success", "error", "partial"] = Field("success")
    message: Optional[str] = Field(None, description="Status message, e.g., describing errors.")
    source_id: int
    source_name: str
    source_type: str
    results: List[StudyResultItem] = Field(default_factory=list)

    # --- ADDED Config for alias usage ---
    model_config = ConfigDict(
        # --- IMPORTANT: Set by_alias=True for serialization ---
        by_alias=True,
        populate_by_name=True # Keep this for validation flexibility
    )
    # --- END ADDED ---
