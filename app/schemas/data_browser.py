# app/schemas/data_browser.py
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime, date
import re
import logging

logger = logging.getLogger(__name__)

# --- Query Request Schemas (Keep as is) ---
AllowedQueryParam = Literal[
    "PatientName", "PatientID", "AccessionNumber", "StudyDate", "StudyTime",
    "ModalitiesInStudy", "ReferringPhysicianName", "StudyDescription",
    "PatientBirthDate",
]
class DataBrowserQueryParam(BaseModel):
    field: AllowedQueryParam = Field(..., description="DICOM tag keyword or standard query parameter.")
    value: str = Field(..., description="Value to search for (string format). Use * for wildcard where supported.")
class DataBrowserQueryRequest(BaseModel):
    source_id: int = Field(..., description="Database ID of the configured source.")
    query_parameters: List[DataBrowserQueryParam] = Field( default_factory=list, description="List of parameters to filter the query.")
    @field_validator('query_parameters')
    @classmethod
    def check_date_format(cls, params: List[DataBrowserQueryParam]):
        # ... (validation logic kept as is) ...
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
    # Field definitions with aliases for INPUT mapping remain the same
    PatientName: Optional[str] = Field(None, alias="00100010")
    PatientID: Optional[str] = Field(None, alias="00100020")
    StudyInstanceUID: str = Field(..., alias="0020000D")
    StudyDate: Optional[str] = Field(None, alias="00080020")
    StudyTime: Optional[str] = Field(None, alias="00080030")
    AccessionNumber: Optional[str] = Field(None, alias="00080050")
    ModalitiesInStudy: Optional[List[str]] = Field(None, alias="00080061")
    ReferringPhysicianName: Optional[str] = Field(None, alias="00080090")
    PatientBirthDate: Optional[str] = Field(None, alias="00100030")
    StudyDescription: Optional[str] = Field(None, alias="00081030")
    NumberOfStudyRelatedSeries: Optional[int] = Field(None, alias="00201206")
    NumberOfStudyRelatedInstances: Optional[int] = Field(None, alias="00201208")

    # Source info fields (no alias needed)
    source_id: int = Field(..., description="ID of the source queried.")
    source_name: str = Field(..., description="Name of the source queried.")
    source_type: str = Field(..., description="Type of the source queried (dicomweb or dimse-qr).")

    model_config = ConfigDict(
        from_attributes=True, # Allow creation from ORM/dataset attributes if needed elsewhere
        populate_by_name=True # NEED THIS to allow using aliases during input validation/parsing
        # by_alias=True IS NOT NEEDED HERE - we want default field names on output
    )

    # Validators remain the same (they operate before serialization)
    @field_validator(
        'PatientName', 'PatientID', 'StudyInstanceUID', 'StudyDate', 'StudyTime',
        'AccessionNumber', 'ReferringPhysicianName', 'PatientBirthDate', 'StudyDescription',
        mode='before'
    )
    @classmethod
    def flatten_dicom_value(cls, v: Any):
        # ... (implementation remains the same) ...
        if isinstance(v, dict) and 'Value' in v:
             value_list = v.get('Value') # Use .get for safety
             if isinstance(value_list, list) and len(value_list) > 0:
                 first_val = value_list[0]
                 if v.get('vr') == 'PN' and isinstance(first_val, dict) and 'Alphabetic' in first_val:
                     return first_val.get('Alphabetic')
                 return str(first_val) if first_val is not None else None
             else: return None
        elif isinstance(v, dict) and 'Value' not in v: return None
        elif isinstance(v, list) and len(v) > 0: return str(v[0]) if v[0] is not None else None
        elif isinstance(v, (str, int, float)): return str(v)
        elif v is None: return None
        else: logger.warning(f"Unhandled DICOM value type: {type(v)}, value: {v}"); return None


    @field_validator('ModalitiesInStudy', mode='before')
    @classmethod
    def process_modalities(cls, v: Any):
        # ... (implementation remains the same) ...
        value_list = None
        if isinstance(v, dict) and 'Value' in v: value_list = v.get('Value')
        elif isinstance(v, list): value_list = v
        elif isinstance(v, str): return [item.strip() for item in v.split('\\') if item.strip()]
        if isinstance(value_list, list): return [str(item) for item in value_list if item is not None]
        logger.warning(f"Could not process ModalitiesInStudy from: {v}"); return None


    @field_validator('NumberOfStudyRelatedSeries', 'NumberOfStudyRelatedInstances', mode='before')
    @classmethod
    def process_counts(cls, v: Any):
        # ... (implementation remains the same) ...
        value_to_parse = None
        if isinstance(v, dict) and 'Value' in v:
             value_list = v.get('Value')
             if isinstance(value_list, list) and len(value_list) > 0: value_to_parse = value_list[0]
        elif isinstance(v, (int, str, float)): value_to_parse = v
        if value_to_parse is not None:
             try:
                 if isinstance(value_to_parse, str) and '.' in value_to_parse: return int(float(value_to_parse))
                 return int(value_to_parse)
             except (ValueError, TypeError): logger.warning(f"Could not convert count value to int: {value_to_parse}"); return None
        return None

class DataBrowserQueryResponse(BaseModel):
    """Response containing the list of studies found."""
    query_status: Literal["success", "error", "partial"] = Field("success")
    message: Optional[str] = Field(None, description="Status message, e.g., describing errors.")
    source_id: int
    source_name: str
    source_type: str
    results: List[StudyResultItem] = Field(default_factory=list)

    # --- CORRECTED Config ---
    model_config = ConfigDict(
        # Remove by_alias=True - We want default field names in the final JSON output
        # Keep populate_by_name=True if StudyResultItem needs it for input validation flexibility
        populate_by_name=True
    )
    # --- END CORRECTED Config ---
