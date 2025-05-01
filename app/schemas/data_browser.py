# app/schemas/data_browser.py
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, model_validator, ConfigDict
from enum import Enum

# Use structlog if available, otherwise fallback for potential logging in validators
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# --- Query Level Enum ---
class QueryLevel(str, Enum):
    """Defines the DICOM Query/Retrieve levels."""
    STUDY = "STUDY"
    SERIES = "SERIES"
    INSTANCE = "INSTANCE"

# --- Query Parameter Schema ---
class DataBrowserQueryParam(BaseModel):
    """Represents a single query parameter (field=value)."""
    field: str # DICOM Keyword or Tag (e.g., "PatientName", "00100010")
    value: str # Value to match (string representation)

# --- Query Request Schema ---
class DataBrowserQueryRequest(BaseModel):
    """Request body for initiating a data browser query."""
    source_id: int = Field(..., description="Database ID of the source configuration to query.")
    source_type: Literal["dicomweb", "dimse-qr"] = Field(..., description="Type of the source to query.")
    query_parameters: List[DataBrowserQueryParam] = Field(default_factory=list, description="List of parameters to filter the query.")
    query_level: QueryLevel = Field(default=QueryLevel.STUDY, description="DICOM query level (STUDY, SERIES, INSTANCE).")

# --- Result Item Schema ---
# Represents a single study/series/instance record *after* transformation
# from raw DICOM JSON for easier frontend consumption.
class StudyResultItem(BaseModel):
    """Formatted result item for display (primarily STUDY level)."""
    # Allow extra fields captured from the source but not explicitly defined here
    model_config = ConfigDict(extra='allow')

    # Source Info (Added by service layer before response)
    source_id: int
    source_name: str
    source_type: Literal["dicomweb", "dimse-qr", "Unknown", "Error"] # Allow error state types

    # --- Common DICOM Fields (Mapped from Tags using Field alias) ---
    # Use Optional[...] = Field(None, alias="TAG_NUMBER_STR")
    # Required fields use Field(..., alias="TAG_NUMBER_STR")

    # Patient Module
    PatientName: Optional[str] = Field(None, alias="00100010")
    PatientID: Optional[str] = Field(None, alias="00100020")
    PatientBirthDate: Optional[str] = Field(None, alias="00100030")
    PatientSex: Optional[str] = Field(None, alias="00100040")

    # Study Module
    StudyInstanceUID: str = Field(..., alias="0020000D") # Study level queries MUST return this
    StudyDate: Optional[str] = Field(None, alias="00080020")
    StudyTime: Optional[str] = Field(None, alias="00080030")
    AccessionNumber: Optional[str] = Field(None, alias="00080050")
    StudyID: Optional[str] = Field(None, alias="00200010")
    StudyDescription: Optional[str] = Field(None, alias="00081030")
    # --- This field expects a List[str] ---
    ModalitiesInStudy: Optional[List[str]] = Field(None, alias="00080061")
    ReferringPhysicianName: Optional[str] = Field(None, alias="00080090")
    NumberOfStudyRelatedSeries: Optional[int] = Field(None, alias="00201206")
    NumberOfStudyRelatedInstances: Optional[int] = Field(None, alias="00201208")

    # Series Module (Add if supporting SERIES level queries later)
    # SeriesInstanceUID: Optional[str] = Field(None, alias="0020000E")
    # SeriesNumber: Optional[int] = Field(None, alias="00200011")
    # Modality: Optional[str] = Field(None, alias="00080060")
    # SeriesDescription: Optional[str] = Field(None, alias="0008103E")
    # NumberOfSeriesRelatedInstances: Optional[int] = Field(None, alias="00201209")

    # Instance Module (Add if supporting INSTANCE level queries later)
    # SOPInstanceUID: Optional[str] = Field(None, alias="00080018")
    # SOPClassUID: Optional[str] = Field(None, alias="00080016")
    # InstanceNumber: Optional[int] = Field(None, alias="00200013")


    # --- Model Validator (Corrected) ---
    # This validator now passes the *entire* Value list to Pydantic fields,
    # letting Pydantic handle coercion based on the field type (List[str] vs Optional[str]).


    @model_validator(mode='before')
    @classmethod
    def transform_dicom_json(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms raw DICOM JSON or partially flattened dict for validation."""
        if not isinstance(data, dict): return data

        processed_data = {}
        # Preserve metadata
        for meta_key in ["source_id", "source_name", "source_type"]:
            if meta_key in data: processed_data[meta_key] = data[meta_key]

        # Process potential DICOM tag fields
        for key, value_obj in data.items():
             if key in ["source_id", "source_name", "source_type"]: continue

             # --- *** NEW HANDLING *** ---
             final_value: Any = None # Value to pass to Pydantic validation

             # Case 1: Value is DICOM JSON structure {'vr': 'XX', 'Value': [...]}
             if isinstance(value_obj, dict) and 'Value' in value_obj and isinstance(value_obj['Value'], list):
                 value_list = value_obj['Value']
                 vr = value_obj.get('vr')

                 if not value_list:
                     final_value = None
                 elif vr == 'PN' and isinstance(value_list[0], dict) and 'Alphabetic' in value_list[0]:
                     final_value = value_list[0].get('Alphabetic')
                 # Keep list for ModalitiesInStudy if non-empty
                 elif key == "00080061" and value_list:
                     final_value = value_list
                 # Otherwise, if list has exactly one item, take that item
                 elif len(value_list) == 1:
                     final_value = value_list[0]
                 # If list has multiple items (and not Modalities), keep the list?
                 # Pydantic will fail if field isn't List[...]. Let's keep list for now.
                 elif len(value_list) > 1:
                      final_value = value_list
                 else: # Empty list already handled
                      final_value = None

             # Case 2: Value is ALREADY flattened (e.g., string, number, or list from C-FIND)
             # Or it's a {'vr': 'LO'} dict without 'Value' (empty element from C-FIND?)
             elif not (isinstance(value_obj, dict) and 'Value' in value_obj):
                 # If it's just {'vr': 'XX'} with no Value, treat as None/Empty
                 if isinstance(value_obj, dict) and 'vr' in value_obj and 'Value' not in value_obj:
                      final_value = None
                 else:
                      # Otherwise, assume it's already the correct type (str, int, list)
                      final_value = value_obj

             # Assign the processed value
             processed_data[key] = final_value
             # --- *** END NEW HANDLING *** ---

        return processed_data



# --- Query Response Schema ---
class DataBrowserQueryResponse(BaseModel):
    """Response structure for data browser queries."""
    query_status: Literal["success", "error", "partial"] = Field(..., description="Status of the query execution.")
    message: Optional[str] = Field(None, description="Optional message, especially on error or partial success.")
    source_id: int = Field(..., description="ID of the source that was queried.")
    source_name: str = Field(..., description="Name of the source that was queried.")
    source_type: Literal["dicomweb", "dimse-qr", "Unknown", "Error"] = Field(..., description="Type of the source that was queried.")
    # Results contain formatted items ready for display
    results: List[StudyResultItem] = Field(default_factory=list, description="List of study/series/instance results matching the query.")
