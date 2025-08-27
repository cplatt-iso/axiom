# app/schemas/data_browser.py
from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field, model_validator, ConfigDict
from enum import Enum

try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

class QueryLevel(str, Enum):
    """Defines the DICOM Query/Retrieve levels."""
    STUDY = "STUDY"
    SERIES = "SERIES"
    INSTANCE = "INSTANCE"

class DataBrowserQueryParam(BaseModel):
    """Represents a single query parameter (field=value)."""
    field: str
    value: str

class DataBrowserQueryRequest(BaseModel):
    """Request body for initiating a data browser query."""
    source_id: int = Field(..., description="Database ID of the source configuration to query.")
    # --- CORRECTED LITERAL ---
    source_type: Literal["dicomweb", "dimse-qr", "google_healthcare"] = Field(..., description="Type of the source to query.")
    # --- END CORRECTION ---
    query_parameters: List[DataBrowserQueryParam] = Field(default_factory=list, description="List of parameters to filter the query.")
    query_level: QueryLevel = Field(default=QueryLevel.STUDY, description="DICOM query level (STUDY, SERIES, INSTANCE).")


class StudyResultItem(BaseModel):
    """Formatted result item for display (primarily STUDY level)."""
    model_config = ConfigDict(extra='allow')

    source_id: int
    source_name: str
    # Make sure this literal includes all expected types including GHC
    source_type: Literal["dicomweb", "dimse-qr", "google_healthcare", "Unknown", "Error"]

    PatientName: Optional[str] = Field(None, alias="00100010")
    PatientID: Optional[str] = Field(None, alias="00100020")
    PatientBirthDate: Optional[str] = Field(None, alias="00100030")
    PatientSex: Optional[str] = Field(None, alias="00100040")
    StudyInstanceUID: str = Field(..., alias="0020000D")
    StudyDate: Optional[str] = Field(None, alias="00080020")
    StudyTime: Optional[str] = Field(None, alias="00080030")
    AccessionNumber: Optional[str] = Field(None, alias="00080050")
    StudyID: Optional[str] = Field(None, alias="00200010")
    StudyDescription: Optional[str] = Field(None, alias="00081030")
    ModalitiesInStudy: Optional[List[str]] = Field(None, alias="00080061")
    ReferringPhysicianName: Optional[str] = Field(None, alias="00080090")
    NumberOfStudyRelatedSeries: Optional[int] = Field(None, alias="00201206")
    NumberOfStudyRelatedInstances: Optional[int] = Field(None, alias="00201208")


    @model_validator(mode='before')
    @classmethod
    def transform_dicom_json(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transforms raw DICOM JSON or partially flattened dict for validation."""
        if not isinstance(data, dict):
             return data

        processed_data = {}
        for meta_key in ["source_id", "source_name", "source_type"]:
            if meta_key in data:
                 processed_data[meta_key] = data[meta_key]

        for key, value_obj in data.items():
             if key in ["source_id", "source_name", "source_type"]:
                 continue

             final_value: Any = None

             if isinstance(value_obj, dict) and 'Value' in value_obj and isinstance(value_obj['Value'], list):
                 value_list = value_obj['Value']
                 vr = value_obj.get('vr')

                 if not value_list:
                     final_value = None
                 elif vr == 'PN' and isinstance(value_list[0], dict) and 'Alphabetic' in value_list[0]:
                     final_value = value_list[0].get('Alphabetic')
                 elif key == "00080061" and value_list:
                     final_value = value_list
                 elif len(value_list) == 1:
                     final_value = value_list[0]
                 elif len(value_list) > 1:
                      final_value = value_list
                 else:
                      final_value = None

             elif not (isinstance(value_obj, dict) and 'Value' in value_obj):
                 if isinstance(value_obj, dict) and 'vr' in value_obj and 'Value' not in value_obj:
                      final_value = None
                 else:
                      final_value = value_obj

             processed_data[key] = final_value

        return processed_data

class DataBrowserQueryResponse(BaseModel):
    """Response structure for data browser queries."""
    query_status: Literal["success", "error", "partial"] = Field(..., description="Status of the query execution.")
    message: Optional[str] = Field(None, description="Optional message, especially on error or partial success.")
    source_id: int = Field(..., description="ID of the source that was queried.")
    source_name: str = Field(..., description="Name of the source that was queried.")
    # Make sure this includes all types the service might add, including error states
    source_type: Literal["dicomweb", "dimse-qr", "google_healthcare", "Unknown", "Error"] = Field(..., description="Type of the source that was queried.")
    results: List[StudyResultItem] = Field(default_factory=list, description="List of study/series/instance results matching the query.")
