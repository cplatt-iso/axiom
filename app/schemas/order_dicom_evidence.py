# app/schemas/order_dicom_evidence.py
import json
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Dict, Any
from datetime import datetime


class OrderDicomEvidenceBase(BaseModel):
    """Base schema for Order DICOM Evidence with common fields."""
    sop_instance_uid: str = Field(..., max_length=128, description="SOP Instance UID of the processed DICOM object")
    study_instance_uid: Optional[str] = Field(None, max_length=128, description="Study Instance UID from the DICOM object")
    series_instance_uid: Optional[str] = Field(None, max_length=128, description="Series Instance UID from the DICOM object")
    accession_number: Optional[str] = Field(None, max_length=64, description="Accession number from the DICOM object")
    match_rule: str = Field(..., max_length=50, description="Rule that matched this DICOM to the order")
    applied_rule_names: Optional[str] = Field(None, description="Comma-separated list of rule names that processed this DICOM")
    applied_rule_ids: Optional[str] = Field(None, max_length=255, description="Comma-separated list of rule IDs")
    destination_results: Optional[str] = Field(None, description="JSON string containing destination results")
    processing_successful: bool = Field(True, description="Whether overall processing was successful")
    source_identifier: Optional[str] = Field(None, max_length=255, description="Source that provided this DICOM object")


class OrderDicomEvidenceCreate(OrderDicomEvidenceBase):
    """Schema for creating Order DICOM Evidence entries."""
    imaging_order_id: int = Field(..., description="ID of the imaging order this evidence belongs to")


class OrderDicomEvidenceUpdate(BaseModel):
    """Schema for updating Order DICOM Evidence entries."""
    applied_rule_names: Optional[str] = None
    applied_rule_ids: Optional[str] = None
    destination_results: Optional[str] = None
    processing_successful: Optional[bool] = None
    
    model_config = ConfigDict(extra='ignore')


class OrderDicomEvidenceRead(OrderDicomEvidenceBase):
    """Schema for reading Order DICOM Evidence entries."""
    id: int
    imaging_order_id: int
    processed_at: datetime
    created_at: datetime
    updated_at: datetime
    
    # Helper property to parse destination results
    def get_destination_results_dict(self) -> Dict[str, Any]:
        """Parse destination_results JSON string into a dictionary."""
        if not self.destination_results:
            return {}
        try:
            return json.loads(self.destination_results)
        except (json.JSONDecodeError, TypeError):
            return {}
    
    model_config = ConfigDict(from_attributes=True)


class OrderDicomEvidenceSummary(BaseModel):
    """Summary schema for showing evidence counts per order."""
    imaging_order_id: int
    total_dicom_objects: int
    successful_objects: int
    failed_objects: int
    unique_studies: int
    match_rules_used: list[str]
    destinations_attempted: list[str]
    first_processed_at: Optional[datetime] = None
    last_processed_at: Optional[datetime] = None
