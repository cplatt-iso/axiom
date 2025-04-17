# app/schemas/processing.py (New File or add to existing)
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class JsonProcessRequest(BaseModel):
    dicom_json: Dict[str, Any] = Field(..., description="DICOM header represented as JSON")
    ruleset_id: Optional[int] = Field(None, description="Specific RuleSet ID to apply (optional, uses active rulesets if None)")
    # Add source identifier if needed (see point 2)
    source_identifier: str = Field(default="api_json", description="Identifier for the source of this request")

class JsonProcessResponse(BaseModel):
    original_json: Dict[str, Any]
    morphed_json: Dict[str, Any]
    matched_ruleset_id: Optional[int]
    matched_rule_ids: List[int]
    errors: List[str] = []
