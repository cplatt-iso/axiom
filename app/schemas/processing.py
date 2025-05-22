# app/schemas/processing.py
from pydantic import BaseModel, Field
# --- Ensure List is imported ---
from typing import Dict, Any, Optional, List
# --- End Import ---

class JsonProcessRequest(BaseModel):
    """Schema for requesting JSON-based DICOM header processing.""" # Added docstring
    dicom_json: Dict[str, Any] = Field(
        ...,
        # Updated description and added example
        description="DICOM header represented as JSON (e.g., from pydicom.dataset.Dataset.to_json_dict)",
        json_schema_extra={"example": {"00100010": {"vr": "PN", "Value": [{"Alphabetic": "Doe^John"}]}}}
    )
    ruleset_id: Optional[int] = Field(
        None,
        description="Optional: Specific RuleSet ID to apply. If None, active rulesets are evaluated based on priority and execution mode." # Expanded description
    )
    source_identifier: str = Field(
        default="api_json",
        description="Identifier for the source of this request (used for rule matching)." # Slightly clearer description
    )
    # Optional: Add dry_run: bool = False later?

class JsonProcessResponse(BaseModel):
    """Schema for the response of JSON-based DICOM header processing.""" # Added docstring
    original_json: Dict[str, Any] = Field(..., description="The original DICOM JSON received.") # Added description
    morphed_json: Dict[str, Any] = Field(..., description="The DICOM JSON after applying matching rules and modifications.") # Added description
    # Renamed matched_ -> applied_ for clarity
    applied_ruleset_id: Optional[int] = Field(None, description="ID of the RuleSet that was primarily applied (e.g., first matched in FIRST_MATCH mode).")
    applied_rule_ids: List[int] = Field(default_factory=list, description="List of IDs of the Rules that were matched and applied.")
    source_identifier: str = Field(..., description="The source identifier used for processing.") # Added source identifier to response
    # Use default_factory for errors list
    errors: List[str] = Field(default_factory=list, description="List of any errors encountered during processing.")
    # warnings: List[str] = Field(default_factory=list, description="List of non-fatal warnings during processing.") # Optional: Add warnings later`
