# app/schemas/rule.py

from typing import List, Optional, Dict, Any, Union
# Use model_validator from pydantic directly
from pydantic import BaseModel, Field, field_validator, model_validator
import enum
from datetime import datetime
import re

# --- Enums ---
class RuleSetExecutionMode(str, enum.Enum):
    FIRST_MATCH = "FIRST_MATCH"
    ALL_MATCHES = "ALL_MATCHES"

class MatchOperation(str, enum.Enum):
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    LESS_THAN = "lt"
    GREATER_EQUAL = "ge"
    LESS_EQUAL = "le"
    CONTAINS = "contains"
    STARTS_WITH = "startswith"
    ENDS_WITH = "endswith"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"
    REGEX = "regex"
    IN = "in" # Value should be a list
    NOT_IN = "not_in" # Value should be a list

class ModifyAction(str, enum.Enum):
    SET = "set"
    DELETE = "delete"
    # ADD_ITEM = "add_item" # Future: For sequences
    # REMOVE_ITEM = "remove_item" # Future: For sequences

# --- Structures for Rules ---

class MatchCriterion(BaseModel):
    tag: str = Field(..., description="DICOM tag string, e.g., '(0010,0010)'")
    op: MatchOperation = Field(..., description="Matching operation to perform")
    value: Any = Field(None, description="Value to compare against (type depends on 'op', required for most ops)")

    @field_validator('tag')
    @classmethod
    def validate_tag_format(cls, v):
        if not re.match(r"^\(\s*[0-9a-fA-F]{4}\s*,\s*[0-9a-fA-F]{4}\s*\)$", v):
             raise ValueError("Tag must be in the format '(gggg,eeee)'")
        return v

    # Corrected model_validator signature for Pydantic v2
    @model_validator(mode='after')
    def check_value_required(self) -> 'MatchCriterion':
        op_value = self.op # Access attributes directly via self in 'after' validator
        value_value = self.value
        tag_value = self.tag

        if op_value not in [MatchOperation.EXISTS, MatchOperation.NOT_EXISTS] and value_value is None:
             raise ValueError(f"Value is required for operation '{op_value.value}' on tag '{tag_value}'")
        if op_value in [MatchOperation.IN, MatchOperation.NOT_IN] and not isinstance(value_value, list):
             raise ValueError(f"Value must be a list for operation '{op_value.value}' on tag '{tag_value}'")
        return self # Return the instance


class TagModification(BaseModel):
    action: ModifyAction = Field(..., description="Action to perform (set or delete)")
    tag: str = Field(..., description="DICOM tag string, e.g., '(0010,0010)'")
    value: Any = Field(None, description="New value (required for 'set' action)")
    vr: Optional[str] = Field(None, max_length=2, description="Explicit VR (e.g., 'PN', 'DA') - strongly recommended for 'set' if tag might not exist")

    @field_validator('tag')
    @classmethod
    def validate_tag_format(cls, v):
        if not re.match(r"^\(\s*[0-9a-fA-F]{4}\s*,\s*[0-9a-fA-F]{4}\s*\)$", v):
             raise ValueError("Tag must be in the format '(gggg,eeee)'")
        return v

    @field_validator('vr')
    @classmethod
    def validate_vr_format(cls, v):
        if v is not None and not re.match(r"^[A-Z]{2}$", v):
             raise ValueError("VR must be two uppercase letters")
        return v.upper() if v else None

    # Corrected model_validator signature for Pydantic v2
    @model_validator(mode='after')
    def check_value_for_set(self) -> 'TagModification':
        action_value = self.action # Access attributes via self
        value_value = self.value
        tag_value = self.tag

        if action_value == ModifyAction.SET and value_value is None:
            raise ValueError(f"Value is required for 'set' action on tag '{tag_value}'")
        return self # Return the instance


class StorageDestination(BaseModel):
    type: str = Field(..., description="Storage type (e.g., 'filesystem', 'cstore', 'gcs')")
    config: Dict[str, Any] = Field(default_factory=dict, description="Backend-specific configuration (e.g., path, ae_title, bucket)")

    # Corrected model_validator signature for Pydantic v2
    @model_validator(mode='after')
    def merge_type_into_config(self) -> 'StorageDestination':
        # Access via self
        type_value = self.type
        config_value = self.config

        # Ensure 'type' is also within the config dict for the backend factory
        if 'type' not in config_value:
             self.config['type'] = type_value # Modify self directly
        elif config_value.get('type', '').lower() != type_value.lower(): # Safer access to type in config
             raise ValueError("Mismatch between 'type' field and 'type' within config")
        return self # Return the instance


# --- Rule Schemas (Using corrected definitions above) ---

class RuleBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    match_criteria: List[MatchCriterion] = Field(default_factory=list, description="List of criteria (implicit AND)")
    tag_modifications: List[TagModification] = Field(default_factory=list, description="List of modifications to apply")
    destinations: List[StorageDestination] = Field(default_factory=list, description="List of storage destinations")

class RuleCreate(RuleBase):
    ruleset_id: int

class RuleUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    match_criteria: Optional[List[MatchCriterion]] = None
    tag_modifications: Optional[List[TagModification]] = None
    destinations: Optional[List[StorageDestination]] = None

class RuleInDBBase(RuleBase):
    id: int
    ruleset_id: int
    created_at: datetime # Changed to required as per Base model
    updated_at: Optional[datetime] = None
    class Config:
        from_attributes = True # Pydantic v2

class Rule(RuleInDBBase):
    pass


# --- RuleSet Schemas ---

class RuleSetBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0 # Removed default from here, should be set in RuleSetCreate if needed
    execution_mode: RuleSetExecutionMode = RuleSetExecutionMode.FIRST_MATCH

class RuleSetCreate(RuleSetBase):
    # Default values are defined in RuleSetBase or can be set here if different for create
    priority: int = 0 # Explicitly setting default here for clarity

class RuleSetUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    execution_mode: Optional[RuleSetExecutionMode] = None

class RuleSetInDBBase(RuleSetBase):
    id: int
    created_at: datetime # Changed to required
    updated_at: Optional[datetime] = None
    rules: List[Rule] = []
    class Config:
        from_attributes = True # Pydantic v2

# *** This is the class that was failing to import ***
class RuleSet(RuleSetInDBBase): # Definition exists here
    pass

class RuleSetSummary(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    execution_mode: RuleSetExecutionMode
    rule_count: int
    class Config:
        from_attributes = True # Pydantic v2
