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
    tag: str = Field(..., description="DICOM tag string, e.g., '0010,0010' or 'PatientName'") # Updated example
    op: MatchOperation = Field(..., description="Matching operation to perform")
    value: Any = Field(None, description="Value to compare against (type depends on 'op', required for most ops)")
    # Future: Add case_sensitive: bool = Field(True) ?

    @field_validator('tag')
    @classmethod
    def validate_tag_format_or_keyword(cls, v):
        v = v.strip()
        # Check for GGGG,EEEE format (flexible spaces, optional parens)
        if re.match(r"^\(?\s*[0-9a-fA-F]{4}\s*,\s*[0-9a-fA-F]{4}\s*\)?$", v):
            # Normalize to '(GGGG,EEEE)' ? Or leave as is? Leaving as is for now.
             return v
        # Basic check for keyword (alphanumeric, no spaces)
        # This isn't perfect validation but prevents obvious errors
        if re.match(r"^[a-zA-Z0-9]+$", v):
             # We can't easily check if it's a *valid* keyword here without pydicom
             # Assume it's okay for schema validation, let backend handle unknown keywords
             return v
        raise ValueError("Tag must be in 'GGGG,EEEE' format (or 'GGGG, EEEE') or a valid DICOM keyword")

    # Corrected model_validator signature for Pydantic v2
    @model_validator(mode='after')
    def check_value_required(self) -> 'MatchCriterion':
        op_value = self.op # Access attributes directly via self in 'after' validator
        value_value = self.value
        tag_value = self.tag

        # Operations requiring a value
        value_required_ops = {
            MatchOperation.EQUALS, MatchOperation.NOT_EQUALS,
            MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN,
            MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL,
            MatchOperation.CONTAINS, MatchOperation.STARTS_WITH, MatchOperation.ENDS_WITH,
            MatchOperation.REGEX, MatchOperation.IN, MatchOperation.NOT_IN
        }

        if op_value in value_required_ops and value_value is None:
             raise ValueError(f"Value is required for operation '{op_value.value}' on tag '{tag_value}'")

        if op_value in [MatchOperation.IN, MatchOperation.NOT_IN] and not isinstance(value_value, list):
             raise ValueError(f"Value must be a list for operation '{op_value.value}' on tag '{tag_value}'")

        if op_value == MatchOperation.REGEX and not isinstance(value_value, str):
            raise ValueError(f"Value must be a string (regex pattern) for operation '{op_value.value}' on tag '{tag_value}'")
        # Optional: Add validation for regex compilability?
        # try:
        #    if op_value == MatchOperation.REGEX: re.compile(value_value)
        # except re.error as e:
        #    raise ValueError(f"Invalid regex pattern for tag '{tag_value}': {e}")

        return self # Return the instance


class TagModification(BaseModel):
    action: ModifyAction = Field(..., description="Action to perform (set or delete)")
    tag: str = Field(..., description="DICOM tag string, e.g., '0010,0010' or 'PatientName'") # Updated example
    value: Any = Field(None, description="New value (required for 'set' action)")
    vr: Optional[str] = Field(None, max_length=2, description="Explicit VR (e.g., 'PN', 'DA') - recommended for 'set' if tag might not exist")

    @field_validator('tag')
    @classmethod
    def validate_tag_format_or_keyword(cls, v):
        v = v.strip()
        # Check for GGGG,EEEE format (flexible spaces, optional parens)
        if re.match(r"^\(?\s*[0-9a-fA-F]{4}\s*,\s*[0-9a-fA-F]{4}\s*\)?$", v):
             return v
        # Basic check for keyword
        if re.match(r"^[a-zA-Z0-9]+$", v):
             return v
        raise ValueError("Tag must be in 'GGGG,EEEE' format (or 'GGGG, EEEE') or a valid DICOM keyword")

    @field_validator('vr')
    @classmethod
    def validate_vr_format(cls, v):
        if v is not None:
             v = v.strip().upper()
             if not re.match(r"^[A-Z]{2}$", v):
                  raise ValueError("VR must be two uppercase letters")
             # Optional: Check against known VRs? Could use pydicom.valuerep.SUPPORTED_VR
             # if v not in pydicom.valuerep.SUPPORTED_VR: raise ValueError(f"Unknown VR: {v}")
             return v
        return None

    # Corrected model_validator signature for Pydantic v2
    @model_validator(mode='after')
    def check_value_for_set(self) -> 'TagModification':
        action_value = self.action # Access attributes via self
        value_value = self.value
        tag_value = self.tag

        if action_value == ModifyAction.SET and value_value is None:
            raise ValueError(f"Value is required for 'set' action on tag '{tag_value}'")
        # Can add more validation here later based on VR if needed
        return self # Return the instance


class StorageDestination(BaseModel):
    type: str = Field(..., description="Storage type (e.g., 'filesystem', 'cstore', 'gcs')")
    config: Dict[str, Any] = Field(default_factory=dict, description="Backend-specific configuration (e.g., path, ae_title, bucket)")

    # Corrected model_validator signature for Pydantic v2
    @model_validator(mode='after')
    def merge_type_into_config(self) -> 'StorageDestination':
        type_value = self.type
        config_value = self.config

        if not type_value: # Ensure type is not empty
             raise ValueError("'type' field cannot be empty")

        # Ensure 'type' is also within the config dict for the backend factory
        if 'type' not in config_value:
             self.config['type'] = type_value # Modify self directly
        elif config_value.get('type'): # Only compare if type exists in config
             if str(config_value['type']).lower() != str(type_value).lower(): # Robust comparison
                  raise ValueError(f"Mismatch between outer 'type' ('{type_value}') and 'type' within config ('{config_value['type']}')")
        else: # If 'type' key exists but is None/empty in config, set it from outer
             self.config['type'] = type_value

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
    # --- NEW FIELD ---
    applicable_sources: Optional[List[str]] = Field(
        None, # Defaults to None (meaning applies to all)
        description="List of source identifiers this rule applies to (e.g., 'scp_listener_a', 'api_json'). Applies to all if null/empty."
    )
    # --- END NEW FIELD ---

    @field_validator('applicable_sources')
    @classmethod
    def validate_sources_not_empty_strings(cls, v):
        if v is not None:
            if not isinstance(v, list):
                 raise ValueError("applicable_sources must be a list of strings or null")
            if any(not isinstance(s, str) or not s.strip() for s in v):
                raise ValueError("Each item in applicable_sources must be a non-empty string")
            # Optional: Validate against known sources from config?
            # known_sources = set(settings.KNOWN_INPUT_SOURCES)
            # unknown = [s for s in v if s not in known_sources]
            # if unknown:
            #    raise ValueError(f"Unknown source identifiers found: {', '.join(unknown)}")
        return v


class RuleCreate(RuleBase):
    ruleset_id: int

class RuleUpdate(BaseModel):
    # Make all fields optional for updates
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    match_criteria: Optional[List[MatchCriterion]] = None
    tag_modifications: Optional[List[TagModification]] = None
    destinations: Optional[List[StorageDestination]] = None
    # --- NEW FIELD (Optional for Update) ---
    applicable_sources: Optional[List[str]] = Field(
        None, # Allows setting it back to null or providing a new list
        description="List of source identifiers this rule applies to. Set to null to apply to all."
    )

    # Add validator for applicable_sources here too, reusing the one from RuleBase
    _validate_sources = field_validator('applicable_sources')(RuleBase.validate_sources_not_empty_strings.__func__) # Reuse validator


class RuleInDBBase(RuleBase):
    id: int
    ruleset_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    model_config = { # Pydantic v2 config class name
        "from_attributes": True
    }


class Rule(RuleInDBBase):
    """Schema for returning a Rule from the API."""
    pass


# --- RuleSet Schemas ---

class RuleSetBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    execution_mode: RuleSetExecutionMode = RuleSetExecutionMode.FIRST_MATCH

class RuleSetCreate(RuleSetBase):
    # Defaults are inherited from RuleSetBase
    pass # No different fields needed currently

class RuleSetUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    execution_mode: Optional[RuleSetExecutionMode] = None

class RuleSetInDBBase(RuleSetBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    # Eagerly load rules when fetching a ruleset detail? Depends on API design.
    # If RuleSet detail endpoint includes rules, this should be List[Rule]
    # If RuleSet list endpoint doesn't include rules, make separate summary schema.
    rules: List[Rule] = []

    model_config = { # Pydantic v2 config class name
        "from_attributes": True
    }

class RuleSet(RuleSetInDBBase):
    """Schema for returning a full RuleSet (including Rules) from the API."""
    pass

class RuleSetSummary(BaseModel):
    """Schema for returning a RuleSet overview (e.g., in lists)."""
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    execution_mode: RuleSetExecutionMode
    rule_count: int = Field(..., description="Number of rules in this ruleset") # Add rule count

    model_config = { # Pydantic v2 config class name
        "from_attributes": True
    }
