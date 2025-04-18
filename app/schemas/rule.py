# app/schemas/rule.py

from typing import List, Optional, Dict, Any, Union, Literal
# Use model_validator from pydantic directly
from pydantic import BaseModel, Field, field_validator, model_validator, StringConstraints
# Import Annotated for discriminated union metadata
from typing_extensions import Annotated
import enum
from datetime import datetime
import re
# No pydicom imports needed here anymore for VR validation
# import pydicom.valuerep
# from pydicom.datadict import _dictionary_vr

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
    PREPEND = "prepend"   # New
    SUFFIX = "suffix"     # New
    REGEX_REPLACE = "regex_replace" # New
    # ADD_ITEM = "add_item" # Future: For sequences
    # REMOVE_ITEM = "remove_item" # Future: For sequences


# --- Helper Validator Functions ---
def _validate_tag_format_or_keyword(v: str) -> str:
    """Shared validator for DICOM tag format or keyword."""
    v = v.strip()
    # Check for GGGG,EEEE format (flexible spaces, optional parens)
    if re.match(r"^\(?\s*[0-9a-fA-F]{4}\s*,\s*[0-9a-fA-F]{4}\s*\)?$", v):
        return v
    # Basic check for keyword (alphanumeric, no spaces)
    if re.match(r"^[a-zA-Z0-9]+$", v):
        return v
    raise ValueError("Tag must be in 'GGGG,EEEE' format (or 'GGGG, EEEE') or a valid DICOM keyword")

def _validate_vr_format(v: Optional[str]) -> Optional[str]:
    """Shared validator for VR format (checks format only)."""
    if v is not None:
        v = v.strip().upper()
        # Only check if it's two uppercase letters.
        # Delegate strict validation against known VRs to pydicom at runtime.
        if not re.match(r"^[A-Z]{2}$", v):
            raise ValueError("VR must be two uppercase letters")
        # *** REMOVED check against pydicom's internal dictionary ***
        # if v not in pydicom.datadict._dictionary_vr.DICT_VR:
        #    raise ValueError(f"Unknown or unsupported VR: {v}")
        return v
    return None


# --- Structures for Rules ---

class MatchCriterion(BaseModel):
    tag: str = Field(..., description="DICOM tag string, e.g., '0010,0010' or 'PatientName'")
    op: MatchOperation = Field(..., description="Matching operation to perform")
    value: Any = Field(None, description="Value to compare against (type depends on 'op', required for most ops)")

    # Re-use validator function
    _validate_tag = field_validator('tag')(_validate_tag_format_or_keyword)

    @model_validator(mode='after')
    def check_value_requirements(self) -> 'MatchCriterion':
        op = self.op
        value = self.value
        tag = self.tag

        value_required_ops = {
            MatchOperation.EQUALS, MatchOperation.NOT_EQUALS,
            MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN,
            MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL,
            MatchOperation.CONTAINS, MatchOperation.STARTS_WITH, MatchOperation.ENDS_WITH,
            MatchOperation.REGEX, MatchOperation.IN, MatchOperation.NOT_IN
        }

        if op in value_required_ops and value is None:
             raise ValueError(f"Value is required for operation '{op.value}' on tag '{tag}'")

        if op in [MatchOperation.IN, MatchOperation.NOT_IN] and not isinstance(value, list):
             raise ValueError(f"Value must be a list for operation '{op.value}' on tag '{tag}'")

        if op == MatchOperation.REGEX:
             if not isinstance(value, str):
                 raise ValueError(f"Value must be a string (regex pattern) for operation '{op.value}' on tag '{tag}'")
             try:
                 re.compile(value)
             except re.error as e:
                 raise ValueError(f"Invalid regex pattern for tag '{tag}': {e}")

        return self


# --- Tag Modification Schemas (Discriminated Union) ---

class TagModificationBase(BaseModel):
    tag: str = Field(..., description="DICOM tag string, e.g., '0010,0010' or 'PatientName'")
    _validate_tag = field_validator('tag')(_validate_tag_format_or_keyword)

class TagSetModification(TagModificationBase):
    action: Literal[ModifyAction.SET] = ModifyAction.SET
    value: Any = Field(..., description="New value for the tag")
    vr: Optional[str] = Field(None, max_length=2, description="Explicit VR (e.g., 'PN', 'DA') - strongly recommended if tag might not exist or to ensure correct type")
    # Use the simplified VR format validator
    _validate_vr = field_validator('vr')(_validate_vr_format)

class TagDeleteModification(TagModificationBase):
    action: Literal[ModifyAction.DELETE] = ModifyAction.DELETE

class TagPrependModification(TagModificationBase):
    action: Literal[ModifyAction.PREPEND] = ModifyAction.PREPEND
    value: str = Field(..., description="String value to prepend")
    @field_validator('value')
    @classmethod
    def check_value_is_string(cls, v):
        if not isinstance(v, str): raise ValueError("Value for 'prepend' action must be a string")
        return v

class TagSuffixModification(TagModificationBase):
    action: Literal[ModifyAction.SUFFIX] = ModifyAction.SUFFIX
    value: str = Field(..., description="String value to append (suffix)")
    @field_validator('value')
    @classmethod
    def check_value_is_string(cls, v):
        if not isinstance(v, str): raise ValueError("Value for 'suffix' action must be a string")
        return v

class TagRegexReplaceModification(TagModificationBase):
    action: Literal[ModifyAction.REGEX_REPLACE] = ModifyAction.REGEX_REPLACE
    pattern: str = Field(..., description="Python-compatible regex pattern to find")
    replacement: str = Field(..., description="Replacement string (can use capture groups like \\1)")

    @field_validator('pattern')
    @classmethod
    def check_pattern_is_valid_regex(cls, v):
        if not isinstance(v, str): raise ValueError("Pattern for 'regex_replace' action must be a string")
        try: re.compile(v)
        except re.error as e: raise ValueError(f"Invalid regex pattern: {e}")
        return v

    @field_validator('replacement')
    @classmethod
    def check_replacement_is_string(cls, v):
        if not isinstance(v, str): raise ValueError("Replacement for 'regex_replace' action must be a string")
        return v

TagModification = Annotated[
    Union[
        TagSetModification, TagDeleteModification, TagPrependModification,
        TagSuffixModification, TagRegexReplaceModification
    ],
    Field(discriminator="action")
]


class StorageDestination(BaseModel):
    type: str = Field(..., description="Storage type (e.g., 'filesystem', 'cstore', 'gcs')")
    config: Dict[str, Any] = Field(default_factory=dict, description="Backend-specific configuration (e.g., path, ae_title, bucket)")

    @model_validator(mode='after')
    def merge_type_into_config(self) -> 'StorageDestination':
        type_value = self.type
        config_value = self.config
        if not type_value: raise ValueError("'type' field cannot be empty")
        config_type_key, found_key = 'type', None
        for k in config_value.keys():
            if k.lower() == 'type': found_key = k; break
        if not found_key: self.config[config_type_key] = type_value
        elif config_value.get(found_key):
            if str(config_value[found_key]).lower() != str(type_value).lower():
                raise ValueError(f"Mismatch between outer 'type' ('{type_value}') and 'type' within config ('{config_value[found_key]}')")
        else: self.config[found_key] = type_value
        return self


# --- Rule Schemas ---

class RuleBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    match_criteria: List[MatchCriterion] = Field(default_factory=list)
    tag_modifications: List[TagModification] = Field(default_factory=list)
    destinations: List[StorageDestination] = Field(default_factory=list)
    applicable_sources: Optional[List[str]] = Field(None, description="List of source identifiers this rule applies to. Applies to all if null/empty.")

    @field_validator('applicable_sources')
    @classmethod
    def validate_sources_not_empty_strings(cls, v):
        if v is not None:
            if not isinstance(v, list): raise ValueError("applicable_sources must be a list of strings or null")
            if any(not isinstance(s, str) or not s.strip() for s in v): raise ValueError("Each item in applicable_sources must be a non-empty string")
        return v

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
    applicable_sources: Optional[List[str]] = Field(None, description="List of source identifiers this rule applies to. Set to null to apply to all.")
    _validate_sources = field_validator('applicable_sources')(RuleBase.validate_sources_not_empty_strings.__func__)

class RuleInDBBase(RuleBase):
    id: int
    ruleset_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    model_config = {"from_attributes": True}

class Rule(RuleInDBBase): pass

# --- RuleSet Schemas ---

class RuleSetBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    execution_mode: RuleSetExecutionMode = RuleSetExecutionMode.FIRST_MATCH

class RuleSetCreate(RuleSetBase): pass

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
    rules: List[Rule] = []
    model_config = {"from_attributes": True}

class RuleSet(RuleSetInDBBase): pass

class RuleSetSummary(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    execution_mode: RuleSetExecutionMode
    rule_count: int = Field(..., description="Number of rules in this ruleset")
    model_config = {"from_attributes": True}
