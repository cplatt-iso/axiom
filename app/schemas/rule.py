# app/schemas/rule.py

from typing import List, Optional, Dict, Any, Union, Literal
from pydantic import BaseModel, Field, field_validator, model_validator, StringConstraints, ConfigDict
from typing_extensions import Annotated
import enum
from datetime import datetime
import re
# --- Import StorageBackendConfigRead ---
from .storage_backend_config import StorageBackendConfigRead
# --- Import ScheduleRead ---
from .schedule import ScheduleRead

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
    IP_ADDRESS_EQUALS = "ip_eq"
    IP_ADDRESS_STARTS_WITH = "ip_startswith"
    IP_ADDRESS_IN_SUBNET = "ip_in_subnet"

class ModifyAction(str, enum.Enum):
    SET = "set"
    DELETE = "delete"
    PREPEND = "prepend"
    SUFFIX = "suffix"
    REGEX_REPLACE = "regex_replace"
    COPY = "copy"
    MOVE = "move"
    CROSSWALK = "crosswalk"


# --- Helper Validator Functions ---
def _validate_tag_format_or_keyword(v: str) -> str:
    """Shared validator for DICOM tag format or keyword."""
    if not isinstance(v, str): raise ValueError("Tag must be a string")
    v = v.strip()
    # Allow explicit keyword 'SOURCE_IP', 'CALLING_AE', 'CALLED_AE' for association matching
    if v.upper() in ['SOURCE_IP', 'CALLING_AE_TITLE', 'CALLED_AE_TITLE']: return v.upper()
    # Original tag validation
    if re.match(r"^\(?\s*[0-9a-fA-F]{4}\s*,\s*[0-9a-fA-F]{4}\s*\)?$", v): return v
    if re.match(r"^[a-zA-Z0-9]+$", v): return v
    raise ValueError("Tag must be in 'GGGG,EEEE' format, a valid DICOM keyword, or one of 'SOURCE_IP', 'CALLING_AE_TITLE', 'CALLED_AE_TITLE'")

def _validate_vr_format(v: Optional[str]) -> Optional[str]:
    """Shared validator for VR format (checks format only)."""
    if v is not None:
        if not isinstance(v, str): raise ValueError("VR must be a string")
        v = v.strip().upper()
        if not re.match(r"^[A-Z]{2}$", v):
            raise ValueError("VR must be two uppercase letters")
        return v
    return None

# --- Association Match Criterion ---
class AssociationMatchCriterion(BaseModel):
    """Defines criteria for matching against DICOM Association info."""
    parameter: Literal['SOURCE_IP', 'CALLING_AE_TITLE', 'CALLED_AE_TITLE'] = Field(..., description="Association parameter to match.")
    op: MatchOperation = Field(..., description="Matching operation.")
    value: Any = Field(..., description="Value to compare against.")

    @model_validator(mode='after')
    def check_value_for_op(self) -> 'AssociationMatchCriterion':
        if self.parameter == 'SOURCE_IP' and self.op == MatchOperation.IP_ADDRESS_IN_SUBNET:
            if not isinstance(self.value, str) or '/' not in self.value:
                raise ValueError("Value for 'ip_in_subnet' must be a CIDR string (e.g., '192.168.1.0/24').")
        return self

# --- Match Criterion ---
class MatchCriterion(BaseModel):
    tag: str = Field(..., description="DICOM tag string (e.g., '0010,0010' or 'PatientName').")
    op: MatchOperation = Field(..., description="Matching operation.")
    value: Any = Field(None, description="Value to compare against (type depends on 'op', required for most ops).")

    @field_validator('tag')
    @classmethod
    def validate_dicom_tag(cls, v: str) -> str:
        v_strip = v.strip()
        v_upper = v_strip.upper()
        if v_upper in ['SOURCE_IP', 'CALLING_AE_TITLE', 'CALLED_AE_TITLE']:
            raise ValueError(f"'{v_upper}' cannot be used in standard MatchCriterion, use AssociationMatchCriterion instead.")
        return _validate_tag_format_or_keyword(v_strip)


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

        ip_ops = {MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET}
        if op in ip_ops:
             raise ValueError(f"Operation '{op.value}' is only valid for Association parameter 'SOURCE_IP'.")

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
    pass

class TagSetModification(TagModificationBase):
    action: Literal[ModifyAction.SET] = ModifyAction.SET
    tag: str = Field(..., description="DICOM tag to set.")
    value: Any = Field(..., description="New value for the tag.")
    vr: Optional[str] = Field(None, max_length=2, description="Explicit VR (e.g., 'PN', 'DA') - strongly recommended.")
    _validate_tag = field_validator('tag')(_validate_tag_format_or_keyword)
    _validate_vr = field_validator('vr')(_validate_vr_format)

class TagDeleteModification(TagModificationBase):
    action: Literal[ModifyAction.DELETE] = ModifyAction.DELETE
    tag: str = Field(..., description="DICOM tag to delete.")
    _validate_tag = field_validator('tag')(_validate_tag_format_or_keyword)

class TagPrependModification(TagModificationBase):
    action: Literal[ModifyAction.PREPEND] = ModifyAction.PREPEND
    tag: str = Field(..., description="DICOM tag to prepend to.")
    value: str = Field(..., description="String value to prepend.")
    _validate_tag = field_validator('tag')(_validate_tag_format_or_keyword)
    @field_validator('value')
    @classmethod
    def check_value_is_string_prepend(cls, v):
        if not isinstance(v, str): raise ValueError("Value for 'prepend' action must be a string")
        return v

class TagSuffixModification(TagModificationBase):
    action: Literal[ModifyAction.SUFFIX] = ModifyAction.SUFFIX
    tag: str = Field(..., description="DICOM tag to append to.")
    value: str = Field(..., description="String value to append (suffix).")
    _validate_tag = field_validator('tag')(_validate_tag_format_or_keyword)
    @field_validator('value')
    @classmethod
    def check_value_is_string_suffix(cls, v):
        if not isinstance(v, str): raise ValueError("Value for 'suffix' action must be a string")
        return v

class TagRegexReplaceModification(TagModificationBase):
    action: Literal[ModifyAction.REGEX_REPLACE] = ModifyAction.REGEX_REPLACE
    tag: str = Field(..., description="DICOM tag to perform regex replace on.")
    pattern: str = Field(..., description="Python-compatible regex pattern to find.")
    replacement: str = Field(..., description="Replacement string (can use capture groups like \\1).")
    _validate_tag = field_validator('tag')(_validate_tag_format_or_keyword)
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

class TagCopyModification(TagModificationBase):
    action: Literal[ModifyAction.COPY] = ModifyAction.COPY
    source_tag: str = Field(..., description="DICOM tag to copy value FROM.")
    destination_tag: str = Field(..., description="DICOM tag to copy value TO (will be created/overwritten).")
    destination_vr: Optional[str] = Field(None, max_length=2, description="Optional: Explicit VR for the destination tag. If omitted, source VR is used.")
    _validate_source_tag = field_validator('source_tag')(_validate_tag_format_or_keyword)
    _validate_destination_tag = field_validator('destination_tag')(_validate_tag_format_or_keyword)
    _validate_vr = field_validator('destination_vr')(_validate_vr_format)

class TagMoveModification(TagModificationBase):
    action: Literal[ModifyAction.MOVE] = ModifyAction.MOVE
    source_tag: str = Field(..., description="DICOM tag to move value FROM (will be deleted).")
    destination_tag: str = Field(..., description="DICOM tag to move value TO (will be created/overwritten).")
    destination_vr: Optional[str] = Field(None, max_length=2, description="Optional: Explicit VR for the destination tag. If omitted, source VR is used.")
    _validate_source_tag = field_validator('source_tag')(_validate_tag_format_or_keyword)
    _validate_destination_tag = field_validator('destination_tag')(_validate_tag_format_or_keyword)
    _validate_vr = field_validator('destination_vr')(_validate_vr_format)

class TagCrosswalkModification(TagModificationBase):
    action: Literal[ModifyAction.CROSSWALK] = ModifyAction.CROSSWALK
    crosswalk_map_id: int = Field(..., description="ID of the CrosswalkMap configuration to use.")

TagModification = Annotated[
    Union[
        TagSetModification, TagDeleteModification, TagPrependModification,
        TagSuffixModification, TagRegexReplaceModification,
        TagCopyModification, TagMoveModification,
        TagCrosswalkModification
    ],
    Field(discriminator="action")
]


# --- Rule Schemas ---

class RuleBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    match_criteria: List[MatchCriterion] = Field(default_factory=list, description="List of criteria based on DICOM tag values.")
    association_criteria: Optional[List[AssociationMatchCriterion]] = Field(None, description="Optional list of criteria based on DICOM association info (Calling AE, Source IP etc.). Applies only to C-STORE/STOW inputs.")
    tag_modifications: List[TagModification] = Field(default_factory=list)
    applicable_sources: Optional[List[str]] = Field(None, description="List of source identifiers this rule applies to. Applies to all if null/empty.")
    schedule_id: Optional[int] = Field(None, description="Optional ID of the Schedule to control when this rule is active.")

    @field_validator('applicable_sources')
    @classmethod
    def validate_sources_not_empty_strings(cls, v):
        if v is not None:
            if not isinstance(v, list): raise ValueError("applicable_sources must be a list of strings or null")
            if any(not isinstance(s, str) or not s.strip() for s in v): raise ValueError("Each item in applicable_sources must be a non-empty string")
        return v

    @model_validator(mode='after')
    def check_association_ops(self) -> 'RuleBase':
         if self.association_criteria:
             for criterion in self.association_criteria:
                  ip_ops = {MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET}
                  if criterion.parameter != 'SOURCE_IP' and criterion.op in ip_ops:
                       raise ValueError(f"Operation '{criterion.op.value}' is only valid for parameter 'SOURCE_IP'.")
         return self


class RuleCreate(RuleBase):
    ruleset_id: int
    destination_ids: Optional[List[int]] = Field(None, description="List of StorageBackendConfig IDs to use as destinations.")


class RuleUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    match_criteria: Optional[List[MatchCriterion]] = None
    association_criteria: Optional[List[AssociationMatchCriterion]] = None
    tag_modifications: Optional[List[TagModification]] = None
    applicable_sources: Optional[List[str]] = Field(None, description="List of source identifiers this rule applies to. Set to null to apply to all.")
    schedule_id: Optional[int] = Field(None, description="Update the Schedule ID for this rule (null means always active).")
    destination_ids: Optional[List[int]] = Field(None, description="List of StorageBackendConfig IDs to use as destinations. Replaces existing list.")

    _validate_sources = field_validator('applicable_sources', mode='before')(RuleBase.validate_sources_not_empty_strings)
    _validate_assoc_ops = model_validator(mode='after')(RuleBase.check_association_ops)


class RuleInDBBase(RuleBase):
    id: int
    ruleset_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    destinations: List[StorageBackendConfigRead] = []
    schedule: Optional[ScheduleRead] = None # Nested schedule object
    model_config = ConfigDict(from_attributes=True)


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
    model_config = ConfigDict(from_attributes=True)


class RuleSet(RuleSetInDBBase): pass

class RuleSetSummary(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    execution_mode: RuleSetExecutionMode
    rule_count: int = Field(..., description="Number of rules in this ruleset")
    model_config = ConfigDict(from_attributes=True)

# --- JSON Processing Schemas ---
class JsonProcessRequest(BaseModel):
    dicom_json: Dict[str, Any] = Field(..., description="DICOM header represented as JSON.")
    ruleset_id: Optional[int] = Field(None, description="Optional specific RuleSet ID to apply.")
    source_identifier: str = Field(default="api_json", description="Source identifier for matching.")

class JsonProcessResponse(BaseModel):
    original_json: Dict[str, Any]
    morphed_json: Dict[str, Any]
    applied_ruleset_id: Optional[int] = None
    applied_rule_ids: List[int] = []
    source_identifier: str
    errors: List[str] = []
    warnings: List[str] = []
