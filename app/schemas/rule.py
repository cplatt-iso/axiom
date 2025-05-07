# filename: backend/app/schemas/rule.py

from typing import List, Optional, Dict, Any, Union, Literal
from pydantic import BaseModel, Field, field_validator, model_validator, StringConstraints, ConfigDict, ValidationInfo
from typing_extensions import Annotated
import enum
from datetime import datetime
import re
from .storage_backend_config import StorageBackendConfigRead
from .schedule import ScheduleRead
import logging

logger = logging.getLogger(__name__)

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
    IN = "in"
    NOT_IN = "not_in"
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

def _validate_tag_format_or_keyword(v: str) -> str:
    if not isinstance(v, str):
        raise ValueError("Tag must be a string")
    v = v.strip()
    # Allow specific non-DICOM parameters only for Association Criteria
    # Check if it's intended for Association Match first (handled elsewhere)
    # if v.upper() in ['SOURCE_IP', 'CALLING_AE_TITLE', 'CALLED_AE_TITLE']:
    #     raise ValueError(f"'{v.upper()}' is only valid in AssociationMatchCriterion.")

    # Check for GGGG,EEEE format
    if re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", v):
        # Extract and format consistently if needed, e.g., strip parens/spaces
        match = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", v)
        if match:
             return f"{match.group(1).upper()},{match.group(2).upper()}" # Return consistent format
        else: # Should not happen based on regex, but safety check
             raise ValueError("Invalid GGGG,EEEE format") # Should be unreachable

    # Check for DICOM Keyword format (alphanumeric)
    if re.match(r"^[a-zA-Z0-9]+$", v):
        # Here you *could* add validation against pydicom's dictionary if strictness is needed
        # from pydicom.datadict import tag_for_keyword
        # try:
        #     if tag_for_keyword(v) is None: raise ValueError() # Check if it's a known keyword
        # except ValueError:
        #      raise ValueError(f"Tag '{v}' is not a recognized DICOM keyword.")
        return v # Assume valid keyword if format matches

    raise ValueError("Tag must be in 'GGGG,EEEE' format or a valid DICOM keyword")


def _validate_vr_format(v: Optional[str]) -> Optional[str]:
    if v is not None:
        if not isinstance(v, str):
            raise ValueError("VR must be a string")
        v = v.strip().upper()
        if not re.match(r"^[A-Z]{2}$", v):
            raise ValueError("VR must be two uppercase letters")
        return v
    return None

class AssociationMatchCriterion(BaseModel):
    parameter: Literal['SOURCE_IP', 'CALLING_AE_TITLE', 'CALLED_AE_TITLE'] = Field(..., description="Association parameter to match.")
    op: MatchOperation = Field(..., description="Matching operation.")
    value: Any = Field(..., description="Value to compare against.")

    @model_validator(mode='after')
    def check_value_for_op(self) -> 'AssociationMatchCriterion':
        ip_subnet_ops = {MatchOperation.IP_ADDRESS_IN_SUBNET}
        ip_string_ops = {MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH}

        if self.parameter == 'SOURCE_IP':
            if self.op in ip_subnet_ops:
                if not isinstance(self.value, str) or '/' not in self.value:
                    raise ValueError(f"Value for '{self.op.value}' must be a CIDR string (e.g., '192.168.1.0/24').")
            elif self.op in ip_string_ops:
                 if not isinstance(self.value, str):
                      raise ValueError(f"Value for '{self.op.value}' must be an IP address string.")
            # Add validation for other ops if needed for SOURCE_IP
        # Add validation for CALLING_AE_TITLE/CALLED_AE_TITLE if specific formats are required
        return self

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
        # Use the refined validator
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
        value_not_allowed_ops = {MatchOperation.EXISTS, MatchOperation.NOT_EXISTS}
        ip_ops = {MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET}

        if op in ip_ops:
             raise ValueError(f"Operation '{op.value}' is only valid for Association parameter 'SOURCE_IP'.")

        if op in value_required_ops and value is None:
             raise ValueError(f"Value is required for operation '{op.value}' on tag '{tag}'")

        if op in value_not_allowed_ops and value is not None:
             logger.warning(f"Value provided for operation '{op.value}' on tag '{tag}' but will be ignored.")
             self.value = None # Ensure value is None for these ops

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
        if not isinstance(v, str):
            raise ValueError("Value for 'prepend' action must be a string")
        return v

class TagSuffixModification(TagModificationBase):
    action: Literal[ModifyAction.SUFFIX] = ModifyAction.SUFFIX
    tag: str = Field(..., description="DICOM tag to append to.")
    value: str = Field(..., description="String value to append (suffix).")
    _validate_tag = field_validator('tag')(_validate_tag_format_or_keyword)
    @field_validator('value')
    @classmethod
    def check_value_is_string_suffix(cls, v):
        if not isinstance(v, str):
            raise ValueError("Value for 'suffix' action must be a string")
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
        if not isinstance(v, str):
            raise ValueError("Pattern for 'regex_replace' action must be a string")
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"Invalid regex pattern: {e}")
        return v
    @field_validator('replacement')
    @classmethod
    def check_replacement_is_string(cls, v):
        if not isinstance(v, str):
            raise ValueError("Replacement for 'regex_replace' action must be a string")
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

# --- Base Schema for Rules ---
class RuleBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    match_criteria: List[MatchCriterion] = Field(default_factory=list)
    association_criteria: Optional[List[AssociationMatchCriterion]] = Field(None)
    tag_modifications: List[TagModification] = Field(default_factory=list)
    applicable_sources: Optional[List[str]] = Field(None)
    schedule_id: Optional[int] = Field(None)
    # --- ADDED: AI Standardization Field ---
    ai_standardization_tags: Optional[List[str]] = Field(
        None, # Default to None (no AI standardization)
        description="Optional list of DICOM tags (keywords or 'GGGG,EEEE') whose values should be standardized using AI."
    )
    # --- END ADDED ---

    @field_validator('applicable_sources')
    @classmethod
    def validate_sources_not_empty_strings(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is not None:
            if not isinstance(v, list):
                raise ValueError("applicable_sources must be a list of strings or null")
            if any(not isinstance(s, str) or not s.strip() for s in v):
                raise ValueError("Each item in applicable_sources must be a non-empty string")
        return v

    # --- ADDED: Validator for AI Standardization Tags ---
    @field_validator('ai_standardization_tags')
    @classmethod
    def validate_ai_tags(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is None:
            return None # Allow null/None
        if not isinstance(v, list):
            raise ValueError("ai_standardization_tags must be a list of strings or null")
        validated_tags = []
        for tag_str in v:
            try:
                # Reuse the existing tag validator
                validated_tags.append(_validate_tag_format_or_keyword(tag_str))
            except ValueError as e:
                # Improve error message context
                raise ValueError(f"Invalid tag '{tag_str}' in ai_standardization_tags: {e}") from e
        # Return the list of validated tags (potentially with consistent formatting)
        # Remove duplicates? Optional, might be desired.
        unique_tags = list(dict.fromkeys(validated_tags))
        if len(unique_tags) != len(validated_tags):
             logger.warning(f"Duplicate tags found in ai_standardization_tags: {validated_tags}. Using unique list: {unique_tags}")
        return unique_tags # Return unique list
    # --- END ADDED ---


    @model_validator(mode='after')
    def check_association_ops(self) -> 'RuleBase':
         if self.association_criteria:
             for criterion in self.association_criteria:
                  ip_ops = {MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET}
                  if criterion.parameter != 'SOURCE_IP' and criterion.op in ip_ops:
                       raise ValueError(f"Operation '{criterion.op.value}' is only valid for parameter 'SOURCE_IP'.")
         return self

# --- Schema for Creating Rules ---
class RuleCreate(RuleBase):
    ruleset_id: int
    destination_ids: Optional[List[int]] = Field(None)

# --- Schema for Updating Rules ---
class RuleUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    match_criteria: Optional[List[MatchCriterion]] = None
    association_criteria: Optional[List[AssociationMatchCriterion]] = None
    tag_modifications: Optional[List[TagModification]] = None
    applicable_sources: Optional[List[str]] = Field(None)
    schedule_id: Optional[int] = Field(None)
    destination_ids: Optional[List[int]] = Field(None)
    # --- ADDED: AI Standardization Field ---
    ai_standardization_tags: Optional[List[str]] = Field(None)
    # --- END ADDED ---

    # Re-apply validators from Base for update fields if they exist
    _validate_sources_update = field_validator('applicable_sources')(lambda v: RuleBase.validate_sources_not_empty_strings(v) if v is not None else None)
    # --- ADDED: Validator reference for AI tags on update ---
    _validate_ai_tags_update = field_validator('ai_standardization_tags')(lambda v: RuleBase.validate_ai_tags(v) if v is not None else None)
    # --- END ADDED ---
    _validate_assoc_ops_update = model_validator(mode='after')(RuleBase.check_association_ops) # This checks assoc_criteria if present


# --- Schema for Reading Rules from DB (Base) ---
class RuleInDBBase(RuleBase):
    id: int
    ruleset_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    destinations: List[StorageBackendConfigRead] = []
    schedule: Optional[ScheduleRead] = None
    # --- ADDED: AI Standardization Field (already inherited from RuleBase) ---
    # ai_standardization_tags: Optional[List[str]] = None # Included via RuleBase inheritance
    # --- END ADDED ---
    model_config = ConfigDict(from_attributes=True)

# --- Schema for Reading a Single Rule ---
class Rule(RuleInDBBase):
    pass

# --- Schemas for Rule Sets ---
class RuleSetBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    execution_mode: RuleSetExecutionMode = RuleSetExecutionMode.FIRST_MATCH

class RuleSetCreate(RuleSetBase):
    pass

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
    rules: List[Rule] = [] # Reads populated rules
    model_config = ConfigDict(from_attributes=True)

class RuleSet(RuleSetInDBBase):
    pass

class RuleSetSummary(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    execution_mode: RuleSetExecutionMode
    rule_count: int = Field(...)
    model_config = ConfigDict(from_attributes=True)

# --- Schemas for JSON API Processing ---
class JsonProcessRequest(BaseModel):
    dicom_json: Dict[str, Any] = Field(...)
    ruleset_id: Optional[int] = Field(None)
    source_identifier: str = Field(default="api_json")

class JsonProcessResponse(BaseModel):
    original_json: Dict[str, Any]
    morphed_json: Dict[str, Any]
    applied_ruleset_id: Optional[int] = None
    applied_rule_ids: List[int] = []
    source_identifier: str
    errors: List[str] = []
    warnings: List[str] = []
