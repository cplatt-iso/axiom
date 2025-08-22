# filename: app/schemas/rule.py

from typing import List, Optional, Dict, Any, Union, Literal, TYPE_CHECKING
from pydantic import BaseModel, Field, field_validator, model_validator, StringConstraints, ConfigDict, ValidationInfo
from typing_extensions import Annotated
import enum
from datetime import datetime
import re
from .storage_backend_config import StorageBackendConfigRead
from .schedule import ScheduleRead
import structlog

logger = structlog.get_logger(__name__)

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
    # Check for GGGG,EEEE format
    match_ggeeee = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", v)
    if match_ggeeee:
        return f"{match_ggeeee.group(1).upper()},{match_ggeeee.group(2).upper()}"

    # Check for DICOM Keyword format (alphanumeric)
    if re.match(r"^[a-zA-Z0-9]+$", v):
        # Optional: Validate against pydicom's dictionary for strictness
        # from pydicom.datadict import tag_for_keyword
        # try:
        #     if tag_for_keyword(v) is None: raise ValueError()
        # except ValueError:
        #      raise ValueError(f"Tag '{v}' is not a recognized DICOM keyword.")
        return v

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
    match_criteria: Optional[List[MatchCriterion]] = Field(None, description="If None or empty, matches all DICOM objects")
    association_criteria: Optional[List[AssociationMatchCriterion]] = Field(None)
    tag_modifications: List[TagModification] = Field(default_factory=list)
    applicable_sources: Optional[List[str]] = Field(None)
    schedule_id: Optional[int] = Field(None)

    ai_prompt_config_ids: Optional[List[int]] = Field(
        default=None, # Explicitly default to None
        description="Optional list of AIPromptConfig IDs to use for AI vocabulary standardization."
    )    

    @field_validator('applicable_sources')
    @classmethod
    def validate_sources_not_empty_strings(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if v is not None:
            if not isinstance(v, list):
                raise ValueError("applicable_sources must be a list of strings or null")
            if any(not isinstance(s, str) or not s.strip() for s in v):
                raise ValueError("Each item in applicable_sources must be a non-empty string")
            
            # Warn about suspicious source identifiers that look like auto-generated IDs
            for source in v:
                if source.startswith('dimse_listener-id-') or source.startswith('dicomweb_source-id-'):
                    # This is likely a frontend bug showing internal IDs instead of real source names
                    raise ValueError(f"Invalid source identifier '{source}'. Source identifiers should be human-readable names like 'DCM4CHE_LISTENER', 'STOW_RS_ENDPOINT', etc., not auto-generated IDs.")
        return v

    @field_validator('ai_prompt_config_ids')
    @classmethod
    def validate_ai_prompt_config_ids(cls, v: Optional[List[int]]) -> Optional[List[int]]:
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError("ai_prompt_config_ids must be a list of integers or null.")
        if not all(isinstance(item, int) for item in v):
            raise ValueError("Each item in ai_prompt_config_ids must be an integer.")
        # Optionally, check for duplicates and log/remove
        unique_ids = list(dict.fromkeys(v))
        if len(unique_ids) != len(v):
            logger.warning("Duplicate IDs found in ai_prompt_config_ids, using unique list.",
                           original_ids=v, unique_ids=unique_ids)
            return unique_ids
        return v


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
    # --- FIELD CHANGE: ai_standardization_tags TO ai_prompt_config_ids ---
    # REMOVE:
    # ai_standardization_tags: Optional[List[str]] = Field(None)
    # ADD:
    ai_prompt_config_ids: Optional[List[int]] = Field(
        default=None, # For updates, default means "no change to this field" unless provided
        description="Optional list of AIPromptConfig IDs. Set to empty list to clear, or null to leave unchanged if field not provided."
    )
    # --- END FIELD CHANGE ---

    # --- VALIDATOR CHANGE: Update validator references ---
    _validate_sources_update = field_validator('applicable_sources', check_fields=False)(
        lambda v: RuleBase.validate_sources_not_empty_strings(v) if v is not None else None
    )
    # REMOVE:
    # _validate_ai_tags_update = field_validator('ai_standardization_tags', check_fields=False)(
    #    lambda v: RuleBase.validate_ai_tags(v) if v is not None else None
    # )
    # ADD:
    _validate_ai_prompt_config_ids_update = field_validator('ai_prompt_config_ids', check_fields=False)(
        lambda v: RuleBase.validate_ai_prompt_config_ids(v) if v is not None else None
    ) # check_fields=False makes it only run if the field is provided
    # --- END VALIDATOR CHANGE ---
    
    # This model_validator might need adjustment based on how Pydantic V2 handles partial updates
    # For now, assuming RuleBase.check_association_ops is safe if association_criteria is None.
    # If RuleBase.check_association_ops expects association_criteria to always be a list (even empty),
    # this might need a condition. However, the current RuleBase allows it to be Optional[List...]].
    @model_validator(mode='after') # Use model_validator from Pydantic
    def _check_association_ops_update(self) -> 'RuleUpdate':
        # This re-runs the logic from RuleBase.check_association_ops
        # It's a bit awkward to call it directly.
        # A better way might be to define the logic in a staticmethod and call it from both.
        # For now, let's assume it works by accessing self.association_criteria which could be None.
        if self.association_criteria is not None: # Only check if association_criteria is being updated
            for criterion in self.association_criteria:
                ip_ops = {MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET}
                if criterion.parameter != 'SOURCE_IP' and criterion.op in ip_ops:
                    raise ValueError(f"Operation '{criterion.op.value}' is only valid for parameter 'SOURCE_IP'.")
        return self
    
    # Also, similar to AIPromptConfigUpdate, ensure at least one field is provided for update
    @model_validator(mode='after')
    def ensure_at_least_one_field_for_update(self) -> 'RuleUpdate':
        if not self.model_fields_set:
            raise ValueError("At least one field must be provided for rule update.")
        return self

# --- Schema for Reading Rules from DB (Base) ---
class RuleSetInDBBase(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    execution_mode: RuleSetExecutionMode
    rule_count: int = Field(default=0)
    rules: List["Rule"] = []  

    model_config = ConfigDict(from_attributes=True)

# --- Schema for Reading a Single Rule ---

# --- Schemas for Rule Sets ---
class RuleSetBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    rule_count: int = Field(default=0)
    execution_mode: RuleSetExecutionMode = RuleSetExecutionMode.FIRST_MATCH

class RuleSetCreate(RuleSetBase):
    pass

class RuleSetUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    execution_mode: Optional[RuleSetExecutionMode] = None

class RuleInDBBase(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    ruleset_id: int
    match_criteria: List[Dict[str, Any]]
    association_criteria: Optional[List[Dict[str, Any]]] = None
    tag_modifications: List[Dict[str, Any]]
    applicable_sources: Optional[List[str]] = None
    schedule_id: Optional[int] = None
    ai_prompt_config_ids: Optional[List[int]] = None
    created_at: datetime
    updated_at: datetime
    destinations: List[StorageBackendConfigRead] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)

class RuleSetSummary(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    execution_mode: RuleSetExecutionMode
    rule_count: int = Field(default=0)

    model_config = ConfigDict(from_attributes=True)

class Rule(RuleInDBBase):
    # Removed type overrides to avoid type conflicts with parent class
    ruleset: Optional[RuleSetSummary] = None
    
    model_config = ConfigDict(from_attributes=True)
    
    # We can convert the Dict types to proper model types at runtime
    def get_typed_match_criteria(self) -> List[MatchCriterion]:
        return [MatchCriterion.model_validate(item) for item in self.match_criteria]
        
    def get_typed_tag_modifications(self) -> List[TagModification]:
        result = []
        for item in self.tag_modifications:
            action_str = item.get("action")
            if action_str is None:
                raise ValueError("Missing 'action' field in tag modification")
                
            try:
                action_enum = ModifyAction(action_str)
                model_class = {
                    ModifyAction.SET: TagSetModification,
                    ModifyAction.DELETE: TagDeleteModification,
                    ModifyAction.PREPEND: TagPrependModification,
                    ModifyAction.SUFFIX: TagSuffixModification,
                    ModifyAction.REGEX_REPLACE: TagRegexReplaceModification,
                    ModifyAction.COPY: TagCopyModification,
                    ModifyAction.MOVE: TagMoveModification,
                    ModifyAction.CROSSWALK: TagCrosswalkModification
                }.get(action_enum)
            except ValueError:
                raise ValueError(f"Unknown action type: {action_str}")
                
            if model_class is None:
                raise ValueError(f"Unknown action type: {action_str}")
                
            result.append(model_class.model_validate(item))
        return result
        
    def get_typed_association_criteria(self) -> Optional[List[AssociationMatchCriterion]]:
        if self.association_criteria is None:
            return None
        return [AssociationMatchCriterion.model_validate(item) for item in self.association_criteria]

class RuleSet(RuleSetInDBBase):
    rules: List[Rule] = []

Rule.model_rebuild()

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
