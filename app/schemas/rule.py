# app/schemas/rule.py

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, model_validator
import enum
from datetime import datetime

# Re-use Enum from model if possible, or redefine for schema clarity
class RuleSetExecutionMode(str, enum.Enum):
    FIRST_MATCH = "FIRST_MATCH"
    ALL_MATCHES = "ALL_MATCHES"

# --- Rule Schemas ---

# Properties shared by models stored in DB
class RuleBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    # Use Dict[str, Any] for flexibility, enforce structure later if needed
    match_criteria: Dict[str, Any] = Field(default_factory=dict)
    tag_modifications: Dict[str, Any] = Field(default_factory=dict)
    destinations: List[Dict[str, Any]] = Field(default_factory=list)


# Properties received via API on creation
class RuleCreate(RuleBase):
    ruleset_id: int


# Properties received via API on update
class RuleUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    match_criteria: Optional[Dict[str, Any]] = None
    tag_modifications: Optional[Dict[str, Any]] = None
    destinations: Optional[List[Dict[str, Any]]] = None
    # ruleset_id cannot be changed


# Properties stored in DB
class RuleInDBBase(RuleBase):
    id: int
    ruleset_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True # Pydantic V2 replaces orm_mode


# Properties returned to client (schema for reading a rule)
class Rule(RuleInDBBase):
    pass


# --- RuleSet Schemas ---

# Properties shared by models stored in DB
class RuleSetBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True
    priority: int = 0
    execution_mode: RuleSetExecutionMode = RuleSetExecutionMode.FIRST_MATCH


# Properties received via API on creation
class RuleSetCreate(RuleSetBase):
    pass


# Properties received via API on update
class RuleSetUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    priority: Optional[int] = None
    execution_mode: Optional[RuleSetExecutionMode] = None


# Properties stored in DB
class RuleSetInDBBase(RuleSetBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    # Include rules when reading a ruleset
    rules: List[Rule] = [] # Default to empty list

    class Config:
        from_attributes = True


# Properties returned to client (schema for reading a ruleset)
class RuleSet(RuleSetInDBBase):
    pass


# Optional: Schema for reading rulesets without full rule details
class RuleSetSummary(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    is_active: bool
    priority: int
    execution_mode: RuleSetExecutionMode
    rule_count: int # Add a count instead of full rules

    class Config:
        from_attributes = True
