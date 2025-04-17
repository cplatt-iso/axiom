# app/db/models/rule.py
import enum
from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy import (
    String, Boolean, Text, ForeignKey, JSON, Enum as DBEnum, Integer
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
# Import dialects for specific JSON type if needed (though often handled by default)
from sqlalchemy.dialects.postgresql import JSONB # Use JSONB for better indexing/performance

from app.db.base import Base
# from .user import User # Import User if relationship uncommented

# Use the same Enum definition for model and schema consistency
class RuleSetExecutionMode(str, enum.Enum):
    FIRST_MATCH = "FIRST_MATCH"
    ALL_MATCHES = "ALL_MATCHES"

class RuleSet(Base):
    """
    A collection of DICOM processing rules.
    Inherits id, created_at, updated_at from Base.
    """
    __tablename__ = 'rule_sets'

    name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True, index=True)
    priority: Mapped[int] = mapped_column(default=0, index=True, comment="Lower numbers execute first")
    execution_mode: Mapped[RuleSetExecutionMode] = mapped_column(
        DBEnum(RuleSetExecutionMode, name="ruleset_execution_mode_enum"),
        default=RuleSetExecutionMode.FIRST_MATCH,
        nullable=False
    )

    # Timestamps inherited from Base

    # Optional relationship to track creator user
    # created_by_user_id: Mapped[Optional[int]] = mapped_column(ForeignKey('users.id'), nullable=True, index=True)
    # created_by_user: Mapped[Optional["User"]] = relationship(back_populates="created_rulesets", lazy="selectin", init=False)

    # Relationship: One RuleSet has many Rules
    rules: Mapped[List["Rule"]] = relationship(
        back_populates="ruleset",
        cascade="all, delete-orphan",
        order_by="Rule.priority",
        lazy="selectin", # Consider changing to 'subquery' or 'joined' if performance dictates
                         # especially if rules are always needed when loading a ruleset.
                         # 'selectin' is good but can issue many queries if loading many rulesets.
    )

    def __repr__(self):
        return f"<RuleSet(id={self.id}, name='{self.name}', is_active={self.is_active})>"


class Rule(Base):
    """
    An individual DICOM processing rule.
    Defines matching criteria, modifications, and destinations.
    Inherits id, created_at, updated_at from Base.
    """
    __tablename__ = 'rules'

    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True, index=True)
    priority: Mapped[int] = mapped_column(default=0, index=True, comment="Lower numbers execute first within a ruleset")

    ruleset_id: Mapped[int] = mapped_column(ForeignKey('rule_sets.id', ondelete="CASCADE"), index=True)

    # --- Core Rule Logic ---
    # Using JSON/JSONB for flexible criteria, modifications, destinations
    # Note: Pydantic schemas should handle validation of the structure within these JSON fields.
    match_criteria: Mapped[Dict[str, Any]] = mapped_column(
        JSONB, # Use JSONB
        nullable=False,
        default={}, # Use empty dict for default
        comment="Criteria object (structure defined/validated by Pydantic schema)"
    )
    tag_modifications: Mapped[List[Dict[str, Any]]] = mapped_column( # Changed from Dict to List
        JSONB, # Use JSONB
        nullable=False,
        default=[], # Use empty list for default
        comment="List of modification action objects (validated by Pydantic)"
    )
    destinations: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSONB, # Use JSONB
        nullable=False,
        default=[], # Use empty list for default
        comment="List of storage destination objects (validated by Pydantic)"
    )

    # --- NEW FIELD ---
    # Store as a list of strings within a JSONB column
    applicable_sources: Mapped[Optional[List[str]]] = mapped_column(
        JSONB,
        nullable=True, # Rule applies to all sources if this is NULL or empty list
        index=True, # Index if you anticipate querying rules by source often
        comment="List of source identifiers (e.g., 'scp_listener_a', 'api_json'). Applies to all if null/empty."
    )
    # --- END NEW FIELD ---


    # Timestamps inherited from Base

    ruleset: Mapped["RuleSet"] = relationship(
        back_populates="rules",
        lazy="joined" # Usually want the ruleset when querying a rule
    )

    def __repr__(self):
        return f"<Rule(id={self.id}, name='{self.name}', ruleset_id={self.ruleset_id})>"
