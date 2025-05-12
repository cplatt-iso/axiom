# filename: backend/app/db/models/rule.py
import enum
from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy import (
    String, Boolean, Text, ForeignKey, JSON, Enum as DBEnum, Integer,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB # Use JSONB for better performance/indexing

# --- ADDED: Import Schedule for type hinting ---
from .schedule import Schedule
# --- END ADDED ---
# --- ADDED: Import StorageBackendConfig for type hinting ---
from .storage_backend_config import StorageBackendConfig
# --- END ADDED ---

from app.db.base import Base, rule_destination_association

class RuleSetExecutionMode(str, enum.Enum):
    FIRST_MATCH = "FIRST_MATCH"
    ALL_MATCHES = "ALL_MATCHES"

class RuleSet(Base):
    __tablename__ = 'rule_sets'

    name: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True, index=True)
    priority: Mapped[int] = mapped_column(default=0, index=True, comment="Lower numbers execute first")
    execution_mode: Mapped[RuleSetExecutionMode] = mapped_column(
        DBEnum(RuleSetExecutionMode, name="ruleset_execution_mode_enum", create_type=False),
        default=RuleSetExecutionMode.FIRST_MATCH,
        nullable=False
    )

    # Timestamps - managed by Base

    rules: Mapped[List["Rule"]] = relationship(
        "Rule",
        back_populates="ruleset",
        cascade="all, delete-orphan",
        order_by="Rule.priority", # Ensure rules are ordered by priority when loaded
        lazy="selectin", # Use selectin loading for rules when loading a ruleset
    )

    def __repr__(self):
        return f"<RuleSet(id={self.id}, name='{self.name}', is_active={self.is_active})>"


class Rule(Base):
    __tablename__ = 'rules'

    name: Mapped[str] = mapped_column(String(100))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(default=True, index=True)
    priority: Mapped[int] = mapped_column(default=0, index=True, comment="Lower numbers execute first within a ruleset")

    ruleset_id: Mapped[int] = mapped_column(ForeignKey('rule_sets.id', ondelete="CASCADE"), index=True)

    match_criteria: Mapped[List[Dict[str, Any]]] = mapped_column( # Store as list of dicts
        JSONB, # Use JSONB
        nullable=False,
        default=[], # Default to empty list
        comment="Criteria list (structure defined/validated by Pydantic schema)"
    )

    association_criteria: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(
        JSONB, # Use JSONB
        nullable=True,
        default=None, # Default to null
        comment="Optional list of criteria based on association details (Calling AE, IP)."
    )

    tag_modifications: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSONB, # Use JSONB
        nullable=False,
        default=[], # Default to empty list
        comment="List of modification action objects (validated by Pydantic)"
    )

    applicable_sources: Mapped[Optional[List[str]]] = mapped_column(
        JSONB, # Use JSONB
        nullable=True,
        default=None, # Default to null
        index=True, # Index if filtering by source is common
        comment="List of source identifiers. Applies to all if null/empty."
    )

    # --- ADDED: AI Standardization Column ---
 #   ai_standardization_tags: Mapped[Optional[List[str]]] = mapped_column(
 #       JSONB, # Store list of strings as JSONB
 #       nullable=True,
 #       default=None, # Default to null (meaning disabled for this rule)
 #       comment="List of DICOM tags (keywords or 'GGGG,EEEE') to standardize using AI."
 #   )
    # --- END ADDED ---
    
    ai_prompt_config_ids: Mapped[Optional[List[int]]] = mapped_column( # <<<< Changed to List[int]
        JSONB, # Or JSON
        nullable=True,
        default=None,
        comment="List of AIPromptConfig IDs to be used for AI vocabulary standardization for this rule."
    )

    # Timestamps - managed by Base

    # Relationships
    ruleset: Mapped["RuleSet"] = relationship(
        "RuleSet",
        back_populates="rules",
        lazy="joined" # Typically want ruleset info when loading a rule
    )

    destinations: Mapped[List["StorageBackendConfig"]] = relationship(
        "StorageBackendConfig",
        secondary=rule_destination_association,
        back_populates="rules",
        lazy="selectin" # Use selectin for loading destinations efficiently
    )

    # Schedule Relationship (existing, ensure type hint is correct)
    schedule_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("schedules.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
        comment="Optional ID of the Schedule controlling when this rule is active."
    )
    schedule: Mapped[Optional["Schedule"]] = relationship(
        "Schedule",
        back_populates="rules", # Assumes Schedule model has 'rules' backref
        lazy="selectin" # Eagerly load schedule details if needed
    )

    def __repr__(self):
        ai_configs_repr = f", ai_prompt_config_ids={self.ai_prompt_config_ids}" if self.ai_prompt_config_ids else ""
        return (f"<Rule(id={self.id}, name='{self.name}', ruleset_id={self.ruleset_id}, "
                f"schedule_id={self.schedule_id}{ai_configs_repr})>")