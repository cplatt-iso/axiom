# app/db/models/rule.py
import enum
from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy import (
    String, Boolean, Text, ForeignKey, JSON, Enum as DBEnum, Integer,
    # Remove Table, Column imports if only used for association table
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB

# Import Base and the centrally defined association table
from app.db.base import Base, rule_destination_association
# --- REMOVED StorageBackendConfig import ---

class RuleSetExecutionMode(str, enum.Enum):
    FIRST_MATCH = "FIRST_MATCH"
    ALL_MATCHES = "ALL_MATCHES"

# --- REMOVED: Association Table Definition moved to base.py ---

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

    rules: Mapped[List["Rule"]] = relationship(
        "Rule", # Use string forward reference
        back_populates="ruleset",
        cascade="all, delete-orphan",
        order_by="Rule.priority",
        lazy="selectin",
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

    match_criteria: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default={},
        comment="Criteria object (structure defined/validated by Pydantic schema)"
    )

    association_criteria: Mapped[Optional[List[Dict[str, Any]]]] = mapped_column(
        JSONB,
        nullable=True,
        default=None,
        comment="Optional list of criteria based on association details (Calling AE, IP)."
    )

    tag_modifications: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        default=[],
        comment="List of modification action objects (validated by Pydantic)"
    )

    applicable_sources: Mapped[Optional[List[str]]] = mapped_column(
        JSONB,
        nullable=True,
        index=True,
        comment="List of source identifiers. Applies to all if null/empty."
    )

    ruleset: Mapped["RuleSet"] = relationship(
        "RuleSet", # Use string forward reference
        back_populates="rules",
        lazy="joined"
    )

    destinations: Mapped[List["StorageBackendConfig"]] = relationship(
        "StorageBackendConfig", # Use string forward reference
        secondary=rule_destination_association, # Use the imported table object
        back_populates="rules",
        lazy="selectin"
    )

    def __repr__(self):
        return f"<Rule(id={self.id}, name='{self.name}', ruleset_id={self.ruleset_id})>"
