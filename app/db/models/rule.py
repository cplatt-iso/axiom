# app/db/models/rule.py
import enum
# --- ADDED: Table import ---
from typing import List, Optional, Dict, Any
from datetime import datetime

from sqlalchemy import (
    String, Boolean, Text, ForeignKey, JSON, Enum as DBEnum, Integer,
    Table, Column # Import Table and Column
)
# --- END ADDED ---
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB

from app.db.base import Base
# --- REMOVED StorageBackendConfig import from here ---
# from .storage_backend_config import StorageBackendConfig
# --- END REMOVED ---

# Use the same Enum definition for model and schema consistency
class RuleSetExecutionMode(str, enum.Enum):
    FIRST_MATCH = "FIRST_MATCH"
    ALL_MATCHES = "ALL_MATCHES"

# --- MOVED: Association Table Definition ---
# Define the association table *before* it's used in relationships
rule_destination_association = Table(
    'rule_destination_association',
    Base.metadata,
    Column('rule_id', Integer, ForeignKey('rules.id', ondelete="CASCADE"), primary_key=True),
    Column('storage_backend_config_id', Integer, ForeignKey('storage_backend_configs.id', ondelete="CASCADE"), primary_key=True)
)
# --- END MOVED ---

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

    rules: Mapped[List["Rule"]] = relationship(
        back_populates="ruleset",
        cascade="all, delete-orphan",
        order_by="Rule.priority",
        lazy="selectin",
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

    match_criteria: Mapped[Dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default={},
        comment="Criteria object (structure defined/validated by Pydantic schema)"
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
        comment="List of source identifiers (e.g., 'scp_listener_a', 'api_json'). Applies to all if null/empty."
    )

    ruleset: Mapped["RuleSet"] = relationship(
        back_populates="rules",
        lazy="joined"
    )

    # --- UPDATED: Relationship uses the locally defined association table ---
    # Type hint uses string forward reference because StorageBackendConfig is defined later
    destinations: Mapped[List["StorageBackendConfig"]] = relationship(
        "StorageBackendConfig", # Use string name
        secondary=rule_destination_association,
        back_populates="rules",
        lazy="selectin"
    )
    # --- END UPDATED ---

    def __repr__(self):
        return f"<Rule(id={self.id}, name='{self.name}', ruleset_id={self.ruleset_id})>"
