# app/db/models/rule.py
import enum
from typing import List, Optional, Dict, Any # Make sure Dict is imported
from datetime import datetime

from sqlalchemy import (
    String, Boolean, Text, ForeignKey, JSON, Enum as DBEnum, Integer
)
# NOTE: Removed Column import as it's not used here anymore
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

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

    # --- Core Rule Logic --- CORRECTED TYPES ---
    # Should be Dict (object), not List[Dict]
    match_criteria: Mapped[Dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, comment="Criteria object (implicit AND)"
    )
    # Should be Dict (object), not List[Dict]
    tag_modifications: Mapped[Dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, comment="Modification action object"
    )
    # Destinations remains List[Dict]
    destinations: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list, comment="List of storage destination objects"
    )

    # Timestamps inherited from Base

    ruleset: Mapped["RuleSet"] = relationship(
        back_populates="rules",
    )

    def __repr__(self):
        return f"<Rule(id={self.id}, name='{self.name}', ruleset_id={self.ruleset_id})>"
