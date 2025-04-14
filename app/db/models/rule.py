# app/db/models/rule.py
import enum
from sqlalchemy import (
    Column, Integer, String, Boolean, Text, ForeignKey, JSON, Enum, DateTime
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.db.base import Base
# from app.db.models.user import User # Import if using ForeignKey to User


class RuleSetExecutionMode(str, enum.Enum):
    FIRST_MATCH = "FIRST_MATCH" # Stop after the first rule matches
    ALL_MATCHES = "ALL_MATCHES" # Evaluate all rules in the set

class RuleSet(Base):
    """
    A collection of DICOM processing rules.
    """
    __tablename__ = 'rule_sets' # Explicitly naming

    name = Column(String(100), unique=True, index=True, nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean(), default=True, index=True)
    priority = Column(Integer, default=0, index=True, comment="Lower numbers execute first") # For ordering rulesets
    execution_mode = Column(
        Enum(RuleSetExecutionMode),
        default=RuleSetExecutionMode.FIRST_MATCH,
        nullable=False
    )

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # created_by_user_id = Column(Integer, ForeignKey('users.id'), nullable=True) # Optional: track creator
    # created_by_user = relationship("User", back_populates="created_rulesets")

    # Relationship: One RuleSet has many Rules
    rules = relationship(
        "Rule",
        back_populates="ruleset",
        cascade="all, delete-orphan", # Delete rules if ruleset is deleted
        order_by="Rule.priority" # Process rules within a set in order
    )

    def __repr__(self):
        return f"<RuleSet(name='{self.name}', is_active={self.is_active})>"


class Rule(Base):
    """
    An individual DICOM processing rule.
    Defines matching criteria, modifications, and destinations.
    """
    __tablename__ = 'rules' # Explicitly naming

    name = Column(String(100), nullable=False)
    description = Column(Text, nullable=True)
    is_active = Column(Boolean(), default=True, index=True)
    priority = Column(Integer, default=0, index=True, comment="Lower numbers execute first within a ruleset")

    # Foreign Key to RuleSet
    ruleset_id = Column(Integer, ForeignKey('rule_sets.id'), nullable=False, index=True)

    # --- Core Rule Logic ---
    # Store complex criteria/actions as JSON(B) for flexibility
    # Alternatively, create separate tables for criteria, modifications, destinations
    # Using JSONB in PostgreSQL is highly recommended for performance with JSON queries

    # Matching Criteria (Example: JSON containing criteria like {'PatientName': 'Test*', 'Modality': 'CT'})
    match_criteria = Column(JSON, nullable=False, default=lambda: {})

    # Tag Modifications (Example: JSON like {'(0010,0010)': 'Anonymized', '(0008,0050)': None}) -> None means delete
    tag_modifications = Column(JSON, nullable=False, default=lambda: {})

    # Destinations (Example: JSON list like [{'type': 'cstore', 'ae_title': 'ARCHIVE'}, {'type': 'filesystem', 'path': '/data/archive'}])
    destinations = Column(JSON, nullable=False, default=lambda: [])

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationship: Many Rules belong to one RuleSet
    ruleset = relationship("RuleSet", back_populates="rules")

    def __repr__(self):
        return f"<Rule(name='{self.name}', ruleset_id={self.ruleset_id})>"
