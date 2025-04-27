# app/db/models/schedule.py
from typing import Optional, List, Dict, Any
from datetime import datetime

from sqlalchemy import String, Boolean, Text, JSON, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.db.base import Base

class Schedule(Base):
    """
    Database model to store reusable schedule definitions for rules.
    """
    __tablename__ = "schedules"

    # Inherits id, created_at, updated_at from Base

    name: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        index=True,
        nullable=False,
        comment="Unique, user-friendly name for this schedule (e.g., 'Overnight Telerad Hours')."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Optional description of when this schedule is active."
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
        comment="Whether this schedule definition is active and can be assigned to rules."
    )
    # Store time ranges as JSON. Structure validation happens at schema/API level.
    # Example: [{"days": ["Mon", "Fri"], "start_time": "17:00", "end_time": "08:00"}]
    time_ranges: Mapped[List[Dict[str, Any]]] = mapped_column(
        JSON, # Use JSONB for PostgreSQL if preferred
        nullable=False,
        default=[],
        comment="List of time range definitions (days, start_time HH:MM, end_time HH:MM)."
    )

    # Relationship back to rules that use this schedule
    rules: Mapped[List["Rule"]] = relationship(back_populates="schedule")

    def __repr__(self):
        return (f"<Schedule(id={self.id}, name='{self.name}', "
                f"enabled={self.is_enabled})>")
