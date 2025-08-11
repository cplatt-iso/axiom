# app/db/models/dicomweb_source_state.py

# --- Imports ---
from sqlalchemy import Column, String, DateTime, Text, Boolean, Integer, text
from sqlalchemy.dialects.postgresql import JSONB # Use JSONB for Postgres for better performance/indexing
# If not using Postgres or want standard JSON, use: from sqlalchemy.types import JSON
from sqlalchemy.sql import func # For server-side default timestamps if needed
# --- Need datetime for type hints if using them, but not required for Column syntax ---
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy.orm import Mapped, mapped_column
# --- End Imports ---

from app.db.base import Base # Use your specific Base import path

class DicomWebSourceState(Base):
    """
    Database model to store the configuration and polling state
    for configured DICOMweb sources.
    NOTE: This model currently mixes configuration and state, which is not ideal.
          Refactoring to separate these is recommended for future maintainability.
    """
    __tablename__ = "dicomweb_source_state"  # type: ignore

    # --- Configuration Fields (Original Column syntax) ---
    source_name = Column(String, unique=True, index=True, nullable=False)
    description = Column(Text, nullable=True)
    base_url = Column(String, nullable=False)
    qido_prefix = Column(String, nullable=False, default="qido-rs")
    wado_prefix = Column(String, nullable=False, default="wado-rs")
    polling_interval_seconds = Column(Integer, nullable=False, default=300)
    is_enabled = Column(
        Boolean,
        nullable=False,
        default=True,
        index=True, # Added index for potential filtering
        comment="Whether this source configuration is generally enabled and available (e.g., for data browser)."
        )
    is_active = Column( # <-- OUR HACKY ADDITION
        Boolean,
        nullable=False,
        default=True, # Default to active if enabled
        server_default=text('true'),
        index=True,
        comment="Whether AUTOMATIC polling for this source is active based on its schedule."
        )
    auth_type = Column(String(50), nullable=False, default="none")
    auth_config = Column(JSONB, nullable=True) # Consider encrypting secrets here
    search_filters = Column(JSONB, nullable=True)

    # --- State Fields (Original Column syntax) ---
    last_processed_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_successful_run: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_run: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # --- ADDED Metrics (Using Column syntax) ---
    found_instance_count = Column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total count of unique instances found by QIDO across all polls."
    )
    queued_instance_count = Column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total count of instances actually queued for metadata processing."
    )
    processed_instance_count = Column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total count of instances successfully processed after being queued."
    )
    # --- END ADDED ---

    # --- Health Status Fields ---
    health_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="UNKNOWN", server_default="UNKNOWN", index=True,
        comment="Current health status of the source (OK, DOWN, ERROR, UNKNOWN)"
    )
    last_health_check: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Timestamp of the last health check attempt"
    )
    last_health_error: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Details of the last health check error, if any"
    )
    # --- END Health Status Fields ---


    # id, created_at, updated_at inherited from Base

    def __repr__(self):
        # Update repr to include the new active flag
        return (f"<DicomWebSourceState(id={self.id}, source_name='{self.source_name}', "
                f"enabled={self.is_enabled}, active={self.is_active}, health={self.health_status}, " # <-- ADDED HEALTH STATUS
                f"queued={self.queued_instance_count})>")
