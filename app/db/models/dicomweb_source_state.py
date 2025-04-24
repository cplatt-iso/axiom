# app/db/models/dicomweb_source_state.py

# --- Imports ---
from sqlalchemy import Column, String, DateTime, Text, Boolean, Integer
from sqlalchemy.dialects.postgresql import JSONB # Use JSONB for Postgres for better performance/indexing
# If not using Postgres or want standard JSON, use: from sqlalchemy.types import JSON
from sqlalchemy.sql import func # For server-side default timestamps if needed
# --- Need datetime for type hints if using them, but not required for Column syntax ---
from datetime import datetime
from typing import Optional, Dict, Any
# --- End Imports ---

from app.db.base import Base # Use your specific Base import path

class DicomWebSourceState(Base):
    """
    Database model to store the configuration and polling state
    for configured DICOMweb sources.
    """
    __tablename__ = "dicomweb_source_state"

    # --- Configuration Fields (Original Column syntax) ---
    source_name = Column(String, unique=True, index=True, nullable=False)
    description = Column(Text, nullable=True)
    base_url = Column(String, nullable=False)
    qido_prefix = Column(String, nullable=False, default="qido-rs")
    wado_prefix = Column(String, nullable=False, default="wado-rs")
    polling_interval_seconds = Column(Integer, nullable=False, default=300)
    is_enabled = Column(Boolean, nullable=False, default=True)
    auth_type = Column(String(50), nullable=False, default="none")
    auth_config = Column(JSONB, nullable=True)
    search_filters = Column(JSONB, nullable=True)

    # --- State Fields (Original Column syntax) ---
    last_processed_timestamp = Column(DateTime(timezone=True), nullable=True)
    last_successful_run = Column(DateTime(timezone=True), nullable=True)
    last_error_run = Column(DateTime(timezone=True), nullable=True)
    last_error_message = Column(Text, nullable=True)

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


    # id, created_at, updated_at inherited from Base

    def __repr__(self):
        # Update repr to include a metric count
        return (f"<DicomWebSourceState(id={self.id}, source_name='{self.source_name}', "
                f"enabled={self.is_enabled}, queued={self.queued_instance_count})>") # Example using queued count
