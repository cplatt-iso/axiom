# app/db/models/dicomweb_source_state.py

# --- Updated Imports ---
from sqlalchemy import Column, String, DateTime, Text, Boolean, Integer
from sqlalchemy.dialects.postgresql import JSONB # Use JSONB for Postgres for better performance/indexing
# If not using Postgres or want standard JSON, use: from sqlalchemy.types import JSON
from sqlalchemy.sql import func # For server-side default timestamps if needed
# --- End Updated Imports ---

from app.db.base import Base # Use your specific Base import path

class DicomWebSourceState(Base):
    """
    Database model to store the configuration and polling state
    for configured DICOMweb sources.
    """
    __tablename__ = "dicomweb_source_state"

    # --- Configuration Fields (Mirrors DicomWebSourceConfigBase Pydantic schema) ---
    source_name = Column(String, unique=True, index=True, nullable=False)
    description = Column(Text, nullable=True)
    base_url = Column(String, nullable=False) # Required configuration
    qido_prefix = Column(String, nullable=False, default="qido-rs")
    wado_prefix = Column(String, nullable=False, default="wado-rs")
    polling_interval_seconds = Column(Integer, nullable=False, default=300)
    is_enabled = Column(Boolean, nullable=False, default=True) # Also part of config
    auth_type = Column(String(50), nullable=False, default="none") # e.g., "none", "basic", "bearer"
    # Use JSONB for PostgreSQL, standard JSON otherwise
    auth_config = Column(JSONB, nullable=True) # Store dict like {'username': 'x', 'password': 'y'} or {'token': 'z'}
    search_filters = Column(JSONB, nullable=True) # Store QIDO filter dict

    # --- State Fields (Managed by the poller) ---
    # Timestamp of the most recent item successfully processed or queried from this source
    last_processed_timestamp = Column(DateTime(timezone=True), nullable=True)
    # Timestamp of the last time the poller ran successfully for this source
    last_successful_run = Column(DateTime(timezone=True), nullable=True)
    # Timestamp of the last time the poller encountered an error for this source
    last_error_run = Column(DateTime(timezone=True), nullable=True)
    # Store the last error message encountered during polling
    last_error_message = Column(Text, nullable=True)

    # Note: Inherits 'id' (PK), 'created_at', 'updated_at' from Base class if defined there

    def __repr__(self):
        # Update repr to include key config fields
        return (f"<DicomWebSourceState(id={self.id}, source_name='{self.source_name}', "
                f"base_url='{self.base_url}', enabled={self.is_enabled}, "
                f"last_success='{self.last_successful_run}')>")
