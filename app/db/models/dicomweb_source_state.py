# app/db/models/dicomweb_source_state.py

from sqlalchemy import Column, String, DateTime, Text, Boolean
from sqlalchemy.sql import func # For server-side default timestamps if needed

from app.db.base import Base # Use your specific Base import path

class DicomWebSourceState(Base):
    """
    Database model to store the polling state for configured DICOMweb sources.
    """
    __tablename__ = "dicomweb_source_state"

    # Corresponds to the 'name' field in Settings.DICOMWEB_SOURCES
    source_name = Column(String, unique=True, index=True, nullable=False) 

    # Timestamp of the most recent item successfully processed or queried from this source
    # Use timezone=True if storing timezone-aware datetimes
    last_processed_timestamp = Column(DateTime(timezone=True), nullable=True)

    # Timestamp of the last time the poller ran successfully for this source
    last_successful_run = Column(DateTime(timezone=True), nullable=True)

    # Timestamp of the last time the poller encountered an error for this source
    last_error_run = Column(DateTime(timezone=True), nullable=True)

    # Store the last error message encountered during polling
    last_error_message = Column(Text, nullable=True)

    # Reflects the 'is_enabled' status from the configuration at startup/update
    # This allows disabling polling without changing the main config file immediately
    # if you add an admin interface later.
    is_enabled = Column(Boolean, default=True, nullable=False)

    # Optional: Add created_at/updated_at if desired
    # created_at = Column(DateTime(timezone=True), server_default=func.now())
    # updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return (f"<DicomWebSourceState(source_name='{self.source_name}', "
                f"enabled={self.is_enabled}, "
                f"last_processed='{self.last_processed_timestamp}', "
                f"last_success='{self.last_successful_run}', "
                f"last_error='{self.last_error_run}')>")
