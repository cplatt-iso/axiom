# app/db/models/processed_study_log.py
from sqlalchemy import (
    Column, Integer, String, DateTime, Enum as DBEnum, ForeignKey,
    UniqueConstraint, Index
)
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column
import enum
from typing import ClassVar
# --- ADDED: Import datetime ---
from datetime import datetime
# --- END ADDED ---

from app.schemas.enums import ProcessedStudySourceType
from app.db.base import Base # Import your Base class

# Enum for source types
#class ProcessedStudySourceType(str, enum.Enum):
#    DICOMWEB = "DICOMWEB"
#    DIMSE_QR = "DIMSE_QR"
#    DIMSE_LISTENER = "DIMSE_LISTENER" # Added for C-STORE SCP input
#    STOW_RS = "STOW_RS"               # Added for STOW-RS input
#    GOOGLE_HEALTHCARE = "GOOGLE_HEALTHCARE" # Added for Google Healthcare input
#    FILE_UPLOAD = "FILE_UPLOAD" # Added for File Upload input
    # Add other source types as needed

class ProcessedStudyLog(Base):
    """
    by a specific polling source or listener to prevent duplicates.
    """
    __tablename__ = "processed_study_log"  # type: ignore

    # Inherits id (PK), created_at, updated_at from Base
    # Inherits id (PK), created_at, updated_at from Base

    source_type: Mapped[ProcessedStudySourceType] = mapped_column(
        # Use DBEnum, create type if DB supports it, use VARCHAR otherwise
        DBEnum(ProcessedStudySourceType, name="processed_study_source_enum", create_type=True, native_enum=False),
        nullable=False,
        index=True,
        comment="Type of the source that initiated processing (DICOMWEB, DIMSE_QR, DIMSE_LISTENER)." # Updated comment
    )

    # This source_id refers to the ID in the *respective* source config table
    # (e.g., dicomweb_source_state.id, dimse_qr_sources.id) OR the unique string
    # identifier for sources without integer IDs (e.g., dimse_listener_configs.instance_id).
    # Use String to accommodate both cases.
    source_id: Mapped[str] = mapped_column( # <<< CHANGED TYPE TO STRING >>>
        String(255), # Increased size to handle potential string IDs like instance_id
        nullable=False,
        index=True, # Index this column
        comment="Identifier of the source config (int ID for DICOMweb/QR, string instance_id for Listener)." # Updated comment
    )

    study_instance_uid: Mapped[str] = mapped_column(
        String(128), # DICOM UIDs can be up to 64 chars, allow some buffer
        nullable=False,
        index=True,
        comment="The Study Instance UID that was processed/queued."
    )

    # Timestamp when the study was first detected by this source during polling
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(), # Defaults to now on insert
        nullable=False,
        comment="Timestamp when the study was first encountered by this source."
    )

    # Timestamp when the retrieval task (WADO/C-MOVE) or processing task was successfully queued
    retrieval_queued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(), # Set when the log entry is created
        nullable=False,
        comment="Timestamp when the retrieval/processing task was queued."
    )

    # Optional status field for future enhancement
    # status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True, index=True)

    # Define unique constraint for the combination
    # Add potentially useful indexes
    __table_args__ = (
        UniqueConstraint('source_type', 'source_id', 'study_instance_uid', name='uq_processed_study_source_uid'),
        Index('ix_processed_study_log_queued_at', 'retrieval_queued_at'),
        # --- Ensure index name is suitable for string type ---
        Index('ix_processed_study_log_source_id_str', 'source_id'), # Index on the string source_id
    )

    def __repr__(self):
        return (f"<ProcessedStudyLog(id={self.id}, type='{self.source_type.value}', " # Use .value for Enum
                f"source_id='{self.source_id}', study_uid='{self.study_instance_uid[:10]}...', " # source_id is now potentially string
                f"queued='{self.retrieval_queued_at}')>")
