# app/db/models/dimse_qr_source.py
from typing import Optional, Dict, Any, List
from datetime import datetime
from sqlalchemy import String, Integer, Boolean, Text, JSON, DateTime
# Use JSONB for PostgreSQL if preferred and dialect is available
# from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base # Import your Base class

class DimseQueryRetrieveSource(Base):
    """
    Database model to store the configuration for remote DIMSE peers
    that Axiom Flow will query using C-FIND and potentially retrieve from
    using C-MOVE or C-GET.
    """
    __tablename__ = "dimse_qr_sources"

    # Inherits id, created_at, updated_at from Base

    name: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        index=True,
        nullable=False,
        comment="Unique, user-friendly name for this remote DIMSE peer configuration."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Optional description of the remote peer or its purpose."
    )

    # --- Remote Peer Details ---
    remote_ae_title: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        index=True,
        comment="AE Title of the remote peer to query/retrieve from."
    )
    remote_host: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        comment="Hostname or IP address of the remote peer."
    )
    remote_port: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Network port of the remote peer's DIMSE service."
    )

    # --- Our Local AE Details for Association ---
    local_ae_title: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="AXIOM_QR_SCU",
        comment="AE Title our SCU will use when associating with the remote peer."
    )

    # --- Polling Configuration ---
    polling_interval_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=300, # Default to 5 minutes
        comment="Frequency in seconds at which to poll the source using C-FIND."
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
        comment="Whether polling this source is active."
    )

    # --- Query Configuration (C-FIND) ---
    query_level: Mapped[str] = mapped_column(
        String(10), # e.g., "STUDY", "SERIES"
        nullable=False,
        default="STUDY",
        comment="Query Retrieve Level for C-FIND (e.g., STUDY)."
    )
    # Store C-FIND query parameters as JSON
    # Example: {"StudyDate": "20240101-", "PatientName": "DOE^JOHN", "ModalitiesInStudy": "CT"}
    query_filters: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON, # Or JSONB for PostgreSQL
        nullable=True,
        comment="JSON object containing key-value pairs for C-FIND query identifiers."
    )
    # Add fields for dynamic date ranges later if needed, similar to DICOMweb

    # --- Retrieval Configuration (C-MOVE/C-GET) ---
    # If using C-MOVE, specify the destination AE Title (one of our configured listeners)
    move_destination_ae_title: Mapped[Optional[str]] = mapped_column(
        String(16),
        nullable=True,
        comment="AE Title of OUR listener where retrieved instances should be sent via C-MOVE."
    )
    # Optional: Add C-GET specific config later if needed

    # --- State Fields (Managed by the poller task) ---
    # Corrected definitions using the imported DateTime AND the datetime type hint
    last_successful_query: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the last successful C-FIND query."
    )
    last_successful_move: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the last successful C-MOVE request completion (if applicable)."
    )
    last_error_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Timestamp of the last error encountered during polling or retrieval."
    )
    last_error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Details of the last error encountered."
    )
    # Consider adding a field to track the last processed StudyDate/Time or specific UIDs
    # last_processed_identifier: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    found_study_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total count of unique studies found by C-FIND across all polls."
    )
    move_queued_study_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total count of studies actually queued for C-MOVE retrieval."
    )
    processed_instance_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total count of instances successfully processed after C-MOVE."
    )

    def __repr__(self):
        return (f"<DimseQueryRetrieveSource(id={self.id}, name='{self.name}', "
                f"remote_ae='{self.remote_ae_title}', host='{self.remote_host}:{self.remote_port}', "
                f"enabled={self.is_enabled})>")
