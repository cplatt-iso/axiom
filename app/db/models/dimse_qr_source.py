# app/db/models/dimse_qr_source.py
from typing import Optional, Dict, Any, List
from datetime import datetime
from sqlalchemy import String, Integer, Boolean, Text, JSON, DateTime, text
# Use JSONB for PostgreSQL if preferred and dialect is available
# from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func, expression # <-- ADDED expression

from app.db.base import Base # Import your Base class

class DimseQueryRetrieveSource(Base):
    """
    Database model to store the configuration for remote DIMSE peers
    that Axiom Flow will query using C-FIND and potentially retrieve from
    using C-MOVE or C-GET. Includes TLS configuration using Google
    Secret Manager resource names for SCU operations.
    """
    __tablename__ = "dimse_qr_sources"

    # --- Basic Config ---
    name: Mapped[str] = mapped_column(
        String(100), unique=True, index=True, nullable=False,
        comment="Unique, user-friendly name for this remote DIMSE peer configuration."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Optional description of the remote peer or its purpose."
    )

    # --- Remote Peer Details ---
    remote_ae_title: Mapped[str] = mapped_column(
        String(16), nullable=False, index=True,
        comment="AE Title of the remote peer to query/retrieve from."
    )
    remote_host: Mapped[str] = mapped_column(
        String(255), nullable=False,
        comment="Hostname or IP address of the remote peer."
    )
    remote_port: Mapped[int] = mapped_column(
        Integer, nullable=False,
        comment="Network port of the remote peer's DIMSE service."
    )

    # --- Our Local AE Details for Association ---
    local_ae_title: Mapped[str] = mapped_column(
        String(16), nullable=False, default="AXIOM_QR_SCU",
        comment="AE Title our SCU will use when associating with the remote peer."
    )

    # --- TLS Configuration (SCU) ---
    tls_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=expression.false(), index=True, # <-- Default False
        comment="Enable TLS for outgoing connections to the remote peer."
    )
    # CA Certificate to verify the remote server's certificate
    tls_ca_cert_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True,
        comment="REQUIRED for TLS: Secret Manager resource name for the CA certificate (PEM) used to verify the remote peer's server certificate."
    )
    # Client Certificate and Key (Optional, for mutual TLS / client authentication)
    tls_client_cert_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True,
        comment="Optional (for mTLS): Secret Manager resource name for OUR client certificate (PEM)."
    )
    tls_client_key_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True,
        comment="Optional (for mTLS): Secret Manager resource name for OUR client private key (PEM)."
    )
    # Maybe add tls_check_hostname: Mapped[bool] = mapped_column(Boolean, default=True) if pynetdicom allows disabling hostname checks?

    # --- Polling Configuration ---
    polling_interval_seconds: Mapped[int] = mapped_column(
        Integer, nullable=False, default=300,
        comment="Frequency in seconds at which to poll the source using C-FIND."
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, index=True,
        comment="Whether this source configuration is generally enabled and available (e.g., for data browser)."
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text('true'), index=True,
        comment="Whether AUTOMATIC polling for this source is active based on its schedule."
    )

    # --- Query Configuration (C-FIND) ---
    query_level: Mapped[str] = mapped_column(
        String(10), nullable=False, default="STUDY",
        comment="Query Retrieve Level for C-FIND (e.g., STUDY)."
    )
    query_filters: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True,
        comment="JSON object containing key-value pairs for C-FIND query identifiers."
    )

    # --- Retrieval Configuration (C-MOVE/C-GET) ---
    move_destination_ae_title: Mapped[Optional[str]] = mapped_column(
        String(16), nullable=True,
        comment="AE Title of OUR listener where retrieved instances should be sent via C-MOVE."
    )

    # --- State Fields (Managed by the poller task) ---
    last_successful_query: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Timestamp of the last successful C-FIND query."
    )
    last_successful_move: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Timestamp of the last successful C-MOVE request completion (if applicable)."
    )
    last_error_time: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Timestamp of the last error encountered during polling or retrieval."
    )
    last_error_message: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Details of the last error encountered."
    )
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
        tls_status = f"TLS={'Enabled' if self.tls_enabled else 'Disabled'}"
        return (f"<DimseQueryRetrieveSource(id={self.id}, name='{self.name}', "
                f"remote_ae='{self.remote_ae_title}', host='{self.remote_host}:{self.remote_port}', "
                f"enabled={self.is_enabled}, active={self.is_active}, {tls_status})>") # Updated repr
