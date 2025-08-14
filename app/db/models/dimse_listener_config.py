# app/db/models/dimse_listener_config.py
from typing import Optional
from sqlalchemy import String, Integer, Boolean, Text, Index, text # Added text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import expression # Added expression

from app.db.base import Base # Import your Base class

class DimseListenerConfig(Base):
    """
    Database model to store the configuration for DIMSE C-STORE SCP listeners.
    Each record defines a potential listener instance. Includes TLS server config.
    """
    __tablename__ = "dimse_listener_configs" # type: ignore

    # Inherits id, created_at, updated_at from Base

    name: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        index=True,
        nullable=False,
        comment="Unique, user-friendly name for this listener configuration."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Optional description of the listener's purpose."
    )
    ae_title: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        index=True,
        comment="The Application Entity Title the listener will use."
    )
    port: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        index=True,
        comment="The network port the listener will bind to."
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=expression.true(), # Added server default
        index=True,
        comment="Whether this listener configuration is active and should be started."
    )
    instance_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        unique=True,
        index=True,
        nullable=True,
        comment="Unique ID matching AXIOM_INSTANCE_ID env var of the listener process using this config."
    )

    # --- TLS Server Configuration ---
    tls_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=expression.false(), # Added server default
        index=True,
        comment="Enable TLS for incoming connections to this listener."
    )
    # Server Certificate and Key (Required if TLS is enabled)
    tls_cert_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512),
        nullable=True,
        comment="REQUIRED if TLS enabled: Secret Manager resource name for the server's TLS certificate (PEM)."
    )
    tls_key_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512),
        nullable=True,
        comment="REQUIRED if TLS enabled: Secret Manager resource name for the server's TLS private key (PEM)."
    )
    # Client Authentication Certificate Authority (Optional, for mTLS)
    tls_ca_cert_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512),
        nullable=True,
        comment="Optional (for mTLS): Secret Manager resource name for the CA certificate (PEM) used to verify client certificates."
    )
    # --- End TLS ---

    listener_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default="pynetdicom",
        server_default=text("'pynetdicom'"),
        index=True,
        comment="The type of listener implementation to use ('pynetdicom' or 'dcm4che')."
    )

    def __repr__(self):
        tls_status = f"TLS={'Enabled' if self.tls_enabled else 'Disabled'}"
        return (f"<DimseListenerConfig(id={self.id}, name='{self.name}', "
                f"ae_title='{self.ae_title}', port={self.port}, enabled={self.is_enabled}, "
                f"instance_id='{self.instance_id}', type='{self.listener_type}', {tls_status})>") # Updated repr
