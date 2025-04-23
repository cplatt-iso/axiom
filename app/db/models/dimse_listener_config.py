# app/db/models/dimse_listener_config.py
from typing import Optional
from sqlalchemy import String, Integer, Boolean, Text, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base # Import your Base class

class DimseListenerConfig(Base):
    """
    Database model to store the configuration for DIMSE C-STORE SCP listeners.
    Each record defines a potential listener instance.
    """
    __tablename__ = "dimse_listener_configs" # New table name

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
        String(16), # DICOM AE Titles are max 16 chars
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
        index=True,
        comment="Whether this listener configuration is active and should be started."
    )
    # This instance_id links the config to a specific running listener process/container.
    # The listener process reads its AXIOM_INSTANCE_ID env var and looks up its config using this field.
    # Making it nullable allows defining configs before assigning them to specific instances.
    # Making it unique ensures only one config applies to a specific instance ID.
    instance_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        unique=True,
        index=True,
        nullable=True, # Nullable: Allows creating configs without immediate assignment
        comment="Unique ID matching AXIOM_INSTANCE_ID env var of the listener process using this config."
    )

    # Add unique constraints on ae_title/port combination if desired
    # __table_args__ = (UniqueConstraint('ae_title', 'port', name='uq_aetitle_port'),)

    def __repr__(self):
        return (f"<DimseListenerConfig(id={self.id}, name='{self.name}', "
                f"ae_title='{self.ae_title}', port={self.port}, enabled={self.is_enabled}, "
                f"instance_id='{self.instance_id}')>")
