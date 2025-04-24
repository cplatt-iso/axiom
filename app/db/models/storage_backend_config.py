# app/db/models/storage_backend_config.py
from typing import Optional, Dict, Any, List
from datetime import datetime

from sqlalchemy import String, Boolean, Text, JSON, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
# --- Import association table from rule.py ---
from .rule import rule_destination_association
# --- END Import ---


class StorageBackendConfig(Base):
    """
    Database model to store configurations for various storage backends
    (e.g., filesystem, cstore, gcs, google_healthcare, stow_rs).
    """
    __tablename__ = "storage_backend_configs"

    # Inherits id, created_at, updated_at from Base

    name: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        index=True,
        nullable=False,
        comment="Unique, user-friendly name for this storage backend configuration."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Optional description of the backend's purpose or location."
    )
    backend_type: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        index=True,
        comment="Identifier for the type of storage backend."
    )
    config: Mapped[Dict[str, Any]] = mapped_column(
        JSON,
        nullable=False,
        default={},
        comment="JSON object containing backend-specific settings (path, AE title, bucket, etc.)."
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        index=True,
        comment="Whether this storage backend configuration is active and usable in rules."
    )

    # --- UPDATED: Relationship uses imported association table ---
    # Type hint uses string forward reference because Rule is defined elsewhere
    rules: Mapped[List["Rule"]] = relationship(
        "Rule", # Use string name
        secondary=rule_destination_association,
        back_populates="destinations"
    )
    # --- END UPDATED ---

    def __repr__(self):
        return (f"<StorageBackendConfig(id={self.id}, name='{self.name}', "
                f"type='{self.backend_type}', enabled={self.is_enabled})>")
