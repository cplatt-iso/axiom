# app/db/models/storage_backend_config.py
from typing import Optional, Dict, Any, List
from datetime import datetime

from sqlalchemy import String, Boolean, Text, JSON, Integer, ForeignKey # Added ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates # Added validates
from sqlalchemy.sql import expression # Added expression

# Import Base and the centrally defined association table
from app.db.base import Base, rule_destination_association
from app.db.models.rule import Rule

# Define allowed backend types - MUST match discriminator values
ALLOWED_BACKEND_TYPES = [
    "filesystem",
    "cstore",
    "gcs",
    "google_healthcare",
    "stow_rs"
]

class StorageBackendConfig(Base):
    """
    Base class for Storage Backend configurations using Single Table Inheritance.
    Contains common fields and the discriminator.
    """
    __tablename__ = "storage_backend_configs" # type: ignore # Keep the same table name

    # --- Common Fields ---
    # Ensure primary key is defined here or inherited correctly from Base
    # If Base doesn't define id, uncomment the next line:
    # id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(
        String(100), unique=True, index=True, nullable=False,
        comment="Unique, user-friendly name for this storage backend configuration."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Optional description of the backend's purpose or location."
    )
    backend_type: Mapped[str] = mapped_column(
        String(50), nullable=False, index=True,
        comment="Discriminator: Identifier for the type of storage backend."
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, index=True,
        comment="Whether this storage backend configuration is active and usable in rules."
    )

    # Relationship to Rules (remains on the base class)
    # Ensure "Rule" is correctly defined or use forward reference if needed
    rules: Mapped[List["Rule"]] = relationship(
        "Rule",
        secondary=rule_destination_association,
        back_populates="destinations"
    )

    # --- SQLAlchemy Inheritance Configuration ---
    __mapper_args__ = {
        "polymorphic_identity": "storage_backend_config", # Identity of the base class (optional but good practice)
        "polymorphic_on": backend_type, # Column used to determine the subclass
    }

    @validates('backend_type')
    def validate_backend_type(self, key, value):
        if value not in ALLOWED_BACKEND_TYPES:
             # This validation helps at the Python level but doesn't add a DB constraint
            raise ValueError(f"Invalid backend_type '{value}'. Allowed types: {ALLOWED_BACKEND_TYPES}")
        return value

    def __repr__(self):
        # Use self.__class__.__name__ to show the actual subclass name
        return (f"<{self.__class__.__name__}(id={self.id}, name='{self.name}', "
                f"type='{self.backend_type}', enabled={self.is_enabled})>")


# --- Subclasses for Specific Backend Types ---

class FileSystemBackendConfig(StorageBackendConfig):
    """Configuration for Filesystem storage backend."""
    # Inherits fields from StorageBackendConfig
    path: Mapped[str] = mapped_column(
        String(512), nullable=True, # Nullable because it only applies to this type
        comment="Path to the directory for storing files."
    )

    __mapper_args__ = {
        "polymorphic_identity": "filesystem", # Value in 'backend_type' column for this subclass
    }
    __table_args__ = {'extend_existing': True}

class GcsBackendConfig(StorageBackendConfig):
    """Configuration for Google Cloud Storage backend."""
    bucket: Mapped[str] = mapped_column(
        String(255), nullable=True,
        comment="Name of the GCS bucket."
    )
    prefix: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True,
        comment="Optional prefix (folder path) within the bucket."
    )
    # credentials_secret_name: Mapped[Optional[str]] = mapped_column(...) # If needed later

    __mapper_args__ = {
        "polymorphic_identity": "gcs",
    }
    __table_args__ = {'extend_existing': True}

class CStoreBackendConfig(StorageBackendConfig):
    """Configuration for DICOM C-STORE SCU backend."""
    remote_ae_title: Mapped[str] = mapped_column(
        String(16), nullable=True, index=True,
        comment="AE Title of the remote C-STORE SCP."
    )
    remote_host: Mapped[str] = mapped_column(
        String(255), nullable=True,
        comment="Hostname or IP address of the remote SCP."
    )
    remote_port: Mapped[int] = mapped_column(
        Integer, nullable=True,
        comment="Network port of the remote SCP."
    )
    local_ae_title: Mapped[Optional[str]] = mapped_column(
        String(16), nullable=True, default="AXIOM_STORE_SCU",
        comment="AE Title OUR SCU will use when associating."
    )

    # --- TLS Configuration (SCU) ---
    tls_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=True, default=False, server_default=expression.false(), # Nullable, default False
        comment="Enable TLS for outgoing connections to the remote peer."
    )
    tls_ca_cert_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True,
        comment="REQUIRED for TLS: Secret Manager resource name for the CA certificate (PEM) used to verify the remote peer's server certificate."
    )
    tls_client_cert_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True,
        comment="Optional (for mTLS): Secret Manager resource name for OUR client certificate (PEM)."
    )
    tls_client_key_secret_name: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True,
        comment="Optional (for mTLS): Secret Manager resource name for OUR client private key (PEM)."
    )

    __mapper_args__ = {
        "polymorphic_identity": "cstore",
    }
    __table_args__ = {'extend_existing': True}

class GoogleHealthcareBackendConfig(StorageBackendConfig):
    """Configuration for Google Cloud Healthcare DICOM Store backend."""
    gcp_project_id: Mapped[str] = mapped_column(String(255), nullable=True)
    gcp_location: Mapped[str] = mapped_column(String(100), nullable=True)
    gcp_dataset_id: Mapped[str] = mapped_column(String(100), nullable=True)
    gcp_dicom_store_id: Mapped[str] = mapped_column(String(100), nullable=True)
    # credentials_secret_name: Mapped[Optional[str]] = mapped_column(...) # If needed later

    __mapper_args__ = {
        "polymorphic_identity": "google_healthcare",
    }
    __table_args__ = {'extend_existing': True}

class StowRsBackendConfig(StorageBackendConfig):
    """Configuration for STOW-RS backend."""
    base_url: Mapped[str] = mapped_column(
        String(512), nullable=True,
        comment="Base URL of the STOW-RS service (e.g., https://dicom.server.com/dicomweb)."
    )
    username_secret_name: Mapped[Optional[str]] = mapped_column( # ADDED
        String(512), nullable=True,
        comment="Optional: Secret Manager resource name for the STOW-RS username."
    )
    password_secret_name: Mapped[Optional[str]] = mapped_column( # ADDED
        String(512), nullable=True,
        comment="Optional: Secret Manager resource name for the STOW-RS password."
    )
    tls_ca_cert_secret_name: Mapped[Optional[str]] = mapped_column( # ADDED
        String(512), nullable=True,
        comment="Optional: Secret Manager resource name for a custom CA certificate (PEM) to verify the STOW-RS server."
    )
    # auth_type: Mapped[Optional[str]] = mapped_column(...) # e.g., 'none', 'basic', 'oauth'
    # credentials_secret_name: Mapped[Optional[str]] = mapped_column(...) # For API keys, tokens etc.

    __mapper_args__ = {
        "polymorphic_identity": "stow_rs",
    }
    __table_args__ = {'extend_existing': True}

# --- IMPORTANT ---
# Ensure your app.db.base.Base class correctly defines id, created_at, updated_at
# or adjust the subclasses if they need to define these explicitly.
# Also ensure the "Rule" relationship target is correct.
