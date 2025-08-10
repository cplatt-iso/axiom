# app/db/models/modality.py
from typing import Optional, TYPE_CHECKING
from sqlalchemy import String, Text, Boolean, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from .facility import Facility

class Modality(Base):
    """
    Database model for DICOM modalities (imaging equipment) that belong to facilities.
    Represents individual pieces of imaging equipment that can query the modality worklist.
    """
    __tablename__ = "modalities"  # type: ignore

    # --- Basic Information ---
    name: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True,
        comment="Descriptive name of the modality."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Optional description of the modality."
    )

    # --- DICOM Network Configuration ---
    ae_title: Mapped[str] = mapped_column(
        String(16), unique=True, index=True, nullable=False,
        comment="DICOM Application Entity Title for this modality."
    )
    ip_address: Mapped[str] = mapped_column(
        String(45), nullable=False, index=True,
        comment="IP address of the modality (supports IPv4 and IPv6)."
    )
    port: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True, default=104,
        comment="DICOM port number (default 104)."
    )

    # --- Modality Type ---
    modality_type: Mapped[str] = mapped_column(
        String(16), nullable=False, index=True,
        comment="DICOM modality type (CT, MR, DR, CR, US, etc.)."
    )

    # --- Security and Access Control ---
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False, index=True,
        comment="Whether this modality is currently active and allowed to query DMWL."
    )
    is_dmwl_enabled: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False, index=True,
        comment="Whether this modality is allowed to query the modality worklist."
    )

    # --- Facility Relationship ---
    facility_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("facilities.id", ondelete="CASCADE"), nullable=False, index=True,
        comment="Foreign key to the facility this modality belongs to."
    )

    # --- Equipment Details ---
    manufacturer: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True,
        comment="Equipment manufacturer."
    )
    model: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True,
        comment="Equipment model name/number."
    )
    software_version: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Software version of the modality."
    )
    station_name: Mapped[Optional[str]] = mapped_column(
        String(16), nullable=True,
        comment="DICOM Station Name."
    )

    # --- Access Control Settings ---
    department: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True, index=True,
        comment="Department this modality belongs to (for filtering worklist items)."
    )
    location: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True,
        comment="Physical location of the modality within the facility."
    )

    # --- Relationships ---
    facility: Mapped["Facility"] = relationship(
        "Facility", back_populates="modalities"
    )

    def __repr__(self) -> str:
        return f"<Modality(id={self.id}, ae_title='{self.ae_title}', type='{self.modality_type}', active={self.is_active})>"

    @property
    def full_address(self) -> str:
        """Return the full network address for this modality."""
        port = self.port or 104
        return f"{self.ip_address}:{port}"

    def can_query_dmwl(self) -> bool:
        """Check if this modality is allowed to query the modality worklist."""
        return self.is_active and self.is_dmwl_enabled and self.facility and self.facility.is_active
