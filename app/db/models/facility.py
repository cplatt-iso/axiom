# app/db/models/facility.py
from typing import Optional, List, TYPE_CHECKING
from sqlalchemy import String, Text, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base

if TYPE_CHECKING:
    from .modality import Modality

class Facility(Base):
    """
    Database model for healthcare facilities that contain modalities.
    Facilities represent physical locations where DICOM imaging equipment is housed.
    """
    __tablename__ = "facilities"  # type: ignore

    # --- Basic Information ---
    name: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False,
        comment="Unique name of the healthcare facility."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Optional description of the facility."
    )

    # --- Address Information ---
    address_line_1: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True,
        comment="Primary address line (street number and name)."
    )
    address_line_2: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True,
        comment="Secondary address line (apartment, suite, building, etc.)."
    )
    city: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="City name."
    )
    state: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="State or province."
    )
    postal_code: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True,
        comment="Postal or ZIP code."
    )
    country: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Country name."
    )

    # --- Contact Information ---
    phone: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True,
        comment="Primary phone number for the facility."
    )
    email: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True,
        comment="Primary email contact for the facility."
    )

    # --- Administrative ---
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, nullable=False, index=True,
        comment="Whether this facility is currently active."
    )
    facility_id: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, unique=True, index=True,
        comment="External facility identifier (e.g., from HIS/RIS)."
    )

    # --- Relationships ---
    modalities: Mapped[List["Modality"]] = relationship(
        "Modality", back_populates="facility", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Facility(id={self.id}, name='{self.name}', active={self.is_active})>"
