# backend/app/db/models/google_healthcare_source.py
from __future__ import annotations # Needed for type hints potentially

from sqlalchemy import String, Boolean, JSON, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base # Import your actual Base

class GoogleHealthcareSource(Base):
    # __tablename__ generated automatically by Base class `declared_attr`
    # id, created_at, updated_at inherited from Base

    name: Mapped[str] = mapped_column(String, index=True, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(String, nullable=True) # Use | None for optional typing

    # Google Healthcare Specific Config
    gcp_project_id: Mapped[str] = mapped_column(String, nullable=False)
    gcp_location: Mapped[str] = mapped_column(String, nullable=False)
    gcp_dataset_id: Mapped[str] = mapped_column(String, nullable=False)
    gcp_dicom_store_id: Mapped[str] = mapped_column(String, nullable=False)

    # Polling Config
    polling_interval_seconds: Mapped[int] = mapped_column(Integer, default=300, nullable=False)
    # Stored as JSONB in Postgres via JSON type decorator
    # Use dict | None for the type hint matching Pydantic
    query_filters: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Control Flags
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False) # Can be used in browser/rules?
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False) # Poller actively runs?

    # rules = relationship("Rule", secondary="rule_google_healthcare_source_assoc", back_populates="google_healthcare_sources") # Add later if needed
