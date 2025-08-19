# app/db/models/order_dicom_evidence.py
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, ForeignKey, Boolean,
    UniqueConstraint, Index
)
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from app.db.base import Base

if TYPE_CHECKING:
    from .imaging_order import ImagingOrder


class OrderDicomEvidence(Base):
    """
    Tracks evidence that DICOM objects matching an order were actually processed.
    This provides the crucial link between orders and the DICOM objects that fulfill them.
    """
    __tablename__ = "order_dicom_evidence"  # type: ignore[assignment]

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    
    # Link to the order this evidence is for
    imaging_order_id: Mapped[int] = mapped_column(
        ForeignKey("imaging_orders.id", ondelete="CASCADE"), 
        index=True,
        comment="ID of the imaging order this evidence belongs to"
    )
    
    # DICOM identifiers - what actually got processed
    sop_instance_uid: Mapped[str] = mapped_column(
        String(128), 
        index=True,
        comment="SOP Instance UID of the processed DICOM object"
    )
    study_instance_uid: Mapped[Optional[str]] = mapped_column(
        String(128), 
        index=True,
        comment="Study Instance UID from the DICOM object"
    )
    series_instance_uid: Mapped[Optional[str]] = mapped_column(
        String(128), 
        comment="Series Instance UID from the DICOM object"
    )
    accession_number: Mapped[Optional[str]] = mapped_column(
        String(64), 
        index=True,
        comment="Accession number from the DICOM object (for verification)"
    )
    
    # Matching information - how we connected this DICOM to the order
    match_rule: Mapped[str] = mapped_column(
        String(50),
        comment="Rule that matched this DICOM to the order (e.g., 'ACCESSION_NUMBER', 'STUDY_INSTANCE_UID')"
    )
    
    # Processing details - what happened to it
    applied_rule_names: Mapped[Optional[str]] = mapped_column(
        Text,
        comment="Comma-separated list of rule names that processed this DICOM object"
    )
    applied_rule_ids: Mapped[Optional[str]] = mapped_column(
        String(255),
        comment="Comma-separated list of rule IDs that processed this DICOM object"
    )
    
    # Destination tracking - where it went and if it worked
    destination_results: Mapped[Optional[str]] = mapped_column(
        Text,
        comment="JSON string containing destination results: {dest_name: {status, message, dest_id, backend_type}}"
    )
    
    processing_successful: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        comment="True if overall processing was successful, False if any critical failures occurred"
    )
    
    # Metadata
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        server_default=func.now(),
        comment="When this DICOM object was processed"
    )
    source_identifier: Mapped[Optional[str]] = mapped_column(
        String(255),
        comment="Identifier of the source that provided this DICOM object"
    )
    
    # Standard timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationships
    imaging_order: Mapped["ImagingOrder"] = relationship(
        "ImagingOrder", 
        back_populates="dicom_evidence"
    )
    
    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint('imaging_order_id', 'sop_instance_uid', name='unique_order_sop'),
        Index('idx_order_dicom_evidence_study_uid', 'study_instance_uid'),
        Index('idx_order_dicom_evidence_processed_at', 'processed_at'),
        Index('idx_order_dicom_evidence_match_rule', 'match_rule'),
    )
    
    def __repr__(self):
        return (f"<OrderDicomEvidence(id={self.id}, order_id={self.imaging_order_id}, "
                f"sop_uid='{self.sop_instance_uid[:20]}...', match_rule='{self.match_rule}', "
                f"successful={self.processing_successful})>")
