from datetime import datetime
from sqlalchemy import (
    String,
    DateTime,
    Enum as DBEnum,
    Text,
    func,
    ForeignKey,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional, TYPE_CHECKING

from app.db.base import Base
from app.schemas.enums import MppsStatus

if TYPE_CHECKING:
    from .imaging_order import ImagingOrder


class Mpps(Base):
    """
    Represents a Modality Performed Procedure Step (MPPS) in the database.
    This table stores the state of a procedure as reported by a modality.
    """
    __tablename__ = "mpps" # type: ignore

    id: Mapped[int] = mapped_column(primary_key=True, index=True)

    # Foreign Key to the original worklist order
    imaging_order_id: Mapped[Optional[int]] = mapped_column(ForeignKey("imaging_orders.id"), index=True, nullable=True)
    imaging_order: Mapped[Optional["ImagingOrder"]] = relationship(back_populates="mpps_messages")

    # Core MPPS Attributes from DICOM
    sop_instance_uid: Mapped[str] = mapped_column(String(128), unique=True, index=True, comment="SOP Instance UID of the MPPS object.")
    status: Mapped[MppsStatus] = mapped_column(DBEnum(MppsStatus, name="mpps_status_enum", create_type=True), nullable=False, comment="The status of the procedure step.")
    
    modality: Mapped[Optional[str]] = mapped_column(String(16), comment="The modality that performed the step.")
    performed_station_ae_title: Mapped[Optional[str]] = mapped_column(String(16), comment="AE Title of the modality that performed the step.")
    performed_procedure_step_start_datetime: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    performed_procedure_step_end_datetime: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Store the raw dataset for forwarding or debugging purposes
    raw_mpps_message: Mapped[Optional[dict]] = mapped_column(JSON, comment="The raw MPPS pydicom dataset stored as JSON.")

    # Standard timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<Mpps(id={self.id}, sop_instance_uid='{self.sop_instance_uid}', status='{self.status}')>"
