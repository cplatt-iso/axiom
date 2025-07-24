# app/db/models/imaging_order.py
from datetime import datetime
from sqlalchemy import String, DateTime, Date, Enum as DBEnum, Text, func, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from .user import User

from app.db.base import Base
from app.schemas.enums import OrderStatus # We'll add this enum



class ImagingOrder(Base):
    __tablename__ = "imaging_orders" # type: ignore

    # Patient Information
    patient_name: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    patient_id: Mapped[str] = mapped_column(String(128), index=True, comment="MRN")
    patient_dob: Mapped[Optional[Date]] = mapped_column(Date)
    patient_sex: Mapped[Optional[str]] = mapped_column(String(16))

    # Order Identifiers
    accession_number: Mapped[str] = mapped_column(String(64), unique=True, index=True, comment="The critical, unique identifier for the study.")
    placer_order_number: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    filler_order_number: Mapped[Optional[str]] = mapped_column(String(128), index=True)

    # Procedure Information
    requested_procedure_description: Mapped[Optional[str]] = mapped_column(String(255))
    requested_procedure_code: Mapped[Optional[str]] = mapped_column(String(64))
    modality: Mapped[str] = mapped_column(String(16), index=True)

    # Scheduling and Location
    scheduled_station_ae_title: Mapped[Optional[str]] = mapped_column(String(16))
    scheduled_station_name: Mapped[Optional[str]] = mapped_column(String(128))
    scheduled_procedure_step_start_datetime: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    
    # Physician Information
    requesting_physician: Mapped[Optional[str]] = mapped_column(String(255))
    referring_physician: Mapped[Optional[str]] = mapped_column(String(255))

    # System and Status Fields
    order_status: Mapped[OrderStatus] = mapped_column(
        DBEnum(OrderStatus, name="order_status_enum", create_type=True, native_enum=False),
        nullable=False,
        default=OrderStatus.SCHEDULED,
        server_default=OrderStatus.SCHEDULED.value,
        index=True
    )
    study_instance_uid: Mapped[Optional[str]] = mapped_column(String(128), unique=True, index=True, nullable=True, comment="Will be generated on first C-FIND, unless provided.")
    
    # Source and Audit
    source: Mapped[str] = mapped_column(String(255), comment="Identifier for the source system, e.g., 'HL7v2_MLLP_LISTENER_MAIN'")
    order_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    raw_hl7_message: Mapped[Optional[str]] = mapped_column(Text, comment="For debugging the fuck-ups.")

    # Relationships
    creator_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))
    creator: Mapped["User"] = relationship("User", back_populates="imaging_orders")

    def __repr__(self) -> str:
        return f"<ImagingOrder(id={self.id}, accn='{self.accession_number}', status='{self.order_status.value}')>"