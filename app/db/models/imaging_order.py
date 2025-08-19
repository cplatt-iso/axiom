# app/db/models/imaging_order.py
from datetime import datetime   # Relationships
from sqlalchemy import String, DateTime, Date, Enum as DBEnum, Text, func, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import List, Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from .user import User
    from .mpps import Mpps
    from .order_dicom_evidence import OrderDicomEvidence

from app.db.base import Base
from app.schemas.enums import OrderStatus # We'll add this enum



class ImagingOrder(Base):
    __tablename__ = "imaging_orders" # type: ignore

    # Patient Information
    patient_name: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    patient_id: Mapped[str] = mapped_column(String(128), index=True, comment="MRN")
    patient_dob: Mapped[Optional[Date]] = mapped_column(Date)
    patient_sex: Mapped[Optional[str]] = mapped_column(String(16))
    # --- NEW PATIENT FIELDS ---
    patient_address: Mapped[Optional[str]] = mapped_column(Text, comment="Patient's full address.")
    patient_phone_number: Mapped[Optional[str]] = mapped_column(String(64), comment="Patient's primary phone number.")
    patient_class: Mapped[Optional[str]] = mapped_column(String(16), comment="e.g., O (Outpatient), I (Inpatient). From PV1-2.")
    visit_number: Mapped[Optional[str]] = mapped_column(String(64), index=True, comment="Patient's visit/encounter number. From PV1-19.")
    # --- END NEW PATIENT FIELDS ---

    # Order Identifiers
    accession_number: Mapped[str] = mapped_column(String(64), unique=True, index=True, comment="The critical, unique identifier for the study.")
    placer_order_number: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    filler_order_number: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    # --- NEW ORDER FIELDS ---
    placer_group_number: Mapped[Optional[str]] = mapped_column(String(128), index=True, comment="Groups related orders together. From ORC-4.")
    # --- END NEW ORDER FIELDS ---

    # Procedure Information
    requested_procedure_description: Mapped[Optional[str]] = mapped_column(String(255))
    requested_procedure_code: Mapped[Optional[str]] = mapped_column(String(64))
    modality: Mapped[str] = mapped_column(String(16), index=True)

    # Scheduling and Location
    scheduled_station_ae_title: Mapped[Optional[str]] = mapped_column(String(16))
    scheduled_station_name: Mapped[Optional[str]] = mapped_column(String(128))
    scheduled_procedure_step_start_datetime: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    scheduled_exam_datetime: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        index=True,
        comment="Datetime the exam is scheduled to take place (ORC.7 TQ.4)."
    )
    
    # Physician Information
    requesting_physician: Mapped[Optional[str]] = mapped_column(String(255))
    referring_physician: Mapped[Optional[str]] = mapped_column(String(255))
    # --- NEW PHYSICIAN FIELDS ---
    attending_physician: Mapped[Optional[str]] = mapped_column(String(255), comment="Attending physician. From PV1-7.")
    # --- END NEW PHYSICIAN FIELDS ---

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
    # --- NEW SOURCE FIELDS ---
    source_sending_application: Mapped[Optional[str]] = mapped_column(String(255), comment="Sending application from MSH-3.")
    source_sending_facility: Mapped[Optional[str]] = mapped_column(String(255), comment="Sending facility from MSH-4.")
    source_receiving_application: Mapped[Optional[str]] = mapped_column(String(255), comment="Receiving application from MSH-5.")
    source_receiving_facility: Mapped[Optional[str]] = mapped_column(String(255), comment="Receiving facility from MSH-6.")
    source_message_control_id: Mapped[Optional[str]] = mapped_column(String(128), comment="Message Control ID from MSH-10.")
    # --- END NEW SOURCE FIELDS ---
    order_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    raw_hl7_message: Mapped[Optional[str]] = mapped_column(Text, comment="For debugging the fuck-ups.")

    # Relationships    
    creator_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"))
    creator: Mapped[Optional["User"]] = relationship("User", back_populates="imaging_orders")
    mpps_messages: Mapped[List["Mpps"]] = relationship("Mpps", back_populates="imaging_order")
    dicom_evidence: Mapped[List["OrderDicomEvidence"]] = relationship(
        "OrderDicomEvidence", 
        back_populates="imaging_order",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<ImagingOrder(id={self.id}, accn='{self.accession_number}', status='{self.order_status.value}')>"