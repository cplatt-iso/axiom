# app/db/models/dicom_exception_log.py
from sqlalchemy import (
    String, Text, DateTime, Integer, ForeignKey, Enum as DBEnum, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from typing import Optional, TYPE_CHECKING
from datetime import datetime
import uuid
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.db.base import Base
# Import the enums we just defined, assuming they are now in app.schemas.enums
from app.schemas.enums import ProcessedStudySourceType, ExceptionProcessingStage, ExceptionStatus

if TYPE_CHECKING:
    # This is for type hinting relationships, if we add them later
    # from .storage_backend_config import StorageBackendConfig # Example
    pass

class DicomExceptionLog(Base):
    __tablename__ = "dicom_exception_log" # type: ignore

    # id: Mapped[int] is inherited from Base

    exception_uuid: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), # Specify PostgreSQL UUID type for the DB
        primary_key=False,
        unique=True,
        index=True,
        default=uuid.uuid4, # Python-side default
        server_default=func.gen_random_uuid(), # PostgreSQL function to generate UUID
        comment="Stable, unique identifier for this exception record."
    )
    # DICOM Identifiers (nullable, as they might not always be available at point of failure)
    study_instance_uid: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    series_instance_uid: Mapped[Optional[str]] = mapped_column(String(128), index=True)
    sop_instance_uid: Mapped[Optional[str]] = mapped_column(String(128), index=True)

    patient_name: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True, # PatientName (0010,0010) can be long
        comment="Patient's Name, if available from DICOM header."
    )
    patient_id: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True, index=True, # PatientID (0010,0020)
        comment="Patient's ID, if available from DICOM header."
    )
    accession_number: Mapped[Optional[str]] = mapped_column(
        String(64), nullable=True, index=True, # AccessionNumber (0008,0050)
        comment="Accession Number, if available from DICOM header."
    )
    modality: Mapped[Optional[str]] = mapped_column(
        String(16), nullable=True, index=True, # Modality (0008,0060) e.g., CT, MR, US
        comment="DICOM Modality, if available from DICOM header."
    )

    failure_timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True,
        comment="Timestamp when the failure was logged."
    )

    processing_stage: Mapped[ExceptionProcessingStage] = mapped_column(
        DBEnum(ExceptionProcessingStage, name="exception_processing_stage_enum", create_type=True, native_enum=False),
        nullable=False, index=True,
        comment="Specific stage in the pipeline where failure occurred."
    )

    error_message: Mapped[str] = mapped_column(
        Text, nullable=False,
        comment="Detailed error message or exception string."
    )

    error_details: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Additional details, like traceback or context-specific info."
    )

    failed_filepath: Mapped[Optional[str]] = mapped_column(
        String(1024), nullable=True,
        comment="Path to the errored DICOM file, if applicable (e.g., from listener or file upload)."
    )

    # Source Information
    original_source_type: Mapped[ProcessedStudySourceType] = mapped_column(
        DBEnum(ProcessedStudySourceType, name="processed_study_source_type_enum_for_exception", create_type=True, native_enum=False), # Potentially new enum instance in DB
        nullable=True, index=True, # Nullable if source unknown or not applicable
        comment="Original source system type that provided the DICOM data."
    )
    # Using string for source_identifier to accommodate various source ID formats (int, string AET, etc.)
    original_source_identifier: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, index=True,
        comment="Identifier of the original source (e.g., Listener AE Title, DICOMweb source name, QR source ID)."
    )
    # For DIMSE, store Calling AET if available
    calling_ae_title: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    # Destination Information (if failure was destination-specific)
    target_destination_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        # If you had a direct FK relationship: ForeignKey("storage_backend_configs.id", name="fk_exception_destination_id"),
        nullable=True, index=True,
        comment="ID of the StorageBackendConfig if failure was during send to a specific destination."
    )
    # To make it easier to query without joins for simple display, store destination name too
    target_destination_name: Mapped[Optional[str]] = mapped_column(
        String(100), nullable=True,
        comment="Name of the target destination at the time of failure."
    )

    # Retry Mechanism Fields
    status: Mapped[ExceptionStatus] = mapped_column(
        DBEnum(ExceptionStatus, name="exception_status_enum", create_type=True, native_enum=False),
        nullable=False, default=ExceptionStatus.NEW, server_default=ExceptionStatus.NEW.value, index=True,
        comment="Current status of this exception record."
    )
    retry_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0",
        comment="Number of automatic retry attempts made."
    )
    next_retry_attempt_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True,
        comment="Timestamp for the next scheduled retry attempt."
    )
    last_retry_attempt_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Timestamp of the last retry attempt."
    )

    # Audit / Resolution
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by_user_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        # If you had a direct FK: ForeignKey("users.id", name="fk_exception_resolver_user_id"),
        nullable=True, index=True
    )
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Consider adding the task_id from Celery if easily available at point of logging
    celery_task_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)


    # If you use a direct FK to storage_backend_configs or users, define relationships:
    # target_destination: Mapped[Optional["StorageBackendConfig"]] = relationship(foreign_keys=[target_destination_id])
    # resolved_by_user: Mapped[Optional["User"]] = relationship(foreign_keys=[resolved_by_user_id])

    __table_args__ = (
        Index('ix_dicom_exception_log_status_next_retry', 'status', 'next_retry_attempt_at'),
        # Add other useful compound indexes as needed
    )

    def __repr__(self):
        return (f"<DicomExceptionLog(id={self.id}, sop='{self.sop_instance_uid}', "
                f"stage='{self.processing_stage.value}', status='{self.status.value}', "
                f"error='{self.error_message[:30]}...')>")