# app/schemas/dicom_exception_log.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime
import uuid # Ensure uuid is imported

from app.schemas.enums import ProcessedStudySourceType, ExceptionProcessingStage, ExceptionStatus

# --- Fields common to the core data of an exception, settable on creation or intrinsic ---
class DicomExceptionLogCore(BaseModel): # Renamed from DicomExceptionLogBase for clarity
    # DICOM Identifiers
    study_instance_uid: Optional[str] = Field(None, max_length=128, examples=["1.2.840.113619.2.55.3.2831208459.717.1398891884.581"])
    series_instance_uid: Optional[str] = Field(None, max_length=128, examples=["1.2.840.113619.2.55.3.2831208459.717.1398891884.582"])
    sop_instance_uid: Optional[str] = Field(None, max_length=128, examples=["1.2.840.113619.2.55.3.2831208459.717.1398891884.583"])

    # Human-friendly fields
    patient_name: Optional[str] = Field(None, max_length=255, examples=["DOE^JANE"])
    patient_id: Optional[str] = Field(None, max_length=128, examples=["PID12345"])
    accession_number: Optional[str] = Field(None, max_length=64, examples=["ACC67890"])
    modality: Optional[str] = Field(None, max_length=16, examples=["CT"])

    processing_stage: ExceptionProcessingStage = Field(..., examples=[ExceptionProcessingStage.DESTINATION_SEND])
    error_message: str = Field(..., examples=["Connection refused to destination 'PACS_Archive'"])
    error_details: Optional[str] = Field(None, examples=["Traceback: ...socket.error..."])
    failed_filepath: Optional[str] = Field(None, max_length=1024, examples=["/dicom_data/errors/failed_instance.dcm"])

    original_source_type: Optional[ProcessedStudySourceType] = Field(None, examples=[ProcessedStudySourceType.DIMSE_LISTENER])
    original_source_identifier: Optional[str] = Field(None, max_length=255, examples=["LISTENER_AE_1"])
    calling_ae_title: Optional[str] = Field(None, max_length=16, examples=["SENDING_MODALITY_AE"])

    target_destination_id: Optional[int] = Field(None, examples=[42])
    target_destination_name: Optional[str] = Field(None, max_length=100, examples=["Backup GCS Bucket"])

    status: ExceptionStatus = Field(default=ExceptionStatus.NEW, examples=[ExceptionStatus.NEW])
    retry_count: int = Field(default=0, ge=0, examples=[0])
    next_retry_attempt_at: Optional[datetime] = Field(None)
    last_retry_attempt_at: Optional[datetime] = Field(None)

    resolved_at: Optional[datetime] = Field(None)
    resolved_by_user_id: Optional[int] = Field(None)
    resolution_notes: Optional[str] = Field(None)

    celery_task_id: Optional[str] = Field(None, max_length=255, examples=["abc-123-def-456"])


# --- Create Schema: Data required to log a new exception ---
class DicomExceptionLogCreate(DicomExceptionLogCore):
    # Inherits all fields from DicomExceptionLogCore.
    # System-generated fields like id, exception_uuid, failure_timestamp, created_at, updated_at
    # are NOT part of the creation payload from the client.
    pass


# --- Read Schema: Data returned from the API (includes system-generated fields) ---
class DicomExceptionLogRead(DicomExceptionLogCore):
    id: int  # System-generated integer PK
    exception_uuid: uuid.UUID # System-generated stable UUID
    failure_timestamp: datetime # System-generated on log creation
    created_at: datetime # System-generated base model timestamp
    updated_at: datetime # System-generated base model timestamp

    model_config = ConfigDict(from_attributes=True)


# --- Update Schema: For PATCH operations ---
class DicomExceptionLogUpdate(BaseModel): # Does not inherit from Core, specifies only updatable fields
    # Only include fields that are realistically updatable via API by a user/system action
    # For example, one might update the status, or add resolution notes.
    # It's generally not good practice to allow updating of original error details or source info.

    # DICOM Identifiers / Patient Info - typically not updated post-creation unless for correction
    # of an initial logging error, which might be a special admin function.
    # For now, let's assume these are NOT commonly updated by typical users.
    # study_instance_uid: Optional[str] = Field(None, max_length=128)
    # ... (other DICOM and patient fields if you decide they are updatable) ...

    status: Optional[ExceptionStatus] = None
    retry_count: Optional[int] = Field(None, ge=0) # e.g., UI might allow resetting retry count
    next_retry_attempt_at: Optional[datetime] = None # UI might allow rescheduling or clearing
    # last_retry_attempt_at is system-set, probably not user-updatable

    resolved_at: Optional[datetime] = None
    resolved_by_user_id: Optional[int] = None # System (associated with logged-in user) would set this
    resolution_notes: Optional[str] = None

    # Fields like error_message, processing_stage, failed_filepath are historical facts,
    # so usually not updated.

    model_config = ConfigDict(extra='ignore') # Important for PATCH: ignore fields not in this schema

# --- Schema for API list responses (paginated results) ---
class DicomExceptionLogListResponse(BaseModel):
    total: int
    items: List[DicomExceptionLogRead]