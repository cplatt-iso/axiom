# app/schemas/dicom_exception_log.py
from pydantic import BaseModel, Field, ConfigDict, field_validator, validator
from typing import Optional, List, Union, Literal
from datetime import datetime
import uuid # Ensure uuid is imported

from app.schemas.enums import ProcessedStudySourceType, ExceptionProcessingStage, ExceptionStatus

class BulkActionScope(BaseModel):
    """Defines the scope of SOP instances for a bulk action."""
    study_instance_uid: Optional[str] = Field(None, max_length=128, description="Apply to all SOPs in this study.")
    series_instance_uid: Optional[str] = Field(None, max_length=128, description="Apply to all SOPs in this series (requires study_instance_uid if ambiguous).")
    exception_uuids: Optional[List[uuid.UUID]] = Field(None, description="Apply to a specific list of exception log UUIDs.")

    @field_validator('series_instance_uid')
    @classmethod
    def series_requires_study_or_uuids(cls, v, values):
        # This validation is a bit tricky as values.data might not be fully populated in Pydantic v2
        # For simplicity, we'll assume if series_instance_uid is given, either study_instance_uid or exception_uuids
        # should provide enough context, or the CRUD layer will handle ambiguity.
        # A more robust check might be needed depending on how you query.
        # if v and not values.data.get('study_instance_uid') and not values.data.get('exception_uuids'):
        #     raise ValueError('If series_instance_uid is provided for scope, study_instance_uid or a list of exception_uuids should also be provided or implied.')
        return v

    @field_validator('*') # Basic check to ensure at least one scope is defined
    @classmethod
    def check_at_least_one_scope_defined(cls, v, values):
        # This validator structure in Pydantic v2 requires careful handling of `values`
        # For now, we'll rely on the endpoint logic to ensure a valid scope is used.
        # A model-level validator would be:
        # @model_validator(mode='after')
        # def check_scope(self) -> 'BulkActionScope':
        #    if not (self.study_instance_uid or self.series_instance_uid or self.exception_uuids):
        #        raise ValueError("At least one scope (study_instance_uid, series_instance_uid, or exception_uuids) must be provided.")
        #    return self
        return v


class BulkActionSetStatusPayload(BaseModel):
    """Payload for setting status in bulk."""
    new_status: ExceptionStatus
    resolution_notes: Optional[str] = None
    # If setting to RETRY_PENDING, this can be used
    clear_next_retry_attempt_at: Optional[bool] = Field(False, description="If true and new_status is RETRY_PENDING, next_retry_attempt_at will be nulled.")


class BulkActionRequeueRetryablePayload(BaseModel):
    """Payload specific for re-queuing (currently empty, but for structure)."""
    # Could add filters here, e.g., "only if not terminally failed"
    pass


class DicomExceptionBulkActionRequest(BaseModel):
    action_type: Literal["SET_STATUS", "REQUEUE_RETRYABLE"] # Add more as needed, e.g., "ARCHIVE_ALL" could be SET_STATUS with ARCHIVED
    scope: BulkActionScope
    payload: Union[BulkActionSetStatusPayload, BulkActionRequeueRetryablePayload, None] = None # Payload depends on action_type

    @field_validator('payload') # In Pydantic v2, use model_validator for this kind of cross-field validation
    @classmethod
    def payload_matches_action_type(cls, v, values):
        # This is a placeholder for more robust validation.
        # Pydantic v2: use `@model_validator(mode='after')`
        # action = values.data.get('action_type')
        # if action == "SET_STATUS" and not isinstance(v, BulkActionSetStatusPayload):
        #     raise ValueError("Payload must be BulkActionSetStatusPayload for SET_STATUS action.")
        # if action == "REQUEUE_RETRYABLE" and not (isinstance(v, BulkActionRequeueRetryablePayload) or v is None):
        #     raise ValueError("Payload must be BulkActionRequeueRetryablePayload or None for REQUEUE_RETRYABLE action.")
        return v

class BulkActionResponse(BaseModel):
    action_type: str
    processed_count: int
    successful_count: int
    failed_count: int
    message: str
    details: Optional[List[str]] = None # E.g., list of UUIDs that failed

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