# app/crud/crud_dicom_exception_log.py
import uuid
from typing import List, Optional, Tuple
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import func, or_ # or_ for more complex text searches

from app.crud.base import CRUDBase
from app.db.models.dicom_exception_log import DicomExceptionLog
from app.schemas.dicom_exception_log import (
    DicomExceptionLogCreate, DicomExceptionLogUpdate
)
# Import enums for type hinting filter parameters
from app.schemas.enums import ExceptionStatus, ExceptionProcessingStage, ProcessedStudySourceType


class CRUDDicomExceptionLog(CRUDBase[DicomExceptionLog, DicomExceptionLogCreate, DicomExceptionLogUpdate]):

    def get_by_uuid(self, db: Session, *, exception_uuid: uuid.UUID) -> Optional[DicomExceptionLog]:
        return db.query(self.model).filter(self.model.exception_uuid == exception_uuid).first()

    def get_multi_with_filters(
        self,
        db: Session,
        *,
        skip: int = 0,
        limit: int = 100,
        search_term: Optional[str] = None, # Generic search term for UIDs, patient info, error messages
        status: Optional[List[ExceptionStatus]] = None, # Allow searching for multiple statuses
        processing_stage: Optional[List[ExceptionProcessingStage]] = None, # Allow multiple stages
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None,
        sop_instance_uid: Optional[str] = None,
        patient_id: Optional[str] = None,
        patient_name: Optional[str] = None, # Will use ILIKE
        accession_number: Optional[str] = None,
        modality: Optional[str] = None,
        original_source_type: Optional[ProcessedStudySourceType] = None,
        original_source_identifier: Optional[str] = None,
        target_destination_id: Optional[int] = None,
        date_from: Optional[datetime] = None, # For failure_timestamp
        date_to: Optional[datetime] = None,   # For failure_timestamp
        celery_task_id: Optional[str] = None,
        sort_by: str = "failure_timestamp", # Default sort field
        sort_order: str = "desc" # "asc" or "desc"
    ) -> Tuple[List[DicomExceptionLog], int]: # Returns items and total count for pagination
        query = db.query(self.model)

        if search_term:
            like_term = f"%{search_term}%"
            query = query.filter(
                or_(
                    self.model.study_instance_uid.ilike(like_term),
                    self.model.series_instance_uid.ilike(like_term),
                    self.model.sop_instance_uid.ilike(like_term),
                    self.model.patient_name.ilike(like_term),
                    self.model.patient_id.ilike(like_term),
                    self.model.accession_number.ilike(like_term),
                    self.model.error_message.ilike(like_term),
                    self.model.failed_filepath.ilike(like_term),
                    self.model.original_source_identifier.ilike(like_term),
                    self.model.target_destination_name.ilike(like_term),
                    self.model.celery_task_id.ilike(like_term),
                )
            )

        if status:
            query = query.filter(self.model.status.in_(status))
        if processing_stage:
            query = query.filter(self.model.processing_stage.in_(processing_stage))
        if study_instance_uid:
            query = query.filter(self.model.study_instance_uid == study_instance_uid)
        if series_instance_uid:
            query = query.filter(self.model.series_instance_uid == series_instance_uid)
        if sop_instance_uid:
            query = query.filter(self.model.sop_instance_uid == sop_instance_uid)
        if patient_id:
            query = query.filter(self.model.patient_id == patient_id)
        if patient_name and not search_term: # Avoid duplicate if search_term already covers it
            query = query.filter(self.model.patient_name.ilike(f"%{patient_name}%"))
        if accession_number and not search_term:
            query = query.filter(self.model.accession_number.ilike(f"%{accession_number}%"))
        if modality:
            query = query.filter(self.model.modality == modality)
        if original_source_type:
            query = query.filter(self.model.original_source_type == original_source_type)
        if original_source_identifier and not search_term:
            query = query.filter(self.model.original_source_identifier.ilike(f"%{original_source_identifier}%"))
        if target_destination_id:
            query = query.filter(self.model.target_destination_id == target_destination_id)
        if date_from:
            query = query.filter(self.model.failure_timestamp >= date_from)
        if date_to:
            # Add 1 day to date_to to make the range inclusive of the end date if only date part is provided
            # Or ensure time component is set to end of day by the caller. For now, simple <=
            query = query.filter(self.model.failure_timestamp <= date_to)
        if celery_task_id and not search_term:
            query = query.filter(self.model.celery_task_id == celery_task_id)

        # Get total count *before* sorting and pagination for accurate total
        # total_count_query = query.statement.with_only_columns(func.count(), maintain_column_froms=True).order_by(None)
        # total_count = db.execute(total_count_query).scalar_one()
        total_count = query.count()

        # Apply sorting
        column_to_sort = getattr(self.model, sort_by, self.model.failure_timestamp) # Default to failure_timestamp
        if sort_order.lower() == "asc":
            query = query.order_by(column_to_sort.asc())
        else:
            query = query.order_by(column_to_sort.desc())

        items = query.offset(skip).limit(limit).all()
        return items, total_count

    def get_retry_pending_exceptions(
        self,
        db: Session,
        *,
        limit: int = 100, # How many to fetch at once for the retry task
        older_than: Optional[datetime] = None # To prioritize older retries or avoid very new ones
    ) -> List[DicomExceptionLog]:
        query = db.query(self.model).filter(self.model.status == ExceptionStatus.RETRY_PENDING)
        query = query.filter(
            or_(
                self.model.next_retry_attempt_at <= datetime.now(timezone.utc),
                self.model.next_retry_attempt_at.is_(None) # Retry immediately if no next_retry_attempt_at
            )
        )
        if older_than:
            query = query.filter(self.model.failure_timestamp < older_than) # Example: process older ones first

        query = query.order_by(self.model.next_retry_attempt_at.asc().nulls_first(), self.model.failure_timestamp.asc())
        return query.limit(limit).all()

    # The base CRUDBase.create, CRUDBase.update, and CRUDBase.remove will work for most needs.
    # `create` will use DicomExceptionLogCreate.
    # `update` will take DicomExceptionLogUpdate, db_obj (the existing model instance), and apply changes.
    # `remove` takes an id.

    # If we need to update specific fields like incrementing retry_count and setting last_retry_attempt_at
    # atomically or without fetching the whole object, we could add specific methods later.
    # For example:
    # def mark_as_retrying(self, db: Session, *, db_obj: DicomExceptionLog) -> DicomExceptionLog:
    #     db_obj.status = ExceptionStatus.RETRY_IN_PROGRESS
    #     db_obj.last_retry_attempt_at = datetime.now(timezone.utc)
    #     db_obj.retry_count = (db_obj.retry_count or 0) + 1
    #     db.add(db_obj)
    #     db.commit()
    #     db.refresh(db_obj)
    #     return db_obj


dicom_exception_log = CRUDDicomExceptionLog(DicomExceptionLog)