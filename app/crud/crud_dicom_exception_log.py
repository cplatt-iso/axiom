# app/crud/crud_dicom_exception_log.py
import uuid
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timezone
from pathlib import Path  # Import Path
import structlog  # Import structlog

from sqlalchemy.orm import Session
from sqlalchemy import func, or_, update as sqlalchemy_update # import update

from app.crud.base import CRUDBase
from app.db.models.dicom_exception_log import DicomExceptionLog
from app.schemas.dicom_exception_log import (
    DicomExceptionLogCreate, DicomExceptionLogUpdate, BulkActionScope
)
from app.schemas.enums import ExceptionStatus, ExceptionProcessingStage, ProcessedStudySourceType

logger = structlog.get_logger(__name__) # Define the logger at module level


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

    def get_many_by_scope(
        self, 
        db: Session, 
        *, 
        scope: BulkActionScope,
        current_user_id: Optional[int] = None # For potential future permission checks
    ) -> List[DicomExceptionLog]:
        """
        Retrieves multiple DicomExceptionLog entries based on the provided scope.
        """
        query = db.query(self.model)
        
        conditions = []
        if scope.exception_uuids:
            conditions.append(self.model.exception_uuid.in_(scope.exception_uuids))
        
        if scope.study_instance_uid:
            study_condition = self.model.study_instance_uid == scope.study_instance_uid
            if scope.series_instance_uid:
                # If series_instance_uid is also provided, it's an AND condition with study
                study_condition = or_(study_condition, self.model.series_instance_uid == scope.series_instance_uid) # This might be too broad if series UID not unique
                # A better way for study + series:
                # conditions.append(and_(self.model.study_instance_uid == scope.study_instance_uid, 
                #                        self.model.series_instance_uid == scope.series_instance_uid))
                # For now, let's simplify: if series is specified, it's the primary condition if study is also there for context.
                # This logic needs refinement based on how UIDs are used.
                # A simple OR might be too broad if series_instance_uid is not globally unique.
                # Let's assume if series_uid is present, it's the more specific filter.
                # If only study_uid, then all series in that study.
                # If study_uid and series_uid, then only that series.
                pass # Will handle below more clearly

        if conditions: # If exception_uuids are primary
             query = query.filter(or_(*conditions))
        
        # Handle study/series scoping if not solely by UUIDs
        if not scope.exception_uuids:
            if scope.study_instance_uid and scope.series_instance_uid:
                query = query.filter(
                    self.model.study_instance_uid == scope.study_instance_uid,
                    self.model.series_instance_uid == scope.series_instance_uid
                )
            elif scope.study_instance_uid:
                query = query.filter(self.model.study_instance_uid == scope.study_instance_uid)
            elif scope.series_instance_uid:
                # Querying by series_instance_uid alone can be ambiguous if not globally unique.
                # Consider requiring study_instance_uid if series_instance_uid is used without exception_uuids.
                # For now, allow it, but be wary.
                query = query.filter(self.model.series_instance_uid == scope.series_instance_uid)
            else:
                # No valid scope criteria if UUIDs, study, or series not provided
                return [] 
                
        return query.all()

    def bulk_update_status(
        self,
        db: Session,
        *,
        exception_log_ids: List[int], # Use internal integer PKs for the update statement
        new_status: ExceptionStatus,
        resolution_notes: Optional[str] = None,
        resolved_by_user_id: Optional[int] = None, # For RESOLVED_MANUALLY
        clear_next_retry_attempt_at: bool = False,
        cleanup_staged_files_on_resolve: bool = True # New flag to control file cleanup
    ) -> int:
        """
        Performs a bulk update of status and resolution fields for given exception log IDs.
        Returns the number of rows affected.
        Handles setting resolved_at and resolved_by_user_id appropriately.
        Optionally cleans up staged files if status moves to a resolved/archived state.
        """
        if not exception_log_ids:
            return 0

        update_values: Dict[str, Any] = {"status": new_status}
        current_time_utc = datetime.now(timezone.utc)

        if new_status in [ExceptionStatus.RESOLVED_MANUALLY, ExceptionStatus.RESOLVED_BY_RETRY, ExceptionStatus.ARCHIVED]:
            update_values["resolved_at"] = current_time_utc
            if new_status == ExceptionStatus.RESOLVED_MANUALLY and resolved_by_user_id:
                update_values["resolved_by_user_id"] = resolved_by_user_id
            
            # File cleanup logic for resolved/archived statuses
            if cleanup_staged_files_on_resolve:
                # Fetch logs to get filepaths BEFORE updating them, as we might nullify the path
                logs_to_cleanup = db.query(self.model.id, self.model.failed_filepath, self.model.exception_uuid).\
                    filter(self.model.id.in_(exception_log_ids)).\
                    filter(self.model.failed_filepath.isnot(None)).all()
                
                cleaned_paths_notes = []
                paths_to_nullify_in_db = []

                for log_id, path_str, exc_uuid in logs_to_cleanup:
                    log = logger.bind(exception_uuid=str(exc_uuid), staged_file=path_str, bulk_update_id=log_id)
                    try:
                        staged_file = Path(path_str)
                        if staged_file.is_file():
                            staged_file.unlink()
                            log.info("Cleaned up staged file due to bulk status change (resolved/archived).")
                            cleaned_paths_notes.append(f"File {staged_file.name} cleaned up.")
                            paths_to_nullify_in_db.append(log_id)
                        else:
                            log.warning("Staged file for cleanup not found, will nullify path.")
                            paths_to_nullify_in_db.append(log_id) # Nullify path even if not found
                    except Exception as e:
                        log.error("Error deleting staged file during bulk status change.", error=str(e))
                
                if paths_to_nullify_in_db:
                    # Update failed_filepath to None for these specific logs
                    db.execute(
                        sqlalchemy_update(self.model)
                        .where(self.model.id.in_(paths_to_nullify_in_db))
                        .values(failed_filepath=None)
                    )
                
                if resolution_notes is None: resolution_notes = ""
                if cleaned_paths_notes:
                    resolution_notes = f"{resolution_notes}\n[Bulk Action System Note: {'; '.join(cleaned_paths_notes)}]".strip()

        elif new_status == ExceptionStatus.RETRY_PENDING and clear_next_retry_attempt_at:
            update_values["next_retry_attempt_at"] = None
            # Resetting other resolution fields if moving away from a resolved state
            update_values["resolved_at"] = None
            update_values["resolved_by_user_id"] = None
        else: # Moving to a non-resolved, non-archived state
            update_values["resolved_at"] = None
            update_values["resolved_by_user_id"] = None
        
        if resolution_notes is not None: # Allow clearing notes by passing empty string or explicit None
            update_values["resolution_notes"] = resolution_notes
        
        # Add last_retry_attempt_at and retry_count updates if needed for RETRY_PENDING/IN_PROGRESS,
        # but typically the worker task handles retry_count. Here we primarily set status.
        if new_status == ExceptionStatus.RETRY_PENDING:
             update_values["last_retry_attempt_at"] = current_time_utc # Mark that a manual re-queue was actioned

        stmt = (
            sqlalchemy_update(self.model)
            .where(self.model.id.in_(exception_log_ids))
            .values(**update_values)
            .execution_options(synchronize_session="fetch") # Or False, depending on needs
        )
        result = db.execute(stmt)
        # db.commit() # Handled by the endpoint's transaction management
        return result.rowcount

dicom_exception_log = CRUDDicomExceptionLog(DicomExceptionLog)