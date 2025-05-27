# app/api/api_v1/endpoints/dicom_exceptions.py
import logging
import structlog
from pathlib import Path
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone # <--- CORRECTED: Added timezone import
from app.core.config import settings

from fastapi import APIRouter, Depends, HTTPException, Query, status, Body # Body is not used yet, but good to have
from sqlalchemy.orm import Session

# Corrected imports for crud, schemas, and models
from app import crud # Assuming app.crud is set up via app/crud/__init__.py
from app.db import models 
from app.api import deps
# Import specific schemas directly
from app.schemas.dicom_exception_log import ( 
    DicomExceptionLogListResponse,
    DicomExceptionLogRead,
    DicomExceptionLogUpdate,
    DicomExceptionBulkActionRequest,
    BulkActionResponse,
    BulkActionSetStatusPayload, # For type checking payload
    BulkActionRequeueRetryablePayload
)
from app.schemas.enums import ExceptionStatus, ExceptionProcessingStage, ProcessedStudySourceType
from app.worker.tasks import retry_pending_exceptions_task 

log_level_str = getattr(settings, 'LOG_LEVEL', "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logger = structlog.get_logger(__name__)

router = APIRouter()

@router.get(
    "/",
    response_model=DicomExceptionLogListResponse, # <--- CORRECTED: Use direct import
    summary="List DICOM Processing Exceptions",
    dependencies=[Depends(deps.get_current_active_user)]
)
def list_dicom_exceptions(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip for pagination."),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of records to return."),
    search_term: Optional[str] = Query(None, description="Generic search term across UIDs, patient info, errors, etc."),
    status_in: Optional[List[ExceptionStatus]] = Query(None, alias="status", description="Filter by one or more exception statuses."),
    processing_stage_in: Optional[List[ExceptionProcessingStage]] = Query(None, alias="processing_stage", description="Filter by one or more processing stages."),
    study_instance_uid: Optional[str] = Query(None, description="Filter by Study Instance UID."),
    series_instance_uid: Optional[str] = Query(None, description="Filter by Series Instance UID."),
    sop_instance_uid: Optional[str] = Query(None, description="Filter by SOP Instance UID."),
    patient_id: Optional[str] = Query(None, description="Filter by Patient ID."),
    patient_name: Optional[str] = Query(None, description="Filter by Patient Name (case-insensitive, partial match)."),
    accession_number: Optional[str] = Query(None, description="Filter by Accession Number."),
    modality: Optional[str] = Query(None, description="Filter by Modality."),
    original_source_type: Optional[ProcessedStudySourceType] = Query(None, description="Filter by original source type."),
    original_source_identifier: Optional[str] = Query(None, description="Filter by original source identifier (e.g., AE Title, source name)."),
    target_destination_id: Optional[int] = Query(None, description="Filter by target destination ID."),
    date_from: Optional[datetime] = Query(None, description="Filter by failure timestamp (from date, inclusive)."),
    date_to: Optional[datetime] = Query(None, description="Filter by failure timestamp (to date, inclusive)."),
    celery_task_id: Optional[str] = Query(None, description="Filter by Celery Task ID."),
    sort_by: str = Query("failure_timestamp", description="Field to sort by."),
    sort_order: str = Query("desc", pattern="^(asc|desc)$", description="Sort order: 'asc' or 'desc'.")
) -> Any:
    exceptions_orm, total_count = crud.dicom_exception_log.get_multi_with_filters(
        db,
        skip=skip,
        limit=limit,
        search_term=search_term,
        status=status_in,
        processing_stage=processing_stage_in,
        study_instance_uid=study_instance_uid,
        series_instance_uid=series_instance_uid,
        sop_instance_uid=sop_instance_uid,
        patient_id=patient_id,
        patient_name=patient_name,
        accession_number=accession_number,
        modality=modality,
        original_source_type=original_source_type,
        original_source_identifier=original_source_identifier,
        target_destination_id=target_destination_id,
        date_from=date_from,
        date_to=date_to,
        celery_task_id=celery_task_id,
        sort_by=sort_by,
        sort_order=sort_order
    )
    items_pydantic = [DicomExceptionLogRead.model_validate(exc_orm) for exc_orm in exceptions_orm]
    return DicomExceptionLogListResponse(total=total_count, items=items_pydantic)


@router.get(
    "/{exception_uuid}",
    response_model=DicomExceptionLogRead, # <--- CORRECTED
    summary="Get a Specific DICOM Processing Exception by UUID",
    dependencies=[Depends(deps.get_current_active_user)]
)
def get_dicom_exception(
    exception_uuid: uuid.UUID,
    db: Session = Depends(deps.get_db)
) -> Any:
    db_exception = crud.dicom_exception_log.get_by_uuid(db, exception_uuid=exception_uuid)
    if not db_exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="DICOM exception record not found."
        )
    return db_exception

@router.patch(
    "/{exception_uuid}",
    response_model=DicomExceptionLogRead, # <--- CORRECTED
    summary="Update a DICOM Processing Exception",
    dependencies=[Depends(deps.get_current_active_user)]
)
def update_dicom_exception(
    exception_uuid: uuid.UUID,
    exception_in: DicomExceptionLogUpdate, # <--- CORRECTED
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user) # models.User should now work
) -> Any:
    db_exception = crud.dicom_exception_log.get_by_uuid(db, exception_uuid=exception_uuid)
    if not db_exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="DICOM exception record not found for update."
        )

    update_data = exception_in.model_dump(exclude_unset=True)
    if "status" in update_data:
        new_status = ExceptionStatus(update_data["status"])
        if new_status in [ExceptionStatus.RESOLVED_MANUALLY, ExceptionStatus.RESOLVED_BY_RETRY, ExceptionStatus.ARCHIVED]:
            if "resolved_at" not in update_data or update_data["resolved_at"] is None:
                update_data["resolved_at"] = datetime.now(timezone.utc) # timezone.utc should now work
            if new_status == ExceptionStatus.RESOLVED_MANUALLY and \
               ("resolved_by_user_id" not in update_data or update_data["resolved_by_user_id"] is None):
                update_data["resolved_by_user_id"] = current_user.id
        elif db_exception.status in [ExceptionStatus.RESOLVED_MANUALLY, ExceptionStatus.RESOLVED_BY_RETRY, ExceptionStatus.ARCHIVED] and \
             new_status not in [ExceptionStatus.RESOLVED_MANUALLY, ExceptionStatus.RESOLVED_BY_RETRY, ExceptionStatus.ARCHIVED]:
            update_data["resolved_at"] = None
            update_data["resolved_by_user_id"] = None
        if db_exception.failed_filepath:
                log = logger.bind(exception_uuid=str(exception_uuid), staged_file=db_exception.failed_filepath) # Ensure logger is available
                try:
                    staged_file_to_delete = Path(db_exception.failed_filepath)
                    if staged_file_to_delete.is_file():
                        staged_file_to_delete.unlink()
                        log.info("Cleaned up staged file due to manual status change (resolved/archived).")
                        update_data["failed_filepath"] = None # Nullify in DB
                        # Optionally add to resolution notes
                        current_api_notes = update_data.get("resolution_notes", db_exception.resolution_notes or "")
                        update_data["resolution_notes"] = f"{current_api_notes}\nStaged file {staged_file_to_delete} cleaned up by API.".strip()
                    else:
                        log.warning("Staged file not found during manual status change, nullifying path.")
                        update_data["failed_filepath"] = None # Nullify anyway
                except Exception as api_del_err:
                    log.error("Error deleting staged file during manual status change via API.", error=str(api_del_err))
                    # Don't fail the API call, but log it. Perhaps add to notes.
                    current_api_notes_err = update_data.get("resolution_notes", db_exception.resolution_notes or "")
                    update_data["resolution_notes"] = f"{current_api_notes_err}\nAPI WARNING: Failed to delete staged file {db_exception.failed_filepath} during status update.".strip()

    updated_exception = crud.dicom_exception_log.update(db, db_obj=db_exception, obj_in=update_data)
    return updated_exception

@router.post(
    "/bulk-actions",
    response_model=BulkActionResponse,
    summary="Perform Bulk Actions on DICOM Processing Exceptions",
    dependencies=[Depends(deps.get_current_active_user)] # Ensure user is authenticated
)
def bulk_dicom_exception_actions(
    request: DicomExceptionBulkActionRequest,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user) # Get current user for logging/audit
) -> Any:
    logger.info("Bulk action request received.", action_type=request.action_type, scope=request.scope.model_dump_json())

    # Validate scope: at least one scope identifier must be present
    if not (request.scope.study_instance_uid or request.scope.series_instance_uid or request.scope.exception_uuids):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid scope: Must provide study_instance_uid, series_instance_uid, or a list of exception_uuids."
        )

    # Fetch the exception logs based on the scope
    # Pass current_user.id if your CRUD method uses it for permissions, etc.
    exception_logs = crud.dicom_exception_log.get_many_by_scope(db, scope=request.scope)

    if not exception_logs:
        return BulkActionResponse(
            action_type=request.action_type,
            processed_count=0,
            successful_count=0,
            failed_count=0,
            message="No matching exception logs found for the given scope."
        )

    processed_count = len(exception_logs)
    log_ids_to_update = [log.id for log in exception_logs]
    successful_count = 0
    action_details = [] # For specific messages, e.g., per-item failures if not batch DB update
    eligible_log_ids_for_requeue: List[int] = [] # Initialize here

    if request.action_type == "SET_STATUS":
        if not isinstance(request.payload, BulkActionSetStatusPayload):
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid payload for SET_STATUS action.")
        
        payload: BulkActionSetStatusPayload = request.payload
        
        # Determine if file cleanup should happen based on settings and new status
        # Example: settings.CLEANUP_STAGED_FILES_ON_BULK_RESOLVE (you'd add this to config)
        # For now, let's assume true if status is RESOLVED/ARCHIVED
        should_cleanup_files = payload.new_status in [ExceptionStatus.RESOLVED_MANUALLY, ExceptionStatus.RESOLVED_BY_RETRY, ExceptionStatus.ARCHIVED]

        successful_count = crud.dicom_exception_log.bulk_update_status(
            db=db,
            exception_log_ids=log_ids_to_update,
            new_status=payload.new_status,
            resolution_notes=payload.resolution_notes,
            resolved_by_user_id=current_user.id if payload.new_status == ExceptionStatus.RESOLVED_MANUALLY else None,
            clear_next_retry_attempt_at=payload.clear_next_retry_attempt_at if payload.clear_next_retry_attempt_at is not None else False,
            cleanup_staged_files_on_resolve=should_cleanup_files
        )
        action_details.append(f"Attempted to set status to '{payload.new_status.value}' for {processed_count} logs.")

    elif request.action_type == "REQUEUE_RETRYABLE":
        # For REQUEUE_RETRYABLE, we effectively set status to RETRY_PENDING
        # and nullify next_retry_attempt_at.
        # We can also add more sophisticated filtering here if needed (e.g., only re-queue if not FAILED_PERMANENTLY)
        # The CRUD method handles this logic for RETRY_PENDING.
        
        # Filter logs that are eligible for re-queue (example: not already in a terminal state)
        eligible_log_ids_for_requeue = [
            log.id for log in exception_logs 
            if log.status not in [
                ExceptionStatus.FAILED_PERMANENTLY, 
                ExceptionStatus.ARCHIVED,
                ExceptionStatus.RESOLVED_MANUALLY,
                ExceptionStatus.RESOLVED_BY_RETRY
            ]
        ]
        
        if not eligible_log_ids_for_requeue:
            return BulkActionResponse(
                action_type=request.action_type,
                processed_count=processed_count, # Total logs in scope
                successful_count=0, # None were eligible/updated
                failed_count=0,
                message="No logs eligible for re-queue in the given scope (e.g., all are already resolved/failed)."
            )

        successful_count = crud.dicom_exception_log.bulk_update_status(
            db=db,
            exception_log_ids=eligible_log_ids_for_requeue,
            new_status=ExceptionStatus.RETRY_PENDING,
            clear_next_retry_attempt_at=True,
            # No specific resolution notes or resolver for re-queue typically
        )
        action_details.append(f"Attempted to re-queue {len(eligible_log_ids_for_requeue)} eligible logs (out of {processed_count} in scope).")

    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported bulk action_type: {request.action_type}")

    # Commit changes after all operations for the request are queued with SQLAlchemy
    try:
        db.commit()
        logger.info("Bulk action committed.", action_type=request.action_type, successful_count=successful_count, processed_count=processed_count)
    except Exception as e:
        db.rollback()
        logger.error("Error during bulk action commit, rolled back.", exc_info=True, action_type=request.action_type)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error during bulk action: {str(e)}")


    return BulkActionResponse(
        action_type=request.action_type,
        processed_count=processed_count, # Total items identified by scope
        successful_count=successful_count, # Actual items updated by CRUD
        failed_count= (len(log_ids_to_update) if request.action_type == "SET_STATUS" else len(eligible_log_ids_for_requeue) if request.action_type == "REQUEUE_RETRYABLE" else processed_count) - successful_count,
        message=f"Bulk action '{request.action_type}' processed.",
        details=action_details
    )

@router.post(
    "/trigger-retry-cycle",
    summary="Manually Trigger a DICOM Exception Retry Cycle",
    status_code=status.HTTP_202_ACCEPTED, # 202 Accepted is good for async task submissions
    dependencies=[Depends(deps.get_current_active_superuser)] # Or get_current_active_user if non-admins can trigger
)
def trigger_exception_retry_cycle(
    # No body needed for this request, it's just a trigger
    current_user: models.User = Depends(deps.get_current_active_user) 
) -> Dict[str, str]:
    """
    Manually initiates a Celery task to process pending DICOM exceptions.
    This is the same task that runs on a schedule.
    """
    try:
        # Enqueue the Celery task
        # .delay() is a shortcut for .apply_async()
        task_result = retry_pending_exceptions_task.delay()  # type: ignore[operator]
        
        logger.info(
            "Manual DICOM exception retry cycle triggered by user.", 
            user_id=current_user.id, 
            user_email=current_user.email,
            celery_task_id=task_result.id
        )
        return {
            "message": "DICOM exception retry cycle initiated.",
            "celery_task_id": task_result.id
        }
    except Exception as e:
        logger.error(
            "Failed to trigger manual DICOM exception retry cycle.",
            user_id=current_user.id,
            user_email=current_user.email,
            error=str(e),
            exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to enqueue exception retry task: {str(e)}"
        )