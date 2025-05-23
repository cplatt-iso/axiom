# app/api/api_v1/endpoints/dicom_exceptions.py
import logging
import structlog
from pathlib import Path
import uuid
from typing import List, Optional, Any
from datetime import datetime, timezone # <--- CORRECTED: Added timezone import
from app.core.config import settings

from fastapi import APIRouter, Depends, HTTPException, Query, status, Body # Body is not used yet, but good to have
from sqlalchemy.orm import Session

# Corrected imports for crud, schemas, and models
from app import crud # Assuming app.crud is set up via app/crud/__init__.py
from app.db import models # <--- CORRECTED: Import models from app.db
from app.api import deps
# Import specific schemas directly
from app.schemas.dicom_exception_log import ( # <--- CORRECTED: Direct schema imports
    DicomExceptionLogListResponse,
    DicomExceptionLogRead,
    DicomExceptionLogUpdate
)
from app.schemas.enums import ExceptionStatus, ExceptionProcessingStage, ProcessedStudySourceType

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