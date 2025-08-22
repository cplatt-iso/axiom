# app/worker/tasks.py
from datetime import datetime, timezone, timedelta
import logging
import shutil
import asyncio
from pathlib import Path
import traceback
from typing import Optional, List, Dict, Any, Union
import uuid

import structlog
from structlog.stdlib import BoundLogger
from celery import shared_task
from sqlalchemy.orm import Session
import pydicom

from app import crud
from app.db.session import SessionLocal
from app.schemas.enums import ProcessedStudySourceType
from app.core.config import settings

from app.worker.executors import (
    execute_file_based_task,
    execute_dicomweb_task,
    execute_ghc_task,
    execute_stow_task
)

from app.services.dustbin_service import dustbin_service

# from app.services import ai_assist_service  # Still needed for initial check

from app.db.models.dicom_exception_log import DicomExceptionLog  # The SQLAlchemy model
from app.schemas.enums import ExceptionStatus, ExceptionProcessingStage  # Our enums
# To attempt send
from app.services.storage_backends import get_storage_backend
# _safe_get_dicom_value might be useful if re-parsing
from app.worker.task_utils import build_storage_backend_config_dict, _safe_get_dicom_value

MAX_EXCEPTION_RETRIES = settings.EXCEPTION_MAX_RETRIES
RETRY_BACKOFF_BASE_SECONDS = settings.EXCEPTION_RETRY_DELAY_SECONDS
EXCEPTION_RETRY_BATCH_SIZE = settings.EXCEPTION_RETRY_BATCH_SIZE
EXCEPTION_RETRY_MAX_BACKOFF_SECONDS = settings.EXCEPTION_RETRY_MAX_BACKOFF_SECONDS

RETRYABLE_EXCEPTIONS = (
    ConnectionRefusedError,
    TimeoutError,
    # Add other exceptions here that should cause Celery to retry the *entire task*
    # e.g., certain DB deadlocks or temporary RabbitMQ issues, if not handled by Celery's broker config.
    # For now, keep it simple as you had it.
)

logger = structlog.get_logger(__name__)


@shared_task(bind=True, name="retry_pending_exceptions_task", acks_late=True,
             max_retries=3, default_retry_delay=300)
def retry_pending_exceptions_task(self):
    task_id = self.request.id
    log: BoundLogger = logger.bind(
        task_id=task_id,
        task_name=self.name)  # type: ignore
    log.info("Task started: retry_pending_exceptions_task")

    db: Optional[Session] = None
    resolved_this_run = 0
    rescheduled_this_run = 0
    permanently_failed_this_run = 0
    skipped_this_run = 0

    try:
        db = SessionLocal()
        exceptions_to_retry: List[DicomExceptionLog] = crud.dicom_exception_log.get_retry_pending_exceptions(
            db, limit=EXCEPTION_RETRY_BATCH_SIZE)

        if not exceptions_to_retry:
            log.info("No pending exceptions found for retry.")
            return {
                "status": "success_no_exceptions_to_retry",
                "processed_this_run": 0}

        log.info(
            f"Found {len(exceptions_to_retry)} exceptions to attempt retry.")

        for exc_log_entry in exceptions_to_retry:
            exc_log: BoundLogger = log.bind(
                exception_log_id=exc_log_entry.id, exception_uuid=str(
                    exc_log_entry.exception_uuid))  # type: ignore
            exc_log.info("Attempting retry for exception.")

            exc_log_entry.status = ExceptionStatus.RETRY_IN_PROGRESS
            exc_log_entry.last_retry_attempt_at = datetime.now(timezone.utc)
            try:
                db.add(exc_log_entry)
                db.commit()
                db.refresh(exc_log_entry)
                exc_log.debug("Marked exception as RETRY_IN_PROGRESS.")
            except Exception as db_update_err:
                exc_log.error(
                    "Failed to mark exception as RETRY_IN_PROGRESS. Skipping this one.",
                    error=str(db_update_err),
                    exc_info=True)
                if db.is_active:
                    db.rollback()
                skipped_this_run += 1
                continue

            retry_successful = False
            new_exception_during_retry: Optional[Exception] = None
            # Ensure resolution_notes is a string for appending
            current_resolution_notes = exc_log_entry.resolution_notes if exc_log_entry.resolution_notes is not None else ""

            try:
                if exc_log_entry.processing_stage == ExceptionProcessingStage.DESTINATION_SEND and \
                   exc_log_entry.target_destination_id is not None and \
                   exc_log_entry.failed_filepath:

                    exc_log.info("Retrying DESTINATION_SEND operation.")
                    staged_dicom_file = Path(exc_log_entry.failed_filepath)
                    if not staged_dicom_file.is_file():
                        exc_log.error(
                            "Staged file for retry does not exist or is not a file.",
                            path=str(staged_dicom_file))
                        raise FileNotFoundError(
                            f"Staged DICOM file for retry not found: {exc_log_entry.failed_filepath}")

                    dataset_to_send: pydicom.Dataset = pydicom.dcmread(
                        str(staged_dicom_file), force=True)
                    exc_log.debug(
                        "Successfully read staged DICOM file for retry.",
                        sop_uid=_safe_get_dicom_value(
                            dataset_to_send,
                            "SOPInstanceUID"))

                    dest_config_model = crud.crud_storage_backend_config.get(
                        db, id=exc_log_entry.target_destination_id)
                    if not dest_config_model:
                        exc_log.error(
                            "Target destination config not found for retry.",
                            target_dest_id=exc_log_entry.target_destination_id)
                        raise ValueError(
                            f"Destination config ID {exc_log_entry.target_destination_id} not found for retry.")
                    if not dest_config_model.is_enabled:
                        exc_log.warning(
                            "Target destination is disabled. Cannot retry.",
                            target_dest_name=dest_config_model.name)
                        raise RuntimeError(
                            f"Destination '{dest_config_model.name}' is disabled, cannot retry send.")

                    storage_config_dict = build_storage_backend_config_dict(
                        dest_config_model, f"{task_id}_retry_{exc_log_entry.id}")
                    if not storage_config_dict:
                        exc_log.error(
                            "Failed to build storage backend config for retry.")
                        raise RuntimeError(
                            "Failed to build storage backend config for retry.")

                    backend = get_storage_backend(storage_config_dict)
                    filename_ctx_retry = f"{_safe_get_dicom_value(dataset_to_send, 'SOPInstanceUID', f'RetrySOP_{uuid.uuid4().hex[:8]}')}.dcm"
                    source_id_for_store = exc_log_entry.original_source_identifier or "ExceptionRetry"

                    exc_log.info(
                        f"Attempting store to destination '{dest_config_model.name}' via retry task.")
                    backend.store(
                        dataset_to_send,
                        original_filepath=staged_dicom_file,
                        filename_context=filename_ctx_retry,
                        source_identifier=source_id_for_store
                    )
                    retry_successful = True
                    exc_log.info(
                        "Retry successful: DESTINATION_SEND completed.")
                else:
                    exc_log.warning(
                        "Retry logic not implemented or data missing for this exception type/stage.",
                        stage=exc_log_entry.processing_stage.value,
                        has_target_id=bool(
                            exc_log_entry.target_destination_id),
                        has_filepath=bool(
                            exc_log_entry.failed_filepath))
                    new_exception_during_retry = NotImplementedError(
                        f"Retry not implemented for stage: {exc_log_entry.processing_stage.value} or required data missing.")
            except Exception as retry_exc:
                exc_log.warning(
                    "Exception occurred during retry attempt itself.",
                    error=str(retry_exc),
                    exc_info=settings.DEBUG)
                new_exception_during_retry = retry_exc
                retry_successful = False

            try:  # Nested try for final DB update of the DicomExceptionLog entry
                if retry_successful:
                    exc_log_entry.status = ExceptionStatus.RESOLVED_BY_RETRY
                    exc_log_entry.resolved_at = datetime.now(timezone.utc)
                    exc_log_entry.resolution_notes = f"{current_resolution_notes}\nResolved by retry task {task_id} at {exc_log_entry.resolved_at}.".strip()
                    exc_log_entry.error_message = f"[RESOLVED VIA RETRY] {exc_log_entry.error_message}"[:2000]
                    exc_log_entry.retry_count = (
                        exc_log_entry.retry_count or 0) + 1
                    resolved_this_run += 1

                    if exc_log_entry.failed_filepath:
                        try:
                            staged_file_to_delete = Path(
                                exc_log_entry.failed_filepath)
                            if staged_file_to_delete.is_file():
                                staged_file_to_delete.unlink()
                                exc_log.info(
                                    "Successfully deleted staged file after successful retry.",
                                    path=str(staged_file_to_delete))
                                # Optionally nullify the path in DB
                                # exc_log_entry.failed_filepath = None
                        except Exception as del_err:
                            exc_log.warning(
                                "Failed to delete staged file after successful retry.",
                                path=exc_log_entry.failed_filepath,
                                error=str(del_err))
                            # Re-fetch or use updated
                            current_notes_for_del_fail = exc_log_entry.resolution_notes if exc_log_entry.resolution_notes is not None else ""
                            exc_log_entry.resolution_notes = f"{current_notes_for_del_fail}\nWARNING: Failed to delete staged file: {exc_log_entry.failed_filepath}.".strip()
                else:  # Retry failed or not applicable
                    exc_log_entry.retry_count = (
                        exc_log_entry.retry_count or 0) + 1
                    # current_resolution_notes is already initialized as string
                    # above

                    if new_exception_during_retry and isinstance(
                        new_exception_during_retry,
                        (FileNotFoundError,
                         ValueError,
                         RuntimeError,
                         NotImplementedError)):
                        exc_log.warning(
                            f"Non-transient error during retry ({type(new_exception_during_retry).__name__}). Marking for manual review.",
                            error_details=str(new_exception_during_retry))
                        exc_log_entry.status = ExceptionStatus.MANUAL_REVIEW_REQUIRED
                        exc_log_entry.resolution_notes = (
                            f"{current_resolution_notes}\nRetry attempt {exc_log_entry.retry_count} by task {task_id} failed due to: {str(new_exception_during_retry)[:500]}. Marked for manual review.".strip())
                        # Counting as a form of permanent failure for this
                        # retry path
                        permanently_failed_this_run += 1
                        # --- CLEANUP STAGED FILE IF MANUAL REVIEW IS THE END OF THE LINE FOR THIS ERROR TYPE ---
                        # Or if the error was FileNotFoundError for the staged
                        # file itself.
                        if isinstance(
                                new_exception_during_retry,
                                FileNotFoundError) and exc_log_entry.failed_filepath and str(new_exception_during_retry).find(
                                exc_log_entry.failed_filepath) != -1:
                            exc_log.info(
                                "Staged file was not found during retry, already gone or path issue. Nullifying path in DB.")
                            exc_log_entry.failed_filepath = None  # Nullify path if file was not found
                        # For other manual review cases, we might keep the
                        # staged file for inspection.
                    elif exc_log_entry.retry_count >= MAX_EXCEPTION_RETRIES:
                        exc_log.warning(
                            f"Max retries ({MAX_EXCEPTION_RETRIES}) reached. Marking as FAILED_PERMANENTLY.")
                        exc_log_entry.status = ExceptionStatus.FAILED_PERMANENTLY
                        exc_log_entry.resolution_notes = (
                            f"{current_resolution_notes}\nMax retries reached. Last attempt by task {task_id}. Last error: {str(new_exception_during_retry)[:500] if new_exception_during_retry else 'Original error persisted'}.".strip())
                        permanently_failed_this_run += 1
                        # --- CLEANUP STAGED FILE FOR FAILED_PERMANENTLY ---
                        if exc_log_entry.failed_filepath:
                            try:
                                staged_file_to_delete_perm = Path(
                                    exc_log_entry.failed_filepath)
                                if staged_file_to_delete_perm.is_file():
                                    staged_file_to_delete_perm.unlink()
                                    exc_log.info(
                                        "Cleaned up staged file for permanently failed exception.",
                                        path=str(staged_file_to_delete_perm))
                                    # exc_log_entry.failed_filepath = None #
                                    # Optionally nullify
                            except Exception as del_err_perm:
                                exc_log.warning(
                                    "Failed to delete staged file for permanently failed exception.",
                                    path=exc_log_entry.failed_filepath,
                                    error=str(del_err_perm))
                                current_notes_after_perm_fail_del = exc_log_entry.resolution_notes if exc_log_entry.resolution_notes is not None else ""
                                exc_log_entry.resolution_notes = f"{current_notes_after_perm_fail_del}\nWARNING: Failed to delete staged file {exc_log_entry.failed_filepath} after marking FAILED_PERMANENTLY.".strip()
                        # --- END CLEANUP ---
                    else:  # Reschedule for another retry
                        exc_log.info(
                            f"Retry attempt {exc_log_entry.retry_count} failed. Scheduling next attempt.")
                        exc_log_entry.status = ExceptionStatus.RETRY_PENDING
                        effective_retry_num_for_backoff = max(
                            1, exc_log_entry.retry_count)
                        backoff_delay_seconds = RETRY_BACKOFF_BASE_SECONDS * \
                            (2 ** (effective_retry_num_for_backoff - 1))
                        backoff_delay_seconds = min(
                            backoff_delay_seconds, EXCEPTION_RETRY_MAX_BACKOFF_SECONDS)
                        exc_log_entry.next_retry_attempt_at = datetime.now(
                            timezone.utc) + timedelta(seconds=backoff_delay_seconds)
                        exc_log.info(
                            f"Next retry scheduled at {exc_log_entry.next_retry_attempt_at} (in {backoff_delay_seconds}s).")
                        rescheduled_this_run += 1
                        if new_exception_during_retry:
                            exc_log_entry.resolution_notes = (
                                f"{current_resolution_notes}\nRetry attempt {exc_log_entry.retry_count} by task {task_id} resulted in error: {str(new_exception_during_retry)[:500]}.".strip())

                    if new_exception_during_retry:
                        exc_log_entry.error_message = (
                            f"[RETRY FAILED {exc_log_entry.retry_count}x] {str(new_exception_during_retry)[:1000]}. Original: {exc_log_entry.error_message[:1000]}")[:2000]
                        new_traceback = traceback.format_exception(
                            type(new_exception_during_retry),
                            new_exception_during_retry,
                            new_exception_during_retry.__traceback__)
                        exc_log_entry.error_details = f"--- Retry Attempt {exc_log_entry.retry_count} Error ---\n{''.join(new_traceback)}\n\n--- Original Error Details ---\n{exc_log_entry.error_details or ''}"[:65530]

                db.add(exc_log_entry)
                db.commit()
                exc_log.debug(
                    "Successfully updated exception log entry post-retry attempt.")
            except Exception as db_final_update_err:
                exc_log.critical(
                    "CRITICAL: Failed to update exception log entry after retry attempt!",
                    error=str(db_final_update_err),
                    exc_info=True)
                if db.is_active:
                    db.rollback()
                skipped_this_run += 1

        log.info("Finished processing batch of retryable exceptions.",
                 resolved=resolved_this_run,
                 rescheduled=rescheduled_this_run,
                 permanent_failures=permanently_failed_this_run,
                 skipped_due_to_errors=skipped_this_run)
        return {
            "status": "success",
            "message": f"Processed {len(exceptions_to_retry)} exceptions. Resolved: {resolved_this_run}, Rescheduled: {rescheduled_this_run}, Permanent Failures: {permanently_failed_this_run}, Skipped: {skipped_this_run}",
            "resolved_count": resolved_this_run,
            "rescheduled_count": rescheduled_this_run,
            "permanently_failed_count": permanently_failed_this_run,
            "skipped_count": skipped_this_run}
    except Exception as task_level_exc:
        log.error(
            "Unhandled critical exception in retry_pending_exceptions_task itself.",
            error=str(task_level_exc),
            exc_info=True)
        if db and db.is_active:
            db.rollback()
        raise
    finally:
        if db and db.is_active:
            db.close()
        log.info("Task finished: retry_pending_exceptions_task.")


def _move_to_error_dir(filepath: Path, task_id: str, context_log: Any):
    if not isinstance(filepath, Path):
        filepath = Path(filepath)
    if not filepath.is_file():
        context_log.warning(
            "Source path for error move is not a file or does not exist.",
            target_filepath=str(filepath))
        return
    error_dir = None
    try:
        error_base_dir_str = getattr(settings, 'DICOM_ERROR_PATH', None)
        if error_base_dir_str:
            error_dir = Path(error_base_dir_str)
        else:
            storage_path_str = getattr(
                settings, 'DICOM_STORAGE_PATH', '/dicom_data/incoming')
            error_dir = Path(storage_path_str).parent / "errors"
            context_log.warning(
                "DICOM_ERROR_PATH not set, using fallback error dir.",
                fallback_error_dir=str(error_dir))
        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        context_log.info(
            "Moved file to error directory.",
            error_path=str(error_path))
    except Exception as move_err:
        fallback_logger = logging.getLogger(
            __name__)  # Standard logger as last resort
        current_logger = getattr(
            context_log,
            'critical',
            fallback_logger.critical)
        current_logger(
            f"CRITICAL - Could not move file to error dir. Task ID: {task_id}, Original Filepath: {str(filepath)}, Target Error Dir: {str(error_dir) if 'error_dir' in locals() else 'UnknownErrorDir'}, Error: {str(move_err)}", exc_info=True)


def _handle_final_file_disposition_with_dustbin(
        original_filepath: Path,
        was_successful: bool,
        rules_matched_and_triggered_actions: bool,
        modifications_made: bool,
        any_destination_failures: bool,
        context_log: Any,
        task_id: str,
        study_instance_uid: Optional[str] = None,
        sop_instance_uid: Optional[str] = None,
        destinations_confirmed: Optional[List[str]] = None):
    """
    Medical-grade file disposition that uses dustbin service instead of immediate deletion.
    NEVER deletes files immediately - always moves to dustbin with verification.
    """
    if not original_filepath or not original_filepath.exists():
        return
    
    # Extract DICOM UIDs if not provided
    if not study_instance_uid or not sop_instance_uid:
        try:
            ds = pydicom.dcmread(str(original_filepath))
            study_instance_uid = study_instance_uid or ds.get('StudyInstanceUID', 'UNKNOWN_STUDY_UID')
            sop_instance_uid = sop_instance_uid or ds.get('SOPInstanceUID', 'UNKNOWN_SOP_UID')
        except Exception as e:
            context_log.warning("Could not read DICOM UIDs for dustbin", error=str(e))
            study_instance_uid = study_instance_uid or f"UNKNOWN_STUDY_{task_id}"
            sop_instance_uid = sop_instance_uid or f"UNKNOWN_SOP_{task_id}"
    
    destinations_confirmed = destinations_confirmed or []
    
    if was_successful and not any_destination_failures:
        # File was processed successfully - move to dustbin with full confirmation
        success = dustbin_service.move_to_dustbin(
            source_file_path=str(original_filepath),
            study_instance_uid=study_instance_uid,
            sop_instance_uid=sop_instance_uid,
            task_id=task_id,
            destinations_confirmed=destinations_confirmed,
            reason="processing_complete_all_destinations_successful"
        )
        if success:
            context_log.info("MEDICAL SAFETY: File moved to dustbin after successful processing with all destinations confirmed")
        else:
            context_log.error("CRITICAL: Failed to move successfully processed file to dustbin - FILE KEPT FOR SAFETY")
            
    elif not rules_matched_and_triggered_actions:
        if settings.DELETE_UNMATCHED_FILES:
            # Even unmatched files go to dustbin for safety
            success = dustbin_service.move_to_dustbin(
                source_file_path=str(original_filepath),
                study_instance_uid=study_instance_uid,
                sop_instance_uid=sop_instance_uid,
                task_id=task_id,
                destinations_confirmed=[],
                reason="no_rules_matched_or_triggered"
            )
            if success:
                context_log.info("MEDICAL SAFETY: Unmatched file moved to dustbin instead of immediate deletion")
            else:
                context_log.error("CRITICAL: Failed to move unmatched file to dustbin - FILE KEPT FOR SAFETY")
        else:
            context_log.info("Kept original file as no rules matched/triggered (per configuration)")
            
    elif any_destination_failures:
        if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
            context_log.warning("Partial destination failure. Moving original file to error directory.")
            _move_to_error_dir(original_filepath, task_id, context_log)
        elif settings.DELETE_ON_PARTIAL_FAILURE_IF_MODIFIED and modifications_made:
            # Even partial failures with modifications go to dustbin for safety
            success = dustbin_service.move_to_dustbin(
                source_file_path=str(original_filepath),
                study_instance_uid=study_instance_uid,
                sop_instance_uid=sop_instance_uid,
                task_id=task_id,
                destinations_confirmed=destinations_confirmed,
                reason="partial_failure_but_modified"
            )
            if success:
                context_log.info("MEDICAL SAFETY: Modified file with partial failures moved to dustbin instead of immediate deletion")
            else:
                context_log.error("CRITICAL: Failed to move partially failed modified file to dustbin - FILE KEPT FOR SAFETY")
        else:
            context_log.info("Kept original file despite partial destination failure (per configuration)")
            
    elif rules_matched_and_triggered_actions and not any_destination_failures and modifications_made and settings.DELETE_ON_NO_DESTINATION:
        # Even files with no destinations go to dustbin for safety
        success = dustbin_service.move_to_dustbin(
            source_file_path=str(original_filepath),
            study_instance_uid=study_instance_uid,
            sop_instance_uid=sop_instance_uid,
            task_id=task_id,
            destinations_confirmed=[],
            reason="rules_matched_modifications_made_no_destinations"
        )
        if success:
            context_log.info("MEDICAL SAFETY: File with rules/modifications but no destinations moved to dustbin")
        else:
            context_log.error("CRITICAL: Failed to move no-destination file to dustbin - FILE KEPT FOR SAFETY")
    
    else:
        context_log.info("File disposition: No action taken, file kept for safety")


def _handle_final_file_disposition(
        original_filepath: Path,
        was_successful: bool,
        rules_matched_and_triggered_actions: bool,
        modifications_made: bool,
        any_destination_failures: bool,
        context_log: Any,
        task_id: str):
    """
    DEPRECATED: Legacy file disposition function.
    This function has been replaced with _handle_final_file_disposition_with_dustbin
    for medical-grade safety. Keeping for reference but should not be used.
    """
    context_log.warning("DEPRECATED: Using legacy file disposition - should use dustbin service instead")
    
    if not original_filepath or not original_filepath.exists():
        return
    if was_successful and not any_destination_failures:
        if settings.DELETE_ON_SUCCESS:
            try:
                original_filepath.unlink(missing_ok=True)
                context_log.info("Deleted original file on success.")
            except OSError as e:
                context_log.warning(
                    "Failed to delete original file on success.",
                    error=str(e))
        else:
            context_log.info("Kept original file on success.")
    elif not rules_matched_and_triggered_actions:
        if settings.DELETE_UNMATCHED_FILES:
            try:
                original_filepath.unlink(missing_ok=True)
                context_log.info(
                    "Deleted original file as no rules matched/triggered.")
            except OSError as e:
                context_log.warning(
                    "Failed to delete unmatched file.", error=str(e))
        else:
            context_log.info(
                "Kept original file as no rules matched/triggered.")
    elif any_destination_failures:
        if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
            context_log.warning(
                "Partial destination failure. Moving original file to error directory.")
            _move_to_error_dir(original_filepath, task_id, context_log)
        elif settings.DELETE_ON_PARTIAL_FAILURE_IF_MODIFIED and modifications_made:
            try:
                original_filepath.unlink(missing_ok=True)
                context_log.info(
                    "Deleted modified original file on partial failure.")
            except OSError as e:
                context_log.warning(
                    "Failed to delete modified file on partial failure.",
                    error=str(e))
        else:
            context_log.info(
                "Kept original file despite partial destination failure.")
    elif rules_matched_and_triggered_actions and not any_destination_failures and modifications_made and settings.DELETE_ON_NO_DESTINATION:
        context_log.info(
            "Rules matched, mods made, no destinations. Deleting original (DELETE_ON_NO_DESTINATION=true).")
        try:
            original_filepath.unlink(missing_ok=True)
        except OSError as e:
            context_log.warning(
                "Failed to delete file with no destinations after modification.",
                error=str(e))
    else:
        context_log.info("Kept original file based on disposition settings.")

# --- Celery Tasks ---


@shared_task(bind=True,
             name="process_dicom_file_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True,
             retry_backoff_max=600,
             retry_jitter=True)
def process_dicom_file_task(self,
                            dicom_filepath_str: str,
                            source_type: str,
                            source_db_id_or_instance_id: Union[int,
                                                               str],
                            association_info: Optional[Dict[str,
                                                            str]] = None):
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    log_source_display_name = f"{source_type}_{source_db_id_or_instance_id}"
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        original_filepath=str(original_filepath),
        source_type=source_type,
        source_id_or_instance=str(source_db_id_or_instance_id),
        log_source_display_name=log_source_display_name)
    log.info("Task started: process_dicom_file_task")

    if not original_filepath.is_file():
        log.error("File not found. Task cannot proceed.")
        return {
            "status": "error_file_not_found",
            "message": "File not found",
            "filepath": dicom_filepath_str,
            "instance_uid": "Unknown",
            "source_display_name": log_source_display_name,
            "applied_rules": [],
            "destinations_processed": {}}

    db: Optional[Session] = None
    final_status_code = "task_init_failed"
    final_message = "Task initialization or pre-execution failed."
    applied_rules_info_res, dest_statuses_res = [], {}
    instance_uid_res = "UnknownFromFileTaskInit"
    rules_matched_res, modifications_made_res = False, False
    processed_ds_res = None

    try:
        db = SessionLocal()

        try:
            # No AI portal management needed here with sync wrapper approach

            rules_matched_res, modifications_made_res, final_status_code, final_message, \
                applied_rules_info_res, dest_statuses_res, processed_ds_res, \
                instance_uid_res, _ = execute_file_based_task(
                    log, db, dicom_filepath_str, source_type,
                    source_db_id_or_instance_id, task_id, association_info,
                    ai_portal=None  # Pass None explicitly
                )
        except Exception as inner_exc:
            import traceback
            log.critical(
                "CRITICAL: Exception occurred INSIDE the main task logic block.",
                error=str(inner_exc),
                exc_info=True)
            traceback.print_exc()  # Force traceback to stderr
            raise inner_exc  # Re-raise to be caught by the main handler

        if final_status_code.startswith("success"):
            try:
                source_type_enum_for_inc = ProcessedStudySourceType(
                    source_type)
                should_commit = False
                if source_type_enum_for_inc == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(
                        source_db_id_or_instance_id, str):
                    if crud.crud_dimse_listener_state.increment_processed_count(
                            db=db, listener_id=source_db_id_or_instance_id, count=1):
                        should_commit = True
                elif source_type_enum_for_inc == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
                    if crud.crud_dimse_qr_source.increment_processed_count(
                            db=db, source_id=source_db_id_or_instance_id, count=1):
                        should_commit = True
                if should_commit:
                    db.commit()
                    log.info(
                        "Successfully incremented and committed processed count.",
                        source_type=source_type)
            except Exception as db_inc_err:
                log.error(
                    "DB Error during processed count increment or commit.",
                    error=str(db_inc_err),
                    exc_info=True)
                if db and db.is_active:
                    log.warning("Rolling back DB session.")
                    db.rollback()

        if db and db.is_active:
            db.close()
            db = None
        return {
            "status": final_status_code,
            "message": final_message,
            "applied_rules": applied_rules_info_res,
            "destinations_processed": dest_statuses_res,
            "source_display_name": log_source_display_name,
            "instance_uid": instance_uid_res,
            "filepath": dicom_filepath_str}

    except Exception as exc:
        log.error(
            "Unhandled exception in task process_dicom_file_task.",
            error=str(exc),
            exc_info=True)
        if db and db.is_active:
            try:
                db.rollback()
            except Exception:
                pass  # Ignore rollback error
            finally:
                db.close()
                db = None
        if any(isinstance(exc, retry_exc_type)
               for retry_exc_type in RETRYABLE_EXCEPTIONS):
            log.info(
                "Caught retryable exception at task level. Retrying.",
                exc_type=type(exc).__name__)
            raise
        final_status_code = "fatal_task_error_unhandled"
        final_message = f"Fatal unhandled error in task: {exc!r}"
        if original_filepath.exists():
            _move_to_error_dir(original_filepath, task_id, log)
        return {
            "status": final_status_code,
            "message": final_message,
            "filepath": dicom_filepath_str,
            "source_display_name": log_source_display_name,
            "instance_uid": instance_uid_res,
            "applied_rules": applied_rules_info_res,
            "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active:
            log.warning("DB session still active in finally block, closing.")
            db.close()
        if original_filepath.exists():
            any_dest_failed_final = any(
                d.get("status") == "error" for d in dest_statuses_res.values())
            status_for_disposition = final_status_code if 'final_status_code' in locals(
            ) else 'unknown_error_state'
            _handle_final_file_disposition_with_dustbin(
                original_filepath=original_filepath,
                was_successful=(
                    status_for_disposition.startswith("success") and not any_dest_failed_final),
                rules_matched_and_triggered_actions=rules_matched_res,
                modifications_made=modifications_made_res,
                any_destination_failures=any_dest_failed_final,
                context_log=log,
                task_id=task_id,
                study_instance_uid=processed_ds_res.get('StudyInstanceUID') if processed_ds_res else None,
                sop_instance_uid=processed_ds_res.get('SOPInstanceUID') if processed_ds_res else None,
                destinations_confirmed=[]  # Will be populated by sender confirmations
            )
        log.info("Task finished: process_dicom_file_task.",
                 final_task_status=final_status_code)


@shared_task(bind=True,
             name="process_dicom_association_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True,
             retry_backoff_max=600,
             retry_jitter=True)
def process_dicom_association_task(self,
                                   dicom_filepaths_list: List[str],
                                   source_type: str,
                                   source_db_id_or_instance_id: Union[int,
                                                                      str],
                                   association_info: Optional[Dict[str,
                                                                   str]] = None):
    """
    Process an entire DICOM association (multiple files) as a single unit.
    This improves efficiency by grouping files that were received together
    and should be sent together, avoiding multiple separate associations to the PACS.
    """
    task_id = self.request.id
    
    # Initialize medical-grade dustbin service for file safety  
    from app.services.dustbin_service import DustbinService
    dustbin_service = DustbinService()
    
    log_source_display_name = f"{source_type}_{source_db_id_or_instance_id}"
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        source_type=source_type,
        source_id_or_instance=str(source_db_id_or_instance_id),
        log_source_display_name=log_source_display_name,
        file_count=len(dicom_filepaths_list))
    log.info("Task started: process_dicom_association_task")

    if not dicom_filepaths_list:
        log.error("No files provided in association. Task cannot proceed.")
        return {
            "status": "error_no_files",
            "message": "No files provided in association",
            "processed_files": [],
            "failed_files": [],
            "source_display_name": log_source_display_name}

    db: Optional[Session] = None
    processed_files = []
    failed_files = []
    overall_success = True
    # NEW: Track processed files by Study UID and destination for study-based
    # batching
    # study_uid -> dest_name -> [file_info]
    study_destination_batches: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    destination_configs: Dict[str, Dict[str, Any]] = {}  # dest_name -> config

    try:
        db = SessionLocal()
        log.info(
            f"Processing association with {len(dicom_filepaths_list)} files.")

        # Process each file in the association
        for i, filepath_str in enumerate(dicom_filepaths_list):
            file_log = log.bind(file_index=i + 1, filepath=filepath_str)
            filepath = Path(filepath_str)

            if not filepath.is_file():
                file_log.error("File not found in association.")
                failed_files.append({
                    "filepath": filepath_str,
                    "error": "File not found",
                    "status": "error_file_not_found"
                })
                overall_success = False
                continue

            try:
                file_log.info("Processing file in association.")

                # Use the existing file processing logic with batching mode
                rules_matched, modifications_made, status_code, message, \
                    applied_rules_info, dest_statuses, processed_ds, \
                    instance_uid, _ = execute_file_based_task(
                        file_log, db, filepath_str, source_type,
                        source_db_id_or_instance_id, f"{task_id}_file_{i + 1}", association_info,
                        # NEW: Don't queue immediately, batch instead & defer cleanup
                        ai_portal=None, queue_immediately=False, defer_file_cleanup=True
                    )

                file_result = {
                    "filepath": filepath_str,
                    "status": status_code,
                    "message": message,
                    "instance_uid": instance_uid,
                    "applied_rules": applied_rules_info,
                    "destinations_processed": dest_statuses
                }

                if not status_code.startswith("success"):
                    overall_success = False
                    file_log.warning("File processing failed in association.")
                    failed_files.append(file_result)
                    continue
                else:
                    file_log.info(
                        "File processing completed successfully in association.")
                    processed_files.append(file_result)

                # NEW: Collect processed files for study-based batch sending
                if processed_ds and dest_statuses:
                    # Get Study Instance UID from the processed dataset
                    from app.worker.task_utils import _safe_get_dicom_value
                    study_uid = _safe_get_dicom_value(
                        processed_ds, "StudyInstanceUID", "UNKNOWN_STUDY")

                    # Skip if study_uid is None or empty
                    if not study_uid:
                        file_log.warning(
                            f"Cannot group file for study batching - no StudyInstanceUID found for instance: {instance_uid}")
                    else:
                        file_log.info(
                            f"Adding file to study batch: Study={study_uid}, Instance={instance_uid}")

                        # Save processed file using medical-grade dustbin system
                        
                        # Save to dustbin instead of directly to processed directory
                        dustbin_filepath = dustbin_service.save_processed_file_to_dustbin(
                            processed_ds=processed_ds,
                            original_filepath=filepath_str,
                            task_id=f"{task_id}_file_{i + 1}",
                            instance_uid=instance_uid or "UNKNOWN_SOP_UID"
                        )
                        processed_filepath = Path(dustbin_filepath)

                        # Group by Study UID and destination for batch sending
                        for dest_name, dest_result in dest_statuses.items():
                            if dest_result.get(
                                    "status") in ["success", "batched"]:  # Only batch successful files per destination
                                # Initialize nested dictionaries if needed
                                if study_uid not in study_destination_batches:
                                    study_destination_batches[study_uid] = {}
                                    file_log.info(
                                        f"Created new study batch group for StudyInstanceUID: {study_uid}")
                                if dest_name not in study_destination_batches[study_uid]:
                                    study_destination_batches[study_uid][dest_name] = [
                                    ]

                                # Store file info with path and metadata
                                file_info = {
                                    "file_path": str(processed_filepath),
                                    "instance_uid": instance_uid,
                                    "original_filepath": filepath_str
                                }
                                study_destination_batches[study_uid][dest_name].append(
                                    file_info)
                                file_log.info(
                                    f"Added file to study batch: Study={study_uid}, Dest={dest_name}, Files={len(study_destination_batches[study_uid][dest_name])}")
                            else:
                                file_log.warning(
                                    f"Skipping file for study batch due to destination failure: Study={study_uid}, Dest={dest_name}, Status={dest_result.get('status')}")

            except Exception as file_exc:
                file_log.error(
                    "Exception processing file in association.",
                    error=str(file_exc),
                    exc_info=True)
                failed_files.append({
                    "filepath": filepath_str,
                    "error": str(file_exc),
                    "status": "error_processing_exception"
                })
                overall_success = False

        # NEW: Log batched files and trigger immediate sends via Redis
        if study_destination_batches:
            total_files = sum(len(files) for study_files in study_destination_batches.values(
            ) for files in study_files.values())
            log.info(
                f"Added files to exam batches: {len(study_destination_batches)} studies, {total_files} total files batched.")

            # Import Redis trigger
            from app.services.redis_batch_triggers import redis_batch_trigger

            # Trigger immediate sends for each study/destination combo via
            # Redis
            for study_uid, destinations in study_destination_batches.items():
                study_file_count = sum(len(files)
                                       for files in destinations.values())
                log.info(
                    f"Study batch added: StudyInstanceUID={study_uid}, Destinations={list(destinations.keys())}, Files={study_file_count}")

                # Get destination IDs and trigger immediate sends
                for dest_name, files in destinations.items():
                    # Find destination ID by name
                    dest_config = crud.crud_storage_backend_config.get_by_name(
                        db, name=dest_name)
                    if dest_config:
                        if dest_config.backend_type == "cstore":
                            log.info(
                                f"Triggering immediate send via Redis for study {study_uid} to destination {dest_name} (ID: {dest_config.id})")
                            try:
                                triggered = redis_batch_trigger.trigger_batch_ready(
                                    study_instance_uid=study_uid,
                                    destination_id=dest_config.id
                                )
                                if triggered:
                                    log.info(
                                        f"Successfully triggered Redis signal for study {study_uid} to {dest_name}")
                                else:
                                    log.warning(
                                        f"No Redis subscribers for study {study_uid} to {dest_name} - batch will remain PENDING")
                            except Exception as redis_exc:
                                log.error(
                                    f"Failed to trigger Redis signal for study {study_uid} to {dest_name}",
                                    error=str(redis_exc),
                                    exc_info=True)
                        else:
                            log.debug(
                                f"Skipping Redis trigger for non-DIMSE destination: {dest_name} (type: {dest_config.backend_type})")
                    else:
                        log.error(
                            f"Destination config not found for name: {dest_name}")

            log.info(
                "Files added to exam_batches table and Redis triggers sent for DIMSE destinations.")
        else:
            log.info(
                "No study batches found - either no successful files or no StudyInstanceUID found.")

        # Update processed counts if overall success
        if overall_success and processed_files:
            try:
                source_type_enum = ProcessedStudySourceType(source_type)
                should_commit = False

                if source_type_enum == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(
                        source_db_id_or_instance_id, str):
                    if crud.crud_dimse_listener_state.increment_processed_count(
                            db=db, listener_id=source_db_id_or_instance_id, count=len(processed_files)):
                        should_commit = True
                elif source_type_enum == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
                    if crud.crud_dimse_qr_source.increment_processed_count(
                            db=db, source_id=source_db_id_or_instance_id, count=len(processed_files)):
                        should_commit = True

                if should_commit:
                    db.commit()
                    log.info(
                        f"Successfully incremented processed count by {len(processed_files)} files.")

            except Exception as db_inc_err:
                log.error(
                    "DB Error during processed count increment.",
                    error=str(db_inc_err),
                    exc_info=True)
                if db and db.is_active:
                    db.rollback()

        final_status = "success_association_processed" if overall_success else "partial_failure_association"
        final_message = f"Association processed: {len(processed_files)} successful, {len(failed_files)} failed"

        log.info("Association processing completed.",
                 successful_files=len(processed_files),
                 failed_files=len(failed_files),
                 overall_success=overall_success)

        # MEDICAL-GRADE SAFETY: Move successfully processed files to dustbin instead of immediate deletion
        if processed_files:
            dustbin_count = 0
            for file_info in processed_files:
                try:
                    filepath = Path(file_info["filepath"])
                    if filepath.exists():
                        # Extract DICOM UIDs from file_info or file directly
                        study_uid = file_info.get("study_instance_uid", "UNKNOWN_STUDY_UID")
                        sop_uid = file_info.get("sop_instance_uid", "UNKNOWN_SOP_UID")
                        destinations_confirmed = file_info.get("destinations_confirmed", [])
                        
                        # If UIDs not in file_info, try to read from file
                        if study_uid == "UNKNOWN_STUDY_UID" or sop_uid == "UNKNOWN_SOP_UID":
                            try:
                                ds = pydicom.dcmread(str(filepath))
                                study_uid = ds.get('StudyInstanceUID', study_uid)
                                sop_uid = ds.get('SOPInstanceUID', sop_uid)
                            except Exception:
                                pass  # Keep the UNKNOWN values
                        
                        success = dustbin_service.move_to_dustbin(
                            source_file_path=str(filepath),
                            study_instance_uid=study_uid,
                            sop_instance_uid=sop_uid,
                            task_id=task_id,
                            destinations_confirmed=destinations_confirmed,
                            reason="association_processing_complete"
                        )
                        
                        if success:
                            dustbin_count += 1
                        else:
                            log.error(
                                "CRITICAL: Failed to move successfully processed file to dustbin - FILE KEPT FOR SAFETY",
                                filepath=str(filepath),
                                study_uid=study_uid,
                                sop_uid=sop_uid,
                                task_id=task_id
                            )
                            
                except Exception as e:
                    log.error(
                        "CRITICAL: Exception while moving processed file to dustbin - FILE KEPT FOR SAFETY",
                        filepath=file_info.get("filepath", "UNKNOWN"),
                        error=str(e),
                        exc_info=True
                    )
            
            if dustbin_count > 0:
                log.info(
                    "MEDICAL SAFETY: Successfully processed files moved to dustbin for verification",
                    files_moved_to_dustbin=dustbin_count,
                    total_processed=len(processed_files)
                )

        return {
            "status": final_status,
            "message": final_message,
            "processed_files": processed_files,
            "failed_files": failed_files,
            "source_display_name": log_source_display_name,
            "total_files": len(dicom_filepaths_list)
        }

    except Exception as exc:
        log.error(
            "Unhandled exception in process_dicom_association_task.",
            error=str(exc),
            exc_info=True)

        if db and db.is_active:
            try:
                db.rollback()
            except Exception:
                pass
            finally:
                db.close()
                db = None

        if any(isinstance(exc, retry_exc_type)
               for retry_exc_type in RETRYABLE_EXCEPTIONS):
            log.info(
                "Caught retryable exception at association task level. Retrying.",
                exc_type=type(exc).__name__)
            raise

        return {
            "status": "fatal_association_task_error",
            "message": f"Fatal error processing association: {exc!r}",
            "processed_files": processed_files,
            "failed_files": failed_files,
            "source_display_name": log_source_display_name,
            "total_files": len(dicom_filepaths_list)
        }

    finally:
        if db and db.is_active:
            db.close()
        log.info("Task finished: process_dicom_association_task.")


@shared_task(bind=True,
             name="process_dicomweb_metadata_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True,
             retry_backoff_max=600,
             retry_jitter=True)
async def process_dicomweb_metadata_task(
        self,
        source_id: int,
        study_uid: str,
        series_uid: str,
        instance_uid: str):
    task_id = self.request.id
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        source_id=source_id,
        instance_uid=instance_uid,
        source_type=ProcessedStudySourceType.DICOMWEB.value)  # Changed DICOMWEB_POLLER to DICOMWEB
    log.info("Task started: process_dicomweb_metadata_task")
    db: Optional[Session] = None
    final_status_code = "task_init_failed"
    final_message = "DICOMweb task initialization failed."
    applied_rules_res, dest_statuses_res = [], {}
    source_name_res = f"DICOMweb_src_{source_id}"

    try:
        db = SessionLocal()
        # No AI portal management needed with sync wrapper

        _, _, final_status_code, final_message, applied_rules_res, \
            dest_statuses_res, _, source_name_res_from_exec, _ = await execute_dicomweb_task(
                log, db, source_id, study_uid, series_uid, instance_uid, task_id,
                ai_portal=None  # Pass None
            )
        if source_name_res_from_exec:
            source_name_res = source_name_res_from_exec

        await asyncio.to_thread(db.close)
        db = None
        return {
            "status": final_status_code,
            "message": final_message,
            "applied_rules": applied_rules_res,
            "destinations_processed": dest_statuses_res,
            "source_name": source_name_res,
            "instance_uid": instance_uid}
    except Exception as exc:
        log.error(
            "Unhandled exception in task process_dicomweb_metadata_task.",
            error=str(exc),
            exc_info=True)
        if db and db.is_active:
            try:
                await asyncio.to_thread(db.rollback)
            except Exception:
                pass
            finally:
                await asyncio.to_thread(db.close)
                db = None
        if any(isinstance(exc, retry_exc_type)
               for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled"
        final_message = f"Fatal error: {exc!r}"
        return {
            "status": final_status_code,
            "message": final_message,
            "instance_uid": instance_uid,
            "source_id": source_id,
            "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active:
            await asyncio.to_thread(db.close)
        current_final_status = final_status_code if 'final_status_code' in locals(
        ) else 'task_ended_in_finally_early_error_dw'
        log.info("Task finished: process_dicomweb_metadata_task.",
                 final_task_status=current_final_status)


@shared_task(bind=True,
             name="process_stow_instance_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True,
             retry_backoff_max=600,
             retry_jitter=True)
def process_stow_instance_task(
        self,
        temp_filepath_str: str,
        source_ip: Optional[str] = None):
    task_id = self.request.id
    temp_filepath = Path(temp_filepath_str)
    source_identifier_log_display = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}"
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        temp_filepath_stow=str(temp_filepath),
        source_ip_stow=source_ip,
        log_source_display_name=source_identifier_log_display,
        source_type=ProcessedStudySourceType.STOW_RS.value)
    log.info("Task started: process_stow_instance_task")

    if not temp_filepath.is_file():
        log.error(
            "Temporary STOW file not found. Task cannot proceed.",
            stow_temp_file=str(temp_filepath))
        return {
            "status": "error_temp_file_not_found",
            "message": "Temporary STOW file not found",
            "filepath": temp_filepath_str,
            "instance_uid": "Unknown",
            "source_display_name": source_identifier_log_display}

    db: Optional[Session] = None
    final_status_code = "task_init_failed_stow"
    final_message = "STOW task initialization failed."
    applied_rules_res, dest_statuses_res = [], {}
    instance_uid_res = "UnknownFromStowTask"

    try:
        db = SessionLocal()
        # No AI portal needed

        _rules_matched, _mods_made, final_status_code, final_message, \
            applied_rules_res, dest_statuses_res, _processed_ds, \
            instance_uid_res, _source_id_match = execute_stow_task(
                log, db, temp_filepath_str, source_ip, task_id,
                ai_portal=None
            )

        db.close()
        db = None
        return {
            "status": final_status_code,
            "message": final_message,
            "applied_rules": applied_rules_res,
            "destinations_processed": dest_statuses_res,
            "source_display_name": source_identifier_log_display,
            "instance_uid": instance_uid_res,
            "original_stow_filepath": temp_filepath_str}

    except Exception as exc:
        log.error(
            "Unhandled exception in task process_stow_instance_task.",
            error=str(exc),
            exc_info=True)
        if db and db.is_active:
            try:
                db.rollback()
            except Exception:
                pass
            finally:
                db.close()
                db = None
        if any(isinstance(exc, retry_exc_type)
               for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled_stow"
        final_message = f"Fatal unhandled error in STOW task: {exc!r}"
        return {
            "status": final_status_code,
            "message": final_message,
            "filepath": temp_filepath_str,
            "source_display_name": source_identifier_log_display,
            "instance_uid": instance_uid_res,
            "applied_rules": applied_rules_res,
            "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active:
            db.close()
        if temp_filepath.exists():
            try:
                temp_filepath.unlink(missing_ok=True)
            except OSError as e:
                log.critical(
                    "FAILED TO DELETE temporary STOW file in finally.",
                    stow_temp_file_final_error=str(temp_filepath),
                    error_detail=str(e))
        current_final_status = final_status_code if 'final_status_code' in locals(
        ) else 'task_ended_in_finally_early_error_stow'
        log.info("Task finished: process_stow_instance_task.",
                 final_task_status=current_final_status)


@shared_task(bind=True,
             name="process_google_healthcare_metadata_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True,
             retry_backoff_max=600,
             retry_jitter=True)
async def process_google_healthcare_metadata_task(
        self, source_id: int, study_uid: str):
    task_id = self.request.id
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        ghc_source_id=source_id,
        ghc_study_uid=study_uid,
        source_type=ProcessedStudySourceType.GOOGLE_HEALTHCARE.value)
    log.info("Task started: process_google_healthcare_metadata_task")
    db: Optional[Session] = None
    final_status_code = "task_init_failed_ghc"
    final_message = "GHC task initialization failed."
    applied_rules_res, dest_statuses_res = [], {}
    instance_uid_res = "UnknownFromGHCTask"
    source_name_res = f"GHC_src_{source_id}"

    try:
        db = SessionLocal()
        # No AI portal needed

        _rules_matched, _mods_made, final_status_code, final_message, applied_rules_res, \
            dest_statuses_res, _processed_ds, instance_uid_res, \
            source_name_res_from_exec = await execute_ghc_task(
                log, db, source_id, study_uid, task_id,
                ai_portal=None
            )
        if source_name_res_from_exec:
            source_name_res = source_name_res_from_exec

        await asyncio.to_thread(db.close)
        db = None
        return {
            "status": final_status_code,
            "message": final_message,
            "applied_rules": applied_rules_res,
            "destinations_processed": dest_statuses_res,
            "source_name": source_name_res,
            "study_uid": study_uid,
            "instance_uid": instance_uid_res}
    except Exception as exc:
        log.error(
            "Unhandled exception in task process_google_healthcare_metadata_task.",
            error=str(exc),
            exc_info=True)
        if db and db.is_active:
            try:
                await asyncio.to_thread(db.rollback)
            except Exception:
                pass
            finally:
                await asyncio.to_thread(db.close)
                db = None
        if any(isinstance(exc, retry_exc_type)
               for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled_ghc"
        final_message = f"Fatal unhandled error in GHC task: {exc!r}"
        return {
            "status": final_status_code,
            "message": final_message,
            "study_uid": study_uid,
            "source_id": source_id,
            "instance_uid": instance_uid_res,
            "applied_rules": applied_rules_res,
            "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active:
            await asyncio.to_thread(db.close)
        current_final_status = final_status_code if 'final_status_code' in locals(
        ) else 'task_ended_in_finally_early_error_ghc'
        log.info(
            "Task finished: process_google_healthcare_metadata_task.",
            final_task_status=current_final_status)

# Default to 30 days, add to Settings
STALE_DATA_CLEANUP_AGE_DAYS = settings.STALE_DATA_CLEANUP_AGE_DAYS
STALE_RETRY_IN_PROGRESS_AGE_HOURS = settings.STALE_RETRY_IN_PROGRESS_AGE_HOURS


@shared_task(name="cleanup_stale_exception_data_task", acks_late=True)
def cleanup_stale_exception_data_task():
    log = logger.bind(task_name="cleanup_stale_exception_data_task")
    log.info("Task started: cleanup_stale_exception_data_task")
    db: Optional[Session] = None
    cleaned_files_count = 0
    reverted_stuck_retries_count = 0

    try:
        db = SessionLocal()

        # --- 1. Cleanup staged files for RESOLVED_MANUALLY or ARCHIVED entries ---
        statuses_for_file_cleanup = [
            ExceptionStatus.RESOLVED_MANUALLY,
            ExceptionStatus.ARCHIVED]
        entries_for_cleanup: List[DicomExceptionLog] = db.query(DicomExceptionLog).filter(
            DicomExceptionLog.status.in_(statuses_for_file_cleanup),
            DicomExceptionLog.failed_filepath.isnot(
                None)  # Only those with a filepath
            # type: ignore # Add CLEANUP_BATCH_SIZE to Settings
        ).limit(settings.CLEANUP_BATCH_SIZE).all()

        if entries_for_cleanup:
            log.info(
                f"Found {len(entries_for_cleanup)} manually resolved/archived exceptions with staged files to clean up.")
            for entry in entries_for_cleanup:
                entry_log: BoundLogger = log.bind(exception_log_id=entry.id, exception_uuid=str(
                    entry.exception_uuid), staged_file=entry.failed_filepath)  # type: ignore
                try:
                    staged_file = Path(entry.failed_filepath)  # type: ignore
                    if staged_file.is_file():
                        staged_file.unlink()
                        entry_log.info(
                            "Successfully deleted staged file for resolved/archived entry.")
                        entry.failed_filepath = None  # Nullify path after deletion
                        entry.resolution_notes = (
                            f"{entry.resolution_notes or ''}\nStaged file {staged_file} auto-cleaned by cleanup task.").strip()
                        db.add(entry)
                        cleaned_files_count += 1
                    else:
                        entry_log.warning(
                            "Staged file not found for resolved/archived entry, nullifying path.")
                        entry.failed_filepath = None  # Nullify path even if not found
                        db.add(entry)
                except Exception as e:
                    entry_log.error(
                        "Error deleting staged file for resolved/archived entry.",
                        error=str(e))
            db.commit()  # Commit changes (nullified paths)

        # --- 2. Revert "stuck" RETRY_IN_PROGRESS entries back to RETRY_PENDING ---
        # Entries stuck for too long (e.g., worker died)
        stuck_threshold = datetime.now(
            timezone.utc) - timedelta(hours=STALE_RETRY_IN_PROGRESS_AGE_HOURS)
        stuck_entries: List[DicomExceptionLog] = db.query(DicomExceptionLog).filter(
            DicomExceptionLog.status == ExceptionStatus.RETRY_IN_PROGRESS,
            DicomExceptionLog.last_retry_attempt_at < stuck_threshold
        ).limit(settings.get("CLEANUP_BATCH_SIZE", 100)).all()  # type: ignore

        if stuck_entries:
            log.info(
                f"Found {len(stuck_entries)} entries stuck in RETRY_IN_PROGRESS. Reverting to RETRY_PENDING.")
            for entry in stuck_entries:
                entry.status = ExceptionStatus.RETRY_PENDING
                entry.resolution_notes = (
                    f"{entry.resolution_notes or ''}\nAutomatically reverted from stuck RETRY_IN_PROGRESS by cleanup task at {datetime.now(timezone.utc)}.").strip()
                # Optionally, adjust next_retry_attempt_at if desired, e.g., retry soon
                # entry.next_retry_attempt_at = datetime.now(timezone.utc) + timedelta(minutes=5)
                db.add(entry)
                reverted_stuck_retries_count += 1
            db.commit()

        # --- 3. (Optional) Advanced: Sweep DICOM_RETRY_STAGING_PATH for truly orphaned files ---
        # This is more complex: list files in dir, check against DB.
        # For now, focusing on DB-driven cleanup.
        log.info(
            "Orphaned file sweep in staging path not yet implemented in this task version.")

        log.info(
            "Cleanup task finished.",
            cleaned_files=cleaned_files_count,
            reverted_stuck_retries=reverted_stuck_retries_count)
        return {
            "status": "success",
            "cleaned_files": cleaned_files_count,
            "reverted_stuck_retries": reverted_stuck_retries_count}

    except Exception as e:
        log.error(
            "Error during cleanup_stale_exception_data_task.",
            error=str(e),
            exc_info=True)
        if db and db.is_active:
            db.rollback()
        raise  # Let Celery handle task failure
    finally:
        if db and db.is_active:
            db.close()


@shared_task(bind=True, acks_late=True, reject_on_worker_lost=True)
def health_monitoring_task(self, force_check: bool = False):
    """
    Periodic task to check the health status of all configured scraper sources.

    This task runs periodically (e.g., every 5-15 minutes) to test connections
    to all DICOMWeb, DIMSE Q/R, and Google Healthcare sources and updates
    their health status in the database.

    Args:
        force_check: If True, check all sources regardless of last check time.
                    If False, only check sources that haven't been checked recently.
    """
    log = structlog.get_logger()
    log.info("Starting health monitoring task", force_check=force_check)

    db = None
    try:
        db = SessionLocal()

        # Import the connection test service
        from app.services.connection_test_service import ConnectionTestService
        from app.schemas.enums import HealthStatus
        from app.db import models

        results = {
            "dicomweb_sources": {"checked": 0, "ok": 0, "down": 0, "error": 0},
            "dimse_qr_sources": {"checked": 0, "ok": 0, "down": 0, "error": 0},
            "google_healthcare_sources": {"checked": 0, "ok": 0, "down": 0, "error": 0},
        }

        # Check DICOMWeb sources
        dicomweb_sources = crud.dicomweb_state.get_all(db, limit=1000)
        for source in dicomweb_sources:
            if not force_check and source.last_health_check:
                # Skip if checked within the last 10 minutes
                time_since_check = datetime.now(
                    timezone.utc) - source.last_health_check
                if time_since_check.total_seconds() < 600:  # 10 minutes
                    continue

            try:
                # Use asyncio.run to run the async function
                health_status, error_message = asyncio.run(
                    ConnectionTestService.test_dicomweb_connection(source)
                )

                # Update health status
                asyncio.run(ConnectionTestService.update_source_health_status(
                    db_session=db,
                    source_type="dicomweb",
                    source_id=source.id,
                    health_status=health_status,
                    error_message=error_message
                ))

                results["dicomweb_sources"]["checked"] += 1
                if health_status == HealthStatus.OK:
                    results["dicomweb_sources"]["ok"] += 1
                elif health_status == HealthStatus.DOWN:
                    results["dicomweb_sources"]["down"] += 1
                else:
                    results["dicomweb_sources"]["error"] += 1

                log.info(
                    f"Checked DICOMWeb source {source.id} ({source.source_name}): {health_status.value}")

            except Exception as e:
                log.error(
                    f"Error checking DICOMWeb source {source.id}: {str(e)}")
                results["dicomweb_sources"]["error"] += 1

        # Check DIMSE Q/R sources
        dimse_qr_sources = crud.crud_dimse_qr_source.get_multi(db, limit=1000)
        for source in dimse_qr_sources:
            if not force_check and source.last_health_check:
                time_since_check = datetime.now(
                    timezone.utc) - source.last_health_check
                if time_since_check.total_seconds() < 600:  # 10 minutes
                    continue

            try:
                health_status, error_message = asyncio.run(
                    ConnectionTestService.test_dimse_qr_connection(source)
                )

                asyncio.run(ConnectionTestService.update_source_health_status(
                    db_session=db,
                    source_type="dimse_qr",
                    source_id=source.id,
                    health_status=health_status,
                    error_message=error_message
                ))

                results["dimse_qr_sources"]["checked"] += 1
                if health_status == HealthStatus.OK:
                    results["dimse_qr_sources"]["ok"] += 1
                elif health_status == HealthStatus.DOWN:
                    results["dimse_qr_sources"]["down"] += 1
                else:
                    results["dimse_qr_sources"]["error"] += 1

                log.info(
                    f"Checked DIMSE Q/R source {source.id} ({source.remote_ae_title}@{source.remote_host}): {health_status.value}")

            except Exception as e:
                log.error(
                    f"Error checking DIMSE Q/R source {source.id}: {str(e)}")
                results["dimse_qr_sources"]["error"] += 1

        # Check Google Healthcare sources
        google_healthcare_sources = crud.google_healthcare_source.get_multi(
            db, limit=1000)
        for source in google_healthcare_sources:
            if not force_check and source.last_health_check:
                time_since_check = datetime.now(
                    timezone.utc) - source.last_health_check
                if time_since_check.total_seconds() < 600:  # 10 minutes
                    continue

            try:
                health_status, error_message = asyncio.run(
                    ConnectionTestService.test_google_healthcare_connection(source))

                asyncio.run(ConnectionTestService.update_source_health_status(
                    db_session=db,
                    source_type="google_healthcare",
                    source_id=source.id,
                    health_status=health_status,
                    error_message=error_message
                ))

                results["google_healthcare_sources"]["checked"] += 1
                if health_status == HealthStatus.OK:
                    results["google_healthcare_sources"]["ok"] += 1
                elif health_status == HealthStatus.DOWN:
                    results["google_healthcare_sources"]["down"] += 1
                else:
                    results["google_healthcare_sources"]["error"] += 1

                log.info(
                    f"Checked Google Healthcare source {source.id} (project: {source.gcp_project_id}): {health_status.value}")

            except Exception as e:
                log.error(
                    f"Error checking Google Healthcare source {source.id}: {str(e)}")
                results["google_healthcare_sources"]["error"] += 1

        total_checked = sum(source_type["checked"]
                            for source_type in results.values())
        total_ok = sum(source_type["ok"] for source_type in results.values())
        total_down = sum(source_type["down"]
                         for source_type in results.values())
        total_error = sum(source_type["error"]
                          for source_type in results.values())

        log.info("Health monitoring task completed",
                 total_checked=total_checked,
                 total_ok=total_ok,
                 total_down=total_down,
                 total_error=total_error,
                 results=results)

        return {
            "status": "success",
            "total_checked": total_checked,
            "total_ok": total_ok,
            "total_down": total_down,
            "total_error": total_error,
            "details": results
        }

    except Exception as e:
        log.error(
            "Error during health monitoring task",
            error=str(e),
            exc_info=True)
        if db and db.is_active:
            db.rollback()
        raise
    finally:
        if db and db.is_active:
            db.close()
