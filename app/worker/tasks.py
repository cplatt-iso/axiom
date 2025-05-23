# app/worker/tasks.py
from datetime import datetime, timezone
import logging
import shutil
import sys
import asyncio
from pathlib import Path
import traceback
from typing import Optional, List, Dict, Any, Union
import uuid

import structlog
from celery import shared_task
from sqlalchemy.orm import Session

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

from app.services import ai_assist_service # Still needed for initial check

from app.db.models.dicom_exception_log import DicomExceptionLog # The SQLAlchemy model
from app.schemas.enums import ExceptionStatus, ExceptionProcessingStage # Our enums
from app.services.storage_backends import get_storage_backend, StorageBackendError # To attempt send
from app.worker.task_utils import build_storage_backend_config_dict, _safe_get_dicom_value # _safe_get_dicom_value might be useful if re-parsing
import pydicom # For reading DICOM file
from datetime import timedelta, timezone # For backoff calculation

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
    log: StdlibBoundLogger = logger.bind(task_id=task_id, task_name=self.name) # type: ignore
    log.info("Task started: retry_pending_exceptions_task")

    db: Optional[Session] = None
    resolved_this_run = 0
    rescheduled_this_run = 0
    permanently_failed_this_run = 0
    skipped_this_run = 0

    try:
        db = SessionLocal()
        exceptions_to_retry: List[DicomExceptionLog] = crud.dicom_exception_log.get_retry_pending_exceptions(
            db, limit=EXCEPTION_RETRY_BATCH_SIZE
        )

        if not exceptions_to_retry:
            log.info("No pending exceptions found for retry.")
            return {"status": "success_no_exceptions_to_retry", "processed_this_run": 0}

        log.info(f"Found {len(exceptions_to_retry)} exceptions to attempt retry.")

        for exc_log_entry in exceptions_to_retry:
            exc_log: StdlibBoundLogger = log.bind(exception_log_id=exc_log_entry.id, exception_uuid=str(exc_log_entry.exception_uuid)) # type: ignore
            exc_log.info("Attempting retry for exception.")

            original_status_before_retry_attempt = exc_log_entry.status # Not used, but good for debugging
            exc_log_entry.status = ExceptionStatus.RETRY_IN_PROGRESS
            exc_log_entry.last_retry_attempt_at = datetime.now(timezone.utc)
            try:
                db.add(exc_log_entry)
                db.commit()
                db.refresh(exc_log_entry)
                exc_log.debug("Marked exception as RETRY_IN_PROGRESS.")
            except Exception as db_update_err:
                exc_log.error("Failed to mark exception as RETRY_IN_PROGRESS. Skipping this one.", error=str(db_update_err), exc_info=True)
                if db.is_active: db.rollback()
                skipped_this_run +=1
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
                        exc_log.error("Staged file for retry does not exist or is not a file.", path=str(staged_dicom_file))
                        raise FileNotFoundError(f"Staged DICOM file for retry not found: {exc_log_entry.failed_filepath}")

                    dataset_to_send: pydicom.Dataset = pydicom.dcmread(str(staged_dicom_file), force=True)
                    exc_log.debug("Successfully read staged DICOM file for retry.", sop_uid=_safe_get_dicom_value(dataset_to_send, "SOPInstanceUID"))

                    dest_config_model = crud.crud_storage_backend_config.get(db, id=exc_log_entry.target_destination_id)
                    if not dest_config_model:
                        exc_log.error("Target destination config not found for retry.", target_dest_id=exc_log_entry.target_destination_id)
                        raise ValueError(f"Destination config ID {exc_log_entry.target_destination_id} not found for retry.")
                    if not dest_config_model.is_enabled:
                        exc_log.warning("Target destination is disabled. Cannot retry.", target_dest_name=dest_config_model.name)
                        raise RuntimeError(f"Destination '{dest_config_model.name}' is disabled, cannot retry send.")

                    storage_config_dict = build_storage_backend_config_dict(dest_config_model, f"{task_id}_retry_{exc_log_entry.id}")
                    if not storage_config_dict:
                        exc_log.error("Failed to build storage backend config for retry.")
                        raise RuntimeError("Failed to build storage backend config for retry.")

                    backend = get_storage_backend(storage_config_dict)
                    filename_ctx_retry = f"{_safe_get_dicom_value(dataset_to_send, 'SOPInstanceUID', f'RetrySOP_{uuid.uuid4().hex[:8]}')}.dcm"
                    source_id_for_store = exc_log_entry.original_source_identifier or "ExceptionRetry"

                    exc_log.info(f"Attempting store to destination '{dest_config_model.name}' via retry task.")
                    backend.store(
                        dataset_to_send,
                        original_filepath=staged_dicom_file,
                        filename_context=filename_ctx_retry,
                        source_identifier=source_id_for_store
                    )
                    retry_successful = True
                    exc_log.info("Retry successful: DESTINATION_SEND completed.")
                else:
                    exc_log.warning("Retry logic not implemented or data missing for this exception type/stage.",
                                    stage=exc_log_entry.processing_stage.value,
                                    has_target_id=bool(exc_log_entry.target_destination_id),
                                    has_filepath=bool(exc_log_entry.failed_filepath))
                    new_exception_during_retry = NotImplementedError(
                        f"Retry not implemented for stage: {exc_log_entry.processing_stage.value} or required data missing."
                    )
            except Exception as retry_exc:
                exc_log.warning("Exception occurred during retry attempt itself.", error=str(retry_exc), exc_info=settings.DEBUG)
                new_exception_during_retry = retry_exc
                retry_successful = False

            try: # Nested try for final DB update of the DicomExceptionLog entry
                if retry_successful:
                    exc_log_entry.status = ExceptionStatus.RESOLVED_BY_RETRY
                    exc_log_entry.resolved_at = datetime.now(timezone.utc)
                    exc_log_entry.resolution_notes = f"{current_resolution_notes}\nResolved by retry task {task_id} at {exc_log_entry.resolved_at}.".strip()
                    exc_log_entry.error_message = f"[RESOLVED VIA RETRY] {exc_log_entry.error_message}"[:2000]
                    exc_log_entry.retry_count = (exc_log_entry.retry_count or 0) + 1
                    resolved_this_run += 1

                    if exc_log_entry.failed_filepath:
                        try:
                            staged_file_to_delete = Path(exc_log_entry.failed_filepath)
                            if staged_file_to_delete.is_file():
                                staged_file_to_delete.unlink()
                                exc_log.info("Successfully deleted staged file after successful retry.", path=str(staged_file_to_delete))
                                # Optionally nullify the path in DB
                                # exc_log_entry.failed_filepath = None
                        except Exception as del_err:
                            exc_log.warning("Failed to delete staged file after successful retry.", path=exc_log_entry.failed_filepath, error=str(del_err))
                            current_notes_for_del_fail = exc_log_entry.resolution_notes if exc_log_entry.resolution_notes is not None else "" # Re-fetch or use updated
                            exc_log_entry.resolution_notes = f"{current_notes_for_del_fail}\nWARNING: Failed to delete staged file: {exc_log_entry.failed_filepath}.".strip()
                else: # Retry failed or not applicable
                    exc_log_entry.retry_count = (exc_log_entry.retry_count or 0) + 1
                    # current_resolution_notes is already initialized as string above
                    
                    if new_exception_during_retry and isinstance(new_exception_during_retry, (FileNotFoundError, ValueError, RuntimeError, NotImplementedError)):
                        exc_log.warning(f"Non-transient error during retry ({type(new_exception_during_retry).__name__}). Marking for manual review.", error_details=str(new_exception_during_retry))
                        exc_log_entry.status = ExceptionStatus.MANUAL_REVIEW_REQUIRED
                        exc_log_entry.resolution_notes = (f"{current_resolution_notes}\nRetry attempt {exc_log_entry.retry_count} by task {task_id} failed due to: {str(new_exception_during_retry)[:500]}. Marked for manual review.".strip())
                        permanently_failed_this_run += 1 # Counting as a form of permanent failure for this retry path
                        # --- CLEANUP STAGED FILE IF MANUAL REVIEW IS THE END OF THE LINE FOR THIS ERROR TYPE ---
                        # Or if the error was FileNotFoundError for the staged file itself.
                        if isinstance(new_exception_during_retry, FileNotFoundError) and exc_log_entry.failed_filepath and str(new_exception_during_retry).find(exc_log_entry.failed_filepath) != -1:
                            exc_log.info("Staged file was not found during retry, already gone or path issue. Nullifying path in DB.")
                            exc_log_entry.failed_filepath = None # Nullify path if file was not found
                        # For other manual review cases, we might keep the staged file for inspection.
                    elif exc_log_entry.retry_count >= MAX_EXCEPTION_RETRIES:
                        exc_log.warning(f"Max retries ({MAX_EXCEPTION_RETRIES}) reached. Marking as FAILED_PERMANENTLY.")
                        exc_log_entry.status = ExceptionStatus.FAILED_PERMANENTLY
                        exc_log_entry.resolution_notes = (f"{current_resolution_notes}\nMax retries reached. Last attempt by task {task_id}. Last error: {str(new_exception_during_retry)[:500] if new_exception_during_retry else 'Original error persisted'}.".strip())
                        permanently_failed_this_run += 1
                        # --- CLEANUP STAGED FILE FOR FAILED_PERMANENTLY ---
                        if exc_log_entry.failed_filepath:
                            try:
                                staged_file_to_delete_perm = Path(exc_log_entry.failed_filepath)
                                if staged_file_to_delete_perm.is_file():
                                    staged_file_to_delete_perm.unlink()
                                    exc_log.info("Cleaned up staged file for permanently failed exception.", path=str(staged_file_to_delete_perm))
                                    # exc_log_entry.failed_filepath = None # Optionally nullify
                            except Exception as del_err_perm:
                                exc_log.warning("Failed to delete staged file for permanently failed exception.", path=exc_log_entry.failed_filepath, error=str(del_err_perm))
                                current_notes_after_perm_fail_del = exc_log_entry.resolution_notes if exc_log_entry.resolution_notes is not None else ""
                                exc_log_entry.resolution_notes = f"{current_notes_after_perm_fail_del}\nWARNING: Failed to delete staged file {exc_log_entry.failed_filepath} after marking FAILED_PERMANENTLY.".strip()
                        # --- END CLEANUP ---
                    else: # Reschedule for another retry
                        exc_log.info(f"Retry attempt {exc_log_entry.retry_count} failed. Scheduling next attempt.")
                        exc_log_entry.status = ExceptionStatus.RETRY_PENDING
                        effective_retry_num_for_backoff = max(1, exc_log_entry.retry_count)
                        backoff_delay_seconds = RETRY_BACKOFF_BASE_SECONDS * (2 ** (effective_retry_num_for_backoff - 1))
                        backoff_delay_seconds = min(backoff_delay_seconds, EXCEPTION_RETRY_MAX_BACKOFF_SECONDS)
                        exc_log_entry.next_retry_attempt_at = datetime.now(timezone.utc) + timedelta(seconds=backoff_delay_seconds)
                        exc_log.info(f"Next retry scheduled at {exc_log_entry.next_retry_attempt_at} (in {backoff_delay_seconds}s).")
                        rescheduled_this_run +=1
                        if new_exception_during_retry:
                             exc_log_entry.resolution_notes = (f"{current_resolution_notes}\nRetry attempt {exc_log_entry.retry_count} by task {task_id} resulted in error: {str(new_exception_during_retry)[:500]}.".strip())

                    if new_exception_during_retry:
                        exc_log_entry.error_message = (f"[RETRY FAILED {exc_log_entry.retry_count}x] "
                                                       f"{str(new_exception_during_retry)[:1000]}. "
                                                       f"Original: {exc_log_entry.error_message[:1000]}")[:2000]
                        new_traceback = traceback.format_exception(type(new_exception_during_retry), new_exception_during_retry, new_exception_during_retry.__traceback__)
                        exc_log_entry.error_details = f"--- Retry Attempt {exc_log_entry.retry_count} Error ---\n{''.join(new_traceback)}\n\n--- Original Error Details ---\n{exc_log_entry.error_details or ''}"[:65530]

                db.add(exc_log_entry)
                db.commit()
                exc_log.debug("Successfully updated exception log entry post-retry attempt.")
            except Exception as db_final_update_err:
                exc_log.critical("CRITICAL: Failed to update exception log entry after retry attempt!", error=str(db_final_update_err), exc_info=True)
                if db.is_active: db.rollback()
                skipped_this_run +=1

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
            "skipped_count": skipped_this_run
        }
    except Exception as task_level_exc:
        log.error("Unhandled critical exception in retry_pending_exceptions_task itself.", error=str(task_level_exc), exc_info=True)
        if db and db.is_active: db.rollback()
        raise
    finally:
        if db and db.is_active: db.close()
        log.info("Task finished: retry_pending_exceptions_task.")

def _move_to_error_dir(filepath: Path, task_id: str, context_log: Any):
    if not isinstance(filepath, Path):
         filepath = Path(filepath)
    if not filepath.is_file():
         context_log.warning("Source path for error move is not a file or does not exist.", target_filepath=str(filepath))
         return
    error_dir = None
    try:
        error_base_dir_str = getattr(settings, 'DICOM_ERROR_PATH', None)
        if error_base_dir_str: error_dir = Path(error_base_dir_str)
        else:
             storage_path_str = getattr(settings, 'DICOM_STORAGE_PATH', '/dicom_data/incoming')
             error_dir = Path(storage_path_str).parent / "errors"
             context_log.warning("DICOM_ERROR_PATH not set, using fallback error dir.", fallback_error_dir=str(error_dir))
        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        context_log.info("Moved file to error directory.", error_path=str(error_path))
    except Exception as move_err:
        fallback_logger = logging.getLogger(__name__) # Standard logger as last resort
        current_logger = getattr(context_log, 'critical', fallback_logger.critical)
        current_logger("CRITICAL - Could not move file to error dir. "
                       f"Task ID: {task_id}, Original Filepath: {str(filepath)}, "
                       f"Target Error Dir: {str(error_dir) if 'error_dir' in locals() else 'UnknownErrorDir'}, "
                       f"Error: {str(move_err)}", exc_info=True)

def _handle_final_file_disposition(
    original_filepath: Path, was_successful: bool, rules_matched_and_triggered_actions: bool,
    modifications_made: bool, any_destination_failures: bool, context_log: Any, task_id: str
):
    if not original_filepath or not original_filepath.exists(): return
    if was_successful and not any_destination_failures:
        if settings.DELETE_ON_SUCCESS:
            try: original_filepath.unlink(missing_ok=True); context_log.info("Deleted original file on success.")
            except OSError as e: context_log.warning("Failed to delete original file on success.", error=str(e))
        else: context_log.info("Kept original file on success.")
    elif not rules_matched_and_triggered_actions:
        if settings.DELETE_UNMATCHED_FILES:
            try: original_filepath.unlink(missing_ok=True); context_log.info("Deleted original file as no rules matched/triggered.")
            except OSError as e: context_log.warning("Failed to delete unmatched file.", error=str(e))
        else: context_log.info("Kept original file as no rules matched/triggered.")
    elif any_destination_failures:
        if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
            context_log.warning("Partial destination failure. Moving original file to error directory.")
            _move_to_error_dir(original_filepath, task_id, context_log)
        elif settings.DELETE_ON_PARTIAL_FAILURE_IF_MODIFIED and modifications_made:
             try: original_filepath.unlink(missing_ok=True); context_log.info("Deleted modified original file on partial failure.")
             except OSError as e: context_log.warning("Failed to delete modified file on partial failure.", error=str(e))
        else: context_log.info("Kept original file despite partial destination failure.")
    elif rules_matched_and_triggered_actions and not any_destination_failures and modifications_made and settings.DELETE_ON_NO_DESTINATION:
        context_log.info("Rules matched, mods made, no destinations. Deleting original (DELETE_ON_NO_DESTINATION=true).")
        try: original_filepath.unlink(missing_ok=True)
        except OSError as e: context_log.warning("Failed to delete file with no destinations after modification.", error=str(e))
    else: context_log.info("Kept original file based on disposition settings.")

# --- Celery Tasks ---

@shared_task(bind=True, name="process_dicom_file_task", acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES, default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(
    self, dicom_filepath_str: str, source_type: str,
    source_db_id_or_instance_id: Union[int, str], association_info: Optional[Dict[str, str]] = None
):
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    log_source_display_name = f"{source_type}_{source_db_id_or_instance_id}"
    log = logger.bind(task_id=task_id, task_name=self.name, original_filepath=str(original_filepath),
                      source_type=source_type, source_id_or_instance=str(source_db_id_or_instance_id),
                      log_source_display_name=log_source_display_name)
    log.info("Task started: process_dicom_file_task")

    if not original_filepath.is_file():
        log.error("File not found. Task cannot proceed.")
        return {"status": "error_file_not_found", "message": "File not found", "filepath": dicom_filepath_str,
                "instance_uid": "Unknown", "source_display_name": log_source_display_name,
                "applied_rules": [], "destinations_processed": {}}

    db: Optional[Session] = None
    final_status_code = "task_init_failed"
    final_message = "Task initialization or pre-execution failed."
    applied_rules_info_res, dest_statuses_res = [], {}
    instance_uid_res = "UnknownFromFileTaskInit"
    rules_matched_res, modifications_made_res = False, False
    processed_ds_res = None 

    try:
        db = SessionLocal()
        
        # No AI portal management needed here with sync wrapper approach

        rules_matched_res, modifications_made_res, final_status_code, final_message, \
        applied_rules_info_res, dest_statuses_res, processed_ds_res, \
        instance_uid_res, _ = execute_file_based_task(
            log, db, dicom_filepath_str, source_type,
            source_db_id_or_instance_id, task_id, association_info,
            ai_portal=None # Pass None explicitly
        )

        if final_status_code.startswith("success"):
            try:
                source_type_enum_for_inc = ProcessedStudySourceType(source_type)
                should_commit = False
                if source_type_enum_for_inc == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(source_db_id_or_instance_id, str):
                    if crud.crud_dimse_listener_state.increment_processed_count(db=db, listener_id=source_db_id_or_instance_id, count=1):
                        should_commit = True
                elif source_type_enum_for_inc == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
                     if crud.crud_dimse_qr_source.increment_processed_count(db=db, source_id=source_db_id_or_instance_id, count=1):
                        should_commit = True
                if should_commit:
                    db.commit()
                    log.info("Successfully incremented and committed processed count.", source_type=source_type)
            except Exception as db_inc_err:
                log.error("DB Error during processed count increment or commit.", error=str(db_inc_err), exc_info=True)
                if db and db.is_active: log.warning("Rolling back DB session."); db.rollback()

        if db and db.is_active: db.close(); db = None
        return {"status": final_status_code, "message": final_message, "applied_rules": applied_rules_info_res,
                "destinations_processed": dest_statuses_res, "source_display_name": log_source_display_name,
                "instance_uid": instance_uid_res, "filepath": dicom_filepath_str}

    except Exception as exc:
        log.error("Unhandled exception in task process_dicom_file_task.", error=str(exc), exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except Exception: pass # Ignore rollback error
            finally: db.close(); db = None
        if any(isinstance(exc, retry_exc_type) for retry_exc_type in RETRYABLE_EXCEPTIONS):
            log.info("Caught retryable exception at task level. Retrying.", exc_type=type(exc).__name__)
            raise
        final_status_code = "fatal_task_error_unhandled"
        final_message = f"Fatal unhandled error in task: {exc!r}"
        if original_filepath.exists(): _move_to_error_dir(original_filepath, task_id, log)
        return {"status": final_status_code, "message": final_message, "filepath": dicom_filepath_str,
                "source_display_name": log_source_display_name, "instance_uid": instance_uid_res,
                "applied_rules": applied_rules_info_res, "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active: log.warning("DB session still active in finally block, closing."); db.close()
        if original_filepath.exists():
            any_dest_failed_final = any(d.get("status") == "error" for d in dest_statuses_res.values())
            status_for_disposition = final_status_code if 'final_status_code' in locals() else 'unknown_error_state'
            _handle_final_file_disposition(
                 original_filepath=original_filepath, was_successful=(status_for_disposition.startswith("success") and not any_dest_failed_final),
                 rules_matched_and_triggered_actions=rules_matched_res, modifications_made=modifications_made_res,
                 any_destination_failures=any_dest_failed_final, context_log=log, task_id=task_id)
        log.info("Task finished: process_dicom_file_task.", final_task_status=final_status_code)


@shared_task(bind=True, name="process_dicomweb_metadata_task", acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES, default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
async def process_dicomweb_metadata_task(self, source_id: int, study_uid: str, series_uid: str, instance_uid: str):
    task_id = self.request.id
    log = logger.bind(task_id=task_id, task_name=self.name, source_id=source_id, instance_uid=instance_uid,
                      source_type=ProcessedStudySourceType.DICOMWEB.value) # Changed DICOMWEB_POLLER to DICOMWEB
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
            ai_portal=None # Pass None
        )
        if source_name_res_from_exec: source_name_res = source_name_res_from_exec

        await asyncio.to_thread(db.close); db = None
        return {"status": final_status_code, "message": final_message, "applied_rules": applied_rules_res,
                "destinations_processed": dest_statuses_res, "source_name": source_name_res, "instance_uid": instance_uid}
    except Exception as exc:
        log.error("Unhandled exception in task process_dicomweb_metadata_task.", error=str(exc), exc_info=True)
        if db and db.is_active:
            try: await asyncio.to_thread(db.rollback)
            except Exception: pass
            finally: await asyncio.to_thread(db.close); db = None
        if any(isinstance(exc, retry_exc_type) for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled"
        final_message = f"Fatal error: {exc!r}"
        return {"status": final_status_code, "message": final_message, "instance_uid": instance_uid, "source_id": source_id,
                "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active: await asyncio.to_thread(db.close)
        current_final_status = final_status_code if 'final_status_code' in locals() else 'task_ended_in_finally_early_error_dw'
        log.info("Task finished: process_dicomweb_metadata_task.", final_task_status=current_final_status)


@shared_task(bind=True, name="process_stow_instance_task", acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES, default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_stow_instance_task(self, temp_filepath_str: str, source_ip: Optional[str] = None):
    task_id = self.request.id
    temp_filepath = Path(temp_filepath_str)
    source_identifier_log_display = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}"
    log = logger.bind(task_id=task_id, task_name=self.name, temp_filepath_stow=str(temp_filepath),
                      source_ip_stow=source_ip, log_source_display_name=source_identifier_log_display,
                      source_type=ProcessedStudySourceType.STOW_RS.value)
    log.info("Task started: process_stow_instance_task")

    if not temp_filepath.is_file():
        log.error("Temporary STOW file not found. Task cannot proceed.", stow_temp_file=str(temp_filepath))
        return {"status": "error_temp_file_not_found", "message": "Temporary STOW file not found",
                "filepath": temp_filepath_str, "instance_uid": "Unknown",
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

        db.close(); db = None
        return {"status": final_status_code, "message": final_message, "applied_rules": applied_rules_res,
                "destinations_processed": dest_statuses_res, "source_display_name": source_identifier_log_display,
                "instance_uid": instance_uid_res, "original_stow_filepath": temp_filepath_str}

    except Exception as exc:
        log.error("Unhandled exception in task process_stow_instance_task.", error=str(exc), exc_info=True)
        if db and db.is_active:
             try: db.rollback()
             except Exception: pass
             finally: db.close(); db = None
        if any(isinstance(exc, retry_exc_type) for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled_stow"
        final_message = f"Fatal unhandled error in STOW task: {exc!r}"
        return {"status": final_status_code, "message": final_message, "filepath": temp_filepath_str,
                "source_display_name": source_identifier_log_display, "instance_uid": instance_uid_res,
                "applied_rules": applied_rules_res, "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active: db.close()
        if temp_filepath.exists():
            try: temp_filepath.unlink(missing_ok=True)
            except OSError as e: log.critical("FAILED TO DELETE temporary STOW file in finally.", stow_temp_file_final_error=str(temp_filepath), error_detail=str(e))
        current_final_status = final_status_code if 'final_status_code' in locals() else 'task_ended_in_finally_early_error_stow'
        log.info("Task finished: process_stow_instance_task.", final_task_status=current_final_status)


@shared_task(bind=True, name="process_google_healthcare_metadata_task", acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES, default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
async def process_google_healthcare_metadata_task(self, source_id: int, study_uid: str):
    task_id = self.request.id
    log = logger.bind(task_id=task_id, task_name=self.name, ghc_source_id=source_id, ghc_study_uid=study_uid,
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
        if source_name_res_from_exec: source_name_res = source_name_res_from_exec

        await asyncio.to_thread(db.close); db = None
        return {"status": final_status_code, "message": final_message, "applied_rules": applied_rules_res,
                "destinations_processed": dest_statuses_res, "source_name": source_name_res,
                "study_uid": study_uid, "instance_uid": instance_uid_res}
    except Exception as exc:
        log.error("Unhandled exception in task process_google_healthcare_metadata_task.", error=str(exc), exc_info=True)
        if db and db.is_active:
            try: await asyncio.to_thread(db.rollback)
            except Exception: pass
            finally: await asyncio.to_thread(db.close); db = None
        if any(isinstance(exc, retry_exc_type) for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled_ghc"
        final_message = f"Fatal unhandled error in GHC task: {exc!r}"
        return {"status": final_status_code, "message": final_message, "study_uid": study_uid, "source_id": source_id,
                "instance_uid": instance_uid_res, "applied_rules": applied_rules_res, "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active: await asyncio.to_thread(db.close)
        current_final_status = final_status_code if 'final_status_code' in locals() else 'task_ended_in_finally_early_error_ghc'
        log.info("Task finished: process_google_healthcare_metadata_task.", final_task_status=current_final_status)

STALE_DATA_CLEANUP_AGE_DAYS = settings.STALE_DATA_CLEANUP_AGE_DAYS # Default to 30 days, add to Settings
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
        statuses_for_file_cleanup = [ExceptionStatus.RESOLVED_MANUALLY, ExceptionStatus.ARCHIVED]
        entries_for_cleanup: List[DicomExceptionLog] = db.query(DicomExceptionLog).filter(
            DicomExceptionLog.status.in_(statuses_for_file_cleanup),
            DicomExceptionLog.failed_filepath.isnot(None) # Only those with a filepath
        ).limit(settings.CLEANUP_BATCH_SIZE).all() # type: ignore # Add CLEANUP_BATCH_SIZE to Settings

        if entries_for_cleanup:
            log.info(f"Found {len(entries_for_cleanup)} manually resolved/archived exceptions with staged files to clean up.")
            for entry in entries_for_cleanup:
                entry_log: StdlibBoundLogger = log.bind(exception_log_id=entry.id, exception_uuid=str(entry.exception_uuid), staged_file=entry.failed_filepath) # type: ignore
                try:
                    staged_file = Path(entry.failed_filepath) # type: ignore
                    if staged_file.is_file():
                        staged_file.unlink()
                        entry_log.info("Successfully deleted staged file for resolved/archived entry.")
                        entry.failed_filepath = None # Nullify path after deletion
                        entry.resolution_notes = (f"{entry.resolution_notes or ''}\nStaged file {staged_file} auto-cleaned by cleanup task.").strip()
                        db.add(entry)
                        cleaned_files_count += 1
                    else:
                        entry_log.warning("Staged file not found for resolved/archived entry, nullifying path.")
                        entry.failed_filepath = None # Nullify path even if not found
                        db.add(entry)
                except Exception as e:
                    entry_log.error("Error deleting staged file for resolved/archived entry.", error=str(e))
            db.commit() # Commit changes (nullified paths)

        # --- 2. Revert "stuck" RETRY_IN_PROGRESS entries back to RETRY_PENDING ---
        # Entries stuck for too long (e.g., worker died)
        stuck_threshold = datetime.now(timezone.utc) - timedelta(hours=STALE_RETRY_IN_PROGRESS_AGE_HOURS)
        stuck_entries: List[DicomExceptionLog] = db.query(DicomExceptionLog).filter(
            DicomExceptionLog.status == ExceptionStatus.RETRY_IN_PROGRESS,
            DicomExceptionLog.last_retry_attempt_at < stuck_threshold
        ).limit(settings.get("CLEANUP_BATCH_SIZE", 100)).all() # type: ignore

        if stuck_entries:
            log.info(f"Found {len(stuck_entries)} entries stuck in RETRY_IN_PROGRESS. Reverting to RETRY_PENDING.")
            for entry in stuck_entries:
                entry.status = ExceptionStatus.RETRY_PENDING
                entry.resolution_notes = (f"{entry.resolution_notes or ''}\nAutomatically reverted from stuck RETRY_IN_PROGRESS by cleanup task at {datetime.now(timezone.utc)}.").strip()
                # Optionally, adjust next_retry_attempt_at if desired, e.g., retry soon
                # entry.next_retry_attempt_at = datetime.now(timezone.utc) + timedelta(minutes=5)
                db.add(entry)
                reverted_stuck_retries_count +=1
            db.commit()

        # --- 3. (Optional) Advanced: Sweep DICOM_RETRY_STAGING_PATH for truly orphaned files ---
        # This is more complex: list files in dir, check against DB.
        # For now, focusing on DB-driven cleanup.
        log.info("Orphaned file sweep in staging path not yet implemented in this task version.")


        log.info("Cleanup task finished.", cleaned_files=cleaned_files_count, reverted_stuck_retries=reverted_stuck_retries_count)
        return {"status": "success", "cleaned_files": cleaned_files_count, "reverted_stuck_retries": reverted_stuck_retries_count}

    except Exception as e:
        log.error("Error during cleanup_stale_exception_data_task.", error=str(e), exc_info=True)
        if db and db.is_active:
            db.rollback()
        raise # Let Celery handle task failure
    finally:
        if db and db.is_active:
            db.close()