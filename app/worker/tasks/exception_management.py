# app/worker/tasks/exception_management.py
from datetime import datetime, timezone, timedelta
import traceback
from pathlib import Path
from typing import Optional, List, Dict, Any
import uuid

import structlog
from structlog.stdlib import BoundLogger
from celery import shared_task
from celery.exceptions import Retry
from sqlalchemy.orm import Session
import pydicom
from redis.exceptions import ConnectionError as RedisConnectionError

from app import crud
from app.db.session import SessionLocal
from app.core.config import settings
from app.db.models.dicom_exception_log import DicomExceptionLog
from app.schemas.enums import ExceptionStatus, ExceptionProcessingStage
from app.services.storage_backends import get_storage_backend
from app.worker.task_utils import build_storage_backend_config_dict, _safe_get_dicom_value
from app.worker.error_handlers import RedisConnectionHandler

# Logger setup
logger = structlog.get_logger(__name__)

# Configuration constants
MAX_EXCEPTION_RETRIES = settings.EXCEPTION_MAX_RETRIES
RETRY_BACKOFF_BASE_SECONDS = settings.EXCEPTION_RETRY_DELAY_SECONDS
EXCEPTION_RETRY_BATCH_SIZE = settings.EXCEPTION_RETRY_BATCH_SIZE
EXCEPTION_RETRY_MAX_BACKOFF_SECONDS = settings.EXCEPTION_RETRY_MAX_BACKOFF_SECONDS
STALE_RETRY_IN_PROGRESS_AGE_HOURS = 6  # Consider entries stuck after 6 hours


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
    except RedisConnectionError as redis_exc:
        log.error(
            "Redis connection error in retry_pending_exceptions_task - Redis service may be down",
            error=str(redis_exc),
            error_type="RedisConnectionError",
            exc_info=True
        )
        if db and db.is_active:
            db.rollback()
        # Retry the task later when Redis is available
        raise self.retry(countdown=300, max_retries=3, exc=redis_exc)
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
