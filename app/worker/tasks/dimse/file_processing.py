# app/worker/tasks/dimse/file_processing.py
from pathlib import Path
from typing import Optional, Dict, Any, Union

import structlog
from celery import shared_task
from sqlalchemy.orm import Session

from app import crud
from app.db.session import SessionLocal
from app.schemas.enums import ProcessedStudySourceType
from app.core.config import settings
from app.worker.executors import execute_file_based_task
from app.worker.tasks.utils import _handle_final_file_disposition_with_dustbin, _move_to_error_dir

# Logger setup
logger = structlog.get_logger(__name__)

# Retryable exceptions
RETRYABLE_EXCEPTIONS = (
    ConnectionRefusedError,
    TimeoutError,
    # Add other exceptions here that should cause Celery to retry the *entire task*
    # e.g., certain DB deadlocks or temporary RabbitMQ issues, if not handled by Celery's broker config.
    # For now, keep it simple as you had it.
)


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
