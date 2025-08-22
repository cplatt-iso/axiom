# app/worker/tasks/dicomweb/stow.py
from pathlib import Path
from typing import Optional

import structlog
from celery import shared_task
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.schemas.enums import ProcessedStudySourceType
from app.core.config import settings
from app.worker.executors import execute_stow_task

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
