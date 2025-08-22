# app/worker/tasks/dicomweb/qido.py
import asyncio
from typing import Optional

import structlog
from celery import shared_task
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.schemas.enums import ProcessedStudySourceType
from app.core.config import settings
from app.worker.executors import execute_dicomweb_task

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
