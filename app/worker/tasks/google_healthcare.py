# app/worker/tasks/google_healthcare.py
import asyncio
from typing import Optional

import structlog
from celery import shared_task
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.schemas.enums import ProcessedStudySourceType
from app.core.config import settings
from app.worker.executors import execute_ghc_task

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
