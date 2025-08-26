# app/worker/celery_app.py
from datetime import timedelta
import logging
import sys
import traceback
import structlog
from celery import Celery, signals
from celery.schedules import crontab
from app.core.config import settings
from app.core.logging_config import configure_celery_logging

# ---------------------------------------------------------------------------
# --- FORCE LOGGING SETUP ON IMPORT ---
# ---------------------------------------------------------------------------

# Force configure logging when this module is imported
# This ensures it happens regardless of signal timing
try:
    configure_celery_logging()
    # Also configure general JSON logging for beat scheduler and other logs
    from app.core.logging_config import configure_json_logging
    configure_json_logging("celery")
    logger = structlog.get_logger(__name__)
    logger.info("Celery JSON logging configured on import", log_level=str(settings.LOG_LEVEL))
except Exception as e:
    print(f"ERROR: Failed to configure logging on import: {e}")

# Disable Celery's automatic logging configuration to prevent conflicts
@signals.setup_logging.connect
def setup_celery_logging(**kwargs):
    """
    Celery setup_logging signal handler.
    We disable this to prevent conflicts with our logging configuration.
    """
    # Do nothing - our logging is already configured
    pass


# Your custom task outcome logger is excellent. Keep it exactly as it is.
# It's the correct way to get structured data about task results.
@signals.task_postrun.connect
def log_task_outcome(sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, einfo=None, **extra_kwargs):
    """
    Logs a structured event summarizing task completion or failure.
    This replaces the default celery.app.trace log for task results.
    """
    log = structlog.get_logger("celery.task_outcome")
    task_name = getattr(task, 'name', 'unknown_task')
    common_info = { "task_id": task_id, "task_name": task_name, "task_args": args, "task_kwargs": kwargs, "status": state }

    if state == 'SUCCESS':
        log.info("Task succeeded", **common_info, result=retval)
    elif state == 'FAILURE':
        exc_type = getattr(einfo.type, '__name__', str(einfo.type)) if einfo else None
        exc_message = str(einfo.exception) if einfo else None
        exc_traceback_str = ''.join(traceback.format_exception(einfo.type, einfo.exception, einfo.tb)) if einfo else None
        log.error("Task failed", **common_info, exception_type=exc_type, exception_message=exc_message, traceback=exc_traceback_str)
    else:
        log.info("Task finished with non-standard state", **common_info, result=retval)


# --- Initialize Celery App ---
try:
    from app.services import ai_assist_service
    structlog.get_logger("celery.startup").info("Explicitly imported ai_assist_service for early init.")
except ImportError as ai_import_err:
     structlog.get_logger("celery.startup").error("Failed to explicitly import ai_assist_service during worker startup.", error=str(ai_import_err), exc_info=True)
except Exception as ai_init_err:
     structlog.get_logger("celery.startup").error("Error occurred during ai_assist_service initialization at worker startup.", error=str(ai_init_err), exc_info=True)


app = Celery(
    "dicom_processor_tasks",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        'app.worker.tasks',
        'app.worker.dicomweb_poller',
        'app.worker.dimse_qr_poller',
        'app.worker.dimse_qr_retriever',
        'app.crosswalk.tasks',
        'app.worker.google_healthcare_poller',
        ]
)

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    worker_concurrency=settings.CELERY_WORKER_CONCURRENCY,
    worker_prefetch_multiplier=settings.CELERY_PREFETCH_MULTIPLIER,
    task_acks_late=settings.CELERY_ACKS_LATE,
    task_default_queue=settings.CELERY_TASK_DEFAULT_QUEUE,
    worker_hijack_root_logger=False, # Good to keep, but our signal is the real hero.
)

# --- Beat Schedule ---
app.conf.beat_schedule = {
    'poll-all-dicomweb-sources-every-minute': {
        'task': 'poll_all_dicomweb_sources',
        'schedule': 60.0,
    },
    'poll-all-dimse-qr-sources-every-minute': {
        'task': 'poll_all_dimse_qr_sources',
        'schedule': 60.0,
    },
    'sync-all-crosswalk-sources-every-hour': {
        'task': 'sync_all_enabled_crosswalk_sources',
        'schedule': 3600.0,
    },
    'poll-all-google-healthcare-sources-every-five-minutes': {
        'task': 'poll_all_google_healthcare_sources',
        'schedule': 300.0,
    },
    "retry-exceptions": {
        "task": "retry_pending_exceptions_task",
        "schedule": timedelta(minutes=settings.EXCEPTION_RETRY_INTERVAL_MINUTES),
    },
    "cleanup-stale-exception-data": {
        "task": "app.worker.tasks.cleanup_stale_exception_data_task",
        "schedule": timedelta(hours=settings.CLEANUP_STALE_DATA_INTERVAL_HOURS),
    },
}

# --- Main execution block ---
if __name__ == '__main__':
    app.start()