# app/worker/celery_app.py
from datetime import timedelta
import logging
import sys
import traceback 
import structlog 
from celery import Celery, signals 
from celery.schedules import crontab
from celery.signals import after_setup_logger, after_setup_task_logger
from app.core.config import settings

# --- Configure Logging ---
# Determine log level from settings or default
log_level_str = getattr(settings, 'LOG_LEVEL', "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

# Flag to prevent configuring structlog multiple times in the same process
_STRUCTLOG_CONFIGURED = True

class IgnoreCeleryAppTraceFilter(logging.Filter):
    def filter(self, record):
        # Return False to drop the log record, True to keep it
        return not record.name == 'celery.app.trace'

def configure_structlog(logger, loglevel):
    """Configures structlog processors and handlers for a given logger."""
    global _STRUCTLOG_CONFIGURED
    if _STRUCTLOG_CONFIGURED:
        # logger.debug("Structlog already configured for this process.")
        return logger

    # Define processors (keep these as they were)
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.format_exc_info,
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        # Render as JSON
        processor=structlog.processors.JSONRenderer(),
        # Processors for foreign logs (from stdlib logging)
        foreign_pre_chain=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            # Maybe add format_exc_info here too if needed for foreign logs
        ],
    )

    # Configure the handler to output to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.addFilter(IgnoreCeleryAppTraceFilter())

    # Clear existing handlers and add our configured one
    # Target the specific logger passed by Celery signals
    if logger is not None:
        # logger.debug(f"Configuring structlog for logger: {logger.name}")
        for h in logger.handlers[:]: # Iterate over a copy
             logger.removeHandler(h)
        logger.addHandler(handler)
        logger.setLevel(loglevel)
        # Prevent propagation if we are configuring a specific child logger
        # to avoid duplicate logs if root logger also gets configured (shouldn't happen with hijack=False)
        # logger.propagate = False # Careful with this, might stop logs going where expected
    else:
        # Fallback: Configure root logger (less ideal, avoid if possible)
        root_logger = logging.getLogger()
        for h in root_logger.handlers[:]:
             root_logger.removeHandler(h)
        root_logger.addHandler(handler)
        root_logger.setLevel(loglevel)

    _STRUCTLOG_CONFIGURED = True
    # Use the name passed by the signal, or __name__ if called directly (unlikely now)
    log_name = logger.name if logger else __name__
    structlog.get_logger(log_name).info("Structlog JSON logging configured", log_level=log_level_str)

    # Return the structured logger instance for potential use
    return structlog.wrap_logger(logger if logger else logging.getLogger())


# --- Celery Signals for Logging Setup ---
@after_setup_logger.connect
def setup_celery_logger(logger, **kwargs):
    """Configure Celery's main logger."""
    configure_structlog(logger, log_level)

@after_setup_task_logger.connect
def setup_celery_task_logger(logger, **kwargs):
    """Configure Celery's task logger."""
    configure_structlog(logger, log_level)


@signals.task_postrun.connect
def log_task_outcome(sender=None, task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, einfo=None, **extra_kwargs):
    """
    Logs a structured event summarizing task completion or failure.
    This replaces the default celery.app.trace log for task results.
    """
    log = structlog.get_logger("celery.task_outcome") # Use a dedicated logger name
    task_name = getattr(task, 'name', 'unknown_task')

    common_info = {
        "task_id": task_id,
        "task_name": task_name,
        "task_args": args,
        "task_kwargs": kwargs,
        "status": state,
        # Consider adding task execution time if easily available or important
        # "runtime_seconds": ...
    }

    if state == 'SUCCESS':
        log.info(
            "Task succeeded",
            **common_info,
            result=retval # IMPORTANT: Keep retval structured
        )
    elif state == 'FAILURE':
        exc_type = getattr(einfo.type, '__name__', str(einfo.type)) if einfo else None
        exc_message = str(einfo.exception) if einfo else None
        # Format traceback nicely if present
        exc_traceback_str = ''.join(traceback.format_exception(einfo.type, einfo.exception, einfo.tb)) if einfo else None

        log.error(
            "Task failed",
            **common_info,
            exception_type=exc_type,
            exception_message=exc_message,
            # Be careful logging full tracebacks, can be very large.
            # Consider logging only if log level is DEBUG, or truncating.
            traceback=exc_traceback_str
        )
    else:
        # Handle other states if necessary (e.g., RETRY, REVOKED)
        log.info(
            "Task finished with non-standard state",
            **common_info,
            result=retval # Include retval even for non-success/failure states
        )

# --- Initialize Celery App ---
try:
    from app.services import ai_assist_service
    structlog.get_logger("celery.startup").info("Explicitly imported ai_assist_service for early init.")
    # You could add checks here too, if needed:
    # structlog.get_logger("celery.startup").info(f"Gemini model after import: {ai_assist_service.gemini_model}")
except ImportError as ai_import_err:
     structlog.get_logger("celery.startup").error("Failed to explicitly import ai_assist_service during worker startup.", error=str(ai_import_err), exc_info=True)
except Exception as ai_init_err:
     # Catch potential errors during the module's top-level execution
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
    worker_concurrency=settings.CELERY_WORKER_CONCURRENCY,  # Use setting
    worker_prefetch_multiplier=settings.CELERY_PREFETCH_MULTIPLIER,  # Use setting
    task_acks_late=settings.CELERY_ACKS_LATE,  # Use setting
    task_default_queue=settings.CELERY_TASK_DEFAULT_QUEUE,
    # Important: Tell Celery not to mess with our root logger configuration
    worker_hijack_root_logger=False,  # Keep this!
)

# --- Beat Schedule ---
app.conf.beat_schedule = {
    'poll-all-dicomweb-sources-every-minute': {
        'task': 'poll_all_dicomweb_sources',
        'schedule': 60.0, # Consider making interval configurable via settings
    },
    'poll-all-dimse-qr-sources-every-minute': {
        'task': 'poll_all_dimse_qr_sources',
        'schedule': 60.0, # Consider making interval configurable via settings
    },
    'sync-all-crosswalk-sources-every-hour': {
        'task': 'sync_all_enabled_crosswalk_sources',
        'schedule': 3600.0, # Consider making interval configurable via settings
    },
    'poll-all-google-healthcare-sources-every-five-minutes': { # Adjust name/interval as needed
        'task': 'poll_all_google_healthcare_sources', # Name of the SCHEDULER task (we need to create this)
        'schedule': 300.0, # Example: Run every 5 minutes
    },
    "retry-exceptions": {
        "task": "retry_pending_exceptions_task", # CHANGED: Use the explicit name from the decorator
        "schedule": timedelta(minutes=settings.EXCEPTION_RETRY_INTERVAL_MINUTES), # e.g., every 5 mins
    },
        "cleanup-stale-exception-data": {
        "task": "app.worker.tasks.cleanup_stale_exception_data_task",
        "schedule": timedelta(hours=settings.CLEANUP_STALE_DATA_INTERVAL_HOURS),
    },
    # 'cleanup-old-files-every-day': {
    #     'task': 'app.worker.tasks.cleanup_task',
    #     'schedule': crontab(hour=0, minute=0),
    # },
}

# --- Main execution block ---
if __name__ == '__main__':
    app.start()
