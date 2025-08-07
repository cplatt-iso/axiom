# app/worker/celery_app.py
from datetime import timedelta
import logging
import logging.config  # Use the config submodule
import sys
import traceback
import structlog
from celery import Celery, signals
from celery.schedules import crontab
from app.core.config import settings

# ---------------------------------------------------------------------------
# --- THE NEW, SIMPLER, AND MORE BRUTAL LOGGING SETUP ---
# ---------------------------------------------------------------------------

# This is the one signal to rule them all. It runs when the worker process
# initializes, BEFORE Celery can set up its own shitty loggers. This is our
# chance to take control of the entire process's logging config.
@signals.setup_logging.connect
def setup_celery_logging(loglevel=settings.LOG_LEVEL, **kwargs):
    """
    This function is connected to the 'setup_logging' signal from Celery.
    It completely replaces Celery's default logging configuration with our
    own structlog-based JSON configuration. This ensures that ALL logs
    (from Celery, kombu, our tasks, etc.) are emitted as structured JSON.
    """
    # A standard Python logging configuration dictionary.
    # This is the modern, correct way to configure logging.
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,  # Let us override existing loggers
        "formatters": {
            # The structlog formatter that will handle rendering to JSON.
            "json_formatter": {
                "()": "structlog.stdlib.ProcessorFormatter",
                "processor": structlog.processors.JSONRenderer(),
                # These processors will be applied to logs from non-structlog libraries (like celery itself)
                "foreign_pre_chain": [
                    structlog.stdlib.add_logger_name,
                    structlog.stdlib.add_log_level,
                    structlog.processors.TimeStamper(fmt="iso"),
                ],
            },
        },
        "handlers": {
            # All logs will go to the console (stdout) through this handler.
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "json_formatter",
            },
        },
        "loggers": {
            # The root logger: all logs start here.
            # We configure it to use our console handler.
            "": {
                "handlers": ["console"],
                "level": loglevel.upper(),
                "propagate": True,
            },
            # We can quiet down noisy libraries here.
            "celery": {
                "level": "INFO",
                "propagate": False, # Do not pass celery logs to the root logger, handle them here.
                "handlers": ["console"],
            },
            "celery.app.trace": {
                # THIS IS THE KEY: We are grabbing Celery's annoying task trace logger...
                "level": "INFO",
                "propagate": False,
                # ...AND SENDING ITS OUTPUT TO DEV/NULL by giving it no handlers.
                # Your custom 'task_postrun' signal handler is the correct way
                # to log task outcomes, so we silence the default one completely.
                "handlers": [],
            },
            "kombu": {"level": "WARNING", "handlers": ["console"], "propagate": False},
            "amqp": {"level": "WARNING", "handlers": ["console"], "propagate": False},
        },
    }

    # Apply the configuration.
    logging.config.dictConfig(logging_config)

    # Now, configure structlog itself to process logs before they hit the handler.
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            # This must be the last processor in the chain.
            structlog.stdlib.render_to_log_kwargs,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    structlog.get_logger(__name__).info("Structlog JSON logging configured for Celery Worker.", log_level=loglevel)


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