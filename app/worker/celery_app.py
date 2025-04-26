# app/worker/celery_app.py
from celery import Celery
from celery.schedules import crontab # Import crontab or timedelta
from app.core.config import settings

# Initialize Celery
# The first argument is the name of the current module ('app.worker.celery_app')
# The 'broker' argument points to our RabbitMQ instance via the settings
# The 'backend' is optional, used if you need to store/retrieve task results
# The 'include' argument tells Celery which modules contain tasks to auto-discover
app = Celery(
    "dicom_processor_tasks",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND, # Can be None if results aren't stored
    include=[
        'app.worker.tasks', # Existing tasks
        'app.worker.dicomweb_poller', # Existing DICOMweb poller
        'app.worker.dimse_qr_poller', # DIMSE Q/R C-FIND poller
        'app.worker.dimse_qr_retriever', # DIMSE Q/R C-MOVE retriever
        'app.crosswalk.tasks',
        ]
)

# Load configuration from the settings object
# Celery configuration keys are usually uppercase
app.conf.update(
    task_serializer='json', # Use json for task serialization
    accept_content=['json'],  # Allow json content
    result_serializer='json', # Use json for results serialization
    timezone='UTC', # Use UTC timezone
    enable_utc=True, # Enable UTC
    worker_concurrency=4, # Default concurrency (can be overridden by CLI) - Set reasonable default
    worker_prefetch_multiplier=1, # Helps ensure tasks are distributed evenly when using ACK late
    task_acks_late=True, # Acknowledge task AFTER it completes (important for reliability)
    # Optional: Setup default queue if not specified elsewhere
    task_default_queue = settings.CELERY_TASK_DEFAULT_QUEUE,
    # Optional: Define queues explicitly (more advanced setup)
    # task_queues=(
    #     Queue('default', routing_key='task.#'),
    #     Queue('dicom_processing', routing_key='dicom.#'),
    # ),
    # task_default_exchange = 'tasks',
    # task_default_routing_key = 'task.default',
)

# --- Updated Beat Schedule ---
app.conf.beat_schedule = {
    'poll-all-dicomweb-sources-every-minute': { # Existing DICOMweb poller schedule
        'task': 'poll_all_dicomweb_sources', # Name of the task in dicomweb_poller.py
        'schedule': 60.0,  # Run every 60 seconds
        # 'args': (), # Add arguments if the task takes any
    },
    'poll-all-dimse-qr-sources-every-five-minutes': { # DIMSE Q/R Poller schedule
        'task': 'poll_all_dimse_qr_sources', # Name of the task in dimse_qr_poller.py
        'schedule': 300.0, # Run every 300 seconds (5 minutes)
        # 'args': (), # Add arguments if the task takes any
    },
    'sync-all-crosswalk-sources-every-hour': {
    'task': 'sync_all_enabled_crosswalk_sources', # Name of the beat task in crosswalk.tasks.py
    'schedule': 3600.0,  # Run every hour by default. Can be overridden by source config later.
    }, 
    # Optional: Keep commented out cleanup task example if needed
    # 'cleanup-old-files-every-day': {
    #     'task': 'app.worker.tasks.cleanup_task',
    #     'schedule': crontab(hour=0, minute=0), # Runs daily at midnight
    # },
}
# --- End Updated Beat Schedule ---


if __name__ == '__main__':
    # Allows running the worker directly using: python -m app.worker.celery_app worker --loglevel=info
    app.start()
