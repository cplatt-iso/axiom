# app/worker/celery_app.py
from celery import Celery
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
        'app.worker.tasks' # List modules containing your tasks here
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


# Optional: Setup periodic tasks (Celery Beat) if needed later
# app.conf.beat_schedule = {
#     'cleanup-old-files-every-day': {
#         'task': 'app.worker.tasks.cleanup_task',
#         'schedule': crontab(hour=0, minute=0), # Runs daily at midnight
#     },
# }


if __name__ == '__main__':
    # Allows running the worker directly using: python -m app.worker.celery_app worker --loglevel=info
    app.start()
