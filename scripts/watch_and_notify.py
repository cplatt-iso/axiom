import sys
import time
import logging
import os
import json
import uuid
import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPConnectionError
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
from typing import Optional

# Add the project root to the Python path to allow importing from 'app'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app import crud
from app.db.models import DimseListenerConfig

# --- Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "default")
CELERY_TASK_NAME = os.getenv("CELERY_TASK_NAME", "process_dicom_file_task")
WATCH_DIRECTORY = "/dicom_data/incoming"
LISTENER_INSTANCE_ID = os.getenv("AXIOM_INSTANCE_ID", "dcm4che_1")


class DICOMProcessor:
    def __init__(self):
        self.listener_config: Optional[DimseListenerConfig] = None
        self.rabbit_connection: Optional[pika.BlockingConnection] = None
        self.rabbit_channel: Optional[BlockingChannel] = None

    def load_config(self) -> bool:
        """Connects to the DB and fetches the DIMSE listener config."""
        db = None
        try:
            logging.info(f"Connecting to DB to fetch listener config for instance_id: '{LISTENER_INSTANCE_ID}'")
            db = SessionLocal()
            self.listener_config = crud.crud_dimse_listener_config.get_by_instance_id(db, instance_id=LISTENER_INSTANCE_ID)
            if self.listener_config:
                logging.info(f"Successfully loaded config: Name='{self.listener_config.name}', AE Title='{self.listener_config.ae_title}'")
                return True
            else:
                logging.error(f"No DIMSE listener configuration found for instance_id: '{LISTENER_INSTANCE_ID}'")
                return False
        except Exception as e:
            logging.error(f"Database error while fetching listener config: {e}", exc_info=True)
            return False
        finally:
            if db:
                db.close()

    def connect_rabbitmq(self) -> bool:
        """Establishes a persistent connection to RabbitMQ."""
        if self.rabbit_connection and self.rabbit_connection.is_open:
            return True
            
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials, heartbeat=600, blocked_connection_timeout=300)
        
        try:
            logging.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST}...")
            self.rabbit_connection = pika.BlockingConnection(parameters)
            self.rabbit_channel = self.rabbit_connection.channel()
            if self.rabbit_channel:
                self.rabbit_channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
                logging.info("Successfully connected to RabbitMQ and declared queue.")
                return True
            else:
                logging.error("Failed to create RabbitMQ channel.")
                return False
        except AMQPConnectionError as e:
            logging.error(f"RabbitMQ connection failed: {e}")
            self.rabbit_connection = None
            self.rabbit_channel = None
            return False

    def send_task(self, filepath: str):
        """Constructs and sends a Celery task message using the existing connection."""
        if not self.listener_config:
            logging.error("Cannot send task, listener configuration not loaded.")
            return

        if not self.rabbit_channel or not self.rabbit_connection or not self.rabbit_connection.is_open:
            logging.warning("RabbitMQ connection lost. Attempting to reconnect...")
            if not self.connect_rabbitmq():
                logging.error("Reconnect failed. Cannot send task.")
                return

        logging.info(f"Queueing task for file: {filepath}")
        task_payload = {
            "task": CELERY_TASK_NAME,
            "id": str(uuid.uuid4()),
            "args": [filepath, "DIMSE_LISTENER", self.listener_config.instance_id],
            "kwargs": {
                "association_info": {
                    "calling_ae_title": "UNKNOWN_CALLER",
                    "called_ae_title": self.listener_config.ae_title
                }
            },
            "retries": 0, "eta": None, "expires": None, "utc": True,
            "callbacks": None, "errbacks": None, "chain": None, "chord": None
        }
        
        body = json.dumps(task_payload)
        properties = pika.BasicProperties(
            content_type='application/json',
            content_encoding='utf-8',
            delivery_mode=2,
        )

        try:
            if self.rabbit_channel:
                self.rabbit_channel.basic_publish(
                    exchange='',
                    routing_key=RABBITMQ_QUEUE,
                    body=body,
                    properties=properties
                )
                logging.debug(f"Successfully sent task for file '{filepath}'")
            else:
                logging.error("RabbitMQ channel is not available.")
        except Exception as e:
            logging.error(f"Failed to publish message: {e}. Connection may be closed.", exc_info=True)
            # Attempt to reconnect for the next message
            self.rabbit_connection = None 
            self.rabbit_channel = None


class NewFileHandler(FileSystemEventHandler):
    def __init__(self, processor: DICOMProcessor):
        self.processor = processor

    def on_created(self, event):
        if event.is_directory or str(event.src_path).endswith('.part'):
            return
        logging.info(f"File created (and ignored by primary logic): {event.src_path}")

    def on_moved(self, event):
        if event.is_directory or str(event.dest_path).endswith('.part'):
            return

        dest_path_str = str(event.dest_path)
        logging.info(f"File move detected, processing: {dest_path_str}")
        
        # Basic check to ensure file is not empty before sending
        try:
            if os.path.getsize(dest_path_str) > 0:
                self.processor.send_task(dest_path_str)
            else:
                logging.warning(f"File is empty, not queueing task: {dest_path_str}")
        except FileNotFoundError:
             logging.warning(f"File not found immediately after move event, skipping: {dest_path_str}")
        except Exception as e:
            logging.error(f"An unexpected error occurred during file processing: {e}", exc_info=True)


if __name__ == "__main__":
    logging.info("--- Starting DICOM Watcher and Notifier Service ---")
    
    processor = DICOMProcessor()

    # Initial setup with retries
    max_retries = 5
    for i in range(max_retries):
        if processor.load_config() and processor.connect_rabbitmq():
            break
        logging.warning(f"Setup failed. Retrying in 10 seconds... ({i+1}/{max_retries})")
        time.sleep(10)
    else:
        logging.critical("Could not initialize after multiple retries. Exiting.")
        sys.exit(1)

    event_handler = NewFileHandler(processor)
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIRECTORY, recursive=False)
    observer.start()
    
    logging.info(f"Successfully started. Watching directory: {WATCH_DIRECTORY}")
    
    try:
        while True:
            # Keep the main thread alive
            time.sleep(60)
            # Periodically check RabbitMQ connection and reconnect if needed
            if not processor.rabbit_connection or not processor.rabbit_connection.is_open:
                logging.warning("Detected closed RabbitMQ connection. Attempting to reconnect...")
                processor.connect_rabbitmq()

    except KeyboardInterrupt:
        logging.info("Shutdown signal received.")
        observer.stop()
        
    observer.join()
    
    if processor.rabbit_connection and processor.rabbit_connection.is_open:
        processor.rabbit_connection.close()
        
    logging.info("--- DICOM Watcher and Notifier Service Stopped ---")
