#!/usr/bin/env python

import os
import sys
import json
import uuid
import pika
from pika.exceptions import AMQPConnectionError
import logging
import time

# Add the project root to the Python path to allow importing from 'app'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Optional
from app.db.session import SessionLocal
from app import crud
from app.db.models import DimseListenerConfig

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- RabbitMQ Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "default")
CELERY_TASK_NAME = os.getenv("CELERY_TASK_NAME", "process_dicom_file_task")
# The instance_id of the dcm4che listener we want to associate the file with.
# This is set by the AXIOM_INSTANCE_ID env var in the docker-compose file for this service.
DCM4CHE_INSTANCE_ID = os.getenv("AXIOM_INSTANCE_ID", "dcm4che_1")


def get_dcm4che_listener_config() -> Optional[DimseListenerConfig]:
    """Connects to the DB and fetches the DIMSE listener config for dcm4che."""
    db = None
    try:
        logging.info(f"Connecting to the database to fetch listener config for instance_id: '{DCM4CHE_INSTANCE_ID}'")
        db = SessionLocal()
        listener_config = crud.crud_dimse_listener_config.get_by_instance_id(db, instance_id=DCM4CHE_INSTANCE_ID)
        if listener_config:
            logging.info(f"Found listener config: Name='{listener_config.name}', AE Title='{listener_config.ae_title}'")
            return listener_config
        else:
            logging.error(f"No DIMSE listener configuration found for instance_id: '{DCM4CHE_INSTANCE_ID}'")
            return None
    except Exception as e:
        logging.error(f"Database error while fetching listener config: {e}", exc_info=True)
        return None
    finally:
        if db:
            db.close()

def connect_to_rabbitmq():
    """Establishes a connection to RabbitMQ, with retries."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    
    max_retries = 10
    retry_delay = 5
    for attempt in range(max_retries):
        try:
            logging.info(f"Connecting to RabbitMQ at {RABBITMQ_HOST} (attempt {attempt + 1}/{max_retries})...")
            connection = pika.BlockingConnection(parameters)
            logging.info("Successfully connected to RabbitMQ.")
            return connection
        except AMQPConnectionError as e:
            logging.warning(f"RabbitMQ connection failed: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    logging.error("Could not connect to RabbitMQ after several retries. Exiting.")
    sys.exit(1)

def send_task_to_celery(filepath: str, listener_config: DimseListenerConfig):
    """Constructs a Celery task message and sends it to the specified queue."""
    if not os.path.exists(filepath):
        logging.error(f"File does not exist, cannot send task: {filepath}")
        sys.exit(1)

    logging.info(f"Preparing Celery task for file: {filepath}")

    # This payload structure is what Celery workers expect.
    # It mimics how a task is sent from another Celery client.
    task_payload = {
        "task": CELERY_TASK_NAME,
        "id": str(uuid.uuid4()),
        "args": [filepath, "DIMSE_LISTENER", listener_config.instance_id], # Use the fetched instance_id
        "kwargs": {
            "association_info": {
                "calling_ae_title": "UNKNOWN_CALLER", # This could be enhanced if known
                "called_ae_title": listener_config.ae_title # Use the fetched AE Title
            }
        },
        "retries": 0,
        "eta": None,
        "expires": None,
        "utc": True,
        "callbacks": None,
        "errbacks": None,
        "chain": None,
        "chord": None
    }
    
    body = json.dumps(task_payload)
    properties = pika.BasicProperties(
        content_type='application/json',
        content_encoding='utf-8',
        delivery_mode=2,  # Make message persistent
    )

    connection = None
    try:
        connection = connect_to_rabbitmq()
        channel = connection.channel()

        # Declare the queue to ensure it exists. This is idempotent.
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

        # Publish the message
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=body,
            properties=properties
        )
        logging.info(f"Successfully sent task for file '{filepath}' to queue '{RABBITMQ_QUEUE}'")

    except Exception as e:
        logging.error(f"An error occurred while sending task to RabbitMQ: {e}", exc_info=True)
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Usage: python notify_rabbitmq.py <path_to_dicom_file>")
        sys.exit(1)
    
    dicom_filepath = sys.argv[1]
    
    # Fetch the listener config from the database first
    listener = get_dcm4che_listener_config()
    
    if listener:
        # If the config is found, proceed to send the task
        send_task_to_celery(dicom_filepath, listener)
    else:
        # If no config is found, log an error and exit
        logging.critical(f"Could not send task for file '{dicom_filepath}' because listener config could not be retrieved.")
        sys.exit(1)
