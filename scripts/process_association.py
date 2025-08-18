#!/usr/bin/env python

import os
import sys
import json
import uuid
import pika
from pika.exceptions import AMQPConnectionError
import logging
import time
from typing import List, Optional

# Add the project root to the Python path to allow importing from 'app'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app import crud
from app.db.models import DimseListenerConfig

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [AssociationProcessor] - %(message)s')

# --- RabbitMQ Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBITMQ_QUEUE = "rules_engine_intake"  # Hardcode this for now to debug
logging.info(f"Using RABBITMQ_QUEUE: {RABBITMQ_QUEUE}")  # Debug logging
CELERY_TASK_NAME = os.getenv("CELERY_TASK_NAME", "process_dicom_association_task") # New task name for associations
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

def send_association_task_to_celery(filepaths: List[str], listener_config: DimseListenerConfig):
    """Constructs a Celery task for an entire association and sends it to the queue."""
    if not filepaths:
        logging.warning("No filepaths provided to process. Exiting.")
        return

    logging.info(f"Preparing Celery task for an association with {len(filepaths)} files.")

    task_payload = {
        "task": CELERY_TASK_NAME,
        "id": str(uuid.uuid4()),
        "args": [filepaths, "DIMSE_LISTENER", listener_config.instance_id],
        "kwargs": {
            "association_info": {
                "calling_ae_title": "UNKNOWN_CALLER", # This could be enhanced if known
                "called_ae_title": listener_config.ae_title
            }
        },
        "retries": 0,
        "eta": None,
        "expires": None,
        "utc": True,
        "callbacks": None,
        "errbacks": None,
        "chain": None,
    }

    connection = connect_to_rabbitmq()
    try:
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(task_payload),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json',
                content_encoding='utf-8',
            )
        )
        logging.info(f"Successfully sent task {task_payload['id']} for {len(filepaths)} files to queue '{RABBITMQ_QUEUE}'.")
    except Exception as e:
        logging.critical(f"Failed to send task to RabbitMQ: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if connection.is_open:
            connection.close()

def main():
    """
    Main entry point. Expects file paths as command-line arguments.
    These arguments are provided by the dcm4che storescp service on association release.
    """
    if len(sys.argv) < 2:
        logging.error("No DICOM files provided as arguments. This script should be called by storescp.")
        sys.exit(1)

    filepaths = sys.argv[1:]
    logging.info(f"Received {len(filepaths)} files from a single association.")
    
    for f in filepaths:
        if not os.path.exists(f):
            logging.error(f"Provided file path does not exist: {f}. Aborting.")
            sys.exit(1)

    listener_config = get_dcm4che_listener_config()
    if not listener_config:
        logging.critical("Could not retrieve listener configuration. Aborting task submission.")
        sys.exit(1)

    send_association_task_to_celery(filepaths, listener_config)

if __name__ == "__main__":
    main()
