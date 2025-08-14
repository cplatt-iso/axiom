
import os
import json
import pika
from pydantic import BaseModel
from typing import Dict, Any

# Assuming scu_service and its dependencies are in the python path
from app.services.network.dimse.scu_service import store_dataset
from app.schemas.storage_backend_config import CStoreBackendConfig
from pydicom import dcmread

# Basic configuration from environment variables
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
PYNETDICOM_QUEUE = "cstore_pynetdicom_jobs"

class CStoreJob(BaseModel):
    file_path: str
    destination_config: Dict[str, Any]

def send_with_pynetdicom(job: CStoreJob):
    """Uses pynetdicom to send a DICOM file."""
    print(f"Processing job for file: {job.file_path}")
    
    try:
        # Load the DICOM dataset from the file
        dataset = dcmread(job.file_path, force=True)
        
        # Create a CStoreBackendConfig object from the dictionary
        config = CStoreBackendConfig(**job.destination_config)
        
        print(f"Destination: {config.remote_ae_title}")

        # Call the existing store_dataset function
        result = store_dataset(
            config=config,
            dataset=dataset
        )
        
        print(f"pynetdicom store_dataset completed for {job.file_path}.")
        print(f"Result: {result}")

        if result.get("status") != "success":
            raise Exception(f"Pynetdicom sending failed: {result.get('message')}")

    except Exception as e:
        print(f"An unexpected error occurred during pynetdicom execution: {e}")
        raise

def callback(ch, method, properties, body):
    try:
        print(f"Received message from {PYNETDICOM_QUEUE}")
        data = json.loads(body)
        job = CStoreJob(**data)
        send_with_pynetdicom(job)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("Job completed and acknowledged.")
    except Exception as e:
        print(f"Failed to process message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    print("Starting pynetdicom sender...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=PYNETDICOM_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=PYNETDICOM_QUEUE, on_message_callback=callback)

    print(f"[*] Waiting for messages on {PYNETDICOM_QUEUE}. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()
