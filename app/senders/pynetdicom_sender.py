
import os
import json
import pika
from pydantic import BaseModel
from typing import Dict, Any, List, Union

# Assuming scu_service and its dependencies are in the python path
from app.services.network.dimse.scu_service import store_dataset
from app.schemas.storage_backend_config import CStoreBackendConfig
from pydicom import dcmread

# Basic configuration from environment variables
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
PYNETDICOM_QUEUE = "cstore_pynetdicom_jobs"

class CStoreJob(BaseModel):
    file_paths: Union[str, List[str]]  # Support both single file (legacy) and batch processing
    destination_config: Dict[str, Any]

def send_with_pynetdicom(job: CStoreJob):
    """Uses pynetdicom to send DICOM files in a single association."""
    # Handle both single file (legacy) and batch formats
    if isinstance(job.file_paths, str):
        file_paths = [job.file_paths]
    else:
        file_paths = job.file_paths
    
    print(f"Processing pynetdicom job for {len(file_paths)} file(s)")
    
    # Create a CStoreBackendConfig object from the dictionary
    config = CStoreBackendConfig(**job.destination_config)
    print(f"Destination: {config.remote_ae_title}")
    
    # Validate all files exist
    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"ERROR: File does not exist: {file_path}")
            raise FileNotFoundError(f"DICOM file not found: {file_path}")
    
    try:
        # Load all datasets and send them in a single association
        datasets = []
        for file_path in file_paths:
            dataset = dcmread(file_path, force=True)
            datasets.append(dataset)
            print(f"Loaded dataset from: {file_path}")
        
        print(f"Sending {len(datasets)} datasets to {config.remote_ae_title} in single association")
        
        # Send all datasets in a single association
        # Note: This assumes store_dataset can handle multiple datasets
        # If not, we might need to modify the scu_service to support batch operations
        for i, dataset in enumerate(datasets):
            print(f"Sending dataset {i+1}/{len(datasets)}")
            result = store_dataset(
                config=config,
                dataset=dataset
            )
            
            if result.get("status") != "success":
                raise Exception(f"Pynetdicom sending failed for dataset {i+1}: {result.get('message')}")
            
            print(f"Successfully sent dataset {i+1}/{len(datasets)}: {file_paths[i]}")
        
        print(f"pynetdicom completed successfully for {len(file_paths)} files in batch.")

    except Exception as e:
        print(f"An unexpected error occurred during pynetdicom batch execution: {e}")
        print(f"Files involved: {file_paths}")
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
