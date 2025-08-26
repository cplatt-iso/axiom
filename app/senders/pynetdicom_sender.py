
import os
import json
import pika
from pydantic import BaseModel
from typing import Dict, Any, List, Union

# Assuming scu_service and its dependencies are in the python path
from app.services.network.dimse.scu_service import store_dataset
from app.schemas.storage_backend_config import CStoreBackendConfig
from pydicom import dcmread

# Structured logging
from app.core.logging_config import configure_json_logging
logger = configure_json_logging("pynetdicom_sender")

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
    
    # Create a CStoreBackendConfig object from the dictionary
    config = CStoreBackendConfig(**job.destination_config)
    
    logger.info("Processing pynetdicom job", 
                file_count=len(file_paths),
                destination_ae_title=config.remote_ae_title,
                destination_host=config.remote_host,
                destination_port=config.remote_port,
                files=[os.path.basename(f) for f in file_paths[:5]])  # Show first 5 filenames
    
    # Validate all files exist
    for file_path in file_paths:
        if not os.path.exists(file_path):
            logger.error("File does not exist", file_path=file_path)
            raise FileNotFoundError(f"DICOM file not found: {file_path}")
    
    try:
        # Load all datasets and send them in a single association
        datasets = []
        for file_path in file_paths:
            dataset = dcmread(file_path, force=True)
            datasets.append(dataset)
            logger.info("Loading DICOM dataset", file_path=os.path.basename(file_path))
        
        logger.info("Initiating pynetdicom transmission", 
                    dataset_count=len(datasets), 
                    destination_ae_title=config.remote_ae_title,
                    destination_host=config.remote_host,
                    destination_port=config.remote_port,
                    files_to_send=[os.path.basename(f) for f in file_paths])
        
        # Send all datasets in a single association
        successful_transmissions = []
        for i, dataset in enumerate(datasets):
            result = store_dataset(
                config=config,
                dataset=dataset
            )
            
            if result.get("status") != "success":
                raise Exception(f"Pynetdicom sending failed for dataset {i+1}: {result.get('message')}")
            
            successful_transmissions.append(os.path.basename(file_paths[i]))
        
        logger.info("pynetdicom transmission completed", 
                    transaction_status="SUCCESS",
                    file_count=len(file_paths),
                    destination_ae_title=config.remote_ae_title,
                    files_sent=successful_transmissions)

    except Exception as e:
        logger.error("Unexpected error during pynetdicom batch execution", 
                     error=str(e), 
                     files=file_paths)
        raise

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        job = CStoreJob(**data)
        config = CStoreBackendConfig(**job.destination_config)
        
        # Handle both single file (legacy) and batch formats
        if isinstance(job.file_paths, str):
            file_paths = [job.file_paths]
        else:
            file_paths = job.file_paths
        
        logger.info("Received pynetdicom transmission job", 
                    queue=PYNETDICOM_QUEUE,
                    file_count=len(file_paths),
                    destination_ae_title=config.remote_ae_title,
                    destination_host=config.remote_host,
                    destination_port=config.remote_port)
        
        send_with_pynetdicom(job)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        logger.info("pynetdicom transmission job completed", 
                    queue=PYNETDICOM_QUEUE,
                    destination_ae_title=config.remote_ae_title,
                    file_count=len(file_paths),
                    status="ACKNOWLEDGED")
    except Exception as e:
        logger.error("Failed to process pynetdicom transmission job", 
                     error=str(e),
                     queue=PYNETDICOM_QUEUE)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    logger.info("Starting pynetdicom sender...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=PYNETDICOM_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=PYNETDICOM_QUEUE, on_message_callback=callback)

    logger.info("Waiting for messages. To exit press CTRL+C", queue=PYNETDICOM_QUEUE)
    channel.start_consuming()

if __name__ == "__main__":
    main()
