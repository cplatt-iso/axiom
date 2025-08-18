#!/usr/bin/env python3
"""
Exam Batch Sender Service

This service processes READY exam batches and sends them to their destinations.
It runs as a background service and handles the actual C-STORE operations.
"""

import asyncio
import json
import pika
from datetime import datetime, timezone
from typing import List, Dict, Any
from pathlib import Path
from sqlalchemy.orm import Session
import structlog

from app.db.session import SessionLocal
from app import crud
from app.db.models.exam_batch import ExamBatch, ExamBatchInstance
from app.core.config import settings
from app.worker.task_utils import build_storage_backend_config_dict

logger = structlog.get_logger(__name__)

class ExamBatchSenderService:
    def __init__(self):
        self.running = False
        self.send_interval = getattr(settings, 'EXAM_BATCH_SEND_INTERVAL', 5)  # seconds
        self.max_concurrent_sends = getattr(settings, 'EXAM_BATCH_MAX_CONCURRENT', 10)
        
    async def start(self):
        """Start the batch sender service."""
        self.running = True
        logger.info("Starting Exam Batch Sender Service",
                   send_interval=self.send_interval,
                   max_concurrent=self.max_concurrent_sends)
        
        while self.running:
            try:
                await self.process_ready_batches()
                await asyncio.sleep(self.send_interval)
            except Exception as e:
                logger.error("Error in sender service loop", error=str(e), exc_info=True)
                await asyncio.sleep(5)  # Brief pause on error
    
    def stop(self):
        """Stop the service."""
        self.running = False
        logger.info("Stopping Exam Batch Sender Service")
    
    async def process_ready_batches(self):
        """Process all READY batches."""
        db = SessionLocal()
        try:
            # Get READY batches with a reasonable limit
            ready_batches = db.query(ExamBatch)\
                .filter(ExamBatch.status == "READY")\
                .limit(self.max_concurrent_sends)\
                .all()
            
            if not ready_batches:
                return
                
            logger.info(f"Processing {len(ready_batches)} ready batches")
            
            for batch in ready_batches:
                try:
                    await self.send_batch(db, batch)
                except Exception as batch_error:
                    logger.error("Failed to send batch",
                               batch_id=batch.id,
                               study_uid=batch.study_instance_uid,
                               error=str(batch_error),
                               exc_info=True)
                    
                    # Mark as failed
                    batch.status = "FAILED"
                    batch.updated_at = datetime.now(timezone.utc)
            
            db.commit()
            
        except Exception as e:
            logger.error("Error processing ready batches", error=str(e), exc_info=True)
            db.rollback()
        finally:
            db.close()
    
    async def send_batch(self, db: Session, batch):
        """Send a single exam batch to its destination."""
        batch_log = logger.bind(
            batch_id=batch.id,
            study_uid=batch.study_instance_uid,
            destination_id=batch.destination_id
        )
        
        # Mark as SENDING to prevent double-processing
        batch.status = "SENDING"
        batch.updated_at = datetime.now(timezone.utc)
        db.commit()  # Commit immediately to claim the batch
        
        try:
            # Get all instances for this batch
            instances = db.query(ExamBatchInstance)\
                .filter(ExamBatchInstance.batch_id == batch.id)\
                .all()
            
            if not instances:
                batch_log.warning("Batch has no instances, marking as failed")
                batch.status = "FAILED"
                return
            
            # Validate all files exist
            file_paths = []
            missing_files = []
            for instance in instances:
                if Path(instance.processed_filepath).exists():
                    file_paths.append(instance.processed_filepath)
                else:
                    missing_files.append(instance.processed_filepath)
            
            if missing_files:
                batch_log.error("Some batch files are missing",
                              missing_count=len(missing_files),
                              total_count=len(instances),
                              missing_files=missing_files[:5])  # Log first 5
                batch.status = "FAILED"
                return
            
            # Get destination config
            storage_backend = crud.crud_storage_backend_config.get(db, id=batch.destination_id)
            if not storage_backend or not storage_backend.is_enabled:
                batch_log.error("Destination not found or disabled")
                batch.status = "FAILED"
                return
            
            # Build config for sender
            config_dict = build_storage_backend_config_dict(storage_backend, f"batch_{batch.id}")
            if not config_dict:
                batch_log.error("Failed to build destination config")
                batch.status = "FAILED"
                return
            
            batch_log.info(f"Sending batch with {len(file_paths)} files to {storage_backend.name}")
            
            # Send to appropriate queue
            await self.queue_batch_for_sending(batch_log, storage_backend, file_paths, config_dict, batch.id)
            
            # Mark as SENT (the actual sending happens asynchronously)
            batch.status = "SENT"
            batch_log.info("Successfully queued batch for sending")
            
        except Exception as send_error:
            batch_log.error("Error sending batch", error=str(send_error), exc_info=True)
            batch.status = "FAILED"
            raise
        finally:
            batch.updated_at = datetime.now(timezone.utc)
    
    async def queue_batch_for_sending(
        self, 
        batch_log: structlog.BoundLogger,
        storage_backend,
        file_paths: List[str],
        config_dict: Dict[str, Any],
        batch_id: int
    ):
        """Queue the batch for the appropriate sender service."""
        
        # Determine sender type and queue
        sender_type = getattr(storage_backend, 'sender_type', 'pynetdicom')
        
        if storage_backend.backend_type != "cstore":
            batch_log.warning(f"Batch sending not supported for backend type: {storage_backend.backend_type}")
            return
        
        # Choose queue based on sender type (following same logic as batch_sender.py)
        if sender_type == "dcm4che":
            queue_name = "cstore_dcm4che_jobs"
        else:
            queue_name = "cstore_pynetdicom_jobs"
        
        # Create batch job payload
        job_payload = {
            "batch_id": batch_id,
            "file_paths": file_paths,  # List of file paths for batch sending
            "destination_config": config_dict,
            "study_info": {
                "batch_type": "exam_batch",
                "file_count": len(file_paths)
            }
        }
        
        batch_log.info(f"Queuing batch to {queue_name}", file_count=len(file_paths))
        
        # Send to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.RABBITMQ_HOST))
        try:
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(job_payload, default=str),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json',
                    content_encoding='utf-8',
                )
            )
            
            batch_log.info(f"Successfully queued batch to {queue_name}")
            
        except Exception as queue_error:
            batch_log.error(f"Failed to queue batch", error=str(queue_error), exc_info=True)
            raise
        finally:
            if connection.is_open:
                connection.close()

# Global service instance
sender_service = ExamBatchSenderService()

async def start_sender_service():
    """Start the sender service - called from main app startup."""
    await sender_service.start()

def stop_sender_service():
    """Stop the sender service - called from app shutdown."""
    sender_service.stop()

if __name__ == "__main__":
    """Run standalone for testing."""
    async def main():
        try:
            await start_sender_service()
        except KeyboardInterrupt:
            stop_sender_service()
    
    asyncio.run(main())
