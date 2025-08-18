#!/usr/bin/env python3

import json
import time
import pika
from typing import Dict, List, Any, Optional
from collections import defaultdict
from sqlalchemy.orm import Session
import structlog
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from app.core.config import settings
from app import crud
from app.worker.task_utils import build_storage_backend_config_dict

logger = structlog.get_logger(__name__)

def send_studies_to_destinations(
    log: structlog.BoundLogger,
    db_session: Session, 
    study_destination_batches: Dict[str, Dict[str, List[Dict[str, Any]]]],
    task_id: str,
    max_parallel_studies: int = 5  # Max concurrent studies to send
):
    """
    Send studies to destinations in parallel threads - no delays, just pure parallel processing.
    Each study gets sent in its own thread with one association per study.
    
    Args:
        log: Structured logger
        db_session: Database session
        study_destination_batches: study_uid -> dest_name -> [file_info]
        task_id: Task ID for logging
        max_parallel_studies: Maximum number of studies to send concurrently
    """
    log.info(f"Starting parallel study-based batch sending for {len(study_destination_batches)} studies")
    
    # Collect all study-destination combinations to process
    study_jobs = []
    for study_uid, destinations in study_destination_batches.items():
        for dest_name, file_infos in destinations.items():
            file_paths = [f["file_path"] for f in file_infos]
            study_jobs.append({
                'study_uid': study_uid,
                'dest_name': dest_name, 
                'file_paths': file_paths,
                'file_count': len(file_paths)
            })
    
    log.info(f"Prepared {len(study_jobs)} study-destination jobs for parallel processing")
    
    # Process studies in parallel using ThreadPoolExecutor
    def send_single_study(study_job):
        """Send a single study batch in its own thread."""
        study_uid = study_job['study_uid']
        dest_name = study_job['dest_name']
        file_paths = study_job['file_paths']
        
        study_log = log.bind(
            study_uid=study_uid, 
            dest_name=dest_name, 
            file_count=len(file_paths),
            thread_name=threading.current_thread().name
        )
        
        try:
            # Get destination configuration
            destination_config = get_destination_config(db_session, dest_name, study_log)
            if not destination_config:
                study_log.error(f"Could not retrieve config for destination '{dest_name}', skipping study")
                return {'status': 'error', 'study_uid': study_uid, 'dest_name': dest_name, 'error': 'No destination config'}
            
            study_log.info(f"Sending study batch: {len(file_paths)} files (parallel thread)")
            
            # Send the study batch - creates one association, sends all files, closes
            send_study_batch(study_log, dest_name, file_paths, destination_config, task_id)
            
            study_log.info(f"Successfully queued study batch of {len(file_paths)} files")
            return {'status': 'success', 'study_uid': study_uid, 'dest_name': dest_name, 'file_count': len(file_paths)}
            
        except Exception as study_error:
            study_log.error(f"Error sending study batch", error=str(study_error), exc_info=True)
            return {'status': 'error', 'study_uid': study_uid, 'dest_name': dest_name, 'error': str(study_error)}
    
    # Execute all study jobs in parallel
    results = []
    with ThreadPoolExecutor(max_workers=max_parallel_studies, thread_name_prefix="StudyBatch") as executor:
        # Submit all jobs
        future_to_job = {executor.submit(send_single_study, job): job for job in study_jobs}
        
        # Collect results as they complete
        for future in as_completed(future_to_job):
            job = future_to_job[future]
            try:
                result = future.result()
                results.append(result)
                
                if result['status'] == 'success':
                    log.info(f"Study batch completed", 
                            study_uid=result['study_uid'], 
                            dest_name=result['dest_name'],
                            file_count=result['file_count'])
                else:
                    log.error(f"Study batch failed",
                             study_uid=result['study_uid'],
                             dest_name=result['dest_name'], 
                             error=result['error'])
                             
            except Exception as exc:
                log.error(f"Study batch thread exception", 
                         study_uid=job['study_uid'],
                         dest_name=job['dest_name'],
                         error=str(exc), exc_info=True)
                results.append({'status': 'error', 'study_uid': job['study_uid'], 'dest_name': job['dest_name'], 'error': str(exc)})
    
    # Summary
    successful = len([r for r in results if r['status'] == 'success'])
    failed = len([r for r in results if r['status'] == 'error'])
    
    log.info(f"Completed parallel study-based batch sending: {successful} successful, {failed} failed")
    return results


def get_destination_config(db_session: Session, dest_name: str, log: structlog.BoundLogger) -> Optional[Dict[str, Any]]:
    """Get destination configuration from database."""
    try:
        # Look up the destination by name
        storage_backend = crud.crud_storage_backend_config.get_by_name(db_session, name=dest_name)
        if not storage_backend:
            log.error(f"Storage backend '{dest_name}' not found")
            return None
            
        if not storage_backend.is_enabled:
            log.warning(f"Storage backend '{dest_name}' is disabled")
            return None
            
        # Build the config dictionary using the existing utility
        config_dict = build_storage_backend_config_dict(storage_backend, f"batch_send_{dest_name}")
        return config_dict
        
    except Exception as e:
        log.error(f"Database error getting destination config", error=str(e), exc_info=True)
        return None


def send_study_batch(
    study_log: structlog.BoundLogger,
    dest_name: str, 
    file_paths: List[str], 
    destination_config: Dict[str, Any], 
    task_id: str
):
    """Send a batch of files for a single study to a destination."""
    
    # Validate all files exist
    for file_path in file_paths:
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Processed file not found: {file_path}")
    
    # Determine sender type and queue
    sender_type = destination_config.get("sender_type", "pynetdicom")
    if destination_config.get("sender_identifier"):
        # Would need to look up sender config here, but for now use the type
        pass
    
    queue_name = "cstore_dcm4che_jobs" if sender_type == "dcm4che" else "cstore_pynetdicom_jobs"
    
    study_log.info(f"Queueing study batch to {queue_name} with {len(file_paths)} files")
    
    # Create the batch job payload
    job_payload = {
        "file_paths": file_paths,  # Use the plural format for batch sending
        "destination_config": destination_config,
        "study_info": {
            "task_id": task_id,
            "file_count": len(file_paths),
            "batch_type": "study_based"
        }
    }
    
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
        
        study_log.info(f"Successfully queued study batch to {queue_name}")
        
    except Exception as queue_error:
        study_log.error(f"Failed to queue study batch", error=str(queue_error), exc_info=True)
        raise
    finally:
        if connection.is_open:
            connection.close()
