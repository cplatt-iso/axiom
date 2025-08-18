# app/services/immediate_batch_processor.py

import asyncio
import threading
from typing import Dict, List, Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select, update, and_
from pathlib import Path

import structlog
from app import crud
from app.db.session import SessionLocal
from app.db.models.exam_batch import ExamBatch, ExamBatchInstance
from app.schemas.enums import ExamBatchStatus
from app.services.redis_batch_triggers import redis_batch_trigger
from app.worker.batch_sender import send_study_batch, get_destination_config

logger = structlog.get_logger(__name__)

class ImmediateBatchProcessor:
    """
    Zero-latency batch processor that immediately processes exam batches when triggered.
    Listens to Redis for immediate batch ready signals and processes them instantly.
    """
    
    def __init__(self):
        self._running = False
        self._active_sends: Dict[str, threading.Lock] = {}  # study_uid:dest_id -> lock
        self._locks_lock = threading.Lock()
    
    def start(self):
        """Start the immediate batch processor."""
        if self._running:
            logger.warning("Immediate batch processor already running")
            return
        
        self._running = True
        logger.info("Starting immediate batch processor")
        
        # Start listening for Redis triggers
        redis_batch_trigger.start_listening(self._handle_batch_ready)
        
        logger.info("Immediate batch processor started")
    
    def stop(self):
        """Stop the immediate batch processor."""
        if not self._running:
            return
        
        self._running = False
        logger.info("Stopping immediate batch processor")
        
        # Stop Redis listener
        redis_batch_trigger.stop_listening()
        
        logger.info("Immediate batch processor stopped")
    
    def _get_send_lock(self, study_uid: str, destination_id: int) -> threading.Lock:
        """Get or create a lock for this study/destination combo to prevent concurrent sends."""
        send_key = f"{study_uid}:{destination_id}"
        
        with self._locks_lock:
            if send_key not in self._active_sends:
                self._active_sends[send_key] = threading.Lock()
            return self._active_sends[send_key]
    
    def _handle_batch_ready(self, study_uid: str, destination_id: int):
        """
        Handle a batch ready signal from Redis.
        This runs in the Redis listener thread.
        """
        log = logger.bind(
            study_uid=study_uid,
            destination_id=destination_id,
            thread=threading.current_thread().name
        )
        
        # Get lock for this study/destination to prevent concurrent processing
        send_lock = self._get_send_lock(study_uid, destination_id)
        
        if not send_lock.acquire(blocking=False):
            log.info("Batch send already in progress, skipping")
            return
        
        try:
            log.info("Processing immediate batch send")
            self._process_batch_immediately(study_uid, destination_id, log)
        finally:
            send_lock.release()
    
    def _process_batch_immediately(self, study_uid: str, destination_id: int, log: structlog.BoundLogger):
        """Process a batch immediately with zero delay."""
        db = None
        batch = None
        
        try:
            db = SessionLocal()
            
            # Find the pending exam batch
            batch = db.execute(
                select(ExamBatch).where(
                    and_(
                        ExamBatch.study_instance_uid == study_uid,
                        ExamBatch.destination_id == destination_id,
                        ExamBatch.status == ExamBatchStatus.PENDING
                    )
                )
            ).scalar_one_or_none()
            
            if not batch:
                log.warning("No pending exam batch found - may have been processed already")
                return
            
            # Get all instances for this batch
            instances = db.execute(
                select(ExamBatchInstance).where(
                    ExamBatchInstance.batch_id == batch.id
                )
            ).scalars().all()
            
            if not instances:
                log.warning("No instances found in exam batch")
                return
            
            log.info(f"Found {len(instances)} instances to send immediately")
            
            # Mark batch as SENDING to prevent other workers from picking it up
            db.execute(
                update(ExamBatch)
                .where(ExamBatch.id == batch.id)
                .values(
                    status=ExamBatchStatus.SENDING,
                    updated_at=datetime.now(timezone.utc)
                )
            )
            db.commit()
            
            # Get destination config
            dest_config = crud.crud_storage_backend_config.get(db, id=destination_id)
            if not dest_config:
                log.error("Destination config not found")
                self._mark_batch_failed(db, batch.id, log)
                return
            
            if not dest_config.is_enabled:
                log.warning(f"Destination '{dest_config.name}' is disabled, skipping send")
                self._mark_batch_failed(db, batch.id, log)
                return
            
            # Only send DIMSE C-STORE batches (other types use individual sends)
            if dest_config.backend_type != "cstore":
                log.info(f"Skipping batch send for non-DIMSE destination type: {dest_config.backend_type}")
                self._mark_batch_completed(db, batch.id, log)
                return
            
            # Build file list
            file_paths = [instance.processed_filepath for instance in instances]
            
            log.info(f"Sending {len(file_paths)} files immediately to destination '{dest_config.name}'")
            
            # Use the existing batch sender logic
            dest_config_dict = get_destination_config(db, dest_config.name, log)
            if not dest_config_dict:
                log.error("Failed to build destination config")
                self._mark_batch_failed(db, batch.id, log)
                return
            
            try:
                # Send the batch using existing logic (this queues to RabbitMQ)
                send_study_batch(
                    study_log=log,
                    dest_name=dest_config.name,
                    file_paths=file_paths,
                    destination_config=dest_config_dict,
                    task_id=f"immediate_batch_{batch.id}"
                )
                
                # Mark as sent (the actual sending happens via RabbitMQ)
                self._mark_batch_completed(db, batch.id, log)
                log.info("Successfully queued batch for immediate sending")
                
            except Exception as send_error:
                log.error("Failed to send batch", error=str(send_error), exc_info=True)
                self._mark_batch_failed(db, batch.id, log)
                
        except Exception as e:
            log.error("Exception during immediate batch processing", error=str(e), exc_info=True)
            if db and batch:
                self._mark_batch_failed(db, batch.id, log)
        finally:
            if db:
                db.close()
    
    def _mark_batch_completed(self, db: Session, batch_id: int, log: structlog.BoundLogger):
        """Mark batch as successfully sent."""
        try:
            db.execute(
                update(ExamBatch)
                .where(ExamBatch.id == batch_id)
                .values(
                    status=ExamBatchStatus.SENT,
                    updated_at=datetime.now(timezone.utc)
                )
            )
            db.commit()
            log.info("Marked batch as SENT")
        except Exception as e:
            log.error("Failed to mark batch as completed", error=str(e))
            db.rollback()
    
    def _mark_batch_failed(self, db: Session, batch_id: int, log: structlog.BoundLogger):
        """Mark batch as failed."""
        try:
            db.execute(
                update(ExamBatch)
                .where(ExamBatch.id == batch_id)
                .values(
                    status=ExamBatchStatus.FAILED,
                    updated_at=datetime.now(timezone.utc)
                )
            )
            db.commit()
            log.info("Marked batch as FAILED")
        except Exception as e:
            log.error("Failed to mark batch as failed", error=str(e))
            db.rollback()

# Global instance
immediate_batch_processor = ImmediateBatchProcessor()
