# app/services/immediate_batch_sender.py

import asyncio
import logging
from typing import Dict, List, Optional, Set
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import select, update, and_, or_
from pathlib import Path

import structlog
from app import crud
from app.db.session import SessionLocal
from app.db.models.exam_batch import ExamBatch, ExamBatchInstance
from app.schemas.enums import ExamBatchStatus
from app.worker.batch_sender import send_study_batch, get_destination_config

logger = structlog.get_logger(__name__)

class ImmediateBatchSender:
    """
    Low-latency batch sender that attempts to send studies immediately after files are processed.
    Opportunistically batches files that arrive while a send is in progress.
    """
    
    def __init__(self):
        self._active_sends: Set[str] = set()  # Track active sends by "study_uid:destination_id"
        self._send_lock = asyncio.Lock()
    
    async def trigger_immediate_send(
        self, 
        study_instance_uid: str, 
        destination_id: int,
        db: Session
    ) -> bool:
        """
        Trigger immediate send for a study/destination combo.
        If a send is already in progress, this will be a no-op (the active send will pick up new files).
        
        Returns True if a send was triggered, False if already in progress.
        """
        send_key = f"{study_instance_uid}:{destination_id}"
        log = logger.bind(
            study_uid=study_instance_uid, 
            destination_id=destination_id,
            send_key=send_key
        )
        
        async with self._send_lock:
            if send_key in self._active_sends:
                log.info("Send already in progress for this study/destination combo - skipping trigger")
                return False
            
            # Mark as active
            self._active_sends.add(send_key)
            log.info("Triggering immediate send for study/destination combo")
        
        try:
            # Use separate DB session if not provided
            close_db = False
            if db is None:
                db = SessionLocal()
                close_db = True
            
            try:
                # Find the exam batch for this study/destination
                batch = db.execute(
                    select(ExamBatch).where(
                        and_(
                            ExamBatch.study_instance_uid == study_instance_uid,
                            ExamBatch.destination_id == destination_id,
                            ExamBatch.status == ExamBatchStatus.PENDING
                        )
                    )
                ).scalar_one_or_none()
                
                if not batch:
                    log.warning("No pending exam batch found - may have been processed already")
                    return False
                
                # Get all instances for this batch
                instances = db.execute(
                    select(ExamBatchInstance).where(
                        ExamBatchInstance.batch_id == batch.id
                    )
                ).scalars().all()
                
                if not instances:
                    log.warning("No instances found in exam batch")
                    return False
                
                log.info(f"Found {len(instances)} instances to send")
                
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
                    # Mark batch as failed
                    db.execute(
                        update(ExamBatch)
                        .where(ExamBatch.id == batch.id)
                        .values(
                            status=ExamBatchStatus.FAILED,
                            updated_at=datetime.now(timezone.utc)
                        )
                    )
                    db.commit()
                    return False
                
                # Build file list
                file_paths = [instance.processed_filepath for instance in instances]
                
                log.info(f"Sending {len(file_paths)} files to destination '{dest_config.name}'")
                
                # Get destination configuration
                destination_config = get_destination_config(db, dest_config.name, log)
                if not destination_config:
                    log.error("Failed to build destination config")
                    success = False
                else:
                    try:
                        # Send the batch using existing logic
                        send_study_batch(
                            study_log=log,
                            dest_name=dest_config.name,
                            file_paths=file_paths,
                            destination_config=destination_config,
                            task_id=f"immediate_send_{batch.id}"
                        )
                        success = True
                        log.info(f"Successfully queued batch for immediate sending")
                    except Exception as send_error:
                        log.error(f"Failed to send batch: {send_error}", exc_info=True)
                        success = False
                
                # Update batch status
                new_status = ExamBatchStatus.SENT if success else ExamBatchStatus.FAILED
                db.execute(
                    update(ExamBatch)
                    .where(ExamBatch.id == batch.id)
                    .values(
                        status=new_status,
                        updated_at=datetime.now(timezone.utc)
                    )
                )
                db.commit()
                
                log.info(f"Batch send completed", success=success, final_status=new_status.value)
                return success
                
            finally:
                if close_db:
                    db.close()
                    
        except Exception as e:
            log.error("Exception during immediate send", error=str(e), exc_info=True)
            return False
        finally:
            # Remove from active sends
            async with self._send_lock:
                self._active_sends.discard(send_key)
                log.debug("Removed from active sends")

    def trigger_immediate_send_sync(
        self, 
        study_instance_uid: str, 
        destination_id: int,
        db: Session
    ) -> bool:
        """
        Synchronous wrapper for immediate send trigger.
        Use this from non-async contexts like Celery tasks.
        """
        try:
            # Create new event loop for this thread if needed
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            return loop.run_until_complete(
                self.trigger_immediate_send(study_instance_uid, destination_id, db)
            )
        except Exception as e:
            logger.error(
                "Exception in synchronous immediate send trigger",
                error=str(e),
                study_uid=study_instance_uid,
                destination_id=destination_id,
                exc_info=True
            )
            return False

# Global instance
immediate_batch_sender = ImmediateBatchSender()
