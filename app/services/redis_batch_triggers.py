# app/services/redis_batch_triggers.py

import json
import redis
from typing import Dict, List, Optional, Set
from datetime import datetime, timezone
import asyncio
import threading

import structlog
from app.core.config import settings

logger = structlog.get_logger(__name__)

class RedisBatchTrigger:
    """
    Redis-based immediate batch trigger system for zero-latency study sending.
    
    When files are processed, they trigger immediate "ready" signals via Redis.
    Background workers listen for these signals and process batches immediately.
    """
    
    def __init__(self):
        self.redis_client = redis.Redis(
            host=getattr(settings, 'REDIS_HOST', 'localhost'),
            port=getattr(settings, 'REDIS_PORT', 6379),
            db=getattr(settings, 'REDIS_DB', 0),
            decode_responses=True
        )
        self.channel = "axiom:exam_batches:ready"
        self._subscriber = None
        self._listening = False
        self._listen_thread = None
    
    def trigger_batch_ready(self, study_instance_uid: str, destination_id: int) -> bool:
        """
        Signal that a study batch is ready for sending.
        This is a non-blocking operation that immediately notifies listeners.
        """
        try:
            message = {
                "study_uid": study_instance_uid,
                "destination_id": destination_id,
                "triggered_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Publish to Redis channel for immediate pickup
            result = self.redis_client.publish(self.channel, json.dumps(message))  # type: ignore
            
            logger.info(
                "Triggered batch ready signal",
                study_uid=study_instance_uid,
                destination_id=destination_id,
                subscribers=result
            )
            
            return result > 0  # True if there were subscribers  # type: ignore
            
        except Exception as e:
            logger.error(
                "Failed to trigger batch ready signal",
                study_uid=study_instance_uid,
                destination_id=destination_id,
                error=str(e),
                exc_info=True
            )
            return False
    
    def start_listening(self, callback_func):
        """
        Start listening for batch ready signals in a background thread.
        callback_func should accept (study_uid, destination_id) arguments.
        """
        if self._listening:
            logger.warning("Already listening for batch triggers")
            return
        
        self._listening = True
        self._listen_thread = threading.Thread(
            target=self._listen_worker,
            args=(callback_func,),
            daemon=True,
            name="RedisBatchTriggerListener"
        )
        self._listen_thread.start()
        logger.info("Started Redis batch trigger listener")
    
    def stop_listening(self):
        """Stop listening for batch ready signals."""
        if not self._listening:
            return
        
        self._listening = False
        if self._subscriber:
            self._subscriber.close()
        
        if self._listen_thread and self._listen_thread.is_alive():
            self._listen_thread.join(timeout=5.0)
        
        logger.info("Stopped Redis batch trigger listener")
    
    def _listen_worker(self, callback_func):
        """Background worker that listens for Redis messages."""
        try:
            self._subscriber = self.redis_client.pubsub()
            self._subscriber.subscribe(self.channel)
            
            logger.info("Redis batch trigger listener started")
            
            for message in self._subscriber.listen():
                if not self._listening:
                    break
                
                if message['type'] != 'message':
                    continue
                
                try:
                    data = json.loads(message['data'])
                    study_uid = data['study_uid']
                    destination_id = data['destination_id']
                    
                    logger.debug(
                        "Received batch ready signal",
                        study_uid=study_uid,
                        destination_id=destination_id
                    )
                    
                    # Call the callback function to handle the batch
                    callback_func(study_uid, destination_id)
                    
                except Exception as cb_error:
                    logger.error(
                        "Error in batch ready callback",
                        error=str(cb_error),
                        message_data=message.get('data'),
                        exc_info=True
                    )
                    
        except Exception as e:
            logger.error("Redis batch trigger listener error", error=str(e), exc_info=True)
        finally:
            if self._subscriber:
                self._subscriber.close()
            logger.info("Redis batch trigger listener stopped")


# Global instance
redis_batch_trigger = RedisBatchTrigger()
