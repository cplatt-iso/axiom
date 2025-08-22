#!/usr/bin/env python3
"""
Dustbin Verification Worker

This worker monitors the dustbin verification queue and ensures that files are only
permanently deleted after ALL destinations have confirmed successful receipt.

Key responsibilities:
1. Process dustbin verification requests
2. Track destination confirmations 
3. Move files to permanent deletion only after full verification
4. Implement retry logic for failed verifications
5. Maintain audit trail of all actions

This is a critical component of the medical-grade file safety system.
"""

import os
import json
import pika
import logging
import structlog
import time
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path

from app.core.config import settings
from app.db.session import SessionLocal
from app import crud
from app.services.dustbin_service import dustbin_service

# Configure structured logging
log = structlog.get_logger(__name__)

class DustbinVerificationWorker:
    """
    Worker that processes dustbin verification queue and manages safe file deletion.
    """
    
    def __init__(self):
        self.queue_name = "dustbin_verification"
        self.connection = None
        self.channel = None
        self.running = False
    
    def connect_to_rabbitmq(self) -> bool:
        """Establish connection to RabbitMQ with retries."""
        max_retries = 10
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                log.info(
                    "Connecting to RabbitMQ for dustbin verification",
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    host=settings.RABBITMQ_HOST
                )
                
                credentials = pika.PlainCredentials(
                    settings.RABBITMQ_USER,
                    settings.RABBITMQ_PASSWORD.get_secret_value() if hasattr(settings.RABBITMQ_PASSWORD, 'get_secret_value') else str(settings.RABBITMQ_PASSWORD)
                )
                
                parameters = pika.ConnectionParameters(
                    host=settings.RABBITMQ_HOST,
                    credentials=credentials
                )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Declare the dustbin verification queue
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                self.channel.basic_qos(prefetch_count=1)
                
                log.info("Successfully connected to RabbitMQ for dustbin verification")
                return True
                
            except Exception as e:
                log.warning(
                    "RabbitMQ connection failed for dustbin verification",
                    error=str(e),
                    attempt=attempt + 1,
                    retry_in_seconds=retry_delay
                )
                time.sleep(retry_delay)
        
        log.error("Could not connect to RabbitMQ after retries - dustbin verification worker cannot start")
        return False
    
    def process_verification_message(self, ch, method, properties, body):
        """Process a single dustbin verification message."""
        try:
            # Parse the verification payload
            verification_data = json.loads(body.decode('utf-8'))
            
            verification_id = verification_data.get('verification_id')
            file_path = verification_data.get('file_path')
            study_uid = verification_data.get('study_instance_uid')
            sop_uid = verification_data.get('sop_instance_uid')
            task_id = verification_data.get('task_id')
            destinations_to_verify = verification_data.get('destinations_to_verify', [])
            queued_timestamp = verification_data.get('queued_timestamp')
            verification_deadline = verification_data.get('verification_deadline')
            
            log.info(
                "Processing dustbin verification request",
                verification_id=verification_id,
                file_path=file_path,
                study_uid=study_uid,
                sop_uid=sop_uid,
                destinations_count=len(destinations_to_verify),
                queued_timestamp=queued_timestamp
            )
            
            # Check if verification deadline has passed
            deadline = datetime.fromisoformat(verification_deadline)
            if datetime.now() > deadline:
                log.warning(
                    "Verification deadline exceeded - handling expired verification",
                    verification_id=verification_id,
                    deadline=verification_deadline,
                    file_path=file_path
                )
                self._handle_expired_verification(verification_data)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            # Check all destination confirmations
            all_confirmed = True
            confirmed_destinations = []
            failed_destinations = []
            
            for destination in destinations_to_verify:
                confirmation_status = self._check_destination_confirmation(
                    verification_id, destination
                )
                
                if confirmation_status.get('confirmed', False):
                    confirmed_destinations.append(destination)
                elif confirmation_status.get('failed', False):
                    failed_destinations.append(destination)
                else:
                    all_confirmed = False
            
            if all_confirmed and len(confirmed_destinations) == len(destinations_to_verify):
                # All destinations confirmed - safe to move to permanent dustbin
                log.info(
                    "All destinations confirmed - moving to permanent dustbin",
                    verification_id=verification_id,
                    file_path=file_path,
                    confirmed_destinations=confirmed_destinations
                )
                
                success = dustbin_service.move_to_dustbin(
                    source_file_path=file_path,
                    study_instance_uid=study_uid,
                    sop_instance_uid=sop_uid,
                    task_id=task_id,
                    destinations_confirmed=confirmed_destinations,
                    reason="all_destinations_verified"
                )
                
                if success:
                    log.info(
                        "MEDICAL SAFETY: File successfully moved to permanent dustbin after full verification",
                        verification_id=verification_id,
                        file_path=file_path,
                        confirmed_destinations=confirmed_destinations
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                else:
                    log.error(
                        "CRITICAL: Failed to move verified file to permanent dustbin",
                        verification_id=verification_id,
                        file_path=file_path
                    )
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    
            elif failed_destinations:
                # Some destinations failed - handle according to policy
                log.warning(
                    "Some destinations failed confirmation",
                    verification_id=verification_id,
                    file_path=file_path,
                    confirmed_destinations=confirmed_destinations,
                    failed_destinations=failed_destinations
                )
                
                self._handle_partial_verification_failure(verification_data, confirmed_destinations, failed_destinations)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            else:
                # Still waiting for confirmations - requeue with delay
                log.info(
                    "Still waiting for destination confirmations - requeueing",
                    verification_id=verification_id,
                    file_path=file_path,
                    confirmed_count=len(confirmed_destinations),
                    total_destinations=len(destinations_to_verify)
                )
                
                # Requeue with delay (basic_nack with requeue=True)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                time.sleep(30)  # 30-second delay before reprocessing
            
        except json.JSONDecodeError as e:
            log.error(
                "Invalid JSON in dustbin verification message",
                error=str(e),
                body=body.decode('utf-8', errors='replace')
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Discard invalid messages
            
        except Exception as e:
            log.error(
                "Error processing dustbin verification message",
                error=str(e),
                exc_info=True
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def _check_destination_confirmation(self, verification_id: str, destination_name: str) -> Dict[str, Any]:
        """
        Check if a specific destination has confirmed receipt.
        
        Returns:
            Dict with confirmation status
        """
        try:
            # Check for confirmation file
            confirmations_dir = Path("/dicom_data/dustbin/confirmations")
            confirmation_file = confirmations_dir / f"{verification_id}_{destination_name}.json"
            
            if confirmation_file.exists():
                with open(confirmation_file, 'r') as f:
                    confirmation_data = json.load(f)
                
                return {
                    'confirmed': confirmation_data.get('success', False),
                    'failed': not confirmation_data.get('success', True),
                    'timestamp': confirmation_data.get('confirmation_timestamp'),
                    'details': confirmation_data.get('confirmation_details', {})
                }
            
            # Also check database for confirmation status
            # This would be implemented based on your specific tracking system
            
            return {'confirmed': False, 'failed': False, 'pending': True}
            
        except Exception as e:
            log.error(
                "Error checking destination confirmation",
                verification_id=verification_id,
                destination=destination_name,
                error=str(e)
            )
            return {'confirmed': False, 'failed': True, 'error': str(e)}
    
    def _handle_expired_verification(self, verification_data: Dict[str, Any]):
        """Handle verification that has exceeded its deadline."""
        try:
            log.warning(
                "MEDICAL SAFETY: Verification deadline exceeded - keeping file for manual review",
                verification_id=verification_data.get('verification_id'),
                file_path=verification_data.get('file_path'),
                deadline=verification_data.get('verification_deadline')
            )
            
            # Move to manual review area instead of deleting
            # This ensures no medical data is lost due to timeout
            
        except Exception as e:
            log.error(
                "Error handling expired verification",
                verification_data=verification_data,
                error=str(e),
                exc_info=True
            )
    
    def _handle_partial_verification_failure(
        self, 
        verification_data: Dict[str, Any], 
        confirmed_destinations: List[str],
        failed_destinations: List[str]
    ):
        """Handle cases where some destinations confirm but others fail."""
        try:
            log.warning(
                "MEDICAL SAFETY: Partial verification failure - keeping file for manual review",
                verification_id=verification_data.get('verification_id'),
                file_path=verification_data.get('file_path'),
                confirmed=confirmed_destinations,
                failed=failed_destinations
            )
            
            # In medical systems, partial failure usually means keep the file
            # This ensures patient data is preserved even if some destinations fail
            
        except Exception as e:
            log.error(
                "Error handling partial verification failure",
                verification_data=verification_data,
                error=str(e),
                exc_info=True
            )
    
    def start_consuming(self):
        """Start consuming messages from the dustbin verification queue."""
        if not self.connect_to_rabbitmq():
            return False
        
        self.running = True
        
        log.info("Starting dustbin verification worker")
        
        try:
            if not self.channel:
                log.error("Channel is not available for consuming")
                return False
                
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.process_verification_message
            )
            
            log.info(
                "Dustbin verification worker ready - waiting for verification requests",
                queue=self.queue_name
            )
            
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            log.info("Dustbin verification worker stopped by user")
            self.stop_consuming()
            
        except Exception as e:
            log.error(
                "Error in dustbin verification worker",
                error=str(e),
                exc_info=True
            )
            
        finally:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
    
    def stop_consuming(self):
        """Stop consuming messages and clean up."""
        self.running = False
        
        if self.channel:
            self.channel.stop_consuming()
        
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        
        log.info("Dustbin verification worker stopped")


if __name__ == "__main__":
    # Configure basic logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    worker = DustbinVerificationWorker()
    worker.start_consuming()
