#!/usr/bin/env python3
"""
Medical-Grade File Dustbin Service

This service implements a medical-grade file safety system with the following principles:
1. NEVER DELETE A FILE UNLESS ALL DESTINATIONS HAVE CONFIRMED RECEIPT
2. Files are moved to dustbin ONLY after successful delivery to ALL destinations
3. Explicit logging of ALL file operations with full audit trail
4. Dustbin files have retention period for disaster recovery

This prevents the critical race condition where files are deleted while still being processed
or before all destinations have successfully received them.
"""

import os
import json
import uuid
import pika
import shutil
import logging
import structlog
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path

from app.core.config import settings
from app.db.session import SessionLocal
from app import crud

# Configure structured logging
log = structlog.get_logger(__name__)

class DustbinService:
    """
    Medical-grade file management service that ensures no DICOM files are deleted 
    until they have been successfully delivered to ALL their destinations.
    """
    
    def __init__(self):
        # Use external volume path if running outside Docker, container path if inside Docker
        if os.path.exists("/dicom_data"):
            self.dustbin_path = Path("/dicom_data/dustbin")
        else:
            # Running outside Docker - use external volume path
            self.dustbin_path = Path("/home/icculus/axiom/axiom-volumes/dicom_dustbin")
        
        # Only try to create directory if we have permissions
        try:
            self.dustbin_path.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            # If we can't create the directory, log a warning but don't fail
            log.warning(
                "Cannot create dustbin directory - may be running outside Docker environment",
                dustbin_path=str(self.dustbin_path)
            )
        
        self.rabbitmq_queue = "dustbin_verification"
        
    def move_to_dustbin(
        self, 
        source_file_path: str,
        study_instance_uid: Optional[str],
        sop_instance_uid: Optional[str],
        task_id: str,
        destinations_confirmed: List[str],
        reason: str = "processing_complete"
    ) -> bool:
        """
        Move a DICOM file to the dustbin with full audit logging.
        
        Args:
            source_file_path: Full path to the source file
            study_instance_uid: DICOM Study Instance UID
            sop_instance_uid: DICOM SOP Instance UID  
            task_id: Celery task ID for tracking
            destinations_confirmed: List of destination names that confirmed receipt
            reason: Reason for dustbin move
            
        Returns:
            bool: True if successfully moved, False otherwise
        """
        try:
            # Handle None UIDs by generating fallback values
            if not study_instance_uid:
                study_instance_uid = f"UNKNOWN_STUDY_{task_id}"
            if not sop_instance_uid:
                sop_instance_uid = f"UNKNOWN_SOP_{task_id}"
                
            # Create timestamped dustbin directory structure
            now = datetime.now()
            dustbin_dir = self.dustbin_path / now.strftime("%Y%m%d") / study_instance_uid
            dustbin_dir.mkdir(parents=True, exist_ok=True)
            
            # Create dustbin file path
            dustbin_file = dustbin_dir / f"{sop_instance_uid}.dcm"
            
            # Create audit metadata file
            audit_file = dustbin_dir / f"{sop_instance_uid}.json"
            audit_data = {
                "original_path": source_file_path,
                "study_instance_uid": study_instance_uid,
                "sop_instance_uid": sop_instance_uid,
                "task_id": task_id,
                "moved_timestamp": now.isoformat(),
                "destinations_confirmed": destinations_confirmed,
                "reason": reason,
                "retention_until": (now + timedelta(days=30)).isoformat(),  # 30-day retention
                "file_size_bytes": os.path.getsize(source_file_path)
            }
            
            # Move file to dustbin (atomic operation)
            shutil.move(source_file_path, dustbin_file)
            
            # Write audit metadata
            with open(audit_file, 'w') as f:
                json.dump(audit_data, f, indent=2)
            
            log.info(
                "File moved to medical dustbin",
                original_path=source_file_path,
                dustbin_path=str(dustbin_file),
                study_uid=study_instance_uid,
                sop_uid=sop_instance_uid,
                task_id=task_id,
                destinations=destinations_confirmed,
                reason=reason
            )
            
            return True
            
        except Exception as e:
            log.error(
                "CRITICAL: Failed to move file to dustbin",
                original_path=source_file_path,
                study_uid=study_instance_uid,
                sop_uid=sop_instance_uid,
                task_id=task_id,
                error=str(e),
                exc_info=True
            )
            return False
    
    def queue_for_dustbin_verification(
        self,
        file_path: str,
        study_instance_uid: Optional[str],
        sop_instance_uid: Optional[str],
        task_id: str,
        destinations_to_verify: List[str]
    ) -> bool:
        """
        Queue a file for dustbin verification once all destinations confirm receipt.
        
        Args:
            file_path: Path to the file
            study_instance_uid: DICOM Study Instance UID
            sop_instance_uid: DICOM SOP Instance UID
            task_id: Celery task ID
            destinations_to_verify: List of destination names to verify
            
        Returns:
            bool: True if successfully queued, False otherwise
        """
        try:
            # Handle None UIDs by generating fallback values
            if not study_instance_uid:
                study_instance_uid = f"UNKNOWN_STUDY_{task_id}"
            if not sop_instance_uid:
                sop_instance_uid = f"UNKNOWN_SOP_{task_id}"
                
            verification_payload = {
                "verification_id": str(uuid.uuid4()),
                "file_path": file_path,
                "study_instance_uid": study_instance_uid,
                "sop_instance_uid": sop_instance_uid,
                "task_id": task_id,
                "destinations_to_verify": destinations_to_verify,
                "queued_timestamp": datetime.now().isoformat(),
                "verification_deadline": (datetime.now() + timedelta(hours=24)).isoformat()
            }
            
            # Send to RabbitMQ dustbin verification queue
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=settings.RABBITMQ_HOST)
            )
            
            try:
                channel = connection.channel()
                channel.queue_declare(queue=self.rabbitmq_queue, durable=True)
                
                channel.basic_publish(
                    exchange='',
                    routing_key=self.rabbitmq_queue,
                    body=json.dumps(verification_payload),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Make message persistent
                        content_type='application/json'
                    )
                )
                
                log.info(
                    "File queued for dustbin verification",
                    verification_id=verification_payload["verification_id"],
                    file_path=file_path,
                    study_uid=study_instance_uid,
                    sop_uid=sop_instance_uid,
                    destinations=destinations_to_verify
                )
                
                return True
                
            finally:
                if connection.is_open:
                    connection.close()
                    
        except Exception as e:
            log.error(
                "CRITICAL: Failed to queue file for dustbin verification",
                file_path=file_path,
                study_uid=study_instance_uid,
                sop_uid=sop_instance_uid,
                task_id=task_id,
                destinations=destinations_to_verify,
                error=str(e),
                exc_info=True
            )
            return False
    
    def verify_destination_receipt(
        self,
        verification_id: str,
        destination_name: str,
        success: bool,
        confirmation_details: Dict[str, Any]
    ) -> bool:
        """
        Record that a destination has confirmed or failed receipt of a file.
        
        Args:
            verification_id: The verification ID from dustbin queue
            destination_name: Name of the destination
            success: Whether the destination confirmed successful receipt
            confirmation_details: Additional details about the confirmation
            
        Returns:
            bool: True if successfully recorded
        """
        try:
            # This would typically interact with a database or Redis to track confirmations
            # For now, we'll implement a simple file-based tracking system
            
            confirmations_dir = self.dustbin_path / "confirmations"
            confirmations_dir.mkdir(parents=True, exist_ok=True)
            
            confirmation_file = confirmations_dir / f"{verification_id}_{destination_name}.json"
            
            confirmation_data = {
                "verification_id": verification_id,
                "destination_name": destination_name,
                "success": success,
                "confirmation_timestamp": datetime.now().isoformat(),
                "confirmation_details": confirmation_details
            }
            
            with open(confirmation_file, 'w') as f:
                json.dump(confirmation_data, f, indent=2)
            
            log.info(
                "Destination receipt verification recorded",
                verification_id=verification_id,
                destination=destination_name,
                success=success,
                details=confirmation_details
            )
            
            return True
            
        except Exception as e:
            log.error(
                "Failed to record destination verification",
                verification_id=verification_id,
                destination=destination_name,
                success=success,
                error=str(e),
                exc_info=True
            )
            return False
    
    def cleanup_expired_dustbin_files(self, dry_run: bool = True) -> Dict[str, Any]:
        """
        Clean up dustbin files that have exceeded their retention period.
        
        Args:
            dry_run: If True, only report what would be deleted without actually deleting
            
        Returns:
            Dict with cleanup statistics
        """
        stats = {
            "files_scanned": 0,
            "files_eligible_for_deletion": 0,
            "files_deleted": 0,
            "bytes_freed": 0,
            "errors": []
        }
        
        try:
            now = datetime.now()
            
            for audit_file in self.dustbin_path.rglob("*.json"):
                stats["files_scanned"] += 1
                
                try:
                    with open(audit_file, 'r') as f:
                        audit_data = json.load(f)
                    
                    retention_until = datetime.fromisoformat(audit_data["retention_until"])
                    
                    if now > retention_until:
                        stats["files_eligible_for_deletion"] += 1
                        
                        # Find corresponding DICOM file
                        dcm_file = audit_file.with_suffix('.dcm')
                        
                        if dcm_file.exists():
                            file_size = dcm_file.stat().st_size
                            
                            if not dry_run:
                                dcm_file.unlink()  # Delete DICOM file
                                audit_file.unlink()  # Delete audit file
                                stats["files_deleted"] += 1
                                stats["bytes_freed"] += file_size
                                
                                log.info(
                                    "Dustbin file deleted after retention period",
                                    dcm_file=str(dcm_file),
                                    audit_file=str(audit_file),
                                    retention_until=retention_until.isoformat(),
                                    file_size_bytes=file_size
                                )
                            else:
                                log.info(
                                    "DRY RUN: Would delete dustbin file",
                                    dcm_file=str(dcm_file),
                                    audit_file=str(audit_file),
                                    retention_until=retention_until.isoformat(),
                                    file_size_bytes=file_size
                                )
                        
                except Exception as e:
                    error_msg = f"Error processing {audit_file}: {str(e)}"
                    stats["errors"].append(error_msg)
                    log.error("Error during dustbin cleanup", file=str(audit_file), error=str(e))
            
            log.info(
                "Dustbin cleanup completed",
                dry_run=dry_run,
                **{k: v for k, v in stats.items() if k != "errors"}
            )
            
        except Exception as e:
            error_msg = f"Critical error during dustbin cleanup: {str(e)}"
            stats["errors"].append(error_msg)
            log.error("Critical error during dustbin cleanup", error=str(e), exc_info=True)
        
        return stats
    
    def save_processed_file_to_dustbin(self, processed_ds, original_filepath: str, 
                                     task_id: str, instance_uid: str) -> str:
        """
        Save a processed DICOM dataset to dustbin with proper directory structure.
        Creates: STUDYDATE/STUDYINSTANCEUID/SOPINSTANCEUID.dcm
        
        Args:
            processed_ds: The processed pydicom dataset
            original_filepath: Path to the original file
            task_id: Task ID for tracking
            instance_uid: DICOM Instance UID
        
        Returns:
            str: Path to the saved file in dustbin
        """
        try:
            # Extract DICOM metadata for directory structure
            study_date = getattr(processed_ds, 'StudyDate', 'UNKNOWN_DATE')
            study_uid = getattr(processed_ds, 'StudyInstanceUID', 'UNKNOWN_STUDY')
            sop_instance_uid = getattr(processed_ds, 'SOPInstanceUID', instance_uid)
            
            # Create directory structure: STUDYDATE/STUDYINSTANCEUID/
            study_dir = self.dustbin_path / study_date / study_uid
            study_dir.mkdir(parents=True, exist_ok=True)
            
            # Create filename: SOPINSTANCEUID.dcm
            dustbin_filename = f"{sop_instance_uid}.dcm"
            dustbin_filepath = study_dir / dustbin_filename
            
            # Save the processed dataset to dustbin
            processed_ds.save_as(str(dustbin_filepath), write_like_original=False)
            
            log.info(
                "Processed file saved to dustbin with proper directory structure",
                original_path=original_filepath,
                dustbin_path=str(dustbin_filepath),
                study_date=study_date,
                study_uid=study_uid,
                sop_instance_uid=sop_instance_uid,
                task_id=task_id
            )
            
            return str(dustbin_filepath)
            
        except Exception as e:
            error_msg = f"Failed to save processed file to dustbin: {str(e)}"
            log.error(
                "Critical error saving processed file to dustbin",
                error=error_msg,
                original_path=original_filepath,
                task_id=task_id,
                instance_uid=instance_uid,
                exc_info=True
            )
            raise Exception(error_msg)


# Global service instance
dustbin_service = DustbinService()
