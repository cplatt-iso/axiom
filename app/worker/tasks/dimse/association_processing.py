# app/worker/tasks/dimse/association_processing.py
from pathlib import Path
from typing import Optional, Dict, Any, Union, List

import structlog
from celery import shared_task
from sqlalchemy.orm import Session
import pydicom

from app import crud
from app.db.session import SessionLocal
from app.schemas.enums import ProcessedStudySourceType
from app.core.config import settings
from app.worker.executors import execute_file_based_task

# Logger setup
logger = structlog.get_logger(__name__)

# Retryable exceptions
RETRYABLE_EXCEPTIONS = (
    ConnectionRefusedError,
    TimeoutError,
    # Add other exceptions here that should cause Celery to retry the *entire task*
    # e.g., certain DB deadlocks or temporary RabbitMQ issues, if not handled by Celery's broker config.
    # For now, keep it simple as you had it.
)


@shared_task(bind=True,
             name="process_dicom_association_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True,
             retry_backoff_max=600,
             retry_jitter=True)
def process_dicom_association_task(self,
                                   dicom_filepaths_list: List[str],
                                   source_type: str,
                                   source_db_id_or_instance_id: Union[int,
                                                                      str],
                                   association_info: Optional[Dict[str,
                                                                   str]] = None):
    """
    Process an entire DICOM association (multiple files) as a single unit.
    This improves efficiency by grouping files that were received together
    and should be sent together, avoiding multiple separate associations to the PACS.
    """
    task_id = self.request.id
    
    # Initialize medical-grade dustbin service for file safety  
    from app.services.dustbin_service import DustbinService
    dustbin_service = DustbinService()
    
    log_source_display_name = f"{source_type}_{source_db_id_or_instance_id}"
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        source_type=source_type,
        source_id_or_instance=str(source_db_id_or_instance_id),
        log_source_display_name=log_source_display_name,
        file_count=len(dicom_filepaths_list))
    log.info("Task started: process_dicom_association_task")

    if not dicom_filepaths_list:
        log.error("No files provided in association. Task cannot proceed.")
        return {
            "status": "error_no_files",
            "message": "No files provided in association",
            "processed_files": [],
            "failed_files": [],
            "source_display_name": log_source_display_name}

    db: Optional[Session] = None
    processed_files = []
    failed_files = []
    overall_success = True
    # NEW: Track processed files by Study UID and destination for study-based
    # batching
    # study_uid -> dest_name -> [file_info]
    study_destination_batches: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    destination_configs: Dict[str, Dict[str, Any]] = {}  # dest_name -> config

    try:
        db = SessionLocal()
        log.info(
            f"Processing association with {len(dicom_filepaths_list)} files.")

        # Process each file in the association
        for i, filepath_str in enumerate(dicom_filepaths_list):
            file_log = log.bind(file_index=i + 1, filepath=filepath_str)
            filepath = Path(filepath_str)

            if not filepath.is_file():
                file_log.error("File not found in association.")
                failed_files.append({
                    "filepath": filepath_str,
                    "error": "File not found",
                    "status": "error_file_not_found"
                })
                overall_success = False
                continue

            try:
                file_log.info("Processing file in association.")

                # Use the existing file processing logic with batching mode
                rules_matched, modifications_made, status_code, message, \
                    applied_rules_info, dest_statuses, processed_ds, \
                    instance_uid, _ = execute_file_based_task(
                        file_log, db, filepath_str, source_type,
                        source_db_id_or_instance_id, f"{task_id}_file_{i + 1}", association_info,
                        # NEW: Don't queue immediately, batch instead & defer cleanup
                        ai_portal=None, queue_immediately=False, defer_file_cleanup=True
                    )

                file_result = {
                    "filepath": filepath_str,
                    "status": status_code,
                    "message": message,
                    "instance_uid": instance_uid,
                    "applied_rules": applied_rules_info,
                    "destinations_processed": dest_statuses
                }

                if not status_code.startswith("success"):
                    overall_success = False
                    file_log.warning("File processing failed in association.")
                    failed_files.append(file_result)
                    continue
                else:
                    file_log.info(
                        "File processing completed successfully in association.")
                    processed_files.append(file_result)

                # NEW: Collect processed files for study-based batch sending
                if processed_ds and dest_statuses:
                    # Get Study Instance UID from the processed dataset
                    from app.worker.task_utils import _safe_get_dicom_value
                    study_uid = _safe_get_dicom_value(
                        processed_ds, "StudyInstanceUID", "UNKNOWN_STUDY")

                    # Skip if study_uid is None or empty
                    if not study_uid:
                        file_log.warning(
                            f"Cannot group file for study batching - no StudyInstanceUID found for instance: {instance_uid}")
                    else:
                        file_log.info(
                            f"Adding file to study batch: Study={study_uid}, Instance={instance_uid}")

                        # Save processed file using medical-grade dustbin system
                        
                        # Save to dustbin instead of directly to processed directory
                        dustbin_filepath = dustbin_service.save_processed_file_to_dustbin(
                            processed_ds=processed_ds,
                            original_filepath=filepath_str,
                            task_id=f"{task_id}_file_{i + 1}",
                            instance_uid=instance_uid or "UNKNOWN_SOP_UID"
                        )
                        processed_filepath = Path(dustbin_filepath)

                        # Group by Study UID and destination for batch sending
                        for dest_name, dest_result in dest_statuses.items():
                            if dest_result.get(
                                    "status") in ["success", "batched"]:  # Only batch successful files per destination
                                # Initialize nested dictionaries if needed
                                if study_uid not in study_destination_batches:
                                    study_destination_batches[study_uid] = {}
                                    file_log.info(
                                        f"Created new study batch group for StudyInstanceUID: {study_uid}")
                                if dest_name not in study_destination_batches[study_uid]:
                                    study_destination_batches[study_uid][dest_name] = [
                                    ]

                                # Store file info with path and metadata
                                file_info = {
                                    "file_path": str(processed_filepath),
                                    "instance_uid": instance_uid,
                                    "original_filepath": filepath_str
                                }
                                study_destination_batches[study_uid][dest_name].append(
                                    file_info)
                                file_log.info(
                                    f"Added file to study batch: Study={study_uid}, Dest={dest_name}, Files={len(study_destination_batches[study_uid][dest_name])}")
                            else:
                                file_log.warning(
                                    f"Skipping file for study batch due to destination failure: Study={study_uid}, Dest={dest_name}, Status={dest_result.get('status')}")

            except Exception as file_exc:
                file_log.error(
                    "Exception processing file in association.",
                    error=str(file_exc),
                    exc_info=True)
                failed_files.append({
                    "filepath": filepath_str,
                    "error": str(file_exc),
                    "status": "error_processing_exception"
                })
                overall_success = False

        # NEW: Log batched files and trigger immediate sends via Redis
        if study_destination_batches:
            total_files = sum(len(files) for study_files in study_destination_batches.values(
            ) for files in study_files.values())
            log.info(
                f"Added files to exam batches: {len(study_destination_batches)} studies, {total_files} total files batched.")

            # Import Redis trigger
            from app.services.redis_batch_triggers import redis_batch_trigger

            # Trigger immediate sends for each study/destination combo via
            # Redis
            for study_uid, destinations in study_destination_batches.items():
                study_file_count = sum(len(files)
                                       for files in destinations.values())
                log.info(
                    f"Study batch added: StudyInstanceUID={study_uid}, Destinations={list(destinations.keys())}, Files={study_file_count}")

                # Get destination IDs and trigger immediate sends
                for dest_name, files in destinations.items():
                    # Find destination ID by name
                    dest_config = crud.crud_storage_backend_config.get_by_name(
                        db, name=dest_name)
                    if dest_config:
                        if dest_config.backend_type == "cstore":
                            log.info(
                                f"Triggering immediate send via Redis for study {study_uid} to destination {dest_name} (ID: {dest_config.id})")
                            try:
                                triggered = redis_batch_trigger.trigger_batch_ready(
                                    study_instance_uid=study_uid,
                                    destination_id=dest_config.id
                                )
                                if triggered:
                                    log.info(
                                        f"Successfully triggered Redis signal for study {study_uid} to {dest_name}")
                                else:
                                    log.warning(
                                        f"No Redis subscribers for study {study_uid} to {dest_name} - batch will remain PENDING")
                            except Exception as redis_exc:
                                log.error(
                                    f"Failed to trigger Redis signal for study {study_uid} to {dest_name}",
                                    error=str(redis_exc),
                                    exc_info=True)
                        else:
                            log.debug(
                                f"Skipping Redis trigger for non-DIMSE destination: {dest_name} (type: {dest_config.backend_type})")
                    else:
                        log.error(
                            f"Destination config not found for name: {dest_name}")

            log.info(
                "Files added to exam_batches table and Redis triggers sent for DIMSE destinations.")
        else:
            log.info(
                "No study batches found - either no successful files or no StudyInstanceUID found.")

        # Update processed counts if overall success
        if overall_success and processed_files:
            try:
                source_type_enum = ProcessedStudySourceType(source_type)
                should_commit = False

                if source_type_enum == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(
                        source_db_id_or_instance_id, str):
                    if crud.crud_dimse_listener_state.increment_processed_count(
                            db=db, listener_id=source_db_id_or_instance_id, count=len(processed_files)):
                        should_commit = True
                elif source_type_enum == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
                    if crud.crud_dimse_qr_source.increment_processed_count(
                            db=db, source_id=source_db_id_or_instance_id, count=len(processed_files)):
                        should_commit = True

                if should_commit:
                    db.commit()
                    log.info(
                        f"Successfully incremented processed count by {len(processed_files)} files.")

            except Exception as db_inc_err:
                log.error(
                    "DB Error during processed count increment.",
                    error=str(db_inc_err),
                    exc_info=True)
                if db and db.is_active:
                    db.rollback()

        final_status = "success_association_processed" if overall_success else "partial_failure_association"
        final_message = f"Association processed: {len(processed_files)} successful, {len(failed_files)} failed"

        log.info("Association processing completed.",
                 successful_files=len(processed_files),
                 failed_files=len(failed_files),
                 overall_success=overall_success)

        # MEDICAL-GRADE SAFETY: Move successfully processed files to dustbin instead of immediate deletion
        if processed_files:
            dustbin_count = 0
            for file_info in processed_files:
                try:
                    filepath = Path(file_info["filepath"])
                    if filepath.exists():
                        # Extract DICOM UIDs from file_info or file directly
                        study_uid = file_info.get("study_instance_uid", "UNKNOWN_STUDY_UID")
                        sop_uid = file_info.get("sop_instance_uid", "UNKNOWN_SOP_UID")
                        destinations_confirmed = file_info.get("destinations_confirmed", [])
                        
                        # If UIDs not in file_info, try to read from file
                        if study_uid == "UNKNOWN_STUDY_UID" or sop_uid == "UNKNOWN_SOP_UID":
                            try:
                                ds = pydicom.dcmread(str(filepath))
                                study_uid = ds.get('StudyInstanceUID', study_uid)
                                sop_uid = ds.get('SOPInstanceUID', sop_uid)
                            except Exception:
                                pass  # Keep the UNKNOWN values
                        
                        success = dustbin_service.move_to_dustbin(
                            source_file_path=str(filepath),
                            study_instance_uid=study_uid,
                            sop_instance_uid=sop_uid,
                            task_id=task_id,
                            destinations_confirmed=destinations_confirmed,
                            reason="association_processing_complete"
                        )
                        
                        if success:
                            dustbin_count += 1
                        else:
                            log.error(
                                "CRITICAL: Failed to move successfully processed file to dustbin - FILE KEPT FOR SAFETY",
                                filepath=str(filepath),
                                study_uid=study_uid,
                                sop_uid=sop_uid,
                                task_id=task_id
                            )
                            
                except Exception as e:
                    log.error(
                        "CRITICAL: Exception while moving processed file to dustbin - FILE KEPT FOR SAFETY",
                        filepath=file_info.get("filepath", "UNKNOWN"),
                        error=str(e),
                        exc_info=True
                    )
            
            if dustbin_count > 0:
                log.info(
                    "MEDICAL SAFETY: Successfully processed files moved to dustbin for verification",
                    files_moved_to_dustbin=dustbin_count,
                    total_processed=len(processed_files)
                )

        return {
            "status": final_status,
            "message": final_message,
            "processed_files": processed_files,
            "failed_files": failed_files,
            "source_display_name": log_source_display_name,
            "total_files": len(dicom_filepaths_list)
        }

    except Exception as exc:
        log.error(
            "Unhandled exception in process_dicom_association_task.",
            error=str(exc),
            exc_info=True)

        if db and db.is_active:
            try:
                db.rollback()
            except Exception:
                pass
            finally:
                db.close()
                db = None

        if any(isinstance(exc, retry_exc_type)
               for retry_exc_type in RETRYABLE_EXCEPTIONS):
            log.info(
                "Caught retryable exception at association task level. Retrying.",
                exc_type=type(exc).__name__)
            raise

        return {
            "status": "fatal_association_task_error",
            "message": f"Fatal error processing association: {exc!r}",
            "processed_files": processed_files,
            "failed_files": failed_files,
            "source_display_name": log_source_display_name,
            "total_files": len(dicom_filepaths_list)
        }

    finally:
        if db and db.is_active:
            db.close()
        log.info("Task finished: process_dicom_association_task.")
