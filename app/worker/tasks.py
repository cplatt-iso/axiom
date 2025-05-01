# app/worker/tasks.py
import json
import os
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union
from copy import deepcopy

# Use structlog for logging
import structlog # type: ignore

from celery import shared_task
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.uid import generate_uid

from app.db.session import SessionLocal
from app import crud
from app.db import models
from app.db.models import ProcessedStudySourceType

from app.services.storage_backends import get_storage_backend, StorageBackendError
from app.services import dicomweb_client

from app.core.config import settings

from app.worker.processing_logic import process_dicom_instance

# Get a logger instance configured by structlog (via celery_app.py signals)
logger = structlog.get_logger(__name__) # Use module name


def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the configured error directory with timestamp and task ID."""
    if not isinstance(filepath, Path):
         filepath = Path(filepath)

    log = logger.bind(task_id=task_id, original_filepath=str(filepath)) # Bind context

    if not filepath.is_file():
         log.warning("Source path is not a file or does not exist. Cannot move to error dir.")
         return
    try:
        error_base_dir_str = getattr(settings, 'DICOM_ERROR_PATH', None)
        if error_base_dir_str:
             error_dir = Path(error_base_dir_str)
        else:
             storage_path = getattr(settings, 'DICOM_STORAGE_PATH', '/dicom_data/incoming')
             error_dir = Path(storage_path).parent / "errors"
             log.warning("DICOM_ERROR_PATH not set in config, using fallback", fallback_error_dir=str(error_dir))

        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        log.info("Moved file to error directory", error_path=str(error_path))
    except Exception as move_err:
        # Use logger directly here as 'log' binding might be stale if error_dir assignment failed
        logger.critical(
            "CRITICAL - Could not move file to error dir",
            task_id=task_id, # Repeat context for critical log
            original_filepath=str(filepath),
            target_error_dir=str(error_dir) if 'error_dir' in locals() else 'Unknown',
            error=str(move_err),
            exc_info=True
        )


RETRYABLE_EXCEPTIONS = (
    ConnectionRefusedError,
    StorageBackendError,
    TimeoutError,
    dicomweb_client.DicomWebClientError
)


@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(
    self,
    dicom_filepath_str: str,
    source_type: str,
    source_db_id_or_instance_id: Union[int, str],
    association_info: Optional[Dict[str, str]] = None
):
    """
    Celery task to process a DICOM file (filesystem/listener) with structured logging.
    """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    log_source_identifier = f"{source_type}_{source_db_id_or_instance_id}" # For logging/fallback

    # Bind common context for this task execution
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        original_filepath=str(original_filepath),
        source_type=source_type,
        source_id_or_instance=source_db_id_or_instance_id,
        association_info=association_info or {}
    )

    log.info(f"Received request from source: {log_source_identifier}") # Keep short message, details are bound

    if not original_filepath.is_file():
        log.error("File not found or is not a file. Cannot process.")
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str, "source": log_source_identifier, "source_type": source_type, "source_id": source_db_id_or_instance_id}

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    instance_uid_str = "Unknown"
    source_identifier_for_matching = log_source_identifier # Default

    try:
        db = SessionLocal()

        # --- Determine Source Identifier for Matching ---
        try:
            source_type_enum = ProcessedStudySourceType(source_type)
            if source_type_enum == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(source_db_id_or_instance_id, str):
                listener_config = crud.crud_dimse_listener_config.get_by_instance_id(db, instance_id=source_db_id_or_instance_id)
                if listener_config:
                    source_identifier_for_matching = listener_config.name
                    log.debug("Matched DIMSE Listener config name for instance ID", config_name=source_identifier_for_matching)
                else:
                    log.warning("Could not find listener config for instance ID. Using default source identifier for matching.", default_identifier=log_source_identifier)
            elif source_type_enum == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
                 qr_config = crud.crud_dimse_qr_source.get(db, id=source_db_id_or_instance_id)
                 if qr_config:
                      source_identifier_for_matching = qr_config.name
                      log.debug("Matched DIMSE QR config name for source ID", config_name=source_identifier_for_matching)
                 else:
                     log.warning("Could not find DIMSE QR config for source ID. Using default source identifier.", default_identifier=log_source_identifier)
            # TODO: Add other source types if needed
            else:
                 log.debug("Using default source identifier for matching", default_identifier=log_source_identifier, source_id_type=type(source_db_id_or_instance_id).__name__)
        except ValueError:
             log.error("Invalid source_type received. Using fallback identifier.", received_source_type=source_type)
        except Exception as e:
             log.error("Error looking up source config name. Using fallback identifier.", error=str(e), exc_info=False) # Don't need full trace usually

        # Update bound logger if identifier changed
        log = log.bind(source_identifier_for_matching=source_identifier_for_matching)

        log.debug("Reading DICOM file...")
        try:
            original_ds = pydicom.dcmread(str(original_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            log = log.bind(instance_uid=instance_uid_str) # Bind UID once read
            log.debug("Successfully read file")
        except InvalidDicomError as e:
            log.error("Invalid DICOM file format", error=str(e))
            move_to_error_dir(original_filepath, task_id) # Pass task_id for error move logging
            return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
        except Exception as read_exc:
            log.error("Error reading DICOM file", error=str(read_exc), exc_info=True)
            raise read_exc # Let celery handle retry for other read errors

        log.debug("Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             log.info("No active rulesets found. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    log.info("Deleting file as no rulesets were active (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    log.warning("Failed to delete unmatched file", error=str(e))
             else:
                 log.info("Leaving file as no rulesets were active (DELETE_UNMATCHED_FILES=false).")
             db.close()
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}

        log.debug("Calling core processing logic...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier_for_matching,
                association_info=association_info
            )
        except Exception as proc_exc:
             log.error("Error during core processing logic", error=str(proc_exc), exc_info=True)
             move_to_error_dir(original_filepath, task_id)
             db.close()
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}

        # Log which rules were applied
        log = log.bind(applied_rules_count=len(applied_rules_info), applied_rules=applied_rules_info)

        if not applied_rules_info:
             log.info("No applicable rules matched this source. No actions taken.")
             final_status = "success"
             final_message = f"No matching rules found for source '{source_identifier_for_matching}'"
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    log.info("Deleting file as no rules matched this source (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    log.warning("Failed to delete file with no matching rules", error=str(e))
             else:
                 log.info("Leaving file as no rules matched this source (DELETE_UNMATCHED_FILES=false).")
             db.close()
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}

        log = log.bind(unique_destination_count=len(unique_destination_configs))

        if not unique_destination_configs:
            log.info("Rules matched, but no destinations configured in matched rules.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            delete_original = modified_ds is not None and settings.DELETE_ON_NO_DESTINATION
            if delete_original:
                try:
                    original_filepath.unlink(missing_ok=True)
                    log.info("Deleting original file as rules matched (and modified?), but had no destinations (DELETE_ON_NO_DESTINATION=true).")
                except OSError as e:
                    log.warning("Failed to delete original file after processing with no destinations", error=str(e))
            else:
                log.info("Leaving original file as rules matched but had no destinations (DELETE_ON_NO_DESTINATION=false or no modification).")
            db.close()
            return {"status": final_status, "message": final_message, "applied_rules": [r['name'] for r in applied_rules_info], "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        log.info("Processing unique destinations...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            dest_log = log.bind(destination_index=i+1, destination_config=dest_config) # Bind destination info
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')
            dest_log = dest_log.bind(destination_id=dest_id)

            dest_log.debug("Attempting destination")
            try:
                storage_backend = get_storage_backend(dest_config)
                filename_context = f"{instance_uid_str}.dcm"

                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=original_filepath,
                    filename_context=filename_context,
                    source_identifier=source_identifier_for_matching
                 )
                if store_result == "duplicate":
                    dest_log.info("Destination reported duplicate instance (HTTP 409). Handled.")
                    success_status[dest_id] = {"status": "duplicate", "result": store_result}
                else:
                    dest_log.info("Destination completed successfully", store_result=store_result)
                    success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                dest_log.error("Failed to store to destination", error=str(e), exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                if isinstance(e, RETRYABLE_EXCEPTIONS): raise # Re-raise to trigger Celery retry
            except Exception as e:
                 dest_log.error("Unexpected error during storage to destination", error=str(e), exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}


        if all_destinations_succeeded:
            log.info("All destinations succeeded.")
            if settings.DELETE_ON_SUCCESS:
                 log.info("Deleting original file (DELETE_ON_SUCCESS=true)")
                 try: original_filepath.unlink(missing_ok=True)
                 except OSError as e: log.warning("Failed to delete original file", error=str(e))
            else:
                 log.info("Leaving original file (DELETE_ON_SUCCESS=false)")
            final_status = "success"
            final_message = f"Processed and stored successfully to {len(unique_destination_configs)} destination(s)."

            # --- Increment Processed Count ---
            processed_incremented = False
            local_db = None # Define before try block
            try:
                local_db = SessionLocal()
                if source_type == ProcessedStudySourceType.DIMSE_LISTENER.value and isinstance(source_db_id_or_instance_id, str):
                    log.debug("Incrementing processed count for DIMSE Listener", listener_instance_id=source_db_id_or_instance_id)
                    processed_incremented = crud.crud_dimse_listener_state.increment_processed_count(db=local_db, listener_id=source_db_id_or_instance_id, count=1)
                elif source_type == ProcessedStudySourceType.DIMSE_QR.value and isinstance(source_db_id_or_instance_id, int):
                     log.debug("Incrementing processed count for DIMSE QR source", dimse_qr_source_id=source_db_id_or_instance_id)
                     processed_incremented = crud.crud_dimse_qr_source.increment_processed_count(db=local_db, source_id=source_db_id_or_instance_id, count=1)
                # TODO: Add other source types if needed
                else:
                    log.warning("Unknown source type or mismatched ID type for processed count increment.", source_id_type=type(source_db_id_or_instance_id).__name__)

                if processed_incremented:
                    log.info("Successfully incremented processed count for source.")
                else:
                    # This case might happen if the listener state/source wasn't found during increment
                    log.warning("Failed to increment processed count for source (perhaps source state record missing?).")
                local_db.close() # Close session after use
            except Exception as e:
                log.error("DB Error incrementing processed count for source", error=str(e), exc_info=True)
                if local_db and local_db.is_active:
                     try: local_db.rollback()
                     except Exception as rb_e: log.error("Error rolling back DB session during count increment error handling", rollback_error=str(rb_e))
                     finally: local_db.close()

        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            log.warning("Some destinations failed", failed_count=failed_count, total_destinations=len(unique_destination_configs))
            if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
                 log.info("Moving original file to error directory due to partial failure.")
                 move_to_error_dir(original_filepath, task_id)
            final_status = "partial_failure"
            final_message = f"Processing complete, but {failed_count} destination(s) failed."

        db.close()
        return {
            "status": final_status,
            "message": final_message,
            "applied_rules": applied_rules_info, # <-- CHANGED THIS: Use the list directly
            "destinations": success_status,
            "source": log_source_identifier,
            "instance_uid": instance_uid_str,
            "source_type": source_type,
            "source_id": source_db_id_or_instance_id
        }   
    except Exception as exc:
        # Log using the logger bound with initial context if possible
        log.error("Unhandled exception during task execution", error=str(exc), error_type=type(exc).__name__, exc_info=True)
        if original_filepath.exists(): move_to_error_dir(original_filepath, task_id)
        final_status = "error"
        final_message = f"Fatal error during processing: {exc!r}"
        if db and db.is_active:
             try: db.rollback()
             except Exception as rb_e: logger.error("Error rolling back DB session in main exception handler", rollback_error=str(rb_e), task_id=task_id) # Use base logger
             finally: db.close()
        # Re-raise retryable exceptions for Celery, otherwise return error dict
        if isinstance(exc, RETRYABLE_EXCEPTIONS):
             log.info("Re-raising retryable exception", error_type=type(exc).__name__)
             raise
        else:
            return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
    finally:
        if db and db.is_active:
            # Use base logger here as 'log' context might be misleading if error happened early
            logger.warning("Closing database session in finally block (may indicate early exit or unclosed session).", task_id=task_id)
            db.close()
        # Use bound logger for final status log
        log.info("Finished task processing.", final_status=final_status, final_message=final_message)



@shared_task(bind=True, name="process_dicomweb_metadata_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicomweb_metadata_task(self, source_id: int, study_uid: str, series_uid: str, instance_uid: str):
    """Celery task to process DICOM metadata from DICOMweb source with structured logging."""
    task_id = self.request.id
    # Bind initial context
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        source_id=source_id,
        study_uid=study_uid,
        series_uid=series_uid,
        instance_uid=instance_uid,
        source_type=ProcessedStudySourceType.DICOMWEB_POLLER.value
    )
    log.info("Received request to process DICOMweb instance metadata.")

    db: Optional[Session] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    original_ds: Optional[pydicom.Dataset] = None
    source_identifier_for_matching: str = f"DICOMweb_Source_{source_id}" # Fallback

    try:
        log.debug("Opening database session...")
        db = SessionLocal()
        log.debug("Fetching configuration for DICOMweb source...")
        source_config = crud.dicomweb_source.get(db, id=source_id)
        if not source_config:
            log.error("Could not find configuration for source ID. Cannot fetch metadata.")
            return {"status": "error", "message": f"Configuration for source ID {source_id} not found.", "instance_uid": instance_uid, "source_id": source_id}

        source_identifier_for_matching = source_config.name
        log = log.bind(source_name=source_identifier_for_matching) # Add source name to context
        log.info("Fetching metadata from source...")

        try:
            client = dicomweb_client
            dicom_metadata = client.retrieve_instance_metadata(
                config=source_config, study_uid=study_uid, series_uid=series_uid, instance_uid=instance_uid
            )
            if dicom_metadata is None:
                 log.warning("Metadata not found (or 404). Skipping instance.")
                 db.close()
                 return {"status": "success", "message": "Instance metadata not found (likely deleted).", "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}
            log.debug("Successfully retrieved metadata.")
        except Exception as fetch_exc:
             log.error("Error fetching metadata", error=str(fetch_exc), exc_info=isinstance(fetch_exc, dicomweb_client.DicomWebClientError))
             raise fetch_exc # Let celery handle retry

        log.debug("Converting JSON metadata to pydicom Dataset...")
        try:
            # Assume dicom_metadata is the raw dictionary from JSON parsing
            original_ds = pydicom.dataset.Dataset.from_json(dicom_metadata)
            # Create basic file meta - necessary for some storage backends
            file_meta = pydicom.dataset.FileMetaDataset()
            file_meta.FileMetaInformationVersion = b'\x00\x01'
            # Extract SOP Class UID safely
            sop_class_uid_data = original_ds.get("SOPClassUID")
            sop_class_uid_val = getattr(sop_class_uid_data, 'value', ['1.2.840.10008.5.1.4.1.1.2'])[0] # Default to CT Image Storage if missing/malformed
            file_meta.MediaStorageSOPClassUID = sop_class_uid_val
            file_meta.MediaStorageSOPInstanceUID = instance_uid
            file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian # Assume default if not present in metadata
            file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
            file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
            original_ds.file_meta = file_meta
        except Exception as e:
            log.error("Failed to convert retrieved DICOMweb JSON metadata to Dataset", error=str(e), exc_info=True)
            return {"status": "error", "message": f"Failed to parse retrieved metadata: {e}", "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

        if original_ds is None: # Should not happen if from_json doesn't raise, but check anyway
             log.error("Dataset object is None after conversion block, cannot proceed.")
             return {"status": "error", "message": "Dataset creation failed unexpectedly.", "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

        log.debug("Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             log.info("No active rulesets found. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

        log.debug("Calling core processing logic...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier_for_matching
            )
        except Exception as proc_exc:
             log.error("Error during core processing logic", error=str(proc_exc), exc_info=True)
             db.close()
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

        log = log.bind(applied_rules_count=len(applied_rules_info), applied_rules=[r['name'] for r in applied_rules_info])

        if not applied_rules_info:
             log.info("No applicable rules matched this source. No actions taken.")
             final_status = "success"
             final_message = f"No matching rules found for source '{source_identifier_for_matching}'"
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

        log = log.bind(unique_destination_count=len(unique_destination_configs))

        if not unique_destination_configs:
            log.info("Rules matched, but no destinations configured.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            db.close()
            return {"status": final_status, "message": final_message, "applied_rules": [r['name'] for r in applied_rules_info], "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        log.info("Processing unique destinations...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            dest_log = log.bind(destination_index=i+1, destination_config=dest_config)
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')
            dest_log = dest_log.bind(destination_id=dest_id)

            dest_log.debug("Attempting destination")
            try:
                storage_backend = get_storage_backend(dest_config)
                filename_context = f"{instance_uid}.dcm"

                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=None, # No original file path for DICOMweb
                    filename_context=filename_context,
                    source_identifier=source_identifier_for_matching
                )

                dest_log.info("Destination completed successfully.", store_result=store_result)
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                dest_log.error("Failed to store to destination", error=str(e), exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                if isinstance(e, RETRYABLE_EXCEPTIONS): raise
            except Exception as e:
                 dest_log.error("Unexpected error storing to destination", error=str(e), exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}

        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed instance and dispatched successfully to {len(unique_destination_configs)} destination(s)."

            # --- Increment Processed Count ---
            processed_incremented = False
            local_db = None
            try:
                log.debug("Incrementing processed count for DICOMweb source")
                local_db = SessionLocal()
                # Assuming crud.dicomweb_state exists and has the increment method
                processed_incremented = crud.dicomweb_state.increment_processed_count(db=local_db, source_name=source_identifier_for_matching, count=1)
                local_db.commit() # Commit after successful increment
                local_db.close()
            except Exception as e:
                log.error("DB Error incrementing processed count for DICOMweb source", error=str(e), exc_info=True)
                if local_db and local_db.is_active:
                     try: local_db.rollback()
                     except Exception as rb_e: log.error("Error rolling back DB session during count increment error handling", rollback_error=str(rb_e))
                     finally: local_db.close()

            if processed_incremented:
                log.info("Successfully incremented processed count for source.")
            else:
                log.warning("Failed to increment processed count for source (perhaps state record missing?).")

        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            log.warning("Some destinations failed", failed_count=failed_count, total_destinations=len(unique_destination_configs))
            final_status = "partial_failure"
            final_message = f"Processing complete, but {failed_count} destination(s) failed dispatch."

        db.close()
        return {"status": final_status, "message": final_message, "applied_rules": [r['name'] for r in applied_rules_info], "destinations": success_status, "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

    except Exception as exc:
        log.error("Unhandled exception during task execution", error=str(exc), error_type=type(exc).__name__, exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except Exception as rb_e: logger.error("Error rolling back DB session in main exception handler", rollback_error=str(rb_e), task_id=task_id)
            finally: db.close()
        if isinstance(exc, RETRYABLE_EXCEPTIONS):
             log.info("Re-raising retryable exception", error_type=type(exc).__name__)
             raise
        else:
             final_status = "error"
             final_message = f"Fatal error during processing: {exc!r}"
             return {"status": final_status, "message": final_message, "source_id": source_id, "instance_uid": instance_uid}
    finally:
        if db and db.is_active:
            logger.warning("Closing database session in finally block (may indicate early exit or unclosed session).", task_id=task_id)
            db.close()
        log.info("Finished task processing.", final_status=final_status, final_message=final_message)



@shared_task(bind=True, name="process_stow_instance_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_stow_instance_task(self, temp_filepath_str: str, source_ip: Optional[str] = None):
    """Celery task to process DICOM instance received via STOW-RS with structured logging."""
    task_id = self.request.id
    temp_filepath = Path(temp_filepath_str)
    source_identifier_for_matching = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}"

    # Bind initial context
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        temp_filepath=temp_filepath_str,
        source_ip=source_ip,
        source_identifier=source_identifier_for_matching,
        source_type=ProcessedStudySourceType.STOW_RS.value # Assuming enum exists
    )

    log.info("Received STOW-RS request.")

    if not temp_filepath.is_file():
        log.error("Temporary file not found. Cannot process.")
        return {"status": "error", "message": "Temporary file not found", "filepath": temp_filepath_str, "source": source_identifier_for_matching}

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    instance_uid_str = "Unknown"

    try:
        db = SessionLocal()

        log.debug("Reading DICOM file from temporary path...")
        try:
            original_ds = pydicom.dcmread(str(temp_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            log = log.bind(instance_uid=instance_uid_str) # Add instance UID to context
            log.debug("Successfully read temp file.")
        except InvalidDicomError as e:
            log.error("Invalid DICOM file format received via STOW-RS", error=str(e))
            # No move_to_error for temp files, just report error
            return {"status": "error", "message": f"Invalid DICOM format: {e}", "filepath": temp_filepath_str, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}
        except Exception as read_exc:
            log.error("Error reading DICOM temp file", error=str(read_exc), exc_info=True)
            raise read_exc # Let celery handle retry

        log.debug("Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             log.info("No active rulesets found. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             db.close()
             # Clean up temp file before returning
             if temp_filepath and temp_filepath.exists():
                 try: os.remove(temp_filepath); log.debug("Removed temp file (no active rulesets).")
                 except OSError as e: log.error("Failed to remove temp file (no active rulesets)", error=str(e))
             return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}

        log.debug("Calling core processing logic...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier_for_matching
            )
        except Exception as proc_exc:
             log.error("Error during core processing logic for STOW instance", error=str(proc_exc), exc_info=True)
             db.close()
             # Clean up temp file before returning
             if temp_filepath and temp_filepath.exists():
                 try: os.remove(temp_filepath); log.debug("Removed temp file (processing error).")
                 except OSError as e: log.error("Failed to remove temp file (processing error)", error=str(e))
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "source": source_identifier_for_matching, "instance_uid": instance_uid_str}

        log = log.bind(applied_rules_count=len(applied_rules_info), applied_rules=[r['name'] for r in applied_rules_info])

        if not applied_rules_info:
             log.info("No applicable rules matched this source. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             db.close()
             # Clean up temp file before returning
             if temp_filepath and temp_filepath.exists():
                 try: os.remove(temp_filepath); log.debug("Removed temp file (no matching rules).")
                 except OSError as e: log.error("Failed to remove temp file (no matching rules)", error=str(e))
             return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}

        log = log.bind(unique_destination_count=len(unique_destination_configs))

        if not unique_destination_configs:
            log.info("Rules matched, but no destinations configured.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            db.close()
            # Clean up temp file before returning
            if temp_filepath and temp_filepath.exists():
                 try: os.remove(temp_filepath); log.debug("Removed temp file (no destinations).")
                 except OSError as e: log.error("Failed to remove temp file (no destinations)", error=str(e))
            return {"status": final_status, "message": final_message, "applied_rules": [r['name'] for r in applied_rules_info], "source": source_identifier_for_matching, "instance_uid": instance_uid_str}

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        log.info("Processing unique destinations...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            dest_log = log.bind(destination_index=i+1, destination_config=dest_config)
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')
            dest_log = dest_log.bind(destination_id=dest_id)

            dest_log.debug("Attempting destination")
            try:
                storage_backend = get_storage_backend(dest_config)
                filename_context = f"{instance_uid_str}.dcm"

                # Pass temp_filepath as original_filepath for storage backends that might use it (like filesystem copy)
                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=temp_filepath,
                    filename_context=filename_context,
                    source_identifier=source_identifier_for_matching
                )

                dest_log.info("Destination completed successfully.", store_result=store_result)
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                dest_log.error("Failed to store STOW instance to destination", error=str(e), exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                if isinstance(e, RETRYABLE_EXCEPTIONS): raise
            except Exception as e:
                 dest_log.error("Unexpected error storing STOW instance to destination", error=str(e), exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}

        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed STOW instance and stored successfully to {len(unique_destination_configs)} destination(s)."
            log.info("STOW instance processed successfully. Metrics increment TBD.") # Placeholder for STOW metrics
        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            log.warning("Some destinations failed for STOW instance", failed_count=failed_count, total_destinations=len(unique_destination_configs))
            final_status = "partial_failure"
            final_message = f"Processing complete for STOW instance, but {failed_count} destination(s) failed."

        db.close()
        return {"status": final_status, "message": final_message, "applied_rules": [r['name'] for r in applied_rules_info], "destinations": success_status, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}

    except Exception as exc:
        log.error("Unhandled exception during STOW task execution", error=str(exc), error_type=type(exc).__name__, exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except Exception as rb_e: logger.error("Error rolling back DB session in STOW main exception handler", rollback_error=str(rb_e), task_id=task_id)
            finally: db.close()
        if isinstance(exc, RETRYABLE_EXCEPTIONS):
             log.info("Re-raising retryable exception", error_type=type(exc).__name__)
             raise
        else:
            final_status = "error"
            final_message = f"Fatal error during processing: {exc!r}"
            return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}
    finally:
        # --- Crucial: Clean up the temporary file ---
        if temp_filepath and temp_filepath.exists():
            try:
                os.remove(temp_filepath)
                log.info("Removed temporary STOW-RS file.")
            except OSError as e:
                # Use base logger as 'log' context might be incomplete if error happened early
                logger.critical("FAILED TO REMOVE temporary STOW-RS file", temp_filepath=str(temp_filepath), error=str(e), task_id=task_id)

        if db and db.is_active:
            logger.warning("Closing database session in STOW finally block (may indicate early exit or unclosed session).", task_id=task_id)
            db.close()

        log.info("Finished processing STOW instance.", final_status=final_status, final_message=final_message)
