# app/worker/tasks.py
import time
import random # Keep for potential delays if needed
import shutil
import logging
import json
import os        # <--- Added import
import tempfile  # <--- Added import (although not used directly in task now, API saves temp file)
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

from celery import shared_task
# Use celery.utils.log.get_task_logger within tasks if preferred, but module logger is often fine too
# from celery.utils.log import get_task_logger
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.uid import generate_uid # <--- Added import
from copy import deepcopy # <--- Added import

# Database access
from app.db.session import SessionLocal
from app.crud.crud_rule import ruleset as crud_ruleset
from app.db import models # <--- Ensure models are imported for type hinting
# from app.db.models import RuleSet # Specific import if preferred

# Storage backend
from app.services.storage_backends import get_storage_backend, StorageBackendError

# Configuration
from app.core.config import settings

# Import the core processing logic
# Note: This core function is expected to handle rule matching, application, and returning destinations
from app.worker.processing_logic import process_dicom_instance as core_process_dicom_instance # Renamed import for clarity

# Get a logger for this module
logger = logging.getLogger(__name__)
# Alternatively, use task logger inside functions: logger = get_task_logger(__name__)


# --- Helper Function ---
def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the designated error directory, adding a timestamp and task ID."""
    # Ensure filepath is a Path object
    if not isinstance(filepath, Path):
         filepath = Path(filepath)

    if not filepath.is_file():
         logger.warning(f"Task {task_id}: Source path {filepath} is not a file or does not exist. Cannot move to error dir.")
         return
    try:
        # Use settings for error path base
        error_base_dir_str = getattr(settings, 'DICOM_ERROR_PATH', None)
        if error_base_dir_str:
             error_dir = Path(error_base_dir_str)
        else:
             # Fallback: relative to incoming storage path's parent
             # Ensure DICOM_STORAGE_PATH is defined
             storage_path = getattr(settings, 'DICOM_STORAGE_PATH', '/dicom_data/incoming')
             error_dir = Path(storage_path).parent / "errors"
             logger.warning(f"Task {task_id}: DICOM_ERROR_PATH not set in config, using fallback: {error_dir}")

        error_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename

        shutil.move(str(filepath), str(error_path))
        logger.info(f"Task {task_id}: Moved file {filepath.name} to {error_path}")
    except Exception as move_err:
        logger.critical(f"Task {task_id}: CRITICAL - Could not move file {filepath.name} to error dir '{error_dir}': {move_err}", exc_info=True)

# --- Constants ---
RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, StorageBackendError, TimeoutError)

# --- Existing DICOM File Processing Task (Filesystem Watcher/Listener) ---

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True, # Acknowledge after task completes (success or failure)
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(self, dicom_filepath_str: str, source_identifier: Optional[str] = None):
    """
    Celery task to process a DICOM file originally from filesystem/listener:
    reads file, fetches rules, calls processing logic, handles storage destinations, and cleans up.
    """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    # Use provided source or default if None. This source ID matches listener config or filesystem watcher ID.
    effective_source = source_identifier or settings.DEFAULT_SCP_SOURCE_ID # Example: "DIMSE_LISTENER_1"

    logger.info(f"Task {task_id}: Received filesystem/listener request for file: {original_filepath} from source: {effective_source}")

    if not original_filepath.is_file(): # Check if it's a file specifically
        logger.error(f"Task {task_id}: File not found or is not a file: {original_filepath}. Cannot process.")
        # Don't retry if file is gone. Mark as error.
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str, "source": effective_source}

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None # Read the original dataset once
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {} # Track destination success/failure
    instance_uid_str = "Unknown"

    try:
        # --- 1. Read DICOM File ---
        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            # force=True helps read files with minor header issues
            original_ds = pydicom.dcmread(str(original_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            logger.debug(f"Task {task_id}: Successfully read file. SOP Instance UID: {instance_uid_str}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath} (UID: {instance_uid_str}): {e}", exc_info=False)
            move_to_error_dir(original_filepath, task_id) # Move invalid file
            # Don't retry invalid files
            return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}
        except Exception as read_exc:
            # Other read errors might be temporary (e.g., network drive issue), allow retry via Celery mechanism
            logger.error(f"Task {task_id}: Error reading DICOM file {original_filepath} (UID: {instance_uid_str}): {read_exc}", exc_info=True)
            # Don't move file yet, allow retry. Raise the exception for Celery to handle.
            raise read_exc # Let Celery handle retry based on autoretry_for or task settings

        # --- 2. Get Rules from Database ---
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud_ruleset.get_active_ordered(db) # Type hint helps
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for instance {instance_uid_str} from {effective_source}. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             # Configurable: delete file if no rules match? Default: leave it.
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting file {original_filepath} as no rulesets were active (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete unmatched file {original_filepath}: {e}")
             else:
                 logger.info(f"Task {task_id}: Leaving file {original_filepath} as no rulesets were active (DELETE_UNMATCHED_FILES=false).")
             db.close() # Close session
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}

        # --- 3. Call Processing Logic ---
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{effective_source}'...")
        try:
            # core_process_dicom_instance expects the dataset, rules, and source ID
            modified_ds, applied_rules_info, unique_destination_configs = core_process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=effective_source # Pass the listener/filesystem source ID
            )
        except Exception as proc_exc:
             # Treat core logic failure as potentially retryable? Or move to error?
             # Let's move to error for now, assuming bad data or logic error
             logger.error(f"Task {task_id}: Error during core processing logic for {original_filepath.name} (UID: {instance_uid_str}): {proc_exc}", exc_info=True)
             move_to_error_dir(original_filepath, task_id)
             db.close() # Close session
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}

        # --- 4. Destination Processing ---
        if not applied_rules_info: # No rules matched this specific source
             logger.info(f"Task {task_id}: No applicable rules matched for source '{effective_source}' on file {original_filepath.name}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             # Leave original file as no rules matched (or delete based on config)
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting file {original_filepath} as no rules matched this source (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete file {original_filepath} with no matching rules: {e}")
             else:
                 logger.info(f"Task {task_id}: Leaving file {original_filepath} as no rules matched this source (DELETE_UNMATCHED_FILES=false).")
             db.close() # Close session
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}

        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured in matched rules for {original_filepath.name}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            # Modifications might have happened. Delete original if configured (or default behavior).
            delete_original = modified_ds is not None and settings.DELETE_ON_NO_DESTINATION # Example config flag
            if delete_original:
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting original file {original_filepath} as rules matched (and modified?), but had no destinations (DELETE_ON_NO_DESTINATION=true).")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after processing with no destinations: {e}")
            else:
                logger.info(f"Task {task_id}: Leaving original file {original_filepath} as rules matched but had no destinations (DELETE_ON_NO_DESTINATION=false or no modification).")
            db.close() # Close session
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}

        # Use the potentially modified dataset for sending
        dataset_to_send = modified_ds if modified_ds is not None else original_ds

        logger.info(f"Task {task_id}: Processing {len(unique_destination_configs)} unique destinations for {original_filepath.name}...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            # Generate dest_id for logging
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                # Pass the potentially modified dataset and original path for context
                # Filename context could be generated here too if needed by storage backend
                filename_context = f"{instance_uid_str}.dcm"
                store_result = storage_backend.store(dataset_to_send, original_filepath=original_filepath, filename_context=filename_context)
                logger.info(f"Task {task_id}: Destination {dest_id} completed. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store to destination {dest_id}: {e}", exc_info=False) # Don't log full trace for expected storage errors
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                # Allow retry if the exception is in RETRYABLE_EXCEPTIONS
                if isinstance(e, RETRYABLE_EXCEPTIONS):
                     raise # Re-raise to trigger Celery retry mechanism
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error during storage to {dest_id}: {e}", exc_info=True) # Log full trace for unexpected
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}
                 # Consider if unexpected errors should be retried
                 # raise e # Re-raise to trigger retry if desired

        # --- 5. Cleanup ---
        if all_destinations_succeeded:
            logger.info(f"Task {task_id}: All {len(unique_destination_configs)} destinations succeeded. Deleting original file: {original_filepath} (DELETE_ON_SUCCESS=true assumed)")
            try:
                # Assume we delete original on full success unless configured otherwise
                if settings.DELETE_ON_SUCCESS:
                     original_filepath.unlink(missing_ok=True)
                else:
                     logger.info(f"Task {task_id}: Leaving original file {original_filepath} as DELETE_ON_SUCCESS is false.")

            except OSError as e:
                 logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after successful processing: {e}")
            final_status = "success"
            final_message = f"Processed and stored successfully to {len(unique_destination_configs)} destination(s)."
        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed. Original file NOT deleted: {original_filepath}")
            # Configurable: move to error on partial failure? Default: leave for potential retry if exceptions were raised
            if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
                 move_to_error_dir(original_filepath, task_id)
            final_status = "partial_failure" # Or "error" if we moved it
            final_message = f"Processing complete, but {failed_count} destination(s) failed."
            # Don't raise exception here if individual storage exceptions were already handled/raised for retry

        db.close() # Close session
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": effective_source, "instance_uid": instance_uid_str}

    except InvalidDicomError as e:
         # This should be caught earlier, but handle defensively
         logger.error(f"Task {task_id}: Invalid DICOM error re-caught for {original_filepath.name}: {e}.")
         # File should have been moved already if caught earlier
         if original_filepath.exists(): move_to_error_dir(original_filepath, task_id)
         if db and db.is_active: db.close()
         return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}
    except Exception as exc:
        # Catch-all for unhandled exceptions during the main flow (e.g., DB connection issues before query)
        logger.error(f"Task {task_id}: Unhandled exception processing {original_filepath.name}: {exc!r}", exc_info=True)
        # Attempt to move to error if file exists and wasn't moved yet
        if original_filepath.exists(): move_to_error_dir(original_filepath, task_id)
        final_status = "error"
        final_message = f"Fatal error during processing: {exc!r}"
        if db and db.is_active:
             try: db.rollback() # Rollback on fatal error
             except: pass # Ignore rollback errors
             finally: db.close()
        # We might return here, or re-raise based on type for Celery retry
        if isinstance(exc, RETRYABLE_EXCEPTIONS):
             raise # Let Celery handle retry
        else:
             # Non-retryable fatal error
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}

    finally:
        # Ensure DB session is closed if it wasn't explicitly closed on success/handled error paths
        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (indicates potential exit before explicit close).")
            db.close()
        logger.info(f"Task {task_id}: Finished processing {original_filepath.name}. Final Status: {final_status}. Message: {final_message}")


# --- DICOMweb Metadata Processing Task ---
@shared_task(bind=True, name="process_dicomweb_metadata_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=(StorageBackendError, ConnectionRefusedError, TimeoutError), # Add TimeoutError
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicomweb_metadata_task(self, source_identifier: str, dicom_metadata: Dict[str, Any]):
    """
    Processes DICOM metadata retrieved from a DICOMweb source (QIDO Poller).
    Converts JSON metadata to Dataset, applies rules, sends to destinations.
    Destinations must fetch the full instance data using provided context (e.g., WADO-RS).
    """
    task_id = self.request.id
    logger.info(f"Task {task_id}: Received DICOMweb metadata from source '{source_identifier}'.")

    db: Optional[Session] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {} # Track destination success/failure
    original_ds: Optional[pydicom.Dataset] = None
    instance_uid_str = "Unknown" # Initialize for logging in case of early failure

    try:
        # 1. Convert Metadata JSON to pydicom Dataset
        logger.debug(f"Task {task_id}: Converting JSON metadata to pydicom Dataset...")
        try:
            # Ensure the input dicom_metadata uses compact keys ("00100010") format
            original_ds = pydicom.dataset.Dataset.from_json(dicom_metadata, bulk_data_threshold=1024) # Use threshold just in case
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown') # Get UID early

            # Create essential file_meta for processing/storage backends
            logger.debug(f"Task {task_id}: Creating minimal file_meta for dataset.")
            file_meta = pydicom.dataset.FileMetaDataset()
            file_meta.FileMetaInformationVersion = b'\x00\x01'
            # Use UIDs from the dataset itself
            sop_class_uid_val = getattr(original_ds.get("SOPClassUID", None), 'value', '1.2.840.10008.5.1.4.1.1.2') # Default CT
            sop_instance_uid_val = instance_uid_str if instance_uid_str != 'Unknown' else generate_uid()
            file_meta.MediaStorageSOPClassUID = sop_class_uid_val
            file_meta.MediaStorageSOPInstanceUID = sop_instance_uid_val
            # Assume Implicit VR LE as transfer syntax often unknown from QIDO metadata
            file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian
            file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
            file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
            original_ds.file_meta = file_meta
            logger.debug(f"Task {task_id}: Assigned minimal file_meta to dataset.")

            logger.debug(f"Task {task_id}: Successfully created dataset from metadata. SOP Instance UID: {instance_uid_str}")
        except Exception as e:
            logger.error(f"Task {task_id}: Failed to convert DICOMweb JSON metadata (UID: {instance_uid_str}) to Dataset: {e}", exc_info=True)
            # Don't retry parsing errors
            return {"status": "error", "message": f"Failed to parse input metadata: {e}", "source": source_identifier, "instance_uid": instance_uid_str}

        # Ensure dataset was created
        if original_ds is None:
             logger.error(f"Task {task_id}: Dataset object is None after conversion block, cannot proceed.")
             return {"status": "error", "message": "Dataset creation failed unexpectedly.", "source": source_identifier, "instance_uid": instance_uid_str}

        # 2. Get Rules from Database
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud_ruleset.get_active_ordered(db)
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found. No action needed for instance {instance_uid_str} from {source_identifier}.")
             final_status = "success"
             final_message = "No active rulesets found"
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier, "instance_uid": instance_uid_str}

        # 3. Call Core Processing Logic
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{source_identifier}'...")
        try:
            # Process the dataset created from metadata
            modified_ds, applied_rules_info, unique_destination_configs = core_process_dicom_instance(
                original_ds=original_ds, # Pass the dataset created from metadata
                active_rulesets=active_rulesets,
                source_identifier=source_identifier # Use the DICOMweb poller source ID
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for instance {instance_uid_str}: {proc_exc}", exc_info=True)
             db.close()
             # Treat core logic errors as non-retryable for metadata task
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "source": source_identifier, "instance_uid": instance_uid_str}

        # 4. Destination Processing
        if not applied_rules_info:
             logger.info(f"Task {task_id}: No applicable rules matched for source '{source_identifier}' on instance {instance_uid_str}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier, "instance_uid": instance_uid_str}

        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured for instance {instance_uid_str}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            db.close()
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "source": source_identifier, "instance_uid": instance_uid_str}

        # Use the potentially modified dataset (metadata + file_meta) for sending context
        dataset_to_send = modified_ds if modified_ds is not None else original_ds

        logger.info(f"Task {task_id}: Processing {len(unique_destination_configs)} unique destinations for instance {instance_uid_str}...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            # Generate dest_id for logging
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            # Add other types if needed
            dest_id = "_".join(part for part in dest_id_parts if part != '?')

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                # Critical: For DICOMweb metadata, original_filepath is None.
                # The storage backend MUST know how to fetch the actual DICOM instance data
                # based on the dataset (containing UIDs) and potentially the source_identifier
                # or configuration passed implicitly via get_storage_backend.
                # We pass dataset_to_send which contains the metadata.
                # filename_context can provide a default filename if backend saves to disk.
                filename_context = f"{instance_uid_str}.dcm"
                store_result = storage_backend.store(
                    dataset_to_send,          # The metadata dataset (potentially modified)
                    original_filepath=None,   # No original file path
                    filename_context=filename_context,
                    source_identifier=source_identifier # Pass source ID for context if needed by backend
                )
                logger.info(f"Task {task_id}: Destination {dest_id} completed for instance {instance_uid_str}. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store instance {instance_uid_str} to destination {dest_id}: {e}", exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                # Allow retry if the exception is in RETRYABLE_EXCEPTIONS
                if isinstance(e, RETRYABLE_EXCEPTIONS):
                     raise # Re-raise to trigger Celery retry
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error storing instance {instance_uid_str} to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}
                 # Consider if unexpected errors should be retried
                 # raise e # Re-raise to trigger retry if desired

        # 5. Final Status Determination (No cleanup needed for DICOMweb input)
        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed instance {instance_uid_str} and dispatched successfully to {len(unique_destination_configs)} destination(s)."
        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed for instance {instance_uid_str}.")
            final_status = "partial_failure"
            final_message = f"Processing complete for instance {instance_uid_str}, but {failed_count} destination(s) failed dispatch."
            # Don't raise exception here if individual storage exceptions were already handled/raised for retry

        db.close() # Close session before returning result
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": source_identifier, "instance_uid": instance_uid_str}

    except Exception as exc:
        # Catch-all for truly unexpected errors during the task execution
        logger.error(f"Task {task_id}: Unhandled exception processing DICOMweb metadata from {source_identifier} (UID: {instance_uid_str}): {exc!r}", exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except: pass
            finally: db.close()
        # Return error status - let Celery handle retry based on task config / exception type
        if isinstance(exc, RETRYABLE_EXCEPTIONS):
            raise # Re-raise for retry
        else:
            # Non-retryable fatal error
             return {"status": "error", "message": f"Fatal error during processing: {exc!r}", "source": source_identifier, "instance_uid": instance_uid_str}
    finally:
        # Ensure DB session is closed in all cases
        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (indicates potential exit before explicit close).")
            db.close()
        logger.info(f"Task {task_id}: Finished processing DICOMweb instance {instance_uid_str} from {source_identifier}. Final Status: {final_status}.")


# --- NEW STOW-RS Instance Processing Task ---
@shared_task(bind=True, name="process_stow_instance_task",
             acks_late=True, # Acknowledge after task completes
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, # Same retryable exceptions as file task
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_stow_instance_task(self, temp_filepath_str: str, source_ip: Optional[str] = None):
    """
    Celery task to process a single DICOM instance file received via STOW-RS.
    The file provided is temporary and MUST be cleaned up by this task.
    """
    task_id = self.request.id
    temp_filepath = Path(temp_filepath_str)
    # Define source identifier for rule matching
    # Using a structured dict might be better long-term, but stick to string for now if other tasks use it
    # effective_source = json.dumps({"type": "STOW_RS", "remote_address": source_ip}) # Option 1: Structured
    effective_source = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}" # Option 2: Simple String ID

    logger.info(f"Task {task_id}: Received STOW-RS request for temp file: {temp_filepath} from source: {effective_source}")

    # Check if temp file exists (it should have been created by the API endpoint)
    if not temp_filepath.is_file():
        logger.error(f"Task {task_id}: Temporary file not found: {temp_filepath}. Cannot process. This might indicate an issue saving the file in the API endpoint.")
        # Don't retry if the temp file is missing.
        return {"status": "error", "message": "Temporary file not found", "filepath": temp_filepath_str, "source": effective_source}

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {} # Track destination success/failure
    instance_uid_str = "Unknown"

    try:
        # --- 1. Read DICOM File (from temporary path) ---
        logger.debug(f"Task {task_id}: Reading DICOM file from temporary path {temp_filepath}...")
        try:
            # Read the full file, as storage backends might need pixels
            original_ds = pydicom.dcmread(str(temp_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            logger.debug(f"Task {task_id}: Successfully read temp file. SOP Instance UID: {instance_uid_str}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file received via STOW-RS: {temp_filepath} (UID: {instance_uid_str}): {e}", exc_info=False)
            # Don't retry invalid DICOM files. Error should have been reported in STOW response.
            # The finally block will clean up the temp file.
            return {"status": "error", "message": f"Invalid DICOM format: {e}", "filepath": temp_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}
        except Exception as read_exc:
            logger.error(f"Task {task_id}: Error reading DICOM temp file {temp_filepath} (UID: {instance_uid_str}): {read_exc}", exc_info=True)
            # This indicates an issue reading the temp file itself, possibly retryable
            raise read_exc # Let Celery handle retry

        # --- 2. Get Rules from Database ---
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud_ruleset.get_active_ordered(db)
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for instance {instance_uid_str} from {effective_source}. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             # Temp file cleaned up in finally block
             db.close()
             return {"status": final_status, "message": final_message, "source": effective_source, "instance_uid": instance_uid_str}

        # --- 3. Call Processing Logic ---
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{effective_source}'...")
        try:
            # Pass the dataset read from the temp file
            modified_ds, applied_rules_info, unique_destination_configs = core_process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=effective_source # Pass the STOW source ID
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for STOW instance {instance_uid_str}: {proc_exc}", exc_info=True)
             db.close()
             # Treat core logic errors as non-retryable?
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "source": effective_source, "instance_uid": instance_uid_str}

        # --- 4. Destination Processing ---
        if not applied_rules_info:
             logger.info(f"Task {task_id}: No applicable rules matched for source '{effective_source}' on instance {instance_uid_str}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             db.close()
             # Temp file cleaned up in finally block
             return {"status": final_status, "message": final_message, "source": effective_source, "instance_uid": instance_uid_str}

        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured for STOW instance {instance_uid_str}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            db.close()
            # Temp file cleaned up in finally block
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "source": effective_source, "instance_uid": instance_uid_str}

        # Use the potentially modified dataset for sending
        dataset_to_send = modified_ds if modified_ds is not None else original_ds

        logger.info(f"Task {task_id}: Processing {len(unique_destination_configs)} unique destinations for STOW instance {instance_uid_str}...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            # Generate dest_id for logging
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                # Pass the dataset and the *temporary* filepath as original_filepath.
                # Storage backends that copy/move need to handle this temporary source.
                filename_context = f"{instance_uid_str}.dcm"
                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=temp_filepath, # Pass temp path!
                    filename_context=filename_context
                )
                logger.info(f"Task {task_id}: Destination {dest_id} completed for instance {instance_uid_str}. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store STOW instance {instance_uid_str} to destination {dest_id}: {e}", exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                # Allow retry if the exception is in RETRYABLE_EXCEPTIONS
                if isinstance(e, RETRYABLE_EXCEPTIONS):
                     raise # Re-raise to trigger Celery retry
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error storing STOW instance {instance_uid_str} to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}
                 # Consider if unexpected errors should be retried
                 # raise e # Re-raise to trigger retry if desired

        # --- 5. Final Status Determination (Cleanup happens in finally) ---
        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed STOW instance {instance_uid_str} and stored successfully to {len(unique_destination_configs)} destination(s)."
        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed for STOW instance {instance_uid_str}.")
            final_status = "partial_failure"
            final_message = f"Processing complete for STOW instance {instance_uid_str}, but {failed_count} destination(s) failed."
            # Don't raise exception here if individual storage exceptions were already handled/raised for retry

        db.close() # Close session before returning result
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": effective_source, "instance_uid": instance_uid_str}

    except Exception as exc:
        # Catch-all for truly unexpected errors during the task execution
        logger.error(f"Task {task_id}: Unhandled exception processing STOW instance from {effective_source} (UID: {instance_uid_str}, File: {temp_filepath}): {exc!r}", exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except: pass
            finally: db.close()
        # Let Celery handle retry based on task config / exception type
        if isinstance(exc, RETRYABLE_EXCEPTIONS):
            raise # Re-raise for retry
        else:
            # Non-retryable fatal error
            final_status = "error"
            final_message = f"Fatal error during processing: {exc!r}"
            return {"status": final_status, "message": final_message, "source": effective_source, "instance_uid": instance_uid_str}

    finally:
        # --- CRITICAL: Clean up the temporary file ---
        if temp_filepath and temp_filepath.exists():
            try:
                os.remove(temp_filepath)
                logger.info(f"Task {task_id}: Removed temporary STOW-RS file: {temp_filepath}")
            except OSError as e:
                # Log critical error if cleanup fails, as this could fill disk space
                logger.critical(f"Task {task_id}: FAILED TO REMOVE temporary STOW-RS file {temp_filepath}: {e}")

        # Ensure DB session is closed if it wasn't explicitly closed on success/handled error paths
        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (indicates potential exit before explicit close).")
            db.close()

        logger.info(f"Task {task_id}: Finished processing STOW instance {instance_uid_str} from {effective_source}. Final Status: {final_status}.")
