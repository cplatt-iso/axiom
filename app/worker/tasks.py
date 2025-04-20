# app/worker/tasks.py
import time
import random # Keep for potential delays if needed
import shutil
import logging
import json
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
from app.worker.processing_logic import process_dicom_instance

# Get a logger for this module
logger = logging.getLogger(__name__)
# Alternatively, use task logger inside functions: logger = get_task_logger(__name__)


# --- Existing DICOM File Processing Task ---

RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, StorageBackendError, TimeoutError)

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=3,
             default_retry_delay=60,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(self, dicom_filepath_str: str, source_identifier: Optional[str] = None):
    """
    Celery task to process a DICOM file from filesystem: reads file, fetches rules,
    calls processing logic, handles storage destinations, and cleans up.
    """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    # Use provided source or default if None
    effective_source = source_identifier or settings.DEFAULT_SCP_SOURCE_ID # Use configured default if unknown

    logger.info(f"Task {task_id}: Received request for file: {original_filepath} from source: {effective_source}")

    if not original_filepath.is_file(): # Check if it's a file specifically
        logger.error(f"Task {task_id}: File not found or is not a file: {original_filepath}. Cannot process.")
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str, "source": effective_source}

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None # Read the original dataset once
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {} # Track destination success/failure

    try:
        # --- 1. Read DICOM File ---
        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            # force=True helps read files with minor header issues
            original_ds = pydicom.dcmread(str(original_filepath), force=True)
            logger.debug(f"Task {task_id}: Successfully read file. SOP Class: {original_ds.get('SOPClassUID', 'N/A')}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath}: {e}", exc_info=False)
            move_to_error_dir(original_filepath, task_id)
            return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": effective_source}
        except Exception as read_exc:
            logger.error(f"Task {task_id}: Error reading DICOM file {original_filepath}: {read_exc}", exc_info=True)
            move_to_error_dir(original_filepath, task_id)
            return {"status": "error", "message": f"Error reading file: {read_exc}", "filepath": dicom_filepath_str, "source": effective_source}

        # --- 2. Get Rules from Database ---
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud_ruleset.get_active_ordered(db) # Type hint helps
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for {original_filepath.name}. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             # TODO: Add config option: delete file if no rules match? Default: leave it.
             # if settings.DELETE_UNMATCHED_FILES:
             #    try: original_filepath.unlink(missing_ok=True) except OSError as e: logger.warning(...)
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

        # --- 3. Call Processing Logic ---
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{effective_source}'...")
        try:
            # process_dicom_instance expects the dataset, rules, and source ID
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=effective_source
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for {original_filepath.name}: {proc_exc}", exc_info=True)
             move_to_error_dir(original_filepath, task_id) # Treat core logic failure as error
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "filepath": dicom_filepath_str, "source": effective_source}

        # --- 4. Destination Processing ---
        if not applied_rules_info: # No rules matched
             logger.info(f"Task {task_id}: No applicable rules matched for source '{effective_source}' on file {original_filepath.name}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             # Leave original file as no rules matched
             # TODO: Configurable deletion?
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured in matched rules for {original_filepath.name}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            # Modifications might have happened. Delete original if config says so.
            delete_original = modified_ds is not None # Delete if modified, default? Add config flag?
            if delete_original: # TODO: Make deletion configurable
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting original file {original_filepath} as rules matched (and modified?), but had no destinations.")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after processing with no destinations: {e}")
            else:
                logger.info(f"Task {task_id}: Leaving original file {original_filepath} as rules matched but caused no modifications (or deletion disabled) and had no destinations.")

            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "filepath": dicom_filepath_str, "source": effective_source}

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
                store_result = storage_backend.store(dataset_to_send, original_filepath=original_filepath)
                logger.info(f"Task {task_id}: Destination {dest_id} completed. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store to destination {dest_id}: {e}", exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error during storage to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}

        # --- 5. Cleanup ---
        if all_destinations_succeeded:
            logger.info(f"Task {task_id}: All {len(unique_destination_configs)} destinations succeeded. Deleting original file: {original_filepath}")
            try:
                original_filepath.unlink(missing_ok=True) # Delete original on full success
            except OSError as e:
                 logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after successful processing: {e}")
            final_status = "success"
            final_message = f"Processed and stored successfully to {len(unique_destination_configs)} destination(s)."
        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(valid_destination_configs)} destinations failed. Original file NOT deleted: {original_filepath}")
            # TODO: Configurable: move to error on partial failure? Default: leave for retry.
            # move_to_error_dir(original_filepath, task_id)
            final_status = "partial_failure"
            final_message = f"Processing complete, but {failed_count} destination(s) failed."
            # Raise exception if retry on partial failure is desired
            # raise StorageBackendError(f"{failed_count} destination(s) failed.")

        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": effective_source}

    except InvalidDicomError as e:
         logger.error(f"Task {task_id}: Invalid DICOM error re-caught for {original_filepath.name}: {e}.")
         move_to_error_dir(original_filepath, task_id)
         return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": effective_source}
    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception processing {original_filepath.name}: {exc!r}", exc_info=True)
        move_to_error_dir(original_filepath, task_id)
        final_status = "error"
        final_message = f"Fatal error during processing: {exc!r}"
        return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

    finally:
        if db:
            logger.debug(f"Task {task_id}: Closing database session.")
            db.close()
        logger.info(f"Task {task_id}: Finished processing {original_filepath.name}. Final Status: {final_status}. Message: {final_message}")


# --- NEW DICOMweb Metadata Processing Task ---
@shared_task(bind=True, name="process_dicomweb_metadata_task",
             acks_late=True,
             max_retries=3, # Configure retries as needed
             default_retry_delay=60,
             autoretry_for=(StorageBackendError, ConnectionRefusedError), # Add relevant retry exceptions
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
# --- NEW DICOMweb Processing Task ---
@shared_task(bind=True, name="process_dicomweb_metadata_task",
             acks_late=True,
             max_retries=3, # Configure retries as needed
             default_retry_delay=60,
             autoretry_for=(StorageBackendError, ConnectionRefusedError), # Add relevant retry exceptions
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicomweb_metadata_task(self, source_identifier: str, dicom_metadata: Dict[str, Any]):
    """
    Processes DICOM metadata retrieved from a DICOMweb source.
    Converts JSON metadata to Dataset, applies rules, sends to destinations.
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
            # Use compact keys ("00100010") format expected by from_json
            # Ensure the input dicom_metadata uses this format (poller should provide this)
            original_ds = pydicom.dataset.Dataset.from_json(dicom_metadata)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown') # Get UID early

            # --- CORRECTED/SIMPLIFIED file_meta Creation ---
            # Always create a new FileMetaDataset for data coming from JSON metadata
            logger.debug(f"Task {task_id}: Creating default file_meta for dataset.")
            file_meta = pydicom.dataset.FileMetaDataset()

            # Set required File Meta Information Version
            file_meta.FileMetaInformationVersion = b'\x00\x01'

            # Get Class/Instance from dataset, provide reasonable defaults/fallbacks
            sop_class_uid = original_ds.get("SOPClassUID", None)
            file_meta.MediaStorageSOPClassUID = getattr(sop_class_uid, 'value', '1.2.840.10008.5.1.4.1.1.2') # Default to CT
            file_meta.MediaStorageSOPInstanceUID = instance_uid_str if instance_uid_str != 'Unknown' else generate_uid()

            # Set Transfer Syntax - Default to Implicit VR Little Endian for safety
            # DICOMweb metadata doesn't usually specify the original transfer syntax
            file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian

            # Set Implementation details
            file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
            file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"

            # Assign the newly created file_meta to the dataset
            original_ds.file_meta = file_meta
            logger.debug(f"Task {task_id}: Assigned default file_meta to dataset.")
            # --- END CORRECTED/SIMPLIFIED file_meta Creation ---

            logger.debug(f"Task {task_id}: Successfully created dataset. SOP Instance UID: {instance_uid_str}")
        except Exception as e:
            logger.error(f"Task {task_id}: Failed to convert DICOMweb JSON metadata to Dataset: {e}", exc_info=True)
            # Cannot proceed without a dataset
            # Use return instead of raise to prevent Celery retrying unfixable data errors
            return {"status": "error", "message": f"Failed to parse input metadata: {e}", "source": source_identifier}

        # Ensure dataset was created before proceeding
        if original_ds is None:
             # Should have been caught above, but belt-and-suspenders
             logger.error(f"Task {task_id}: Dataset object is None after conversion block, cannot proceed.")
             return {"status": "error", "message": "Dataset creation failed unexpectedly.", "source": source_identifier}


        # 2. Get Rules from Database
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud_ruleset.get_active_ordered(db)
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found. No action needed for instance {instance_uid_str}.")
             final_status = "success"
             final_message = "No active rulesets found"
             # Ensure DB session is closed before returning
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier, "instance_uid": instance_uid_str}

        # 3. Call Core Processing Logic
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{source_identifier}'...")
        try:
            # Process the dataset created from metadata
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds, # Pass the dataset created from metadata
                active_rulesets=active_rulesets,
                source_identifier=source_identifier
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for instance {instance_uid_str}: {proc_exc}", exc_info=True)
             # Decide if this should be retried by Celery or marked as error
             db.close() # Ensure DB session closed on error path
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

        # Use the potentially modified dataset for sending
        dataset_to_send = modified_ds if modified_ds is not None else original_ds

        logger.info(f"Task {task_id}: Processing {len(unique_destination_configs)} unique destinations for instance {instance_uid_str}...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            # Generate dest_id for logging (same as in file task)
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                filename_context = f"{instance_uid_str}.dcm"
                store_result = storage_backend.store(dataset_to_send, original_filepath=None, filename_context=filename_context)
                logger.info(f"Task {task_id}: Destination {dest_id} completed. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store instance {instance_uid_str} to destination {dest_id}: {e}", exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error storing instance {instance_uid_str} to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}

        # 5. Final Status Determination (No cleanup needed for DICOMweb input)
        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed instance {instance_uid_str} and stored successfully to {len(unique_destination_configs)} destination(s)."
        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed for instance {instance_uid_str}.")
            final_status = "partial_failure"
            final_message = f"Processing complete for instance {instance_uid_str}, but {failed_count} destination(s) failed."
            # If retry on partial failure is desired, raise the appropriate exception here
            # raise StorageBackendError(f"{failed_count} destination(s) failed.")

        db.close() # Close session before returning result
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": source_identifier, "instance_uid": instance_uid_str}

    except Exception as exc:
        # Catch-all for truly unexpected errors during the task execution
        logger.error(f"Task {task_id}: Unhandled exception processing DICOMweb metadata from {source_identifier}: {exc!r}", exc_info=True)
        # instance_uid_str might be set if conversion succeeded before outer error
        # Ensure DB session is closed
        if db and db.is_active:
            db.rollback() # Rollback any potential transaction
            db.close()
        # Return error status - Celery might retry based on task config if exception wasn't caught by autoretry_for
        return {"status": "error", "message": f"Fatal error during processing: {exc!r}", "source": source_identifier, "instance_uid": instance_uid_str}
    finally:
        # Ensure DB session is closed in all cases, even if already closed
        if db and db.is_active:
            logger.debug(f"Task {task_id}: Closing database session in finally block.")
            db.close()
        # Log final task status - message might be default if error occurred before final_message was set
        logger.info(f"Task {task_id}: Finished processing DICOMweb instance {instance_uid_str} from {source_identifier}. Final Status: {final_status}.")

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
