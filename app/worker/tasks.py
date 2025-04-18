# app/worker/tasks.py
import time
import random # Keep for potential delays if needed
import shutil
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any # Added imports

from celery import shared_task
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError

# Database access
from app.db.session import SessionLocal
from app.crud.crud_rule import ruleset as crud_ruleset
from app.db.models import RuleSet # Import RuleSet model for type hinting

# Storage backend
from app.services.storage_backends import get_storage_backend, StorageBackendError

# Configuration
from app.core.config import settings

# Import the new processing logic
from app.worker.processing_logic import process_dicom_instance # <-- Import new function

# Get a logger for this module
logger = logging.getLogger(__name__)


# --- Celery Task (Refactored) ---

RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, StorageBackendError, TimeoutError)

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=3,
             default_retry_delay=60,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(self, dicom_filepath_str: str, source_identifier: Optional[str] = None):
    """
    Celery task to process a DICOM file: reads file, fetches rules,
    calls processing logic, handles storage destinations, and cleans up.
    """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    # Use provided source or default if None
    effective_source = source_identifier or "unknown"

    logger.info(f"Task {task_id}: Received request for file: {original_filepath} from source: {effective_source}")

    if not original_filepath.exists():
        logger.error(f"Task {task_id}: File not found: {original_filepath}. Cannot process.")
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
            with open(original_filepath, 'rb') as fp:
                 original_ds = pydicom.dcmread(fp, force=True)
            logger.debug(f"Task {task_id}: Successfully read file. SOP Class: {original_ds.get('SOPClassUID', 'N/A')}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath}: {e}", exc_info=False) # Don't need full trace usually
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
        # Fetch active rulesets ordered by priority, with rules eagerly loaded
        active_rulesets: List[RuleSet] = crud_ruleset.get_active_ordered(db) # Type hint helps
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for {original_filepath.name}. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             # Configurable: delete original if no rules? For now, leave it.
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

        # --- 3. Call Processing Logic ---
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{effective_source}'...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=effective_source
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic: {proc_exc}", exc_info=True)
             move_to_error_dir(original_filepath, task_id) # Treat core logic failure as error
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "filepath": dicom_filepath_str, "source": effective_source}

        # --- 4. Destination Processing ---
        if not applied_rules_info: # No rules matched
             logger.info(f"Task {task_id}: No applicable rules matched for source '{effective_source}' on file {original_filepath.name}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             # Leave original file as no rules matched
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured in matched rules for {original_filepath.name}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            # If modifications happened (modified_ds is not None), delete original. Otherwise, leave it.
            if modified_ds is not None:
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting original file {original_filepath} as rules matched and modified, but had no destinations.")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after processing with no destinations: {e}")
            else:
                logger.info(f"Task {task_id}: Leaving original file {original_filepath} as rules matched but caused no modifications and had no destinations.")

            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "filepath": dicom_filepath_str, "source": effective_source}

        # If we have destinations, we must have matched rules, and modified_ds might or might not exist (if rules only added destinations)
        # Use the modified dataset if it exists, otherwise use the original (e.g., if rules only route, no mods)
        dataset_to_send = modified_ds if modified_ds is not None else original_ds

        logger.info(f"Task {task_id}: Processing {len(unique_destination_configs)} unique destinations for {original_filepath.name}...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            # Add other types here...
            dest_id = "_".join(part for part in dest_id_parts if part != '?') # Cleaner ID


            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                # Pass the potentially modified dataset
                store_result = storage_backend.store(dataset_to_send, original_filepath) # Pass original filepath for context if needed by backend
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
                original_filepath.unlink(missing_ok=True)
            except OSError as e:
                 logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath}: {e}")
            final_status = "success"
            final_message = f"Processed and stored successfully to {len(unique_destination_configs)} destination(s)."
        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed. Original file NOT deleted: {original_filepath}")
            # Move to error dir if any destination fails? Or leave in place? Let's leave in place for retry.
            # move_to_error_dir(original_filepath, task_id) # Optional: Move on partial failure
            final_status = "partial_failure" # Or maybe "error" to trigger retry if configured?
            final_message = f"Processing complete, but {failed_count} destination(s) failed."
            # Raise an exception if you want Celery's retry mechanism for partial failures
            # raise StorageBackendError(f"{failed_count} destination(s) failed.")

        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": effective_source}

    except InvalidDicomError as e:
         # Should be caught during initial read, but keep as safeguard
         logger.error(f"Task {task_id}: Invalid DICOM error re-caught for {original_filepath.name}: {e}.")
         move_to_error_dir(original_filepath, task_id)
         return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": effective_source}
    except Exception as exc:
        # Catch-all for unexpected errors (e.g., DB connection issues before processing)
        logger.error(f"Task {task_id}: Unhandled exception processing {original_filepath.name}: {exc!r}", exc_info=True)
        move_to_error_dir(original_filepath, task_id)
        final_status = "error"
        final_message = f"Fatal error during processing: {exc!r}"
        # Celery's autoretry should handle DB connection errors etc. if configured
        return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

    finally:
        if db:
            logger.debug(f"Task {task_id}: Closing database session.")
            db.close()
        logger.info(f"Task {task_id}: Finished processing {original_filepath.name}. Final Status: {final_status}. Message: {final_message}")


def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the designated error directory, adding a timestamp and task ID."""
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
             error_dir = Path(settings.DICOM_STORAGE_PATH).parent / "errors"
             logger.warning(f"Task {task_id}: DICOM_ERROR_PATH not set in config, using fallback: {error_dir}")

        error_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename

        shutil.move(str(filepath), str(error_path))
        logger.info(f"Task {task_id}: Moved file {filepath.name} to {error_path}")
    except Exception as move_err:
        logger.critical(f"Task {task_id}: CRITICAL - Could not move file {filepath.name} to error dir: {move_err}", exc_info=True)
