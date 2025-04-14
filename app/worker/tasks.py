# app/worker/tasks.py
import time
import random # Keep for potential delays if needed, remove simulated error
import shutil
import logging
from pathlib import Path
from copy import deepcopy

from celery import shared_task
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag

# Database access
from app.db.session import SessionLocal
from app.crud.crud_rule import ruleset as crud_ruleset # Renamed import
from app.db.models import RuleSetExecutionMode

# Storage backend
from app.services.storage_backends import get_storage_backend, StorageBackendError

# Configuration
from app.core.config import settings

# Get a logger for this module
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def parse_dicom_tag(tag_str: str) -> Tag | None:
    """Parses a string like '(0010,0010)' into a pydicom Tag object."""
    try:
        parts = tag_str.strip("() ").split(',')
        if len(parts) == 2:
            return Tag(int(parts[0], 16), int(parts[1], 16))
        else:
            logger.warning(f"Could not parse tag string: {tag_str}")
            return None
    except ValueError:
        logger.warning(f"Could not parse tag string (ValueError): {tag_str}")
        return None


def check_match(dataset: pydicom.Dataset, criteria: dict) -> bool:
    """
    Checks if a dataset matches the given criteria dictionary.
    Basic implementation: Exact match or wildcard '*' for strings.
    Assumes criteria values are strings for simplicity initially.
    """
    for tag_key, expected_value in criteria.items():
        tag = parse_dicom_tag(tag_key)
        if not tag:
            logger.warning(f"Skipping invalid tag key in match criteria: {tag_key}")
            continue # Skip this criterion if tag is invalid

        # Use get() to avoid KeyError if tag doesn't exist
        actual_value = dataset.get(tag, None)

        if actual_value is None:
            # Tag doesn't exist in dataset, so doesn't match (unless expected_value is also None?)
            # Decide if missing tag means no match (common)
            return False

        # Convert actual value to string for basic comparison
        # TODO: Handle different VRs more robustly (dates, numbers, sequences)
        actual_value_str = str(actual_value.value)
        expected_value_str = str(expected_value) # Ensure expected is also string

        # Simple matching logic
        if expected_value_str == '*': # Wildcard matches anything (if tag exists)
            continue
        elif '*' in expected_value_str: # Basic wildcard support
            # Convert DICOM wildcard ( * ? ) to regex-like (.* .) ?
            pattern = expected_value_str.replace('*', '.*').replace('?', '.')
            # Naive match - needs proper regex for complex cases
            # This simple check looks for start/end wildcards mostly
            if expected_value_str.startswith('*') and expected_value_str.endswith('*'):
                 if expected_value_str.strip('*') not in actual_value_str:
                      return False
            elif expected_value_str.startswith('*'):
                 if not actual_value_str.endswith(expected_value_str.strip('*')):
                      return False
            elif expected_value_str.endswith('*'):
                  if not actual_value_str.startswith(expected_value_str.strip('*')):
                       return False
            # TODO: Implement proper regex matching if needed
            else: # Treat as literal if wildcard isn't start/end (simplistic)
                 if actual_value_str != expected_value_str:
                      return False
        elif actual_value_str != expected_value_str:
            return False # Exact match failed

    return True # All criteria matched


def apply_modifications(dataset: pydicom.Dataset, modifications: dict):
    """
    Applies tag modifications to the dataset IN-PLACE.
    Basic implementation.
    modifications format: {'(gggg,eeee)': 'new_value', '(gggg,eeee)': None} (None deletes)
    """
    # Important: Work on a deep copy to avoid modifying the original dataset
    # if multiple rulesets or FIRST_MATCH=False logic could re-use it.
    # However, if we always work linearly, modifying in-place might be fine,
    # but safer to copy initially. Caller needs to use the return value.
    # ---> DECISION: Modify in-place for now for simplicity, caller manages copies.
    # modified_dataset = deepcopy(dataset) # Use if isolation is needed

    for tag_key, new_value in modifications.items():
        tag = parse_dicom_tag(tag_key)
        if not tag:
            logger.warning(f"Skipping invalid tag key in modifications: {tag_key}")
            continue

        try:
            if new_value is None:
                # Delete the tag if it exists
                if tag in dataset:
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_key} ({tag})")
            else:
                # Add or update the tag
                # Need VR - inferring 'LO' (Long String) is a bad default.
                # Should ideally get VR from data dictionary or config
                # Using existing VR if tag exists, defaulting to LO otherwise (USE WITH CAUTION)
                vr = dataset[tag].VR if tag in dataset else 'LO'
                dataset.add_new(tag, vr, new_value)
                logger.debug(f"Set tag {tag_key} ({tag}) to '{new_value}' (VR: {vr})")
        except Exception as e:
            logger.error(f"Failed to apply modification for tag {tag_key}: {e}", exc_info=True)
            # Decide whether to continue applying other modifications or fail the task
            # For now, log and continue

    # return modified_dataset # Use if deepcopy was used


# --- Celery Task ---

# Define retryable exceptions (e.g., network issues, temporary DB errors)
# Non-retryable: InvalidDicomError, rule parsing errors, permanent storage errors?
RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, StorageBackendError, TimeoutError) # Add DB errors later
# Could check StorageBackendError subtypes if we create them


@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True, # Acknowledge after task completes/fails permanently
             max_retries=3,
             default_retry_delay=60, # Seconds
             autoretry_for=RETRYABLE_EXCEPTIONS, # Auto-retry for specific exceptions
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(self, dicom_filepath_str: str):
    """
    Celery task to process a single DICOM file:
    1. Read DICOM file.
    2. Query active rulesets.
    3. Match rules based on criteria.
    4. Apply tag modifications.
    5. Send to configured destinations.
    6. Clean up original file on full success.
    """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    logger.info(f"Task {task_id}: Received request to process DICOM file: {original_filepath}")

    if not original_filepath.exists():
        logger.error(f"Task {task_id}: File not found: {original_filepath}. Cannot process.")
        # Non-retryable error - file is gone.
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str}

    db: SessionLocal | None = None
    modified_ds: pydicom.Dataset | None = None
    applied_rules_info = [] # Keep track of what happened
    destinations_to_process = []
    success_status = {} # Track success per destination

    try:
        # --- 1. Read DICOM File ---
        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            # Read the file, defer loading pixel data until needed (if ever)
            ds = pydicom.dcmread(original_filepath, stop_before_pixels=True) # Optimization
            # Make a copy to modify, keeping original ds intact for reference if needed
            modified_ds = deepcopy(ds)
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath}: {e}", exc_info=True)
            # Move to error directory - non-retryable
            raise # Re-raise to be caught by outer try/except for error handling

        # --- 2. Get Rules from Database ---
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        rulesets = crud_ruleset.get_active_ordered(db)
        if not rulesets:
             logger.info(f"Task {task_id}: No active rulesets found. Nothing to do for {original_filepath.name}")
             # Treat as success, but nothing was done
             return {"status": "success", "message": "No active rulesets matched", "filepath": dicom_filepath_str}

        # --- 3. Rule Matching & Modification ---
        logger.debug(f"Task {task_id}: Evaluating {len(rulesets)} rulesets...")
        matched_in_ruleset = False

        for ruleset_obj in rulesets:
            logger.debug(f"Task {task_id}: Evaluating RuleSet '{ruleset_obj.name}' (Priority: {ruleset_obj.priority})")
            if not ruleset_obj.rules:
                 logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' has no rules. Skipping.")
                 continue

            matched_rule_in_set = False
            for rule_obj in ruleset_obj.rules: # Rules should be ordered by priority already
                 if not rule_obj.is_active:
                     logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' is inactive. Skipping.")
                     continue

                 logger.debug(f"Task {task_id}: Checking rule '{rule_obj.name}' (Priority: {rule_obj.priority})")
                 try:
                     criteria = rule_obj.match_criteria # JSON field from model
                     if check_match(ds, criteria): # Match against original ds
                         logger.info(f"Task {task_id}: Rule '{ruleset_obj.name}' / '{rule_obj.name}' MATCHED.")
                         matched_rule_in_set = True

                         # Apply modifications (in-place on modified_ds)
                         modifications = rule_obj.tag_modifications
                         if modifications:
                              logger.debug(f"Task {task_id}: Applying modifications for rule '{rule_obj.name}'...")
                              apply_modifications(modified_ds, modifications)
                         else:
                              logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' has no modifications defined.")

                         # Add destinations
                         destinations_to_process.extend(rule_obj.destinations)

                         applied_rules_info.append(f"{ruleset_obj.name}/{rule_obj.name}")

                         # Check ruleset execution mode
                         if ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                             logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                             break # Stop processing rules in this ruleset
                 except Exception as match_exc:
                     logger.error(f"Task {task_id}: Error processing rule '{ruleset_obj.name}/{rule_obj.name}': {match_exc}", exc_info=True)
                     # Decide if this is fatal for the task or just this rule
                     # For now, log and continue to next rule

            if matched_rule_in_set:
                matched_in_ruleset = True
                # If ANY ruleset matched, check global execution mode (if we had one)
                # Or just continue to process all rulesets unless one stops early
                # (Currently only FIRST_MATCH *within* a ruleset is handled)

        # --- 4. Destination Processing ---
        if not destinations_to_process:
            logger.info(f"Task {task_id}: No matching rules generated any destinations for {original_filepath.name}.")
            # Considered success as rules evaluated but no output required
            return {"status": "success", "message": "Rules matched, but no destinations configured", "applied_rules": applied_rules_info, "filepath": dicom_filepath_str}

        logger.info(f"Task {task_id}: Processing {len(destinations_to_process)} destinations for {original_filepath.name}...")
        logger.debug(f"Task {task_id}: Final modified dataset state before storage: {modified_ds.get('PatientName', 'N/A')}, {modified_ds.get('PatientID', 'N/A')}")

        all_destinations_succeeded = True
        for i, dest_config in enumerate(destinations_to_process):
            dest_id = f"Dest_{i+1}_{dest_config.get('type','?')}" # Simple identifier for logging
            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                store_result = storage_backend.store(modified_ds, original_filepath)
                logger.info(f"Task {task_id}: Destination {dest_id} completed. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store to destination {dest_id}: {e}", exc_info=True)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                # Decide: stop on first failure, or try all? Currently trying all.
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error during storage to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}


        # --- 5. Cleanup ---
        if all_destinations_succeeded:
            logger.info(f"Task {task_id}: All destinations succeeded. Deleting original file: {original_filepath}")
            try:
                original_filepath.unlink(missing_ok=True)
            except OSError as e:
                 logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after successful processing: {e}")
                 # Don't fail the whole task for this, but log it.
            final_status = "success"
            final_message = "Processed and stored successfully."
        else:
            logger.warning(f"Task {task_id}: One or more destinations failed. Original file NOT deleted: {original_filepath}")
            # Decide: should this be retried? The retry logic currently wraps the whole process.
            # If a StorageBackendError was raised and caught by autoretry_for, the whole task retries.
            # If a non-retryable error occurred, it won't retry.
            final_status = "partial_failure"
            final_message = "Processing complete, but one or more destinations failed."
            # Re-raise the last critical error? Or just return status? For now, return status.
            # Maybe raise a non-retryable exception here if partial failure is unacceptable?
            # raise Exception("Partial destination failure") # Would trigger MaxRetriesExceededError path

        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status}

    except InvalidDicomError:
         # Specific handling for invalid DICOM - non-retryable
         logger.error(f"Task {task_id}: Invalid DICOM error caught for {original_filepath.name}. Moving to errors.")
         move_to_error_dir(original_filepath, task_id)
         return {"status": "error", "message": "Invalid DICOM file format", "filepath": dicom_filepath_str}
    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception during processing {original_filepath.name}: {exc!r}", exc_info=True)
        try:
            # Check if autoretry handled it. If not, or if max retries exceeded from here...
            if self.request.retries >= self.max_retries:
                 logger.error(f"Task {task_id}: Max retries exceeded for {original_filepath.name}. Moving to errors.")
                 move_to_error_dir(original_filepath, task_id)
                 return {"status": "error", "message": f"Max retries exceeded or fatal error: {exc!r}", "filepath": dicom_filepath_str}
            else:
                 # This path might be hit if autoretry_for didn't catch the exception type.
                 # Manually retry if it seems appropriate, otherwise let it fail.
                 logger.warning(f"Task {task_id}: Non-auto-retried exception encountered. Depending on exception type, may not retry: {exc!r}")
                 # Re-raise to potentially trigger manual retry or fail task.
                 raise exc
        except Exception as final_error:
             # Catch potential errors during error handling itself
             logger.critical(f"Task {task_id}: CRITICAL Error during error handling for {original_filepath.name}: {final_error!r}", exc_info=True)
             return {"status": "error", "message": f"CRITICAL error during error handling: {final_error!r}", "filepath": dicom_filepath_str}


    finally:
        # --- Ensure DB Session is Closed ---
        if db:
            logger.debug(f"Task {task_id}: Closing database session.")
            db.close()


def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the designated error directory."""
    if not filepath.exists():
         logger.warning(f"Task {task_id}: Original file {filepath} not found for moving to error dir.")
         return

    try:
        error_base_dir = Path(settings.DICOM_STORAGE_PATH).parent # Assumes incoming is one level down
        error_dir = error_base_dir / "errors"
        error_dir.mkdir(parents=True, exist_ok=True)
        error_path = error_dir / filepath.name
        shutil.move(str(filepath), str(error_path))
        logger.info(f"Task {task_id}: Moved failed file {filepath.name} to {error_path}")
    except Exception as move_err:
        logger.critical(f"Task {task_id}: CRITICAL - Could not move failed file {filepath.name} to error dir: {move_err}")
