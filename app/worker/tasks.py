# app/worker/tasks.py
import time
# import random # Remove random simulation parts
import shutil
import logging
from pathlib import Path
from copy import deepcopy
# --- ADD Typing Imports ---
from typing import Union, Optional, List, Dict, Any
# --- End Add Typing Imports ---

from celery import shared_task
import pydicom
from pydicom.errors import InvalidDicomError
# Ensure Tag is imported
from pydicom.tag import Tag

# Database access
from app.db.session import SessionLocal
# Use the CRUD object directly as imported in app/crud/__init__.py
from app.crud import ruleset as crud_ruleset
from app.db.models import RuleSetExecutionMode

# Storage backend
from app.services.storage_backends import get_storage_backend, StorageBackendError

# Configuration
from app.core.config import settings

# Get a logger for this module
logger = logging.getLogger(__name__)


# --- Helper Functions ---

# --- FIX Type Hint ---
# Use Optional[Tag] which comes from `typing`
def parse_dicom_tag(tag_str: str) -> Optional[Tag]:
# --- End FIX Type Hint ---
    """Parses a string like '(0010,0010)' into a pydicom Tag object."""
    try:
        # Check if tag_str is already a Tag object (defensive check)
        if isinstance(tag_str, Tag):
            return tag_str
        parts = tag_str.strip("() ").split(',')
        if len(parts) == 2:
            return Tag(int(parts[0], 16), int(parts[1], 16))
        else:
            logger.warning(f"Could not parse non-standard tag string: {tag_str}")
            return None
    except ValueError:
        logger.warning(f"Could not parse tag string (ValueError): {tag_str}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing tag string '{tag_str}': {e}", exc_info=True)
        return None


def check_match(dataset: pydicom.Dataset, criteria: dict) -> bool:
    """
    Checks if a dataset matches the given criteria dictionary.
    Basic implementation: Exact match or wildcard '*' for strings.
    Assumes criteria values are strings for simplicity initially.
    """
    if not criteria: # Empty criteria always matches? Or never matches? Assume matches.
        return True

    for tag_key, expected_value in criteria.items():
        tag = parse_dicom_tag(tag_key)
        if not tag:
            logger.warning(f"Skipping invalid tag key in match criteria: {tag_key}")
            continue # Skip this criterion if tag is invalid

        actual_value = dataset.get(tag, None)

        if actual_value is None:
            # Tag doesn't exist in dataset. Does it match if expected_value is also None?
            # Let's decide: If criteria expects null/None, and tag is missing, consider it a match.
            if expected_value is None:
                continue # Tag missing and expected to be missing (or null), matches.
            else:
                return False # Tag missing, but criteria expected a value. No match.

        # If we reach here, actual_value is not None (it has a value).
        if expected_value is None:
             return False # Tag has a value, but criteria expected it to be missing/None. No match.

        # Convert actual value to string for basic comparison
        # TODO: Handle different VRs more robustly (dates, numbers, sequences)
        actual_value_str = str(actual_value.value)
        expected_value_str = str(expected_value) # Ensure expected is also string

        # Simple matching logic (case-sensitive for now)
        if expected_value_str == '*': # Wildcard matches anything (if tag exists and value not None)
            continue
        elif '*' in expected_value_str:
             # Simple wildcard: only check start/end for now
            if expected_value_str.startswith('*') and expected_value_str.endswith('*'):
                 if expected_value_str.strip('*') not in actual_value_str:
                      return False
            elif expected_value_str.startswith('*'):
                 if not actual_value_str.endswith(expected_value_str.lstrip('*')):
                      return False
            elif expected_value_str.endswith('*'):
                  if not actual_value_str.startswith(expected_value_str.rstrip('*')):
                       return False
            else: # Treat as literal if wildcard isn't only start/end
                 if actual_value_str != expected_value_str:
                      return False
        elif actual_value_str != expected_value_str:
            return False # Exact match failed

    return True # All criteria matched


def apply_modifications(dataset: pydicom.Dataset, modifications: dict):
    """
    Applies tag modifications to the dataset IN-PLACE.
    modifications format: {'(gggg,eeee)': 'new_value', '(gggg,eeee)': None} (None deletes)
    """
    if not modifications:
        return # Nothing to do

    logger.debug(f"Applying modifications: {modifications}")
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
                    logger.debug(f"Tag {tag_key} ({tag}) not present, cannot delete.")
            else:
                # Add or update the tag
                # CRITICAL TODO: Handle VR correctly! Lookup VR from dict or require in modifications.
                # Using existing VR if tag exists, defaulting to 'LO' otherwise (HIGHLY UNSAFE)
                if tag in dataset:
                    # Existing tag: Check if VR needs update? Generally no, just update value.
                    dataset[tag].value = new_value
                    logger.debug(f"Updated tag {tag_key} ({tag}) to '{new_value}'")
                else:
                    # New tag: Need VR. Defaulting to 'LO' is wrong long-term.
                    vr = 'LO' # FIXME: Determine VR properly
                    # Use DataElement instead of add_new for more control if needed
                    dataset.add_new(tag, vr, new_value)
                    logger.warning(f"Added new tag {tag_key} ({tag}) = '{new_value}' with guessed VR '{vr}'") # Warn about guessing VR

        except Exception as e:
            logger.error(f"Failed to apply modification for tag {tag_key} ('{new_value}'): {e}", exc_info=True)
            # Decide whether to continue applying other modifications or fail the task
            # For now, log and continue


# --- Celery Task ---

# Define retryable exceptions (e.g., network issues, temporary DB errors)
# Consider adding specific database errors if applicable (e.g., from psycopg2.errors)
RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, ConnectionAbortedError, ConnectionResetError, TimeoutError,
                        StorageBackendError) # Add DB/other transient errors later if needed

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=3,
             default_retry_delay=60,
             autoretry_for=RETRYABLE_EXCEPTIONS,
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
    logger.info(f"Task {task_id}: Starting processing for DICOM file: {original_filepath}")

    if not original_filepath.exists():
        logger.error(f"Task {task_id}: File not found: {original_filepath}. Cannot process.")
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str}

    db: SessionLocal | None = None
    original_ds: pydicom.Dataset | None = None
    modified_ds: pydicom.Dataset | None = None
    applied_rules_info = []
    destinations_to_process = []
    success_status = {}

    try:
        # --- 1. Read DICOM File ---
        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            original_ds = pydicom.dcmread(original_filepath, stop_before_pixels=False) # Read full file for now
            # Deep copy for modifications - ensures original is unchanged for matching
            modified_ds = deepcopy(original_ds)
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath}: {e}", exc_info=True)
            raise # Re-raise to be caught by outer try/except for error handling

        # --- 2. Get Rules from Database ---
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        rulesets = crud_ruleset.get_active_ordered(db)
        if not rulesets:
             logger.info(f"Task {task_id}: No active rulesets found. Processing complete for {original_filepath.name}.")
             return {"status": "success", "message": "No active rulesets configured", "filepath": dicom_filepath_str}

        # --- 3. Rule Matching & Modification ---
        logger.debug(f"Task {task_id}: Evaluating {len(rulesets)} rulesets...")
        matched_a_rule = False # Track if *any* rule matched across all sets

        for ruleset_obj in rulesets:
            logger.debug(f"Task {task_id}: Evaluating RuleSet '{ruleset_obj.name}' (ID: {ruleset_obj.id}, Priority: {ruleset_obj.priority})")
            if not ruleset_obj.is_active: # Double check, though query filters
                 logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' is inactive. Skipping.")
                 continue
            if not ruleset_obj.rules:
                 logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' has no rules. Skipping.")
                 continue

            # Flag to track match within the current ruleset for FIRST_MATCH logic
            matched_in_this_ruleset = False
            # Rules are already ordered by priority due to model relationship definition
            for rule_obj in ruleset_obj.rules:
                 if not rule_obj.is_active:
                     logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' (ID: {rule_obj.id}) is inactive. Skipping.")
                     continue

                 logger.debug(f"Task {task_id}: Checking rule '{rule_obj.name}' (ID: {rule_obj.id}, Priority: {rule_obj.priority})")
                 try:
                     criteria = rule_obj.match_criteria
                     if not criteria:
                          logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' has no criteria, assuming match.")
                          is_match = True
                     else:
                          is_match = check_match(original_ds, criteria) # Match against the original dataset

                     if is_match:
                         logger.info(f"Task {task_id}: Rule '{ruleset_obj.name}' / '{rule_obj.name}' MATCHED.")
                         matched_in_this_ruleset = True
                         matched_a_rule = True # Mark that at least one rule matched overall

                         # Apply modifications (in-place on modified_ds)
                         modifications = rule_obj.tag_modifications
                         if modifications:
                              apply_modifications(modified_ds, modifications)
                         else:
                              logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' has no modifications.")

                         # Add destinations from this rule to the list to process
                         destinations_to_process.extend(rule_obj.destinations or []) # Handle potentially null/empty list

                         applied_rules_info.append(f"RuleSet: {ruleset_obj.name} (ID: {ruleset_obj.id}) / Rule: {rule_obj.name} (ID: {rule_obj.id})")

                         # Check ruleset execution mode
                         if ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                             logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                             break # Stop processing further rules in THIS ruleset

                 except Exception as match_exc:
                     # Log error but continue evaluating other rules/rulesets
                     logger.error(f"Task {task_id}: Error processing matching/modification for rule '{ruleset_obj.name}/{rule_obj.name}': {match_exc}", exc_info=True)

            # If in FIRST_MATCH mode and a rule in this set matched, don't evaluate subsequent rulesets
            if ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH and matched_in_this_ruleset:
                 logger.debug(f"Task {task_id}: FIRST_MATCH triggered in ruleset '{ruleset_obj.name}'. Stopping ruleset evaluation.")
                 break # Stop processing further rulesets

        # --- 4. Destination Processing ---
        if not matched_a_rule:
            logger.info(f"Task {task_id}: No rules matched for {original_filepath.name}. Processing complete.")
            return {"status": "success", "message": "No matching rules found", "filepath": dicom_filepath_str}

        if not destinations_to_process:
            logger.info(f"Task {task_id}: Matching rules were found, but none configured destinations for {original_filepath.name}.")
            # If modifications were applied, we might still want to save it somewhere? Or discard?
            # Current logic: considered success, but nothing stored. Original file cleanup depends on destination success.
            return {"status": "success", "message": "Rules matched, but no destinations configured", "applied_rules": applied_rules_info, "filepath": dicom_filepath_str}

        # Deduplicate destinations if necessary? Or allow sending multiple times? Currently allows multiple.
        logger.info(f"Task {task_id}: Processing {len(destinations_to_process)} destination actions for {original_filepath.name}...")
        logger.debug(f"Task {task_id}: Final dataset PatientName: {modified_ds.get('PatientName', 'N/A')}, PatientID: {modified_ds.get('PatientID', 'N/A')}")

        all_destinations_succeeded = True
        for i, dest_config in enumerate(destinations_to_process):
            if not isinstance(dest_config, dict) or not dest_config.get("type"):
                logger.error(f"Task {task_id}: Invalid destination config at index {i}: {dest_config}. Skipping.")
                all_destinations_succeeded = False # Mark failure as config is bad
                success_status[f"Dest_{i+1}_Invalid"] = {"status": "error", "message": "Invalid destination config format"}
                continue

            dest_id = f"Dest_{i+1}_{dest_config['type']}"
            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                # Ensure the modified dataset has file_meta for backends that might need it
                if not hasattr(modified_ds, 'file_meta'):
                     modified_ds.file_meta = deepcopy(original_ds.file_meta) # Copy from original if missing
                store_result = storage_backend.store(modified_ds, original_filepath)
                logger.info(f"Task {task_id}: Destination {dest_id} completed successfully.")
                success_status[dest_id] = {"status": "success", "result": str(store_result)} # Ensure result is serializable
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store to destination {dest_id} (StorageBackendError): {e}", exc_info=False) # Log less verbosely for expected storage errors
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error during storage to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}

        # --- 5. Cleanup ---
        if all_destinations_succeeded:
            logger.info(f"Task {task_id}: All {len(destinations_to_process)} destinations succeeded. Deleting original file: {original_filepath}")
            try:
                original_filepath.unlink(missing_ok=True)
            except OSError as e:
                 logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after successful processing: {e}")
            final_status = "success"
            final_message = "Processed and stored successfully to all destinations."
        else:
            logger.warning(f"Task {task_id}: One or more destinations failed. Original file NOT deleted: {original_filepath}")
            final_status = "partial_failure"
            final_message = "Processing complete, but one or more destinations failed."
            # NOTE: This currently prevents Celery auto-retry unless the StorageBackendError bubbles up
            # and is in RETRYABLE_EXCEPTIONS. If we want partial failures to retry,
            # we might need to re-raise an appropriate exception here.

        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status}

    except InvalidDicomError:
         logger.error(f"Task {task_id}: Invalid DICOM error caught for {original_filepath.name}. Moving to errors.")
         move_to_error_dir(original_filepath, task_id)
         # Don't retry invalid files
         return {"status": "error", "message": "Invalid DICOM file format", "filepath": dicom_filepath_str}
    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception during processing {original_filepath.name}: {exc!r}", exc_info=True)
        # Check if autoretry is configured and hasn't handled this specific exception type
        # This path handles errors *before* destination processing typically
        # If this exception is NOT in RETRYABLE_EXCEPTIONS, autoretry won't trigger.
        if isinstance(exc, RETRYABLE_EXCEPTIONS):
             logger.warning(f"Task {task_id}: Caught retryable exception, allowing autoretry mechanism to handle: {exc!r}")
             raise exc # Re-raise so Celery's autoretry_for catches it
        else:
             # Non-retryable error encountered before or during core processing
             logger.error(f"Task {task_id}: Non-retryable error for {original_filepath.name}. Moving to errors.")
             move_to_error_dir(original_filepath, task_id)
             return {"status": "error", "message": f"Non-retryable processing error: {exc!r}", "filepath": dicom_filepath_str}

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
        # Define error path relative to incoming base (adjust if layout differs)
        error_base_dir = Path(settings.DICOM_STORAGE_PATH).parent
        error_dir = error_base_dir / "errors"
        error_dir.mkdir(parents=True, exist_ok=True)

        # Add timestamp or task ID to error filename to avoid collisions?
        # error_filename = f"{filepath.stem}_{task_id}{filepath.suffix}"
        error_path = error_dir / filepath.name # Use original name for now

        shutil.move(str(filepath), str(error_path))
        logger.info(f"Task {task_id}: Moved failed/invalid file {filepath.name} to {error_path}")
    except Exception as move_err:
        # Log critical failure - file might be stuck in incoming now
        logger.critical(f"Task {task_id}: CRITICAL - Could not move file {filepath.name} to error dir: {move_err}", exc_info=True)
