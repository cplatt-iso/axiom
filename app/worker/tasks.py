# app/worker/tasks.py
import time
import random # Keep for potential delays if needed
import shutil
import logging
from pathlib import Path
from copy import deepcopy
from datetime import date, time as dt_time, datetime
from decimal import Decimal
import re

from celery import shared_task
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, tag_for_keyword, keyword_for_tag
from pydicom.valuerep import DSfloat, IS, DSdecimal # Import specific VR types

# Database access
from app.db.session import SessionLocal
from app.crud.crud_rule import ruleset as crud_ruleset
from app.db.models import RuleSetExecutionMode

# Storage backend
from app.services.storage_backends import get_storage_backend, StorageBackendError
from app.schemas.rule import MatchOperation, ModifyAction # Import Enums used

# Configuration
from app.core.config import settings

# Get a logger for this module
logger = logging.getLogger(__name__)

# --- Helper Functions ---

# Enhanced Tag Parsing (Handles keywords too)
def parse_dicom_tag(tag_str: str) -> Tag | None:
    """Parses a string like '(0010,0010)' or 'PatientName' into a pydicom Tag object."""
    tag_str = tag_str.strip()
    # Check for (gggg,eeee) format
    if re.match(r"^\(\s*[0-9a-fA-F]{4}\s*,\s*[0-9a-fA-F]{4}\s*\)$", tag_str):
        try:
            parts = tag_str.strip("() ").split(',')
            if len(parts) == 2:
                return Tag(int(parts[0], 16), int(parts[1], 16))
        except ValueError:
            pass # Fall through if parsing fails
    # Check for keyword
    try:
        tag = tag_for_keyword(tag_str)
        if tag:
            return Tag(tag)
    except ValueError:
        logger.warning(f"Could not parse tag string/keyword: {tag_str}")
        return None

    logger.warning(f"Could not parse tag string/keyword: {tag_str}")
    return None


# Enhanced Matching Logic
def check_match(dataset: pydicom.Dataset, criteria: list[dict]) -> bool:
    """
    Checks if a dataset matches ALL criteria in the list (implicit AND).
    Handles various operators and basic type coercion.
    """
    if not criteria:
        return True # Empty criteria list always matches

    for criterion in criteria:
        tag_str = criterion.get("tag")
        op_str = criterion.get("op")
        expected_value = criterion.get("value") # Can be None for exists/not_exists

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key in match criteria: {tag_str}")
            continue # Or should this invalidate the whole rule? For now, skip criterion.

        try:
            op = MatchOperation(op_str) # Validate operator
        except ValueError:
            logger.warning(f"Skipping invalid match operation: {op_str}")
            continue

        actual_data_element = dataset.get(tag, None)

        # --- Handle existence checks first ---
        if op == MatchOperation.EXISTS:
            if actual_data_element is None:
                logger.debug(f"Match fail: Tag {tag_str} does not exist (op: EXISTS)")
                return False # Tag must exist
            else:
                continue # Condition met, check next criterion
        elif op == MatchOperation.NOT_EXISTS:
            if actual_data_element is not None:
                logger.debug(f"Match fail: Tag {tag_str} exists (op: NOT_EXISTS)")
                return False # Tag must not exist
            else:
                continue # Condition met, check next criterion

        # --- For other ops, the tag must exist ---
        if actual_data_element is None:
             logger.debug(f"Match fail: Tag {tag_str} does not exist (required for op: {op.value})")
             return False

        # Extract actual value (handle MultiValue carefully)
        # For simplicity, basic comparisons often work on the first item or string representation
        actual_value = actual_data_element.value
        if isinstance(actual_value, pydicom.multival.MultiValue):
             # How to handle MultiValue depends on operator.
             # 'eq', 'ne': Check if expected value matches ANY item?
             # 'contains': Check if expected value is substring of ANY item?
             # 'in', 'not_in': Check if ANY item from actual is in expected list?
             # For now, basic approach: check first item or string representation
             if not actual_value: # Empty list/sequence
                 actual_value_cmp = None
             else:
                 actual_value_cmp = actual_value[0] # Compare against first item
             actual_value_str_list = [str(v) for v in actual_value]
        else:
             actual_value_cmp = actual_value
             actual_value_str_list = [str(actual_value)]


        # --- Perform comparison based on operator ---
        match = False
        try:
            # TODO: More robust type handling based on VR
            # Basic type coercion attempt (int, float, string)
            exp_val_str = str(expected_value)

            if op == MatchOperation.EQUALS:
                # Try numeric comparison first if possible
                try:
                    if isinstance(actual_value_cmp, (int, float, Decimal, DSfloat, IS, DSdecimal)) and isinstance(expected_value, (int, float)):
                         match = (float(actual_value_cmp) == float(expected_value))
                    elif isinstance(actual_value_cmp, (date, datetime, dt_time)):
                         # Add date/time comparison logic if needed
                         match = (str(actual_value_cmp) == exp_val_str) # Basic string compare for now
                    else: # Default to string comparison
                         # Handle multivalue: check if expected value matches any item string
                         match = any(av_str == exp_val_str for av_str in actual_value_str_list)
                except (ValueError, TypeError): # Fallback to string if numeric fails
                    match = any(av_str == exp_val_str for av_str in actual_value_str_list)

            elif op == MatchOperation.NOT_EQUALS:
                 try:
                    if isinstance(actual_value_cmp, (int, float, Decimal, DSfloat, IS, DSdecimal)) and isinstance(expected_value, (int, float)):
                         match = (float(actual_value_cmp) != float(expected_value))
                    elif isinstance(actual_value_cmp, (date, datetime, dt_time)):
                         match = (str(actual_value_cmp) != exp_val_str)
                    else: # String comparison - check if expected value is different from ALL items
                         match = all(av_str != exp_val_str for av_str in actual_value_str_list)
                 except (ValueError, TypeError):
                     match = all(av_str != exp_val_str for av_str in actual_value_str_list)

            elif op == MatchOperation.CONTAINS:
                 if not isinstance(expected_value, str):
                     logger.warning(f"Non-string value provided for 'contains' op on tag {tag_str}. Skipping.")
                     continue
                 # Check if substring exists in any of the string representations
                 match = any(expected_value in av_str for av_str in actual_value_str_list)

            elif op == MatchOperation.STARTS_WITH:
                 if not isinstance(expected_value, str):
                     logger.warning(f"Non-string value provided for 'startswith' op on tag {tag_str}. Skipping.")
                     continue
                 match = any(av_str.startswith(expected_value) for av_str in actual_value_str_list)

            elif op == MatchOperation.ENDS_WITH:
                  if not isinstance(expected_value, str):
                     logger.warning(f"Non-string value provided for 'endswith' op on tag {tag_str}. Skipping.")
                     continue
                  match = any(av_str.endswith(expected_value) for av_str in actual_value_str_list)

            elif op == MatchOperation.REGEX:
                 if not isinstance(expected_value, str):
                     logger.warning(f"Non-string value provided for 'regex' op on tag {tag_str}. Skipping.")
                     continue
                 try:
                      # Match against the full string representation of potentially multi-value element
                      full_str_val = str(actual_data_element).strip() # Or join multivalue?
                      match = bool(re.search(expected_value, full_str_val))
                 except re.error as regex_err:
                      logger.warning(f"Invalid regex '{expected_value}' for tag {tag_str}: {regex_err}. Skipping criterion.")
                      continue

            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 # Attempt numeric comparison
                 try:
                     actual_float = float(actual_value_cmp) # Compare against first item if multi-value
                     expected_float = float(expected_value)
                     if op == MatchOperation.GREATER_THAN: match = actual_float > expected_float
                     elif op == MatchOperation.LESS_THAN: match = actual_float < expected_float
                     elif op == MatchOperation.GREATER_EQUAL: match = actual_float >= expected_float
                     elif op == MatchOperation.LESS_EQUAL: match = actual_float <= expected_float
                 except (ValueError, TypeError):
                     logger.warning(f"Cannot perform numeric comparison for op '{op.value}' on tag {tag_str} "
                                    f"with values '{actual_value_cmp}', '{expected_value}'. Skipping.")
                     continue # Skip if types incompatible for numeric comparison

            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"Expected value for 'in' op must be a list for tag {tag_str}. Skipping.")
                     continue
                 # Check if any actual value item is in the expected list
                 # Requires careful type handling - basic string check for now
                 expected_val_strs = {str(item) for item in expected_value}
                 match = any(av_str in expected_val_strs for av_str in actual_value_str_list)

            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"Expected value for 'not_in' op must be a list for tag {tag_str}. Skipping.")
                     continue
                 # Check if *no* actual value item is in the expected list
                 expected_val_strs = {str(item) for item in expected_value}
                 match = all(av_str not in expected_val_strs for av_str in actual_value_str_list)

            # If match is False after checking the operation, the rule fails
            if not match:
                 logger.debug(f"Match fail: Tag {tag_str} ('{actual_value}') failed op '{op.value}' with value '{expected_value}'")
                 return False

        except Exception as e:
            logger.error(f"Error during matching for tag {tag_str} with op {op.value}: {e}", exc_info=True)
            return False # Fail match on error

    return True # All criteria in the list were met


# Enhanced Modification Logic
def apply_modifications(dataset: pydicom.Dataset, modifications: list[dict]):
    """
    Applies tag modifications to the dataset IN-PLACE based on a list of actions.
    Handles 'set' and 'delete'. Looks up VR if not provided.
    """
    if not modifications:
        return # Nothing to do

    for mod in modifications:
        action_str = mod.get("action")
        tag_str = mod.get("tag")
        new_value = mod.get("value") # Required for 'set'
        vr = mod.get("vr") # Optional, but recommended for 'set'

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key in modifications: {tag_str}")
            continue

        try:
            action = ModifyAction(action_str)
        except ValueError:
            logger.warning(f"Skipping invalid modification action: {action_str}")
            continue

        try:
            if action == ModifyAction.DELETE:
                # Delete the tag if it exists
                if tag in dataset:
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_str} ({tag})")
                else:
                    logger.debug(f"Tag {tag_str} ({tag}) not found for deletion.")
            elif action == ModifyAction.SET:
                 if new_value is None: # Should be caught by schema validation, but double-check
                     logger.warning(f"Cannot perform 'set' action for tag {tag_str}: value is missing.")
                     continue

                 # Determine VR
                 final_vr = vr # Use provided VR if available
                 if not final_vr:
                      # Try to look up in dictionary
                      try:
                          final_vr = dictionary_VR(tag)
                          logger.debug(f"Inferred VR '{final_vr}' for tag {tag_str} from dictionary.")
                      except KeyError:
                          # If not in dictionary (e.g., private tag), MUST provide VR or guess (dangerous)
                          # Attempt to use existing VR if element exists, otherwise fallback (e.g., UN or LO)
                          if tag in dataset:
                               final_vr = dataset[tag].VR
                               logger.warning(f"Using existing VR '{final_vr}' for unknown tag {tag_str}. Provide VR explicitly if different.")
                          else:
                               final_vr = 'UN' # Unknown is safest default if VR is mandatory
                               logger.warning(f"Could not determine VR for new tag {tag_str}. Defaulting to '{final_vr}'. Specify VR in rule for correctness.")

                 # Add or update the tag using determined VR
                 # pydicom handles basic type conversion (e.g., int to IS, float to DS)
                 # but complex types (dates, times) might need pre-formatting
                 # TODO: Pre-format values based on VR (e.g., dates, times) before setting
                 dataset.add_new(tag, final_vr, new_value)
                 logger.debug(f"Set tag {tag_str} ({tag}) to '{new_value}' (VR: {final_vr})")

            # TODO: Implement other actions like ADD_ITEM for sequences

        except Exception as e:
            logger.error(f"Failed to apply modification ({action.value}) for tag {tag_str}: {e}", exc_info=True)
            # Decide whether to continue applying other modifications or fail the task
            # For now, log and continue


# --- Celery Task (Main logic updated to use new structures) ---

RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, StorageBackendError, TimeoutError)

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=3,
             default_retry_delay=60,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(self, dicom_filepath_str: str):
    """ Enhanced task using structured rules """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    logger.info(f"Task {task_id}: Received request to process DICOM file: {original_filepath}")

    if not original_filepath.exists():
        logger.error(f"Task {task_id}: File not found: {original_filepath}. Cannot process.")
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str}

    db: SessionLocal | None = None
    modified_ds: pydicom.Dataset | None = None
    applied_rules_info = []
    destinations_to_process = []
    success_status = {}

    try:
        # --- 1. Read DICOM File ---
        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            ds = pydicom.dcmread(original_filepath, stop_before_pixels=True)
            modified_ds = deepcopy(ds) # Work on a copy
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath}: {e}", exc_info=True)
            raise # Caught by outer try/except for error handling

        # --- 2. Get Rules from Database ---
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        rulesets = crud_ruleset.get_active_ordered(db)
        if not rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for {original_filepath.name}")
             return {"status": "success", "message": "No active rulesets matched", "filepath": dicom_filepath_str}

        # --- 3. Rule Matching & Modification ---
        logger.debug(f"Task {task_id}: Evaluating {len(rulesets)} rulesets...")
        matched_in_ruleset = False

        for ruleset_obj in rulesets:
            logger.debug(f"Task {task_id}: Evaluating RuleSet '{ruleset_obj.name}' (Priority: {ruleset_obj.priority})")
            if not ruleset_obj.rules:
                 continue

            matched_rule_in_set = False
            for rule_obj in ruleset_obj.rules:
                 if not rule_obj.is_active: continue

                 logger.debug(f"Task {task_id}: Checking rule '{rule_obj.name}' (Priority: {rule_obj.priority})")
                 try:
                     criteria = rule_obj.match_criteria # Expected: List[Dict]
                     if isinstance(criteria, list) and check_match(ds, criteria): # Use enhanced check_match
                         logger.info(f"Task {task_id}: Rule '{ruleset_obj.name}' / '{rule_obj.name}' MATCHED.")
                         matched_rule_in_set = True

                         # Apply modifications (in-place on modified_ds)
                         modifications = rule_obj.tag_modifications # Expected: List[Dict]
                         if isinstance(modifications, list):
                              logger.debug(f"Task {task_id}: Applying {len(modifications)} modifications for rule '{rule_obj.name}'...")
                              apply_modifications(modified_ds, modifications) # Use enhanced apply_modifications
                         else:
                              logger.warning(f"Task {task_id}: Modifications for rule '{rule_obj.name}' is not a list: {modifications}. Skipping.")


                         # Add destinations
                         rule_destinations = rule_obj.destinations # Expected: List[Dict]
                         if isinstance(rule_destinations, list):
                              # Validate structure slightly (contains dicts with 'type')
                              valid_destinations = [d for d in rule_destinations if isinstance(d, dict) and 'type' in d]
                              destinations_to_process.extend(valid_destinations)
                              if len(valid_destinations) != len(rule_destinations):
                                   logger.warning(f"Task {task_id}: Some destinations for rule '{rule_obj.name}' were invalid.")
                         else:
                              logger.warning(f"Task {task_id}: Destinations for rule '{rule_obj.name}' is not a list: {rule_destinations}. Skipping.")


                         applied_rules_info.append(f"{ruleset_obj.name}/{rule_obj.name}")

                         if ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                             logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                             break
                 except Exception as match_exc:
                     logger.error(f"Task {task_id}: Error processing rule '{ruleset_obj.name}/{rule_obj.name}': {match_exc}", exc_info=True)

            if matched_rule_in_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                 # If we finished a FIRST_MATCH ruleset because a rule matched,
                 # should we stop processing subsequent rulesets? Add a global setting if needed.
                 # For now, continue processing all rulesets.
                 pass

        # --- 4. Destination Processing ---
        if not destinations_to_process:
            logger.info(f"Task {task_id}: No destinations generated for {original_filepath.name}.")
            # If rules applied modifications but no destination, the modified file isn't saved anywhere by default.
            # Decide if this is desired behavior. For now, original is left untouched if no destinations.
            message = "No destinations configured for matched rules." if applied_rules_info else "No matching rules found."
            return {"status": "success", "message": message, "applied_rules": applied_rules_info, "filepath": dicom_filepath_str}

        logger.info(f"Task {task_id}: Processing {len(destinations_to_process)} destinations for {original_filepath.name}...")
        # Log final state? Be careful with PHI if logging full dataset
        # logger.debug(f"Task {task_id}: Final modified dataset state before storage: {modified_ds.get('PatientName', 'N/A')}, {modified_ds.get('PatientID', 'N/A')}")

        all_destinations_succeeded = True
        for i, dest_config_wrapper in enumerate(destinations_to_process):
            # Extract the actual config dict (was wrapped in StorageDestination schema)
            dest_config = dest_config_wrapper.get('config', dest_config_wrapper) # Handle both formats
            dest_type = dest_config.get('type','?')
            dest_id = f"Dest_{i+1}_{dest_type}"

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config) # Pass the inner config dict
                store_result = storage_backend.store(modified_ds, original_filepath)
                logger.info(f"Task {task_id}: Destination {dest_id} completed. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store to destination {dest_id}: {e}", exc_info=False) # Keep log cleaner
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
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
                 logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath}: {e}")
            final_status = "success"
            final_message = "Processed and stored successfully."
        else:
            logger.warning(f"Task {task_id}: One or more destinations failed. Original file NOT deleted: {original_filepath}")
            final_status = "partial_failure"
            final_message = "Processing complete, but one or more destinations failed."
            # Consider raising a non-retryable error if partial failure requires intervention
            # raise Exception("Partial destination failure reported by task.")


        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status}

    except InvalidDicomError:
         logger.error(f"Task {task_id}: Invalid DICOM error caught for {original_filepath.name}. Moving to errors.")
         move_to_error_dir(original_filepath, task_id)
         return {"status": "error", "message": "Invalid DICOM file format", "filepath": dicom_filepath_str}
    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception during processing {original_filepath.name}: {exc!r}", exc_info=True)
        # Auto-retry should handle RETRYABLE_EXCEPTIONS
        # If it's not retryable, or max retries are hit by auto-retry, it lands here.
        logger.error(f"Task {task_id}: Non-retryable error or max retries exceeded for {original_filepath.name}. Moving to errors.")
        move_to_error_dir(original_filepath, task_id)
        # Return final error status
        return {"status": "error", "message": f"Fatal error or max retries exceeded: {exc!r}", "filepath": dicom_filepath_str}

    finally:
        if db:
            logger.debug(f"Task {task_id}: Closing database session.")
            db.close()


def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the designated error directory."""
    if not filepath.exists():
         logger.warning(f"Task {task_id}: Original file {filepath} not found for moving to error dir.")
         return

    try:
        error_base_dir = Path(settings.DICOM_STORAGE_PATH).parent
        error_dir = error_base_dir / "errors"
        error_dir.mkdir(parents=True, exist_ok=True)
        # Add timestamp or UUID to error filename to prevent collisions?
        error_path = error_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filepath.name}"
        shutil.move(str(filepath), str(error_path))
        logger.info(f"Task {task_id}: Moved failed file {filepath.name} to {error_path}")
    except Exception as move_err:
        logger.critical(f"Task {task_id}: CRITICAL - Could not move failed file {filepath.name} to error dir: {move_err}")
