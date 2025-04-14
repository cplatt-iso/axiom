# app/worker/tasks.py
import time
import random # Keep for potential delays if needed
import shutil
import logging
import json # Import json
from pathlib import Path
from copy import deepcopy
from datetime import date, time as dt_time, datetime
from decimal import Decimal
import re
from typing import Optional, Any, List, Dict # Ensure List/Dict are imported

from celery import shared_task
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, tag_for_keyword, keyword_for_tag, repeater_has_keyword
from pydicom.valuerep import DSfloat, IS, DSdecimal # Import specific VR types for checking

# Database access
from app.db.session import SessionLocal
from app.crud.crud_rule import ruleset as crud_ruleset
from app.db.models import RuleSetExecutionMode

# Storage backend
from app.services.storage_backends import get_storage_backend, StorageBackendError
# Import Enums used in rules directly
from app.schemas.rule import MatchOperation, ModifyAction

# Configuration
from app.core.config import settings

# Get a logger for this module
logger = logging.getLogger(__name__)

# --- Helper Functions ---

# Enhanced Tag Parsing (Handles keywords too)
def parse_dicom_tag(tag_str: str) -> Optional[BaseTag]:
    """Parses a string like '(0010,0010)' or 'PatientName' into a pydicom Tag object."""
    if not isinstance(tag_str, str):
         logger.warning(f"Invalid type for tag string: {type(tag_str)}. Expected str.")
         return None
    tag_str = tag_str.strip()
    # Check for (gggg,eeee) format
    if re.match(r"^\(\s*[0-9a-fA-F]{4}\s*,\s*[0-9a-fA-F]{4}\s*\)$", tag_str):
        try:
            parts = tag_str.strip("() ").replace(" ", "").split(',') # Remove spaces too
            if len(parts) == 2:
                return Tag(int(parts[0], 16), int(parts[1], 16))
        except ValueError:
            pass # Fall through if parsing fails
    # Check for keyword, excluding sequence repeaters which aren't real tags
    if not repeater_has_keyword(tag_str):
        try:
            tag_val = tag_for_keyword(tag_str)
            if tag_val:
                return Tag(tag_val)
        except (ValueError, TypeError): # Catch potential errors if keyword is weird type
            pass # Fall through

    logger.warning(f"Could not parse tag string/keyword: '{tag_str}'")
    return None


# Enhanced Matching Logic
def check_match(dataset: pydicom.Dataset, criteria: List[Dict[str, Any]]) -> bool:
    """
    Checks if a dataset matches ALL criteria in the list (implicit AND).
    Handles various operators and basic type coercion.
    Expects criteria in the format: [{'tag': '...', 'op': '...', 'value': ...}, ...]
    """
    if not criteria:
        return True # Empty criteria list always matches

    for criterion in criteria:
        # Basic validation of criterion structure
        if not isinstance(criterion, dict):
             logger.warning(f"Skipping invalid criterion (not a dict): {criterion}")
             continue
        tag_str = criterion.get("tag")
        op_str = criterion.get("op")
        expected_value = criterion.get("value") # Can be None for exists/not_exists

        if not tag_str or not op_str:
             logger.warning(f"Skipping invalid criterion (missing tag or op): {criterion}")
             continue

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key in match criteria: {tag_str}")
            # Consider this a failure? If the tag in the rule is bad, the rule can't match.
            return False

        try:
            op = MatchOperation(op_str) # Validate operator string against Enum
        except ValueError:
            logger.warning(f"Skipping invalid match operation '{op_str}' for tag {tag_str}")
            return False # Invalid operator means the rule is bad

        actual_data_element = dataset.get(tag, None)

        # --- Handle existence checks first ---
        if op == MatchOperation.EXISTS:
            if actual_data_element is None:
                logger.debug(f"Match fail: Tag {tag_str} does not exist (op: EXISTS)")
                return False # Criterion failed
            else:
                continue # Criterion met, check next criterion
        elif op == MatchOperation.NOT_EXISTS:
            if actual_data_element is not None:
                logger.debug(f"Match fail: Tag {tag_str} exists (op: NOT_EXISTS)")
                return False # Criterion failed
            else:
                continue # Criterion met, check next criterion

        # --- For other ops, the tag must exist ---
        if actual_data_element is None:
             logger.debug(f"Match fail: Tag {tag_str} does not exist (required for op: {op.value})")
             return False

        # Extract actual value (handle MultiValue by converting to list)
        actual_value = actual_data_element.value
        if isinstance(actual_value, pydicom.multival.MultiValue):
             actual_value_list = list(actual_value)
        else:
             actual_value_list = [actual_value] # Treat single value as list for consistency

        # --- Perform comparison based on operator ---
        match = False # Assume false until proven true for the current criterion
        try:
            # Comparison functions often need to iterate through the actual value list
            if op == MatchOperation.EQUALS:
                match = any(_values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.NOT_EQUALS:
                match = all(not _values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.CONTAINS:
                if not isinstance(expected_value, str):
                    logger.warning(f"'contains' requires a string value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                    continue
                match = any(expected_value in str(av) for av in actual_value_list)
            elif op == MatchOperation.STARTS_WITH:
                if not isinstance(expected_value, str):
                    logger.warning(f"'startswith' requires a string value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                    continue
                match = any(str(av).startswith(expected_value) for av in actual_value_list)
            elif op == MatchOperation.ENDS_WITH:
                if not isinstance(expected_value, str):
                    logger.warning(f"'endswith' requires a string value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                    continue
                match = any(str(av).endswith(expected_value) for av in actual_value_list)
            elif op == MatchOperation.REGEX:
                if not isinstance(expected_value, str):
                     logger.warning(f"'regex' requires a string pattern, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                     continue
                try:
                     match = any(bool(re.search(expected_value, str(av))) for av in actual_value_list)
                except re.error as regex_err:
                     logger.warning(f"Invalid regex '{expected_value}' for tag {tag_str}: {regex_err}. Skipping criterion.")
                     continue
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 match = any(_compare_numeric(av, expected_value, op) for av in actual_value_list)
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'in' requires a list value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                     continue
                 match = any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'not_in' requires a list value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                     continue
                 match = all(not _values_equal(av, ev) for av in actual_value_list for ev in expected_value)

            # --- Check match result ---
            if not match:
                 actual_repr = repr(actual_value_list[0]) if len(actual_value_list) == 1 else f"{len(actual_value_list)} values"
                 logger.debug(f"Match fail: Tag {tag_str} ('{actual_repr}') failed op '{op.value}' with value '{expected_value}'")
                 return False # If any criterion fails, the whole rule fails (AND logic)

        except Exception as e:
            logger.error(f"Error during matching for tag {tag_str} with op {op.value}: {e}", exc_info=True)
            return False # Fail match on unexpected error during comparison

    # If loop completes without returning False, all criteria matched
    return True

# --- Internal helper for check_match ---
def _values_equal(actual_val: Any, expected_val: Any) -> bool:
    """Compares two values, attempting numeric conversion first."""
    try:
        if isinstance(actual_val, (int, float, Decimal, DSfloat, IS, DSdecimal)) and isinstance(expected_val, (int, float, str)):
             try:
                  expected_float = float(expected_val)
                  # Use Decimal for potentially better precision if needed, else float is fine
                  return float(actual_val) == expected_float
             except (ValueError, TypeError):
                  pass
        elif isinstance(actual_val, (date, datetime, dt_time)):
             # TODO: Proper date/time comparison - parse expected_val?
             return str(actual_val) == str(expected_val)
    except (ValueError, TypeError):
         pass
    return str(actual_val) == str(expected_val)

def _compare_numeric(actual_val: Any, expected_val: Any, op: MatchOperation) -> bool:
     """Performs numeric comparison, returns False if types are incompatible."""
     try:
         actual_float = float(actual_val)
         expected_float = float(expected_val)
         if op == MatchOperation.GREATER_THAN: return actual_float > expected_float
         if op == MatchOperation.LESS_THAN: return actual_float < expected_float
         if op == MatchOperation.GREATER_EQUAL: return actual_float >= expected_float
         if op == MatchOperation.LESS_EQUAL: return actual_float <= expected_float
         return False
     except (ValueError, TypeError):
         logger.debug(f"Cannot compare non-numeric values: '{actual_val}', '{expected_val}'")
         return False


# Enhanced Modification Logic
def apply_modifications(dataset: pydicom.Dataset, modifications: List[Dict[str, Any]]):
    """
    Applies tag modifications to the dataset IN-PLACE based on a list of actions.
    """
    if not modifications:
        return

    for mod in modifications:
        if not isinstance(mod, dict):
             logger.warning(f"Skipping invalid modification (not a dict): {mod}")
             continue
        action_str = mod.get("action")
        tag_str = mod.get("tag")
        new_value = mod.get("value")
        vr = mod.get("vr")

        if not action_str or not tag_str:
             logger.warning(f"Skipping invalid modification (missing action or tag): {mod}")
             continue

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
                if tag in dataset:
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_str} ({tag})")
                else:
                    logger.debug(f"Tag {tag_str} ({tag}) not found for deletion.")
            elif action == ModifyAction.SET:
                 final_vr = vr
                 if not final_vr:
                      try:
                          final_vr = dictionary_VR(tag)
                          logger.debug(f"Inferred VR '{final_vr}' for tag {tag_str} from dictionary.")
                      except KeyError:
                          if tag in dataset:
                               final_vr = dataset[tag].VR
                               logger.warning(f"Using existing VR '{final_vr}' for unknown tag {tag_str}. Provide VR explicitly.")
                          else:
                               final_vr = 'UN'
                               logger.warning(f"Could not determine VR for new tag {tag_str}. Defaulting to '{final_vr}'. Specify VR.")

                 # TODO: Pre-process value based on VR (e.g., dates, times, numbers)
                 processed_value = new_value
                 dataset.add_new(tag, final_vr, processed_value)
                 logger.debug(f"Set tag {tag_str} ({tag}) to '{processed_value}' (VR: {final_vr})")

        except Exception as e:
            logger.error(f"Failed to apply modification ({action.value}) for tag {tag_str}: {e}", exc_info=True)


# --- Celery Task (Updated Destination Handling) ---

RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, StorageBackendError, TimeoutError)

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=3,
             default_retry_delay=60,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(self, dicom_filepath_str: str):
    """ Enhanced task using structured rules from schema """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    logger.info(f"Task {task_id}: Received request for file: {original_filepath}")

    if not original_filepath.exists():
        logger.error(f"Task {task_id}: File not found: {original_filepath}. Cannot process.")
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str}

    db: SessionLocal | None = None
    modified_ds: pydicom.Dataset | None = None
    applied_rules_info = []
    destinations_to_process = [] # Collects raw destination dicts from rules
    success_status = {}

    try:
        # --- 1. Read DICOM File ---
        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            ds = pydicom.dcmread(original_filepath, force=True)
            modified_ds = deepcopy(ds) # Work on a copy
            logger.debug(f"Task {task_id}: Successfully read file. SOP Class: {ds.get('SOPClassUID', 'N/A')}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath}: {e}", exc_info=True)
            raise

        # --- 2. Get Rules from Database ---
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        rulesets = crud_ruleset.get_active_ordered(db)
        if not rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for {original_filepath.name}")
             return {"status": "success", "message": "No active rulesets found", "filepath": dicom_filepath_str}

        # --- 3. Rule Matching & Modification ---
        logger.debug(f"Task {task_id}: Evaluating {len(rulesets)} rulesets...")
        any_rule_matched = False

        for ruleset_obj in rulesets:
            logger.debug(f"Task {task_id}: Evaluating RuleSet '{ruleset_obj.name}' (ID: {ruleset_obj.id}, Priority: {ruleset_obj.priority})")
            if not ruleset_obj.rules:
                 logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' has no rules.")
                 continue

            matched_rule_in_this_set = False
            for rule_obj in ruleset_obj.rules:
                 if not rule_obj.is_active:
                     logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' (ID: {rule_obj.id}) is inactive.")
                     continue

                 logger.debug(f"Task {task_id}: Checking rule '{rule_obj.name}' (ID: {rule_obj.id}, Priority: {rule_obj.priority})")
                 try:
                     criteria_list = rule_obj.match_criteria
                     if not isinstance(criteria_list, list):
                          logger.warning(f"Task {task_id}: Match criteria for rule '{rule_obj.name}' is not a list: {type(criteria_list)}. Skipping rule.")
                          continue

                     if check_match(ds, criteria_list):
                         logger.info(f"Task {task_id}: Rule '{ruleset_obj.name}' / '{rule_obj.name}' MATCHED.")
                         any_rule_matched = True
                         matched_rule_in_this_set = True

                         modifications_list = rule_obj.tag_modifications
                         if isinstance(modifications_list, list):
                              if modifications_list:
                                  logger.debug(f"Task {task_id}: Applying {len(modifications_list)} modifications for rule '{rule_obj.name}'...")
                                  apply_modifications(modified_ds, modifications_list)
                              else:
                                  logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' matched but has no modifications defined.")
                         else:
                              logger.warning(f"Task {task_id}: Modifications for rule '{rule_obj.name}' is not a list: {type(modifications_list)}. Skipping.")

                         rule_destinations = rule_obj.destinations
                         if isinstance(rule_destinations, list):
                              # Add raw destination dicts from the rule to the processing list
                              destinations_to_process.extend(rule_destinations)
                         else:
                              logger.warning(f"Task {task_id}: Destinations for rule '{rule_obj.name}' is not a list: {type(rule_destinations)}. Skipping.")

                         applied_rules_info.append(f"{ruleset_obj.name}/{rule_obj.name} (ID:{rule_obj.id})")

                         if ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                             logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                             break

                 except Exception as match_exc:
                     logger.error(f"Task {task_id}: Error processing rule '{ruleset_obj.name}/{rule_obj.name}': {match_exc}", exc_info=True)

            if matched_rule_in_this_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                break

        # --- 4. Destination Processing ---
        if not any_rule_matched:
             logger.info(f"Task {task_id}: No rules matched for {original_filepath.name}. No actions taken.")
             return {"status": "success", "message": "No matching rules found", "filepath": dicom_filepath_str}

        if not destinations_to_process:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured for {original_filepath.name}.")
            return {"status": "success", "message": "Rules matched, modifications applied, but no destinations configured", "applied_rules": applied_rules_info, "filepath": dicom_filepath_str}

        # De-duplicate destinations using JSON strings
        unique_dest_strings = set()
        valid_destination_configs = [] # Store the validated inner config dicts
        for dest_outer_obj in destinations_to_process:
            # Validate the outer structure somewhat expected from DB/schema
            if not isinstance(dest_outer_obj, dict):
                 logger.warning(f"Task {task_id}: Skipping destination object that is not a dictionary: {dest_outer_obj}")
                 continue
            inner_config = dest_outer_obj.get('config') # This is the dict we need
            dest_type = dest_outer_obj.get('type') # Type is at the outer level

            if not isinstance(inner_config, dict) or not dest_type:
                 logger.warning(f"Task {task_id}: Skipping invalid destination object (missing type or config is not dict): {dest_outer_obj}")
                 continue

            # Add type to inner_config if missing, as expected by factory
            if 'type' not in inner_config:
                 inner_config['type'] = dest_type
            elif inner_config['type'].lower() != dest_type.lower():
                 logger.warning(f"Task {task_id}: Mismatch between outer type '{dest_type}' and inner config type '{inner_config['type']}' for destination. Using outer type. Config: {inner_config}")
                 inner_config['type'] = dest_type # Ensure type consistency

            try:
                # Use the inner_config for de-duplication and processing
                json_string = json.dumps(inner_config, sort_keys=True, separators=(',', ':'))
                if json_string not in unique_dest_strings:
                    unique_dest_strings.add(json_string)
                    valid_destination_configs.append(inner_config) # Store the config we will use
            except TypeError as e:
                 logger.warning(f"Could not serialize destination config for de-duplication: {inner_config}. Error: {e}")

        logger.info(f"Task {task_id}: Processing {len(valid_destination_configs)} unique destinations for {original_filepath.name}...")

        all_destinations_succeeded = True
        for i, dest_config in enumerate(valid_destination_configs): # Iterate over the inner configs
            dest_type = dest_config.get('type','?')
            dest_id = f"Dest_{i+1}_{dest_type}"

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                # Pass the validated inner config dictionary to the factory
                storage_backend = get_storage_backend(dest_config)
                store_result = storage_backend.store(modified_ds, original_filepath)
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

        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status}

    except InvalidDicomError as e:
         logger.error(f"Task {task_id}: Invalid DICOM error caught for {original_filepath.name}: {e}. Moving to errors.")
         move_to_error_dir(original_filepath, task_id)
         return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str}
    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception processing {original_filepath.name}: {exc!r}", exc_info=True)
        logger.error(f"Task {task_id}: Assuming non-retryable or max retries exceeded. Moving to errors.")
        move_to_error_dir(original_filepath, task_id)
        return {"status": "error", "message": f"Fatal error or max retries exceeded: {exc!r}", "filepath": dicom_filepath_str}

    finally:
        if db:
            logger.debug(f"Task {task_id}: Closing database session.")
            db.close()


def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the designated error directory, adding a timestamp."""
    if not filepath.exists():
         logger.warning(f"Task {task_id}: Original file {filepath} not found for moving to error dir.")
         return
    try:
        error_base_dir = Path(settings.DICOM_STORAGE_PATH).parent
        error_dir = error_base_dir / "errors"
        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_filename = f"{timestamp}_{filepath.name}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        logger.info(f"Task {task_id}: Moved failed file {filepath.name} to {error_path}")
    except Exception as move_err:
        logger.critical(f"Task {task_id}: CRITICAL - Could not move file {filepath.name} to error dir {error_dir}: {move_err}", exc_info=True)
