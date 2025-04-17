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
from sqlalchemy.orm import Session # Import Session explicitly
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, tag_for_keyword, keyword_for_tag, repeater_has_keyword
from pydicom.valuerep import DSfloat, IS, DSdecimal # Import specific VR types for checking

# Database access
from app.db.session import SessionLocal
from app.crud.crud_rule import ruleset as crud_ruleset
from app.db.models import RuleSetExecutionMode, Rule # Import Rule model

# Storage backend
from app.services.storage_backends import get_storage_backend, StorageBackendError
# Import Enums used in rules directly
from app.schemas.rule import MatchOperation, ModifyAction

# Configuration
from app.core.config import settings

# Get a logger for this module
logger = logging.getLogger(__name__)

# --- Helper Functions (Keep existing: parse_dicom_tag, check_match, _values_equal, _compare_numeric, apply_modifications) ---
# (No changes needed in these helpers for source matching)
# Enhanced Tag Parsing (Handles keywords too)
def parse_dicom_tag(tag_str: str) -> Optional[BaseTag]:
    """Parses a string like '(0010,0010)' or 'PatientName' into a pydicom Tag object."""
    if not isinstance(tag_str, str):
         logger.warning(f"Invalid type for tag string: {type(tag_str)}. Expected str.")
         return None
    tag_str = tag_str.strip()
    # Check for (gggg,eeee) format with optional parens and flexible spacing
    match_ge = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", tag_str)
    if match_ge:
        try:
            return Tag(int(match_ge.group(1), 16), int(match_ge.group(2), 16))
        except ValueError:
            pass # Fall through if parsing fails
    # Check for keyword, excluding sequence repeaters which aren't real tags
    # Use regex to ensure it's a valid keyword format (alphanumeric)
    if re.match(r"^[a-zA-Z0-9]+$", tag_str) and not repeater_has_keyword(tag_str):
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
                    continue # Skip criterion, don't fail whole rule
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
            # Ensure all ops are handled or have a default case
            # else: logger.warning(f"Unhandled match operation '{op.value}' for tag {tag_str}") # Add if needed

            # --- Check match result ---
            if not match:
                 # Only log details if comparison was actually performed (i.e., op wasn't skipped)
                 if op not in [MatchOperation.CONTAINS, MatchOperation.STARTS_WITH, MatchOperation.ENDS_WITH, MatchOperation.REGEX, MatchOperation.IN, MatchOperation.NOT_IN] or isinstance(expected_value, (str, list)):
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
        # Attempt numeric comparison if both look numeric
        if isinstance(actual_val, (int, float, Decimal, DSfloat, IS, DSdecimal)) and isinstance(expected_val, (int, float, str)):
             try:
                  # Using Decimal for expected_val avoids float precision issues if expected_val is "1.0"
                  expected_decimal = Decimal(str(expected_val))
                  actual_decimal = Decimal(str(actual_val)) # Convert actual to Decimal too for fair comparison
                  return actual_decimal == expected_decimal
             except (ValueError, TypeError):
                  pass # Fallback to string comparison if conversion fails
        # Attempt date/time comparison (basic string compare for now)
        elif isinstance(actual_val, (date, datetime, dt_time)):
             # TODO: Implement robust date/time parsing of expected_val based on tag VR or heuristics
             return str(actual_val) == str(expected_val)
    except (ValueError, TypeError, Exception) as e: # Catch broader errors during conversion
         logger.debug(f"Could not compare values numerically/temporally: {actual_val} vs {expected_val}. Error: {e}. Falling back to string comparison.")
         pass
    # Fallback to string comparison
    return str(actual_val) == str(expected_val)

def _compare_numeric(actual_val: Any, expected_val: Any, op: MatchOperation) -> bool:
     """Performs numeric comparison using Decimal, returns False if types are incompatible."""
     try:
         # Use Decimal for more precise comparisons
         actual_decimal = Decimal(str(actual_val))
         expected_decimal = Decimal(str(expected_val))
         if op == MatchOperation.GREATER_THAN: return actual_decimal > expected_decimal
         if op == MatchOperation.LESS_THAN: return actual_decimal < expected_decimal
         if op == MatchOperation.GREATER_EQUAL: return actual_decimal >= expected_decimal
         if op == MatchOperation.LESS_EQUAL: return actual_decimal <= expected_decimal
         return False
     except (ValueError, TypeError, Exception): # Catch Decimal conversion errors too
         logger.debug(f"Cannot compare non-numeric values using Decimal: '{actual_val}', '{expected_val}'")
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
        new_value = mod.get("value") # Keep original value for logging/potential use
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
                 # Attempt to determine VR if not provided
                 if not final_vr:
                      if tag in dataset:
                           final_vr = dataset[tag].VR
                           logger.debug(f"Using existing VR '{final_vr}' for tag {tag_str} ({tag}).")
                      else:
                           try:
                               final_vr = dictionary_VR(tag)
                               logger.debug(f"Inferred VR '{final_vr}' for tag {tag_str} ({tag}) from dictionary.")
                           except KeyError:
                               final_vr = 'UN' # Unknown VR
                               logger.warning(f"Could not determine VR for new tag {tag_str} ({tag}). Defaulting to '{final_vr}'. Specify VR in rule for clarity.")

                 # Basic type coercion/validation based on VR (can be expanded)
                 processed_value = new_value
                 try:
                      if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'):
                           processed_value = int(new_value)
                      elif final_vr in ('FL', 'FD', 'OD', 'OF'):
                           processed_value = float(new_value)
                      elif final_vr == 'DS':
                           # DS can be list of strings, handle conversion carefully
                           if isinstance(new_value, list):
                                processed_value = [str(v) for v in new_value]
                           else:
                                processed_value = str(new_value) # Store as string
                      # TODO: Add DA, TM, DT parsing/validation later
                      # Add other VR-specific processing here...
                 except (ValueError, TypeError) as conv_err:
                      logger.warning(f"Value '{new_value}' for tag {tag_str} ({tag}) could not be coerced to expected type for VR '{final_vr}': {conv_err}. Setting as is.")
                      processed_value = new_value # Set original value if coercion fails

                 # Use add_new which handles replacing existing tag or adding new one
                 dataset.add_new(tag, final_vr, processed_value)
                 logger.debug(f"Set tag {tag_str} ({tag}) to '{processed_value}' (VR: {final_vr})")

        except Exception as e:
            logger.error(f"Failed to apply modification ({action.value}) for tag {tag_str}: {e}", exc_info=True)


# --- Celery Task (Updated with Source Identifier) ---

RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, StorageBackendError, TimeoutError)

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=3,
             default_retry_delay=60,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(self, dicom_filepath_str: str, source_identifier: Optional[str] = None): # <-- Added source_identifier
    """
    Enhanced task using structured rules from schema. Now considers source_identifier.
    """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    # Use provided source or default if None (for backward compatibility or cases where source isn't passed)
    effective_source = source_identifier or "unknown"

    logger.info(f"Task {task_id}: Received request for file: {original_filepath} from source: {effective_source}")

    if not original_filepath.exists():
        logger.error(f"Task {task_id}: File not found: {original_filepath}. Cannot process.")
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str, "source": effective_source}

    db: Optional[Session] = None # Use Optional[Session] type hint
    modified_ds: Optional[pydicom.Dataset] = None # Use Optional
    applied_rules_info = []
    destinations_to_process = []
    success_status = {}
    final_status = "unknown"
    final_message = "Task did not complete."

    try:
        # --- 1. Read DICOM File ---
        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            # Ensure file handles are closed properly
            with open(original_filepath, 'rb') as fp:
                 ds = pydicom.dcmread(fp, force=True)
            modified_ds = deepcopy(ds) # Work on a copy
            logger.debug(f"Task {task_id}: Successfully read file. SOP Class: {ds.get('SOPClassUID', 'N/A')}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath}: {e}", exc_info=True)
            move_to_error_dir(original_filepath, task_id) # Move invalid file immediately
            return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": effective_source}
        except Exception as read_exc: # Catch other potential read errors
            logger.error(f"Task {task_id}: Error reading DICOM file {original_filepath}: {read_exc}", exc_info=True)
            move_to_error_dir(original_filepath, task_id)
            return {"status": "error", "message": f"Error reading file: {read_exc}", "filepath": dicom_filepath_str, "source": effective_source}

        # --- 2. Get Rules from Database ---
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        # Fetch active rulesets ordered by priority
        # Note: Loading rules eagerly (selectinload is good)
        rulesets = crud_ruleset.get_active_ordered(db)
        if not rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for {original_filepath.name}")
             final_status = "success"
             final_message = "No active rulesets found"
             # Still need to handle cleanup if needed - maybe delete original file? Configurable?
             # For now, assume no rules means no action, leave file.
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

        # --- 3. Rule Matching & Modification ---
        logger.debug(f"Task {task_id}: Evaluating {len(rulesets)} rulesets for source '{effective_source}'...")
        any_rule_matched_and_applied = False # Track if any rule fully processed (match+action)

        for ruleset_obj in rulesets:
            logger.debug(f"Task {task_id}: Evaluating RuleSet '{ruleset_obj.name}' (ID: {ruleset_obj.id}, Priority: {ruleset_obj.priority})")
            if not ruleset_obj.rules:
                 logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' has no rules.")
                 continue

            matched_rule_in_this_set = False
            for rule_obj in ruleset_obj.rules: # rules should be ordered by priority from DB query/model
                 if not rule_obj.is_active:
                     logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' (ID: {rule_obj.id}) is inactive. Skipping.")
                     continue

                 # --- SOURCE CHECK ---
                 # Check if the rule applies to the current source_identifier
                 # Rule applies if applicable_sources is None/empty OR contains the effective_source
                 rule_sources = rule_obj.applicable_sources # This is List[str] or None from DB model
                 if rule_sources and effective_source not in rule_sources:
                      logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' (ID: {rule_obj.id}) does not apply to source '{effective_source}'. Skipping.")
                      continue # Skip this rule, check next one
                 # --- END SOURCE CHECK ---

                 logger.debug(f"Task {task_id}: Checking rule '{rule_obj.name}' (ID: {rule_obj.id}, Priority: {rule_obj.priority}) against source '{effective_source}'")
                 try:
                     # Validate criteria structure (assuming it's stored as JSON list of dicts now)
                     criteria_list = rule_obj.match_criteria
                     if not isinstance(criteria_list, list):
                          logger.warning(f"Task {task_id}: Match criteria for rule '{rule_obj.name}' is not a list: {type(criteria_list)}. Skipping rule matching.")
                          continue # Skip evaluating this rule's match

                     # Perform matching
                     if check_match(ds, criteria_list):
                         logger.info(f"Task {task_id}: Rule '{ruleset_obj.name}' / '{rule_obj.name}' MATCHED.")
                         any_rule_matched_and_applied = True # Mark that we found a match and will apply actions
                         matched_rule_in_this_set = True

                         # Apply Modifications
                         modifications_list = rule_obj.tag_modifications
                         if isinstance(modifications_list, list):
                              if modifications_list:
                                  logger.debug(f"Task {task_id}: Applying {len(modifications_list)} modifications for rule '{rule_obj.name}'...")
                                  # Use the modified_ds copy for modifications
                                  apply_modifications(modified_ds, modifications_list)
                              else:
                                  logger.debug(f"Task {task_id}: Rule '{rule_obj.name}' matched but has no modifications defined.")
                         else:
                              logger.warning(f"Task {task_id}: Modifications for rule '{rule_obj.name}' is not a list: {type(modifications_list)}. Skipping modifications.")

                         # Collect Destinations
                         rule_destinations = rule_obj.destinations
                         if isinstance(rule_destinations, list):
                              destinations_to_process.extend(rule_destinations)
                              logger.debug(f"Task {task_id}: Added {len(rule_destinations)} destinations from rule '{rule_obj.name}'.")
                         else:
                              logger.warning(f"Task {task_id}: Destinations for rule '{rule_obj.name}' is not a list: {type(rule_destinations)}. Skipping destinations.")

                         applied_rules_info.append(f"{ruleset_obj.name}/{rule_obj.name} (ID:{rule_obj.id})")

                         # Handle Execution Mode for the RuleSet
                         if ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                             logger.debug(f"Task {task_id}: Ruleset '{ruleset_obj.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                             break # Stop processing rules within this ruleset

                 except Exception as rule_proc_exc:
                     logger.error(f"Task {task_id}: Error processing rule '{ruleset_obj.name}/{rule_obj.name}': {rule_proc_exc}", exc_info=True)
                     # Decide if one rule error should stop all processing? For now, continue to next rule/ruleset.

            # After checking all rules in a set, if we matched one and mode is FIRST_MATCH, break outer loop too
            if matched_rule_in_this_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                logger.debug(f"Task {task_id}: FIRST_MATCH rule found in set '{ruleset_obj.name}'. Stopping ruleset evaluation.")
                break # Stop processing further rulesets

        # --- 4. Destination Processing ---
        if not any_rule_matched_and_applied:
             logger.info(f"Task {task_id}: No applicable rules matched for source '{effective_source}' on file {original_filepath.name}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             # Leave original file as no rules matched
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

        if not destinations_to_process:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured in matched rules for {original_filepath.name}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            # Since modifications *might* have happened, should we delete original? Assume yes for now.
            # This might need to be configurable.
            try:
                 original_filepath.unlink(missing_ok=True)
                 logger.info(f"Task {task_id}: Deleting original file {original_filepath} as rules matched but had no destinations.")
            except OSError as e:
                 logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after processing with no destinations: {e}")

            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "filepath": dicom_filepath_str, "source": effective_source}

        # De-duplicate destinations based on their JSON representation
        unique_dest_strings = set()
        valid_destination_configs = []
        for dest_outer_obj in destinations_to_process:
            if not isinstance(dest_outer_obj, dict):
                 logger.warning(f"Task {task_id}: Skipping destination object that is not a dictionary: {dest_outer_obj}")
                 continue
            # The destination object directly from DB/Schema should have type and config
            dest_type = dest_outer_obj.get('type')
            inner_config = dest_outer_obj.get('config')

            if not dest_type or not isinstance(inner_config, dict):
                 logger.warning(f"Task {task_id}: Skipping invalid destination object (missing type or config is not dict): {dest_outer_obj}")
                 continue

            # Ensure type from outer level is in config dict for the factory
            processing_config = inner_config.copy() # Work with a copy
            processing_config['type'] = dest_type

            try:
                # Serialize the combined config for de-duplication
                # Use separators=(',', ':') for compact representation
                json_string = json.dumps(processing_config, sort_keys=True, separators=(',', ':'))
                if json_string not in unique_dest_strings:
                    unique_dest_strings.add(json_string)
                    valid_destination_configs.append(processing_config)
            except TypeError as e:
                 logger.warning(f"Task {task_id}: Could not serialize destination config for de-duplication: {processing_config}. Error: {e}")

        logger.info(f"Task {task_id}: Processing {len(valid_destination_configs)} unique destinations for {original_filepath.name}...")

        all_destinations_succeeded = True
        for i, dest_config in enumerate(valid_destination_configs):
            dest_type = dest_config.get('type','?')
            # Create a more descriptive ID for logging
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            # Add other types here...
            dest_id = "_".join(dest_id_parts)


            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                # Pass the *modified* dataset
                store_result = storage_backend.store(modified_ds, original_filepath)
                logger.info(f"Task {task_id}: Destination {dest_id} completed. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store to destination {dest_id}: {e}", exc_info=False) # Don't need full traceback for expected errors
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error during storage to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}

        # --- 5. Cleanup ---
        if all_destinations_succeeded:
            logger.info(f"Task {task_id}: All {len(valid_destination_configs)} destinations succeeded. Deleting original file: {original_filepath}")
            try:
                original_filepath.unlink(missing_ok=True)
            except OSError as e:
                 logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath}: {e}")
            final_status = "success"
            final_message = f"Processed and stored successfully to {len(valid_destination_configs)} destination(s)."
        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(valid_destination_configs)} destinations failed. Original file NOT deleted: {original_filepath}")
            final_status = "partial_failure"
            final_message = f"Processing complete, but {failed_count} destination(s) failed."

        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": effective_source}

    except InvalidDicomError as e:
         # This case should ideally be caught earlier during reading
         logger.error(f"Task {task_id}: Invalid DICOM error re-caught for {original_filepath.name}: {e}.")
         move_to_error_dir(original_filepath, task_id) # Ensure move happens
         return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": effective_source}
    except Exception as exc:
        # Catch-all for unexpected errors during the main processing flow
        logger.error(f"Task {task_id}: Unhandled exception processing {original_filepath.name}: {exc!r}", exc_info=True)
        logger.info(f"Task {task_id}: Moving file to errors directory due to unhandled exception.")
        move_to_error_dir(original_filepath, task_id)
        # Determine if retry is appropriate (Celery handles autoretry_for specified exceptions)
        # For other exceptions caught here, we might not want to retry.
        # Log the final status before potentially raising for retry or returning error state.
        final_status = "error"
        final_message = f"Fatal error during processing: {exc!r}"
        # Don't raise self.retry() here unless you specifically want to override Celery's autoretry
        return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source}

    finally:
        if db:
            logger.debug(f"Task {task_id}: Closing database session.")
            db.close()
        logger.info(f"Task {task_id}: Finished processing {original_filepath.name}. Final Status: {final_status}. Message: {final_message}")


def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the designated error directory, adding a timestamp."""
    if not filepath.is_file(): # Check if it's actually a file
         logger.warning(f"Task {task_id}: Source path {filepath} is not a file or does not exist. Cannot move to error dir.")
         return
    try:
        # Ensure error base path is correctly derived relative to project or using an absolute path from settings
        # Assuming DICOM_STORAGE_PATH is '/dicom_data/incoming', parent is '/dicom_data'
        error_base_dir = Path(settings.DICOM_STORAGE_PATH).parent
        error_dir = error_base_dir / "errors"
        error_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Add task_id to filename for better traceability
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename

        shutil.move(str(filepath), str(error_path))
        logger.info(f"Task {task_id}: Moved file {filepath.name} to {error_path}")
    except Exception as move_err:
        # Log critically if the move fails, as the file might be stuck in incoming
        logger.critical(f"Task {task_id}: CRITICAL - Could not move file {filepath.name} to error dir {error_dir}: {move_err}", exc_info=True)
