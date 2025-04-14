# app/worker/tasks.py
import time
import random
import shutil
import logging
from pathlib import Path
from copy import deepcopy
# Import Union for type hinting
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from celery import shared_task
import pydicom
from pydicom.errors import InvalidDicomError
# Import the Tag class correctly
from pydicom.tag import Tag
from pydicom.datadict import dictionary_VR

# Database access
from app.db.session import SessionLocal
from sqlalchemy.orm import Session
from app.crud.crud_rule import ruleset as crud_ruleset
from app.db.models import RuleSetExecutionMode

# Storage backend
from app.services.storage_backends import get_storage_backend, StorageBackendError

# Configuration
from app.core.config import settings

# Get a logger for this module
logger = logging.getLogger(__name__)

# --- Helper Functions ---

# Use Union for compatibility
def parse_dicom_tag(tag_str: str) -> Union[Tag, None]:
    """Parses a string like '(0010,0010)' into a pydicom Tag object."""
    # First try standard Tag constructor which handles various formats
    try:
        if not isinstance(tag_str, str):
            logger.warning(f"Invalid input type for tag parsing (expected str): {type(tag_str)}")
            return None
        tag = Tag(tag_str)
        return tag
    except ValueError:
        # Handle cases Tag() constructor might fail on
        try:
             cleaned_tag_str = tag_str.strip("() ")
             if not ',' in cleaned_tag_str and not cleaned_tag_str.replace('0x','').isalnum():
                  logger.warning(f"Tag string appears to be a keyword, not '(gggg,eeee)': {tag_str}")
                  return None

             parts = cleaned_tag_str.split(',')
             if len(parts) == 2:
                  group_str = parts[0].strip()
                  elem_str = parts[1].strip()
                  group = int(group_str, 16)
                  element = int(elem_str, 16)
                  return Tag(group, element)
             else:
                  logger.warning(f"Could not parse tag string (wrong parts): {tag_str}")
                  return None
        except ValueError:
             logger.warning(f"Could not parse tag string (ValueError on manual parse): {tag_str}", exc_info=False)
             return None
        except Exception as e:
             logger.error(f"Unexpected error manually parsing tag string '{tag_str}': {e}", exc_info=True)
             return None
    except Exception as e:
        logger.error(f"Unexpected error parsing tag string '{tag_str}' with Tag(): {e}", exc_info=True)
        return None


def check_match(dataset: pydicom.Dataset, criteria: dict) -> bool:
    """
    Checks if a dataset matches the given criteria dictionary.
    Basic implementation: Exact match or wildcard '*' for strings.
    """
    if not isinstance(criteria, dict):
        logger.error(f"Match criteria must be a dictionary, received: {type(criteria)}")
        return False

    for tag_key, expected_value in criteria.items():
        tag = parse_dicom_tag(tag_key)
        if not tag:
            logger.warning(f"Skipping invalid tag key in match criteria: {tag_key}")
            continue

        actual_data_element = dataset.get(tag, None)

        if actual_data_element is None:
            if expected_value is None or expected_value == "__MISSING__":
                 logger.debug(f"Match check: Tag {tag_key} correctly missing as expected.")
                 continue
            else:
                 logger.debug(f"Match check: Tag {tag_key} missing, expected '{expected_value}', NO MATCH.")
                 return False

        actual_value = actual_data_element.value

        # --- Start Enhanced Matching Block (NEEDS FURTHER DEVELOPMENT) ---
        try:
            actual_value_str = str(actual_value)
        except Exception as e:
            logger.warning(f"Could not convert actual value of tag {tag_key} to string for matching: {e}")
            actual_value_str = "[CONVERSION_ERROR]"

        expected_value_str = str(expected_value) if expected_value is not None else ""

        if expected_value_str == '*':
            logger.debug(f"Match check: Tag {tag_key} wildcard '*' matched value '{actual_value_str}'.")
            continue
        elif actual_value_str != expected_value_str:
            logger.debug(f"Match check: Tag {tag_key} value '{actual_value_str}' != expected '{expected_value_str}', NO MATCH.")
            return False
        else:
            logger.debug(f"Match check: Tag {tag_key} value '{actual_value_str}' == expected '{expected_value_str}', MATCH.")
        # --- End Enhanced Matching Block ---

    logger.debug("All criteria specified were matched successfully.")
    return True


def apply_modifications(dataset: pydicom.Dataset, modifications: dict):
    """
    Applies tag modifications to the dataset IN-PLACE.
    modifications format: {'(gggg,eeee)': 'new_value', '(gggg,eeee)': None} (None deletes)
    """
    if not isinstance(modifications, dict):
        logger.error(f"Tag modifications must be a dictionary, received: {type(modifications)}")
        return

    for tag_key, new_value in modifications.items():
        tag = parse_dicom_tag(tag_key)
        if not tag:
            logger.warning(f"Skipping invalid tag key in modifications: {tag_key}")
            continue

        try:
            if new_value is None or new_value == "__DELETE__":
                if tag in dataset:
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_key} ({tag})")
                else:
                    logger.debug(f"Attempted to delete non-existent tag {tag_key} ({tag}). Ignoring.")
            else:
                vr = None
                if tag in dataset:
                    if isinstance(dataset[tag], pydicom.DataElement):
                         vr = dataset[tag].VR
                    else:
                         logger.warning(f"Attempting to modify tag {tag_key} which is part of a sequence. Simple modification might not work as expected.")
                         continue
                if not vr:
                    try:
                        vr = dictionary_VR(tag)
                        logger.debug(f"Looked up VR for new tag {tag_key}: '{vr}'")
                    except KeyError:
                        vr = 'UN'
                        logger.warning(f"VR for new tag {tag_key} not found in dictionary, defaulting to 'UN'. Specify VR in modifications if needed.")

                value_to_set = new_value
                if isinstance(new_value, dict) and 'Value' in new_value:
                    value_to_set = new_value.get('Value')
                    if 'VR' in new_value:
                        override_vr = new_value.get('VR')
                        if isinstance(override_vr, str) and len(override_vr) == 2 and override_vr.isalpha() and override_vr.isupper():
                             vr = override_vr
                             logger.debug(f"Overriding VR for tag {tag_key} to '{vr}' from modification config.")
                        else:
                            logger.warning(f"Invalid VR '{override_vr}' specified in modification config for tag {tag_key}. Using determined VR '{vr}'.")

                # TODO: Value Type Conversion based on VR

                dataset.add_new(tag, vr, value_to_set)
                log_value = (str(value_to_set)[:50] + '...') if isinstance(value_to_set, str) and len(value_to_set) > 53 else str(value_to_set)
                logger.debug(f"Set tag {tag_key} ({tag}) to '{log_value}' (VR: {vr})")

        except Exception as e:
            logger.error(f"Failed to apply modification for tag {tag_key} to value '{new_value}': {e}", exc_info=True)

# --- Celery Task ---

RETRYABLE_EXCEPTIONS = (ConnectionRefusedError, StorageBackendError, TimeoutError)

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=3,
             default_retry_delay=60,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(self, dicom_filepath_str: str):
    """ Celery task to process a single DICOM file. """
    task_id = self.request.id if self.request else 'NO_REQUEST_ID'
    original_filepath = Path(dicom_filepath_str)
    log_prefix = f"Task {task_id} ({original_filepath.name})"
    logger.info(f"{log_prefix}: Received request.")

    if not original_filepath.exists():
        logger.error(f"{log_prefix}: File not found: {original_filepath}. Cannot process.")
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str}

    db: Session | None = None
    ds: pydicom.Dataset | None = None
    modified_ds: pydicom.Dataset | None = None
    applied_rules_info: List[str] = []
    destinations_to_process: List[Dict[str, Any]] = []
    success_status: Dict[str, Dict[str, Any]] = {}
    final_status = "unknown" # Initialize final status
    final_message = "Processing did not complete."

    try:
        # --- 1. Read DICOM File ---
        logger.debug(f"{log_prefix}: Reading DICOM file...")
        try:
            ds = pydicom.dcmread(original_filepath, stop_before_pixels=True)
            modified_ds = deepcopy(ds)
            logger.debug(f"{log_prefix}: DICOM read successfully. PatientName='{ds.get('PatientName', 'N/A')}', SOP UID='{ds.get('SOPInstanceUID', 'N/A')}'")
        except InvalidDicomError as e:
            logger.error(f"{log_prefix}: Invalid DICOM file: {e}", exc_info=False)
            raise

        # --- 2. Get Rules from Database ---
        logger.debug(f"{log_prefix}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"{log_prefix}: Querying active rulesets...")
        rulesets = crud_ruleset.get_active_ordered(db)
        if not rulesets:
             logger.info(f"{log_prefix}: No active rulesets found. Processing complete (no rules applied).")
             final_status="success_no_rules"
             final_message="No active rulesets found"
             # Skip directly to cleanup

        else:
            # --- 3. Rule Matching & Modification ---
            logger.debug(f"{log_prefix}: Evaluating {len(rulesets)} rulesets...")
            matched_in_any_ruleset = False

            for ruleset_obj in rulesets:
                if not ruleset_obj.is_active: continue

                ruleset_log_prefix = f"{log_prefix} [RuleSet: {ruleset_obj.name} (ID: {ruleset_obj.id})]"
                logger.debug(f"{ruleset_log_prefix}: Evaluating (Priority: {ruleset_obj.priority})")
                if not ruleset_obj.rules:
                     logger.debug(f"{ruleset_log_prefix}: Has no associated rules. Skipping.")
                     continue

                matched_rule_in_this_set = False
                active_rules_in_set = [r for r in ruleset_obj.rules if r.is_active]
                if not active_rules_in_set:
                    logger.debug(f"{ruleset_log_prefix}: Has no *active* rules. Skipping.")
                    continue

                for rule_obj in active_rules_in_set:
                     rule_log_prefix = f"{ruleset_log_prefix} [Rule: {rule_obj.name} (ID: {rule_obj.id})]"
                     logger.debug(f"{rule_log_prefix}: Checking (Priority: {rule_obj.priority})")
                     try:
                         criteria = rule_obj.match_criteria
                         if check_match(ds, criteria):
                             logger.info(f"{rule_log_prefix}: MATCHED.")
                             matched_rule_in_this_set = True
                             matched_in_any_ruleset = True

                             modifications = rule_obj.tag_modifications
                             if modifications:
                                  logger.debug(f"{rule_log_prefix}: Applying modifications...")
                                  apply_modifications(modified_ds, modifications)
                             else:
                                  logger.debug(f"{rule_log_prefix}: No modifications defined.")

                             if rule_obj.destinations:
                                  logger.debug(f"{rule_log_prefix}: Adding {len(rule_obj.destinations)} destinations.")
                                  destinations_to_process.extend(rule_obj.destinations)
                             else:
                                   logger.debug(f"{rule_log_prefix}: No destinations defined.")

                             applied_rules_info.append(f"RuleSet: {ruleset_obj.name} (ID: {ruleset_obj.id}) / Rule: {rule_obj.name} (ID: {rule_obj.id})")

                             if ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                                 logger.debug(f"{ruleset_log_prefix}: Mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                                 break
                     except Exception as match_exc:
                         logger.error(f"{rule_log_prefix}: Error during processing: {match_exc}", exc_info=True)

                if matched_rule_in_this_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                     pass

            # --- 4. Destination Processing ---
            if not destinations_to_process:
                 if matched_in_any_ruleset:
                      logger.info(f"{log_prefix}: Rules matched, but no destinations were configured for matched rules.")
                      final_status="success_no_destinations"
                      final_message="Rules matched, no destinations specified."
                 else:
                      logger.info(f"{log_prefix}: No matching rules found after evaluating all rulesets.")
                      final_status="success_no_match"
                      final_message="No matching rules found"

            else: # Destinations were found
                 unique_dest_configs_map = {str(d): d for d in destinations_to_process}
                 unique_dest_configs = list(unique_dest_configs_map.values())
                 if len(unique_dest_configs) < len(destinations_to_process):
                      logger.warning(f"{log_prefix}: Duplicate destinations specified, processing {len(unique_dest_configs)} unique destinations.")

                 logger.info(f"{log_prefix}: Processing {len(unique_dest_configs)} unique destination actions...")
                 if modified_ds is None:
                      logger.error(f"{log_prefix}: Internal error: modified dataset is missing before storage processing!")
                      raise ValueError("Internal error: modified dataset not available for storage.")

                 pat_name = modified_ds.get("PatientName", "N/A")
                 pat_id = modified_ds.get("PatientID", "N/A")
                 inst_name = modified_ds.get("InstitutionName", "N/A")
                 logger.debug(f"{log_prefix}: Final modified dataset tags (examples): PatientName='{pat_name}', PatientID='{pat_id}', InstitutionName='{inst_name}'")

                 all_destinations_succeeded = True
                 for i, dest_config in enumerate(unique_dest_configs):
                      dest_type = dest_config.get('type','unknown_type')
                      dest_details_parts = []
                      if dest_type == "filesystem":
                           dest_details_parts.append(f"path={dest_config.get('path', '?')}")
                      elif dest_type == "cstore":
                           dest_details_parts.append(f"ae={dest_config.get('ae_title','?')}")
                           dest_details_parts.append(f"host={dest_config.get('host','?')}")
                           dest_details_parts.append(f"port={dest_config.get('port','?')}")
                      dest_id = f"Dest_{i+1}_{dest_type}({', '.join(dest_details_parts)})"

                      logger.debug(f"{log_prefix}: Attempting destination {dest_id}")
                      try:
                          storage_backend = get_storage_backend(dest_config)
                          store_result = storage_backend.store(modified_ds, original_filepath)
                          logger.info(f"{log_prefix}: Destination {dest_id} completed successfully.")
                          success_status[dest_id] = {"status": "success", "result": str(store_result)}
                      except StorageBackendError as e:
                          logger.error(f"{log_prefix}: Failed to store to destination {dest_id}: {e}", exc_info=True)
                          all_destinations_succeeded = False
                          success_status[dest_id] = {"status": "error", "message": str(e)}
                      except Exception as e:
                           logger.error(f"{log_prefix}: Unexpected error during storage process for {dest_id}: {e}", exc_info=True)
                           all_destinations_succeeded = False
                           success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}

                 # Assign final status based on destination outcomes
                 if all_destinations_succeeded:
                      final_status = "success"
                      final_message = "Processed and stored successfully to all destinations."
                 else:
                      final_status = "partial_failure"
                      final_message = "Processing complete, but one or more destinations failed."

        # --- 5. Cleanup ---
        # Cleanup if processing was successful overall, even if no destinations needed
        if final_status in ["success", "success_no_destinations", "success_no_rules", "success_no_match"]:
            # Only delete if the original file still exists (important if error occurred before this point)
            if original_filepath.is_file():
                 logger.info(f"{log_prefix}: Processing deemed successful. Deleting original file: {original_filepath}")
                 try:
                     original_filepath.unlink(missing_ok=True)
                 except OSError as e:
                     logger.warning(f"{log_prefix}: Failed to delete original file {original_filepath} after successful processing: {e}")
            else:
                 logger.info(f"{log_prefix}: Processing deemed successful, original file already gone: {original_filepath}")

        else: # Handles "partial_failure" and potentially unknown if logic missed a case
            if original_filepath.is_file():
                 logger.warning(f"{log_prefix}: Processing resulted in status '{final_status}'. Original file NOT deleted: {original_filepath}")
            else:
                 logger.warning(f"{log_prefix}: Processing resulted in status '{final_status}'. Original file was already gone: {original_filepath}")

        # Return final result
        if db: db.close() # Close DB session before returning
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status}

    except InvalidDicomError:
         logger.error(f"{log_prefix}: Invalid DICOM error caught. Moving to errors.")
         if db: db.close()
         move_to_error_dir(original_filepath, task_id)
         return {"status": "error", "message": "Invalid DICOM file format", "filepath": dicom_filepath_str}
    except Exception as exc:
        logger.error(f"{log_prefix}: Unhandled exception during processing: {exc!r}", exc_info=True)
        if db: db.close()
        try:
            retries_done = self.request.retries if self.request else 0
            max_retries_allowed = getattr(self, 'max_retries', 0) or 0
            is_final_retry = retries_done >= max_retries_allowed
            is_non_retryable_error_type = not isinstance(exc, RETRYABLE_EXCEPTIONS)

            if is_final_retry or is_non_retryable_error_type:
                 log_reason = "Max retries exceeded" if is_final_retry else "Non-retryable error type"
                 logger.error(f"{log_prefix}: {log_reason} for error: {exc!r}. Moving to errors.")
                 move_to_error_dir(original_filepath, task_id)
                 return {"status": "error", "message": f"{log_reason}: {exc!r}", "filepath": dicom_filepath_str}
            else:
                 logger.warning(f"{log_prefix}: Re-raising exception to allow Celery autoretry: {exc!r}")
                 raise exc
        except Exception as final_error_handling_exc:
             logger.critical(f"{log_prefix}: CRITICAL Error during final error handling for primary exception {exc!r}: {final_error_handling_exc!r}", exc_info=True)
             return {"status": "error", "message": f"CRITICAL error during error handling: {final_error_handling_exc!r}", "filepath": dicom_filepath_str}

    finally:
        if db and db.is_active:
            logger.warning(f"{log_prefix}: Database session found active in final 'finally' block. Closing.")
            db.close()


def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the designated error directory."""
    log_prefix = f"Task {task_id} ({filepath.name})"
    if not filepath.is_file():
         logger.warning(f"{log_prefix}: Original file {filepath} not found or is not a file. Cannot move to error dir.")
         return

    try:
        error_dir = filepath.parent.parent / "errors"
        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        error_filename = f"{filepath.stem}_{timestamp}{filepath.suffix}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        logger.info(f"{log_prefix}: Moved failed file {filepath.name} to {error_path}")
    except Exception as move_err:
        logger.critical(f"{log_prefix}: CRITICAL - Could not move failed file {filepath.name} to error dir: {move_err}", exc_info=True)
