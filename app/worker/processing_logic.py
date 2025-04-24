# app/worker/processing_logic.py

import logging
import re
import json
from copy import deepcopy
from typing import Optional, Any, List, Dict, Tuple, Union
from datetime import date, time as dt_time, datetime
from decimal import Decimal
from pydantic import TypeAdapter

import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, tag_for_keyword, repeater_has_keyword
from pydicom.valuerep import DSfloat, IS, DSdecimal, VR
from pydicom.dataset import DataElement
from pydicom.multival import MultiValue

from app.schemas.rule import MatchOperation, ModifyAction, MatchCriterion, TagModification, TagSetModification, TagPrependModification, TagSuffixModification, TagRegexReplaceModification
# --- Import StorageBackendConfig model for type hinting ---
from app.db.models.rule import RuleSet, RuleSetExecutionMode
from app.db.models.storage_backend_config import StorageBackendConfig # Import the model
# --- END Import ---
# --- Import StorageBackendError ---
from app.services.storage_backends import StorageBackendError, get_storage_backend # Import error and factory
# --- END Import ---


logger = logging.getLogger(__name__)

# --- Helper Functions (parse_dicom_tag, _values_equal, _compare_numeric, check_match, apply_modifications) ---
# ... (These functions remain unchanged) ...
def parse_dicom_tag(tag_str: str) -> Optional[BaseTag]:
    """Parses a string like '(0010,0010)' or 'PatientName' into a pydicom Tag object."""
    if not isinstance(tag_str, str):
         logger.warning(f"Invalid type for tag string: {type(tag_str)}. Expected str.")
         return None
    tag_str = tag_str.strip()
    match_ge = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", tag_str)
    if match_ge:
        try:
            return Tag(int(match_ge.group(1), 16), int(match_ge.group(2), 16))
        except ValueError:
            pass
    if re.match(r"^[a-zA-Z0-9]+$", tag_str) and not repeater_has_keyword(tag_str):
        try:
            tag_val = tag_for_keyword(tag_str)
            if tag_val:
                return Tag(tag_val)
        except (ValueError, TypeError):
            pass

    logger.warning(f"Could not parse tag string/keyword: '{tag_str}'")
    return None

def _values_equal(actual_val: Any, expected_val: Any) -> bool:
    """Compares two values, attempting numeric conversion first."""
    try:
        if isinstance(actual_val, (int, float, Decimal, DSfloat, IS, DSdecimal)) and isinstance(expected_val, (int, float, str, Decimal)):
             try:
                  expected_decimal = Decimal(str(expected_val))
                  actual_decimal = Decimal(str(actual_val))
                  return actual_decimal == expected_decimal
             except (ValueError, TypeError):
                  pass
        elif isinstance(actual_val, (date, datetime, dt_time)):
             return str(actual_val) == str(expected_val)
    except (ValueError, TypeError, Exception) as e:
         logger.debug(f"Could not compare values numerically/temporally: {actual_val} vs {expected_val}. Error: {e}. Falling back to string comparison.")
         pass
    return str(actual_val) == str(expected_val)

def _compare_numeric(actual_val: Any, expected_val: Any, op: MatchOperation) -> bool:
     """Performs numeric comparison using Decimal, returns False if types are incompatible."""
     try:
         actual_decimal = Decimal(str(actual_val))
         expected_decimal = Decimal(str(expected_val))
         if op == MatchOperation.GREATER_THAN: return actual_decimal > expected_decimal
         if op == MatchOperation.LESS_THAN: return actual_decimal < expected_decimal
         if op == MatchOperation.GREATER_EQUAL: return actual_decimal >= expected_decimal
         if op == MatchOperation.LESS_EQUAL: return actual_decimal <= expected_decimal
         return False
     except (ValueError, TypeError, Exception):
         logger.debug(f"Cannot compare non-numeric values using Decimal: '{actual_val}', '{expected_val}'")
         return False

def check_match(dataset: pydicom.Dataset, criteria: List[MatchCriterion]) -> bool:
    """
    Checks if a dataset matches ALL criteria in the list (implicit AND).
    Handles various operators and basic type coercion.
    Uses MatchCriterion schema objects.
    """
    if not criteria:
        return True

    for criterion in criteria:
        if not isinstance(criterion, MatchCriterion):
             logger.warning(f"Skipping invalid criterion object: {criterion}")
             continue

        tag_str = criterion.tag
        op = criterion.op
        expected_value = criterion.value

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key '{tag_str}' in match criteria {criterion.model_dump()}")
            return False

        actual_data_element = dataset.get(tag, None)

        if op == MatchOperation.EXISTS:
            if actual_data_element is None: logger.debug(f"Match fail: Tag {tag_str} does not exist (op: EXISTS)"); return False
            else: continue
        elif op == MatchOperation.NOT_EXISTS:
            if actual_data_element is not None: logger.debug(f"Match fail: Tag {tag_str} exists (op: NOT_EXISTS)"); return False
            else: continue

        if actual_data_element is None: logger.debug(f"Match fail: Tag {tag_str} does not exist (required for op: {op.value})"); return False

        actual_value = actual_data_element.value
        is_multi_value = isinstance(actual_value, MultiValue)
        actual_value_list = list(actual_value) if is_multi_value else [actual_value]

        match = False
        try:
            if op == MatchOperation.EQUALS: match = any(_values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.NOT_EQUALS: match = all(not _values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.CONTAINS:
                if not isinstance(expected_value, str): logger.warning(f"'contains' requires string value, got {type(expected_value)}. Skipping {tag_str}."); continue
                match = any(expected_value in str(av) for av in actual_value_list)
            elif op == MatchOperation.STARTS_WITH:
                if not isinstance(expected_value, str): logger.warning(f"'startswith' requires string value, got {type(expected_value)}. Skipping {tag_str}."); continue
                match = any(str(av).startswith(expected_value) for av in actual_value_list)
            elif op == MatchOperation.ENDS_WITH:
                if not isinstance(expected_value, str): logger.warning(f"'endswith' requires string value, got {type(expected_value)}. Skipping {tag_str}."); continue
                match = any(str(av).endswith(expected_value) for av in actual_value_list)
            elif op == MatchOperation.REGEX:
                match = any(bool(re.search(expected_value, str(av))) for av in actual_value_list)
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 match = any(_compare_numeric(av, expected_value, op) for av in actual_value_list)
            elif op == MatchOperation.IN:
                 match = any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 match = not any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)

            if not match:
                 actual_repr = repr(actual_value_list[0]) if len(actual_value_list) == 1 else f"{len(actual_value_list)} values"
                 logger.debug(f"Match fail: Tag {tag_str} ('{actual_repr}') failed op '{op.value}' with value '{expected_value}'")
                 return False
        except Exception as e:
            logger.error(f"Error during matching for tag {tag_str} with op {op.value}: {e}", exc_info=True)
            return False
    return True

def apply_modifications(dataset: pydicom.Dataset, modifications: List[TagModification]):
    """
    Applies tag modifications to the dataset IN-PLACE based on a list of actions
    defined by TagModification schema objects (discriminated union).
    """
    if not modifications:
        return

    STRING_LIKE_VRS = {'AE', 'AS', 'CS', 'DA', 'DS', 'DT', 'IS', 'LO', 'LT', 'PN', 'SH', 'ST', 'TM', 'UI', 'UR', 'UT'}

    for mod in modifications:
        tag_str = mod.tag
        action = mod.action

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key '{tag_str}' in modification: {mod.model_dump()}")
            continue

        try:
            current_element: Optional[DataElement] = dataset.get(tag, None)
            current_vr = current_element.VR if current_element else None
            current_value = current_element.value if current_element else None

            if action == ModifyAction.DELETE:
                if tag in dataset: del dataset[tag]; logger.debug(f"Deleted tag {tag_str} ({tag})")
                else: logger.debug(f"Tag {tag_str} ({tag}) not found for deletion.")
            elif action == ModifyAction.SET:
                 new_value = mod.value; vr = mod.vr; final_vr = vr
                 if not final_vr:
                      if current_element: final_vr = current_vr; logger.debug(f"Using existing VR '{final_vr}' for SET on tag {tag_str} ({tag}).")
                      else:
                           try: final_vr = dictionary_VR(tag); logger.debug(f"Inferred VR '{final_vr}' for SET on tag {tag_str} ({tag}) from dictionary.")
                           except KeyError: final_vr = 'UN'; logger.warning(f"Could not determine VR for new tag {tag_str} ({tag}) in SET. Defaulting to '{final_vr}'. Specify VR in rule for clarity.")
                 processed_value = new_value
                 if new_value is not None:
                     try:
                          if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'): processed_value = [int(v) for v in new_value] if isinstance(new_value, list) else int(new_value)
                          elif final_vr in ('FL', 'FD', 'OD', 'OF'): processed_value = [float(v) for v in new_value] if isinstance(new_value, list) else float(new_value)
                          elif final_vr == 'DS': processed_value = [str(v) for v in new_value] if isinstance(new_value, list) else str(new_value)
                          elif final_vr == 'IS': processed_value = [str(v) for v in new_value] if isinstance(new_value, list) else str(new_value)
                     except (ValueError, TypeError) as conv_err: logger.warning(f"Value '{new_value}' for tag {tag_str} ({tag}) could not be coerced to expected type for VR '{final_vr}': {conv_err}. Setting as is."); processed_value = new_value
                 else: processed_value = None; logger.debug(f"Setting tag {tag_str} ({tag}) to None (will likely be empty value).")
                 dataset[tag] = DataElement(tag, final_vr, processed_value)
                 logger.debug(f"Set tag {tag_str} ({tag}) to '{processed_value}' (VR: {final_vr})")
            elif action in [ModifyAction.PREPEND, ModifyAction.SUFFIX]:
                 if not current_element: logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) does not exist."); continue
                 if current_vr not in STRING_LIKE_VRS: logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) has non-string-like VR '{current_vr}'."); continue
                 text_to_add = mod.value
                 if isinstance(current_value, MultiValue):
                      modified_list = [(text_to_add + str(item) if action == ModifyAction.PREPEND else str(item) + text_to_add) for item in current_value]
                      dataset[tag].value = modified_list
                      logger.debug(f"Applied {action.value} to all {len(modified_list)} items of multivalue tag {tag_str} ({tag}).")
                 else:
                      original_str = str(current_value)
                      modified_value = text_to_add + original_str if action == ModifyAction.PREPEND else original_str + text_to_add
                      dataset[tag].value = modified_value
                      logger.debug(f"Applied {action.value} to tag {tag_str} ({tag}). New value: '{modified_value}'")
            elif action == ModifyAction.REGEX_REPLACE:
                 if not current_element: logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) does not exist."); continue
                 if current_vr not in STRING_LIKE_VRS: logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) has non-string-like VR '{current_vr}'."); continue
                 pattern = mod.pattern; replacement = mod.replacement
                 try:
                      if isinstance(current_value, MultiValue):
                           modified_list = [re.sub(pattern, replacement, str(item)) for item in current_value]
                           dataset[tag].value = modified_list
                           logger.debug(f"Applied {action.value} to multivalue tag {tag_str} ({tag}) using pattern '{pattern}'.")
                      else:
                           original_str = str(current_value)
                           modified_value = re.sub(pattern, replacement, original_str)
                           dataset[tag].value = modified_value
                           logger.debug(f"Applied {action.value} to tag {tag_str} ({tag}) using pattern '{pattern}'. New value: '{modified_value}'")
                 except re.error as regex_err: logger.error(f"Error applying {action.value} regex for tag {tag_str} ({tag}): {regex_err}", exc_info=True)
        except Exception as e:
            logger.error(f"Failed to apply modification ({action.value}) for tag {tag_str} ({tag}): {e}", exc_info=True)


# --- Main Processing Function ---

def process_dicom_instance(
    original_ds: pydicom.Dataset,
    active_rulesets: List[RuleSet],
    source_identifier: str
) -> Tuple[Optional[pydicom.Dataset], List[str], List[Dict[str, Any]]]:
    """
    Processes a single DICOM dataset against a list of rulesets.

    Args:
        original_ds: The original pydicom dataset.
        active_rulesets: List of active, ordered RuleSet DB model objects (including their Rules and Destinations).
        source_identifier: The identifier of the source system.

    Returns:
        A tuple containing:
        - modified_ds: The modified dataset (a deepcopy), or None if no modifications occurred.
        - applied_rules_info: List of strings describing the matched rules.
        - destinations_to_process: List of unique destination config dictionaries (from linked StorageBackendConfig).
    """
    logger.debug(f"Processing instance from source '{source_identifier}' against {len(active_rulesets)} rulesets.")
    modified_ds: Optional[pydicom.Dataset] = None
    applied_rules_info = []
    # --- UPDATED: Store full destination config dicts ---
    destinations_to_process: List[Dict[str, Any]] = []
    # --- END UPDATED ---
    any_rule_matched = False
    modifications_applied = False

    for ruleset in active_rulesets:
        logger.debug(f"Evaluating RuleSet '{ruleset.name}' (ID: {ruleset.id}, Priority: {ruleset.priority})")
        rules_to_evaluate = ruleset.rules or []
        if not rules_to_evaluate:
             logger.debug(f"RuleSet '{ruleset.name}' has no rules.")
             continue

        matched_rule_in_this_set = False
        for rule in rules_to_evaluate:
             if not rule.is_active:
                 logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) is inactive. Skipping.")
                 continue

             rule_sources = rule.applicable_sources
             if rule_sources and source_identifier not in rule_sources:
                  logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) does not apply to source '{source_identifier}'. Skipping.")
                  continue

             logger.debug(f"Checking rule '{rule.name}' (ID: {rule.id}, Priority: {rule.priority}) against source '{source_identifier}'")
             try:
                 match_criteria_schemas = [MatchCriterion(**crit) for crit in rule.match_criteria or []]

                 if check_match(original_ds, match_criteria_schemas):
                     logger.info(f"Rule '{ruleset.name}' / '{rule.name}' MATCHED.")
                     any_rule_matched = True
                     matched_rule_in_this_set = True

                     if rule.tag_modifications and modified_ds is None:
                          modified_ds = deepcopy(original_ds)
                          logger.debug("Created deep copy of dataset for modifications.")

                     if rule.tag_modifications and modified_ds is not None:
                          try:
                            ta_tag_modification = TypeAdapter(TagModification)
                            mod_schemas = [ta_tag_modification.validate_python(mod) for mod in rule.tag_modifications]
                          except Exception as val_err:
                               logger.error(f"Rule '{rule.name}': Failed to validate tag_modifications against schema: {val_err}. Skipping modifications.", exc_info=True)
                               mod_schemas = []

                          if mod_schemas:
                              logger.debug(f"Applying {len(mod_schemas)} modifications for rule '{rule.name}'...")
                              apply_modifications(modified_ds, mod_schemas)
                              modifications_applied = True
                          else:
                              logger.debug(f"Rule '{rule.name}' matched but had no valid modifications defined.")

                     # --- UPDATED: Collect Destinations from Relationship ---
                     # rule.destinations is now a list of StorageBackendConfig objects
                     rule_destinations: List[StorageBackendConfig] = rule.destinations or []
                     for dest_config_obj in rule_destinations:
                          if dest_config_obj.is_enabled:
                                # Construct the dictionary needed by get_storage_backend
                                # It expects {'type': ..., **config_dict}
                                backend_config_dict = dest_config_obj.config or {}
                                # Ensure 'type' is present, using the model's backend_type
                                backend_config_dict['type'] = dest_config_obj.backend_type
                                destinations_to_process.append(backend_config_dict)
                                logger.debug(f"Added destination '{dest_config_obj.name}' (Type: {dest_config_obj.backend_type}) from rule '{rule.name}'.")
                          else:
                               logger.debug(f"Skipping disabled destination '{dest_config_obj.name}' from rule '{rule.name}'.")
                     # --- END UPDATED ---

                     applied_rules_info.append(f"{ruleset.name}/{rule.name} (ID:{rule.id})")

                     if ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                         logger.debug(f"RuleSet '{ruleset.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                         break

             except Exception as rule_proc_exc:
                 logger.error(f"Error processing rule '{ruleset.name}/{rule.name}': {rule_proc_exc}", exc_info=True)

        if matched_rule_in_this_set and ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
            logger.debug(f"FIRST_MATCH rule found in set '{ruleset.name}'. Stopping ruleset evaluation.")
            break

    if not any_rule_matched:
        logger.info(f"No applicable rules matched for source '{source_identifier}'. No modifications or destinations applied.")
        return None, [], []

    # --- De-duplicate destinations based on the full config dictionary ---
    unique_dest_strings = set()
    unique_destination_configs = []
    for dest_config_dict in destinations_to_process:
        try:
            # Serialize the config dict for de-duplication
            # Use separators=(',', ':') for compact, consistent serialization
            json_string = json.dumps(dest_config_dict, sort_keys=True, separators=(',', ':'))
            if json_string not in unique_dest_strings:
                unique_dest_strings.add(json_string)
                unique_destination_configs.append(dest_config_dict) # Add the dictionary
        except TypeError as e:
             logger.warning(f"Could not serialize destination config for de-duplication: {dest_config_dict}. Error: {e}. Skipping this instance.")

    logger.debug(f"Found {len(unique_destination_configs)} unique destinations after processing rules.")

    return modified_ds if modifications_applied else None, applied_rules_info, unique_destination_configs
