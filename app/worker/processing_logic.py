# app/worker/processing_logic.py

import logging
import re
import json # Need json for de-duplication
from copy import deepcopy
from typing import Optional, Any, List, Dict, Tuple, Union
from datetime import date, time as dt_time, datetime
from decimal import Decimal

import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, tag_for_keyword, keyword_for_tag, repeater_has_keyword
from pydicom.valuerep import DSfloat, IS, DSdecimal, VR # Import VR base class
from pydicom.dataset import DataElement
from pydicom.multival import MultiValue # Explicitly import MultiValue

# Import Enums and Schemas used in rules directly
from app.schemas.rule import MatchOperation, ModifyAction, MatchCriterion, TagModification
# Import the RuleSet DB Model for type hinting the function argument
from app.db.models.rule import RuleSet, RuleSetExecutionMode # Import RuleSet model and Enum

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
    # Check for (gggg,eeee) format with optional parens and flexible spacing
    match_ge = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", tag_str)
    if match_ge:
        try:
            # Use BaseTag for wider compatibility if needed, but Tag is standard
            return Tag(int(match_ge.group(1), 16), int(match_ge.group(2), 16))
        except ValueError:
            pass # Fall through if parsing fails
    # Check for keyword, excluding sequence repeaters which aren't real tags
    # Use regex to ensure it's a valid keyword format (alphanumeric)
    if re.match(r"^[a-zA-Z0-9]+$", tag_str) and not repeater_has_keyword(tag_str):
        try:
            tag_val = tag_for_keyword(tag_str)
            if tag_val:
                return Tag(tag_val) # Convert numeric tag value back to Tag object
        except (ValueError, TypeError): # Catch potential errors if keyword is weird type
            pass # Fall through

    logger.warning(f"Could not parse tag string/keyword: '{tag_str}'")
    return None


# --- Internal helper for check_match ---
def _values_equal(actual_val: Any, expected_val: Any) -> bool:
    """Compares two values, attempting numeric conversion first."""
    try:
        # Attempt numeric comparison if both look numeric
        if isinstance(actual_val, (int, float, Decimal, DSfloat, IS, DSdecimal)) and isinstance(expected_val, (int, float, str, Decimal)):
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


# Enhanced Matching Logic
def check_match(dataset: pydicom.Dataset, criteria: List[MatchCriterion]) -> bool:
    """
    Checks if a dataset matches ALL criteria in the list (implicit AND).
    Handles various operators and basic type coercion.
    Uses MatchCriterion schema objects.
    """
    if not criteria:
        return True # Empty criteria list always matches

    for criterion in criteria:
        # Schema validation should happen upstream, but basic check here is good
        if not isinstance(criterion, MatchCriterion):
             logger.warning(f"Skipping invalid criterion object: {criterion}")
             continue

        tag_str = criterion.tag
        op = criterion.op # Already an Enum
        expected_value = criterion.value # Can be None for exists/not_exists

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key '{tag_str}' in match criteria {criterion.model_dump()}") # Log dumped model
            return False # If the tag in the rule is bad, the rule can't match.

        actual_data_element = dataset.get(tag, None)

        # --- Handle existence checks first ---
        if op == MatchOperation.EXISTS:
            if actual_data_element is None:
                logger.debug(f"Match fail: Tag {tag_str} does not exist (op: EXISTS)")
                return False
            else:
                continue
        elif op == MatchOperation.NOT_EXISTS:
            if actual_data_element is not None:
                logger.debug(f"Match fail: Tag {tag_str} exists (op: NOT_EXISTS)")
                return False
            else:
                continue

        # --- For other ops, the tag must exist ---
        if actual_data_element is None:
             logger.debug(f"Match fail: Tag {tag_str} does not exist (required for op: {op.value})")
             return False

        # Extract actual value (handle MultiValue by converting to list)
        actual_value = actual_data_element.value
        is_multi_value = isinstance(actual_value, MultiValue)
        actual_value_list = list(actual_value) if is_multi_value else [actual_value]

        # --- Perform comparison based on operator ---
        match = False # Assume false until proven true for the current criterion
        try:
            # Comparison functions often need to iterate through the actual value list
            if op == MatchOperation.EQUALS:
                match = any(_values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.NOT_EQUALS:
                # Should match if *any* actual value is not equal to the expected value?
                # Or if *all* actual values are not equal? Let's assume *all* must be unequal.
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
                # Schema validation ensures expected_value is string and pattern compiles
                match = any(bool(re.search(expected_value, str(av))) for av in actual_value_list)
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 # Check if *any* value in a multi-value list satisfies the comparison
                 match = any(_compare_numeric(av, expected_value, op) for av in actual_value_list)
            elif op == MatchOperation.IN:
                 # Schema validation ensures expected_value is list
                 # Match if *any* actual value is equal to *any* expected value
                 match = any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 # Schema validation ensures expected_value is list
                 # Match if *all* actual values are *not* equal to *any* expected value.
                 # Equivalent to: not (any(av == ev for ...))
                 match = not any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)

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


# --- Modification Logic (UPDATED for new actions) ---

# Define VRs that generally support string-like operations
STRING_LIKE_VRS = {'AE', 'AS', 'CS', 'DA', 'DS', 'DT', 'IS', 'LO', 'LT', 'PN', 'SH', 'ST', 'TM', 'UI', 'UR', 'UT'}

def apply_modifications(dataset: pydicom.Dataset, modifications: List[TagModification]):
    """
    Applies tag modifications to the dataset IN-PLACE based on a list of actions
    defined by TagModification schema objects (discriminated union).
    """
    if not modifications:
        return

    for mod in modifications:
        # Schema validation happens upstream, mod is one of the specific types
        tag_str = mod.tag
        action = mod.action # The discriminator field (Enum member)

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key '{tag_str}' in modification: {mod.model_dump()}")
            continue

        try:
            current_element: Optional[DataElement] = dataset.get(tag, None)
            current_vr = current_element.VR if current_element else None
            current_value = current_element.value if current_element else None

            # --- Action Dispatch ---
            if action == ModifyAction.DELETE:
                if tag in dataset:
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_str} ({tag})")
                else:
                    logger.debug(f"Tag {tag_str} ({tag}) not found for deletion.")

            elif action == ModifyAction.SET:
                 # mod is TagSetModification here
                 new_value = mod.value
                 vr = mod.vr
                 final_vr = vr
                 if not final_vr:
                      if current_element:
                           final_vr = current_vr
                           logger.debug(f"Using existing VR '{final_vr}' for SET on tag {tag_str} ({tag}).")
                      else:
                           try:
                               final_vr = dictionary_VR(tag)
                               logger.debug(f"Inferred VR '{final_vr}' for SET on tag {tag_str} ({tag}) from dictionary.")
                           except KeyError:
                               final_vr = 'UN' # Unknown VR
                               logger.warning(f"Could not determine VR for new tag {tag_str} ({tag}) in SET. Defaulting to '{final_vr}'. Specify VR in rule for clarity.")

                 # Basic type coercion/validation based on VR (can be expanded)
                 processed_value = new_value
                 # Apply type coercion only if value is not None (e.g., setting empty string is valid)
                 if new_value is not None:
                     try:
                          if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'):
                               # Handle lists for multi-value integers
                               if isinstance(new_value, list):
                                   processed_value = [int(v) for v in new_value]
                               else:
                                   processed_value = int(new_value)
                          elif final_vr in ('FL', 'FD', 'OD', 'OF'):
                               if isinstance(new_value, list):
                                   processed_value = [float(v) for v in new_value]
                               else:
                                   processed_value = float(new_value)
                          elif final_vr == 'DS':
                               # DS must be string or list of strings per DICOM standard
                               processed_value = [str(v) for v in new_value] if isinstance(new_value, list) else str(new_value)
                          elif final_vr == 'IS':
                               # IS must be string or list of strings per DICOM standard
                               processed_value = [str(v) for v in new_value] if isinstance(new_value, list) else str(new_value)
                          # TODO: Add DA, TM, DT parsing/validation later
                          # Add other VR-specific processing here...
                     except (ValueError, TypeError) as conv_err:
                          logger.warning(f"Value '{new_value}' for tag {tag_str} ({tag}) could not be coerced to expected type for VR '{final_vr}': {conv_err}. Setting as is.")
                          processed_value = new_value # Set original value if coercion fails
                 else:
                     # Handle explicit setting to None -> empty value for most VRs?
                     # Or should this be an error? DICOM allows empty values.
                     # Let pydicom handle creating an element with None value, which usually means empty string.
                     processed_value = None
                     logger.debug(f"Setting tag {tag_str} ({tag}) to None (will likely be empty value).")


                 # Use DataElement constructor for more control, or add_new/replace
                 dataset[tag] = DataElement(tag, final_vr, processed_value)
                 logger.debug(f"Set tag {tag_str} ({tag}) to '{processed_value}' (VR: {final_vr})")

            elif action in [ModifyAction.PREPEND, ModifyAction.SUFFIX]:
                 # mod is TagPrependModification or TagSuffixModification
                 if not current_element:
                      logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) does not exist.")
                      continue
                 if current_vr not in STRING_LIKE_VRS:
                      logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) has non-string-like VR '{current_vr}'.")
                      continue

                 text_to_add = mod.value # Already validated as string by schema
                 # Handle MultiValue vs SingleValue
                 if isinstance(current_value, MultiValue):
                      # Modify all elements in the list
                      modified_list = []
                      for item in current_value:
                           item_str = str(item)
                           modified_item = text_to_add + item_str if action == ModifyAction.PREPEND else item_str + text_to_add
                           modified_list.append(modified_item)
                      dataset[tag].value = modified_list # Assign back modified list
                      logger.debug(f"Applied {action.value} to all {len(modified_list)} items of multivalue tag {tag_str} ({tag}).")
                 else:
                      # Single value
                      original_str = str(current_value)
                      modified_value = text_to_add + original_str if action == ModifyAction.PREPEND else original_str + text_to_add
                      dataset[tag].value = modified_value # Assign back modified string
                      logger.debug(f"Applied {action.value} to tag {tag_str} ({tag}). New value: '{modified_value}'")


            elif action == ModifyAction.REGEX_REPLACE:
                 # mod is TagRegexReplaceModification
                 if not current_element:
                      logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) does not exist.")
                      continue
                 if current_vr not in STRING_LIKE_VRS:
                      logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) has non-string-like VR '{current_vr}'.")
                      continue

                 pattern = mod.pattern         # Already validated as string/compilable regex
                 replacement = mod.replacement # Already validated as string

                 try:
                      # Handle MultiValue vs SingleValue
                      if isinstance(current_value, MultiValue):
                           # Apply regex to each item in the list
                           modified_list = [re.sub(pattern, replacement, str(item)) for item in current_value]
                           dataset[tag].value = modified_list
                           logger.debug(f"Applied {action.value} to multivalue tag {tag_str} ({tag}) using pattern '{pattern}'.")
                      else:
                           # Single value
                           original_str = str(current_value)
                           modified_value = re.sub(pattern, replacement, original_str)
                           dataset[tag].value = modified_value
                           logger.debug(f"Applied {action.value} to tag {tag_str} ({tag}) using pattern '{pattern}'. New value: '{modified_value}'")
                 except re.error as regex_err:
                      # Should be caught by schema validation, but catch runtime errors just in case
                      logger.error(f"Error applying {action.value} regex for tag {tag_str} ({tag}): {regex_err}", exc_info=True)


        except Exception as e:
            logger.error(f"Failed to apply modification ({action.value}) for tag {tag_str} ({tag}): {e}", exc_info=True)


# --- Main Processing Function ---

def process_dicom_instance(
    original_ds: pydicom.Dataset,
    active_rulesets: List[RuleSet], # Pass RuleSet model objects directly
    source_identifier: str
) -> Tuple[Optional[pydicom.Dataset], List[str], List[Dict[str, Any]]]:
    """
    Processes a single DICOM dataset against a list of rulesets.

    Args:
        original_ds: The original pydicom dataset.
        active_rulesets: List of active, ordered RuleSet DB model objects (including their Rules).
        source_identifier: The identifier of the source system.

    Returns:
        A tuple containing:
        - modified_ds: The modified dataset (a deepcopy), or None if no modifications occurred.
        - applied_rules_info: List of strings describing the matched rules.
        - destinations_to_process: List of unique destination config dictionaries.
    """
    logger.debug(f"Processing instance from source '{source_identifier}' against {len(active_rulesets)} rulesets.")
    modified_ds: Optional[pydicom.Dataset] = None # Start with None, create copy only if needed
    applied_rules_info = []
    destinations_to_process = []
    any_rule_matched = False # Track if *any* rule matched (even if no modifications/destinations)
    modifications_applied = False # Track if modifications actually happened

    for ruleset in active_rulesets:
        logger.debug(f"Evaluating RuleSet '{ruleset.name}' (ID: {ruleset.id}, Priority: {ruleset.priority})")
        # Ensure ruleset.rules is not None before iterating
        rules_to_evaluate = ruleset.rules or []
        if not rules_to_evaluate:
             logger.debug(f"RuleSet '{ruleset.name}' has no rules.")
             continue

        matched_rule_in_this_set = False
        # Assume rules within the ruleset are already ordered by priority from the DB query
        for rule in rules_to_evaluate:
             if not rule.is_active:
                 logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) is inactive. Skipping.")
                 continue

             # --- SOURCE CHECK ---
             # rule.applicable_sources is loaded eagerly as a relationship -> List[Source] or similar
             # Need to adapt this check if it's a list of string names directly on the Rule model
             # Assuming rule.applicable_sources is List[str] or None as defined in schema/model
             rule_sources = rule.applicable_sources
             if rule_sources and source_identifier not in rule_sources:
                  logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) does not apply to source '{source_identifier}'. Skipping.")
                  continue
             # --- END SOURCE CHECK ---

             logger.debug(f"Checking rule '{rule.name}' (ID: {rule.id}, Priority: {rule.priority}) against source '{source_identifier}'")
             try:
                 # Convert DB model criteria/mods/destinations (likely stored as JSON) to Pydantic schemas for processing
                 # Assuming rule.match_criteria is already a list of dicts compatible with MatchCriterion
                 match_criteria_schemas = [MatchCriterion(**crit) for crit in rule.match_criteria or []]

                 # Perform matching using the original dataset
                 if check_match(original_ds, match_criteria_schemas):
                     logger.info(f"Rule '{ruleset.name}' / '{rule.name}' MATCHED.")
                     any_rule_matched = True
                     matched_rule_in_this_set = True

                     # Create the copy *only* when the first modification is needed
                     if rule.tag_modifications and modified_ds is None:
                          modified_ds = deepcopy(original_ds)
                          logger.debug("Created deep copy of dataset for modifications.")

                     # Apply Modifications (to the copy if it exists)
                     if rule.tag_modifications and modified_ds is not None:
                          # Assuming rule.tag_modifications is list of dicts compatible with TagModification union
                          try:
                              mod_schemas = [pydicom.serialization.pydantic_validate_json(TagModification, mod) for mod in rule.tag_modifications]
                          except Exception as val_err:
                               logger.error(f"Rule '{rule.name}': Failed to validate tag_modifications against schema: {val_err}. Skipping modifications.", exc_info=True)
                               mod_schemas = [] # Skip mods for this rule

                          if mod_schemas:
                              logger.debug(f"Applying {len(mod_schemas)} modifications for rule '{rule.name}'...")
                              apply_modifications(modified_ds, mod_schemas)
                              modifications_applied = True # Mark that state has changed
                          else:
                              logger.debug(f"Rule '{rule.name}' matched but had no valid modifications defined.")

                     # Collect Destinations
                     # Assuming rule.destinations is list of dicts {type: ..., config: ...}
                     rule_destinations = rule.destinations or []
                     for dest_dict in rule_destinations:
                          dest_type = dest_dict.get('type')
                          dest_config = dest_dict.get('config')
                          if dest_type and isinstance(dest_config, dict):
                                processing_config = dest_config.copy()
                                processing_config['type'] = dest_type
                                destinations_to_process.append(processing_config)
                                logger.debug(f"Added destination type '{dest_type}' from rule '{rule.name}'.")
                          else:
                               logger.warning(f"Rule '{rule.name}': Skipping invalid destination object: {dest_dict}")


                     applied_rules_info.append(f"{ruleset.name}/{rule.name} (ID:{rule.id})")

                     # Handle Execution Mode for the RuleSet
                     if ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                         logger.debug(f"RuleSet '{ruleset.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                         break # Stop processing rules within this ruleset

             except Exception as rule_proc_exc:
                 logger.error(f"Error processing rule '{ruleset.name}/{rule.name}': {rule_proc_exc}", exc_info=True)
                 # Decide if one rule error should stop all processing? For now, continue to next rule/ruleset.

        # After checking all rules in a set, if we matched one and mode is FIRST_MATCH, break outer loop too
        if matched_rule_in_this_set and ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
            logger.debug(f"FIRST_MATCH rule found in set '{ruleset.name}'. Stopping ruleset evaluation.")
            break # Stop processing further rulesets

    if not any_rule_matched:
        logger.info(f"No applicable rules matched for source '{source_identifier}'. No modifications or destinations applied.")
        # Return None for dataset because no rules matched at all
        return None, [], []

    # --- De-duplicate destinations ---
    unique_dest_strings = set()
    unique_destination_configs = []
    for dest_config in destinations_to_process:
        try:
            # Serialize the config for de-duplication
            json_string = json.dumps(dest_config, sort_keys=True, separators=(',', ':'))
            if json_string not in unique_dest_strings:
                unique_dest_strings.add(json_string)
                unique_destination_configs.append(dest_config)
        except TypeError as e:
             logger.warning(f"Could not serialize destination config for de-duplication: {dest_config}. Error: {e}. Skipping this instance.")

    logger.debug(f"Found {len(unique_destination_configs)} unique destinations after processing rules.")

    # Return the modified dataset ONLY if modifications were actually applied.
    # Otherwise, the caller should use the original dataset for sending.
    # The applied_rules_info and destinations are returned regardless if any rule matched.
    return modified_ds if modifications_applied else None, applied_rules_info, unique_destination_configs
