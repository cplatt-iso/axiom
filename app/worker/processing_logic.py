# --- START OF FILE processing_logic.py ---

# filename: backend/app/worker/processing_logic.py

import logging
import re
import json
import asyncio # Import asyncio
from copy import deepcopy
from typing import Optional, Any, List, Dict, Tuple, Union, Literal
from datetime import date, time as dt_time, datetime, timezone, time # Keep time import
from decimal import Decimal, InvalidOperation # Import InvalidOperation
from pydantic import BaseModel, Field, TypeAdapter

# --- Pydicom Imports ---
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, tag_for_keyword, repeater_has_keyword, keyword_for_tag # Added keyword_for_tag
from pydicom.valuerep import DSfloat, IS, DSdecimal, VR # Keep VR import
from pydicom.dataset import DataElement, Dataset
from pydicom.multival import MultiValue
from pydicom.sequence import Sequence

# --- App Imports ---
from app.schemas.rule import (
    MatchOperation, ModifyAction as ModifyActionEnum, MatchCriterion, AssociationMatchCriterion,
    TagModification, TagSetModification, TagDeleteModification,
    TagPrependModification, TagSuffixModification, TagRegexReplaceModification,
    TagCopyModification, TagMoveModification, TagCrosswalkModification
)
# Import specific model types needed
# Ensure these paths are correct relative to where this file runs
from app.db.models import RuleSet, RuleSetExecutionMode, Rule, CrosswalkMap, Schedule
from app.db.models.storage_backend_config import StorageBackendConfig # Import base model for type hint

from app.services.storage_backends import StorageBackendError, get_storage_backend
from app.core.config import settings
from app import crud
from app.crosswalk import service as crosswalk_service

# --- ADDED: AI Service and Async Bridge ---
from app.services import ai_assist_service
try:
    import anyio # To run async AI call from sync context
    ANYIO_AVAILABLE = True
except ImportError:
    ANYIO_AVAILABLE = False
# --- END ADDED ---

logger = logging.getLogger(__name__)

# --- Constants and Helpers ---
ORIGINAL_ATTRIBUTES_SEQUENCE_TAG = Tag(0x0400, 0x0550)
# Other constants would go here if needed

# --- is_schedule_active ---
def is_schedule_active(schedule: Optional[Schedule], current_time: datetime) -> bool:
    """Checks if a schedule is active at the given time."""
    if schedule is None or not schedule.is_enabled:
        return True # No schedule or disabled schedule means always active

    if not isinstance(schedule.time_ranges, list):
        logger.warning(f"Schedule ID {schedule.id} ('{schedule.name}') has invalid time_ranges (not a list). Assuming inactive.")
        return False

    if not schedule.time_ranges:
        logger.debug(f"Schedule ID {schedule.id} ('{schedule.name}') has no time ranges defined. Assuming inactive.")
        return False

    current_day_str = current_time.strftime("%a") # Use %a for abbreviated day name (e.g., 'Mon')
    current_time_obj = current_time.time()

    logger.debug(f"Checking schedule '{schedule.name}' ID: {schedule.id}. Current: {current_time.isoformat()} ({current_day_str} {current_time_obj})")

    for time_range in schedule.time_ranges:
        try:
            # Validate time range structure
            days = time_range.get("days") # Should be list like ["Mon", "Tue"]
            start_time_str = time_range.get("start_time") # e.g., "09:00:00"
            end_time_str = time_range.get("end_time") # e.g., "17:00:00"

            if not isinstance(days, list) or not start_time_str or not end_time_str:
                logger.warning(f"Schedule {schedule.id}: Skipping invalid time range format: {time_range}")
                continue

            # Normalize day abbreviations for comparison (e.g., ensure consistent case like 'Mon')
            normalized_days = [day[:3].title() for day in days if isinstance(day, str)]

            if current_day_str not in normalized_days:
                # logger.debug(f"Schedule '{schedule.name}': Day {current_day_str} not in range days {normalized_days}")
                continue

            # Parse times
            start_time = time.fromisoformat(start_time_str)
            end_time = time.fromisoformat(end_time_str)

            # logger.debug(f"Schedule '{schedule.name}': Comparing {current_time_obj} with range {start_time} - {end_time}")

            # Check if current time falls within the range (handles overnight)
            if start_time <= end_time: # Normal range (e.g., 09:00 - 17:00)
                if start_time <= current_time_obj < end_time:
                    logger.info(f"Schedule '{schedule.name}' ACTIVE: Time {current_time_obj} within {start_time_str}-{end_time_str} on {current_day_str}.")
                    return True
            else: # Overnight range (e.g., 22:00 - 06:00)
                if current_time_obj >= start_time or current_time_obj < end_time:
                    logger.info(f"Schedule '{schedule.name}' ACTIVE (overnight): Time {current_time_obj} within {start_time_str}-{end_time_str} on {current_day_str}.")
                    return True

        except (ValueError, TypeError) as e:
            logger.error(f"Schedule {schedule.id}: Error parsing time range {time_range}: {e}", exc_info=True)
            continue # Skip malformed range

    # If loop completes without finding an active range
    logger.debug(f"Schedule '{schedule.name}' INACTIVE: No matching active range found for {current_time_obj} on {current_day_str}.")
    return False


# --- parse_dicom_tag ---
def parse_dicom_tag(tag_str: str) -> Optional[BaseTag]:
    """Parses a DICOM tag string (keyword or GGGG,EEEE) into a pydicom Tag."""
    if not isinstance(tag_str, str):
        logger.warning(f"Invalid type for tag string: {type(tag_str)}")
        return None
    tag_str = tag_str.strip()

    # Match GGGG,EEEE format (allow optional parentheses and spaces)
    match_ge = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", tag_str, re.IGNORECASE)
    if match_ge:
        try:
            group1_str = match_ge.group(1)
            group2_str = match_ge.group(2)
            group1 = int(group1_str, 16)
            group2 = int(group2_str, 16)
            return Tag(group1, group2)
        # *** SYNTAX FIX: Corrected Python 3 except syntax ***
        except (ValueError, IndexError):
            logger.warning(f"Could not parse hexadecimal group/element from '{tag_str}'")
            pass # Fall through

    # Match Keyword format
    if re.match(r"^[a-zA-Z0-9]+$", tag_str):
        try:
            if not repeater_has_keyword(tag_str):
                 tag_val = tag_for_keyword(tag_str)
                 if tag_val: return Tag(tag_val)
                 else: logger.debug(f"Keyword '{tag_str}' not found in pydicom dictionary.") # More specific log
            else:
                 logger.warning(f"Keyword '{tag_str}' corresponds to a repeater group, cannot parse as single tag.")
        except (ValueError, TypeError) as e:
             logger.warning(f"Error converting keyword '{tag_str}' to tag: {e}")
             pass # Fall through

    logger.warning(f"Could not parse DICOM tag from string/keyword: '{tag_str}'")
    return None

# --- _values_equal ---
def _values_equal(actual_val: Any, expected_val: Any) -> bool:
    """Compares two values, trying numeric comparison first."""
    if actual_val is None and expected_val is None: return True
    if actual_val is None or expected_val is None: return False

    # Try numeric comparison
    try:
        # Handle pydicom specific numeric types
        if isinstance(actual_val, (DSfloat, IS, DSdecimal)): actual_decimal = actual_val.real
        else: actual_decimal = Decimal(str(actual_val))

        # Ensure expected value is also treated as Decimal for comparison
        expected_decimal = Decimal(str(expected_val))
        return actual_decimal == expected_decimal
    except (ValueError, TypeError, InvalidOperation):
        pass # Fallback to string comparison if numeric fails

    # String comparison as fallback
    try:
        return str(actual_val) == str(expected_val)
    except Exception:
        # If string conversion fails for some reason
        return False


# --- _compare_numeric ---
def _compare_numeric(actual_val: Any, expected_val: Any, op: MatchOperation) -> bool:
    """Performs numeric comparison (gt, lt, ge, le)."""
    try:
         # Handle pydicom specific numeric types
         if isinstance(actual_val, (DSfloat, IS, DSdecimal)): actual_decimal = actual_val.real
         else: actual_decimal = Decimal(str(actual_val))
         # Ensure expected value is also treated as Decimal
         expected_decimal = Decimal(str(expected_val))

         if op == MatchOperation.GREATER_THAN: return actual_decimal > expected_decimal
         if op == MatchOperation.LESS_THAN: return actual_decimal < expected_decimal
         if op == MatchOperation.GREATER_EQUAL: return actual_decimal >= expected_decimal
         if op == MatchOperation.LESS_EQUAL: return actual_decimal <= expected_decimal
         # Should not happen if called correctly, but default to false
         return False
    except (ValueError, TypeError, InvalidOperation):
        # If conversion to Decimal fails, comparison is false
        return False

# --- check_match ---
def check_match(dataset: pydicom.Dataset, criteria: List[MatchCriterion]) -> bool:
    """Checks if a dataset matches all provided tag criteria."""
    if not criteria: return True # No criteria means it matches

    for criterion in criteria:
        # Basic validation of criterion structure
        if not isinstance(criterion, MatchCriterion) or not hasattr(criterion, 'tag') or not hasattr(criterion, 'op'):
            logger.warning(f"Skipping malformed criterion: {criterion}")
            continue # Skip this invalid criterion

        tag_str = criterion.tag
        op = criterion.op
        expected_value = criterion.value

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key '{tag_str}' in criterion.")
            # If a tag is invalid, does the whole match fail? Let's assume yes for safety.
            return False

        actual_data_element = dataset.get(tag, None)

        # Handle existence checks first
        if op == MatchOperation.EXISTS:
            if actual_data_element is None:
                logger.debug(f"Match fail: Tag {tag_str} ({tag}) does not exist (required by EXISTS).")
                return False
            else:
                continue # This criterion is met, check next
        elif op == MatchOperation.NOT_EXISTS:
            if actual_data_element is not None:
                logger.debug(f"Match fail: Tag {tag_str} ({tag}) exists (required by NOT_EXISTS).")
                return False
            else:
                continue # This criterion is met, check next

        # If we need a value, but the tag doesn't exist, it's a fail
        if actual_data_element is None:
            logger.debug(f"Match fail: Tag {tag_str} ({tag}) does not exist for comparison op '{op.value}'.")
            return False

        actual_value = actual_data_element.value
        is_multi_value = isinstance(actual_value, MultiValue)

        # Normalize actual value(s) into a list for consistent checking
        if is_multi_value:
            # Filter out potential None values within MultiValue if necessary
            actual_value_list = [item for item in actual_value if item is not None]
            # If MultiValue was present but all items were None, treat as empty list for checks?
            # Or maybe compare against None? Let's stick to filtering Nones for now.
        elif actual_value is not None:
            actual_value_list = [actual_value]
        else:
            # Tag exists but value is None
            actual_value_list = [None] # Keep None for equality checks

        match_found_for_criterion = False
        try:
            # --- Perform comparison based on operator ---
            if op == MatchOperation.EQUALS:
                # Check if *any* actual value equals the expected value
                match_found_for_criterion = any(_values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.NOT_EQUALS:
                # Check if *all* actual values are not equal to the expected value
                # Note: If actual_value_list is empty (e.g., empty MultiValue), this is True. Is that intended?
                # Let's assume it means "no value present equals the expected value".
                match_found_for_criterion = all(not _values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.CONTAINS:
                if not isinstance(expected_value, str):
                    logger.warning(f"'contains' requires a string value for comparison on tag {tag_str}. Got {type(expected_value)}.")
                    return False # Malformed rule value
                # Check if expected string is in the string representation of *any* actual value
                match_found_for_criterion = any(expected_value in str(av) for av in actual_value_list if av is not None)
            elif op == MatchOperation.STARTS_WITH:
                if not isinstance(expected_value, str):
                    logger.warning(f"'startswith' requires a string value for comparison on tag {tag_str}. Got {type(expected_value)}.")
                    return False
                # Check if string representation of *any* actual value starts with expected string
                match_found_for_criterion = any(str(av).startswith(expected_value) for av in actual_value_list if av is not None)
            elif op == MatchOperation.ENDS_WITH:
                if not isinstance(expected_value, str):
                    logger.warning(f"'endswith' requires a string value for comparison on tag {tag_str}. Got {type(expected_value)}.")
                    return False
                # Check if string representation of *any* actual value ends with expected string
                match_found_for_criterion = any(str(av).endswith(expected_value) for av in actual_value_list if av is not None)
            elif op == MatchOperation.REGEX:
                 if not isinstance(expected_value, str):
                     logger.warning(f"'regex' requires a string pattern value for comparison on tag {tag_str}. Got {type(expected_value)}.")
                     return False
                 try:
                     # Check if regex pattern matches *any* actual value's string representation
                     match_found_for_criterion = any(bool(re.search(expected_value, str(av))) for av in actual_value_list if av is not None)
                 except re.error as regex_err:
                     logger.error(f"Invalid regex pattern '{expected_value}' for tag {tag_str}: {regex_err}")
                     return False # Rule configuration error
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 # Check if *any* actual numeric value satisfies the comparison with expected value
                 match_found_for_criterion = any(_compare_numeric(av, expected_value, op) for av in actual_value_list if av is not None)
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'in' requires a list value for comparison on tag {tag_str}. Got {type(expected_value)}.")
                     return False
                 # Check if *any* actual value is equal to *any* value in the expected list
                 expected_value_set = set(map(str, expected_value)) # Optimization: convert expected to strings once
                 match_found_for_criterion = any(str(av) in expected_value_set for av in actual_value_list)
                 # Original logic using _values_equal (might be slower for large lists):
                 # match_found_for_criterion = any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'not_in' requires a list value for comparison on tag {tag_str}. Got {type(expected_value)}.")
                     return False
                 # Check if *all* actual values are NOT equal to *any* value in the expected list
                 expected_value_set = set(map(str, expected_value)) # Optimization
                 match_found_for_criterion = all(str(av) not in expected_value_set for av in actual_value_list)
                 # Original logic using _values_equal:
                 # The previous logic `all(not _values_equal(av, ev) ...)` was likely wrong.
                 # It should mean: no actual value is found within the expected list.
                 # match_found_for_criterion = all(not any(_values_equal(av, ev) for ev in expected_value) for av in actual_value_list)

            # Operators not applicable to DICOM tag values directly
            elif op in [MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET]:
                 logger.warning(f"IP address operator '{op.value}' used incorrectly on DICOM tag '{tag_str}'. Match fails.")
                 return False
            else:
                 logger.warning(f"Unhandled match operator '{op.value}' for tag {tag_str}. Match fails.")
                 return False # Unknown operator

            # --- Check if the criterion was met ---
            if not match_found_for_criterion:
                 # Log details about the failure
                 actual_repr = repr(actual_value) # Use original value for logging clarity
                 if len(actual_repr) > 100: actual_repr = actual_repr[:100] + "..."
                 logger.debug(f"Match fail: Tag {tag_str} ({tag}) value '{actual_repr}' failed op '{op.value}' with expected value '{expected_value}'")
                 return False # This criterion failed, so the whole rule match fails

        except Exception as e:
            logger.error(f"Error evaluating match criterion for tag {tag_str} ({tag}) with op {op.value}: {e}", exc_info=True)
            return False # Fail on any error during evaluation

    # If the loop completes without returning False, all criteria matched
    return True


# --- check_association_match ---
def check_association_match(assoc_info: Optional[Dict[str, str]], criteria: Optional[List[AssociationMatchCriterion]]) -> bool:
    """Checks if association info matches all provided criteria."""
    if not criteria: return True # No criteria means match
    if not assoc_info:
        logger.debug("Association criteria exist, but no association info provided. Match fails.")
        return False # Criteria exist, but no info to check against

    logger.debug(f"Checking association criteria against: {assoc_info}")

    for criterion in criteria:
        if not isinstance(criterion, AssociationMatchCriterion):
            logger.warning(f"Skipping invalid association criterion: {criterion}")
            continue # Skip invalid criterion format

        param = criterion.parameter # e.g., "CALLING_AE_TITLE", "SOURCE_IP"
        op = criterion.op
        expected_value = criterion.value

        # Map parameter enum/string to the actual key in assoc_info dict
        # Standardize keys for lookup (e.g., lowercase)
        param_key_map = {
            "CALLING_AE_TITLE": "calling_ae_title",
            "CALLED_AE_TITLE": "called_ae_title",
            "SOURCE_IP": "source_ip",
            # Add other potential parameters here if needed
        }
        lookup_key = param_key_map.get(param)

        if not lookup_key:
            logger.warning(f"Unknown association parameter '{param}' in criterion. Match fails.")
            return False # Unsupported parameter

        actual_value: Optional[str] = assoc_info.get(lookup_key)

        # Note: For association parameters, EXISTS/NOT_EXISTS might just check presence in dict
        if op == MatchOperation.EXISTS:
             if lookup_key not in assoc_info: # More accurate check than just value being None
                 logger.debug(f"Assoc match fail: Param '{param}' ('{lookup_key}') not found in assoc info (required by EXISTS).")
                 return False
             else: continue # Criterion met
        if op == MatchOperation.NOT_EXISTS:
             if lookup_key in assoc_info:
                 logger.debug(f"Assoc match fail: Param '{param}' ('{lookup_key}') found in assoc info (required by NOT_EXISTS).")
                 return False
             else: continue # Criterion met


        # For other ops, if the key doesn't exist, the value is effectively None.
        # Should a missing param fail other checks? Yes, typically.
        if actual_value is None:
             logger.debug(f"Assoc match fail: Param '{param}' ('{lookup_key}') not found or is None in assoc info, required for op '{op.value}'.")
             return False

        match = False
        try:
            # --- Perform comparison based on operator ---
            if op == MatchOperation.EQUALS:
                match = _values_equal(actual_value, expected_value)
            elif op == MatchOperation.NOT_EQUALS:
                match = not _values_equal(actual_value, expected_value)
            elif op == MatchOperation.CONTAINS:
                match = str(expected_value) in str(actual_value)
            elif op == MatchOperation.STARTS_WITH:
                match = str(actual_value).startswith(str(expected_value))
            elif op == MatchOperation.ENDS_WITH:
                match = str(actual_value).endswith(str(expected_value))
            elif op == MatchOperation.REGEX:
                 if not isinstance(expected_value, str):
                     logger.warning(f"'regex' requires a string pattern for assoc param '{param}'. Got {type(expected_value)}.")
                     return False
                 try:
                     match = bool(re.search(expected_value, str(actual_value)))
                 except re.error as regex_err:
                     logger.error(f"Invalid regex pattern '{expected_value}' for assoc param {param}: {regex_err}")
                     return False # Rule config error
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'in' requires a list value for assoc param '{param}'. Got {type(expected_value)}.")
                     return False
                 # Check if the single actual_value is equal to *any* value in the expected list
                 match = any(_values_equal(actual_value, ev) for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'not_in' requires a list value for assoc param '{param}'. Got {type(expected_value)}.")
                     return False
                 # Check if the single actual_value is NOT equal to *any* value in the expected list
                 match = not any(_values_equal(actual_value, ev) for ev in expected_value)

            # TODO: Implement IP Address operations if needed
            elif op == MatchOperation.IP_ADDRESS_EQUALS:
                 logger.warning(f"IP_ADDRESS_EQUALS matching NYI for assoc param '{param}'. Match fails.")
                 match = False # Not implemented
            elif op == MatchOperation.IP_ADDRESS_STARTS_WITH:
                 logger.warning(f"IP_ADDRESS_STARTS_WITH matching NYI for assoc param '{param}'. Match fails.")
                 match = False # Not implemented
            elif op == MatchOperation.IP_ADDRESS_IN_SUBNET:
                 logger.warning(f"IP_ADDRESS_IN_SUBNET matching NYI for assoc param '{param}'. Match fails.")
                 match = False # Not implemented

            # Operators not applicable to association parameters
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 logger.warning(f"Unsupported numeric operator '{op.value}' for assoc param '{param}'. Match fails.")
                 return False
            else:
                 # Handles case where op was EXIST/NOT_EXIST and already continued, or unknown op
                 if op not in [MatchOperation.EXISTS, MatchOperation.NOT_EXISTS]:
                     logger.warning(f"Unknown or unhandled operator '{op.value}' for assoc param '{param}'. Match fails.")
                     return False

            # --- Check if the criterion was met ---
            if not match and op not in [MatchOperation.EXISTS, MatchOperation.NOT_EXISTS]: # Avoid double-logging for existence checks
                 logger.debug(f"Assoc match fail: Param '{param}' value '{actual_value}' failed op '{op.value}' with expected value '{expected_value}'")
                 return False # This criterion failed

        except Exception as e:
            logger.error(f"Error evaluating assoc criterion for param {param} with op {op.value}: {e}", exc_info=True)
            return False # Fail on any error

    # If the loop completes, all criteria matched
    logger.debug("All association criteria matched.")
    return True

# --- _add_original_attribute ---
def _add_original_attribute(dataset: Dataset, original_element: Optional[DataElement], modification_description: str, source_identifier: str):
    """Adds original attribute info to the Original Attributes Sequence."""
    log_enabled = getattr(settings, 'LOG_ORIGINAL_ATTRIBUTES', False)
    if not log_enabled:
        return # Do nothing if logging is disabled

    try:
        # Ensure the sequence exists and is a valid Sequence
        if ORIGINAL_ATTRIBUTES_SEQUENCE_TAG not in dataset:
            # Create sequence with correct VR 'SQ'
            dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG] = DataElement(
                ORIGINAL_ATTRIBUTES_SEQUENCE_TAG, 'SQ', Sequence([])
            )
            logger.debug(f"Created OriginalAttributesSequence ({ORIGINAL_ATTRIBUTES_SEQUENCE_TAG}) for {source_identifier}.")
        elif not isinstance(dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG].value, (Sequence, list)):
             # If it exists but isn't a sequence, log error and cannot proceed
             logger.error(f"Existing OriginalAttributesSequence tag {ORIGINAL_ATTRIBUTES_SEQUENCE_TAG} is not a Sequence. Cannot append original attribute info.")
             return

        # Create the item to add to the sequence
        item = Dataset()

        # Add the original element (if it existed)
        if original_element is not None:
            # Use deepcopy to avoid modifying the original if it's still referenced elsewhere
            item.add(deepcopy(original_element))

        # Create the nested ModifiedAttributesSequence
        mod_item = Dataset()
        # AttributeModificationDateTime (0040,A73A), VR=DT, VM=1
        mod_item.add_new(Tag(0x0040, 0xA73A), "DT", datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S.%f')[:26])
        # ModifyingSystem (0400,0563), VR=LO, VM=1
        mod_item.add_new(Tag(0x0400, 0x0563), "LO", source_identifier[:64]) # Truncate to VR length limit
        # ReasonForTheAttributeModification (0400,0562), VR=LO, VM=1
        mod_item.add_new(Tag(0x0400, 0x0562), "LO", modification_description[:64]) # Truncate

        # Add the ModifiedAttributesSequence (0400,0561), VR=SQ, VM=1 to the item
        item.add_new(Tag(0x0400, 0x0561), 'SQ', Sequence([mod_item]))

        # Append the fully constructed item to the main OriginalAttributesSequence
        dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG].value.append(item)

        log_tag = original_element.tag if original_element else "N/A (Element did not exist)"
        logger.debug(f"Logged modification details for tag {log_tag} to OriginalAttributesSequence.")

    except Exception as e:
        # Catch potential errors during dataset manipulation
        log_tag = original_element.tag if original_element else "N/A"
        logger.error(f"Failed to add item to OriginalAttributesSequence for tag {log_tag}: {e}", exc_info=True)


# --- _apply_crosswalk_modification ---
def _apply_crosswalk_modification(dataset: pydicom.Dataset, mod: TagCrosswalkModification, source_identifier: str):
    """Applies crosswalk modifications."""
    # Need DB access, create a session scope
    from app.db.session import SessionLocal # Local import ok here
    db_session = None
    try:
        db_session = SessionLocal()
        logger.debug(f"Processing CROSSWALK modification using Map ID: {mod.crosswalk_map_id}")

        # 1. Get CrosswalkMap configuration from DB
        crosswalk_map_config: Optional[CrosswalkMap] = crud.crud_crosswalk_map.get(db_session, id=mod.crosswalk_map_id)
        if not crosswalk_map_config:
            logger.error(f"CrosswalkMap ID {mod.crosswalk_map_id} not found in database. Skipping modification.")
            return
        if not crosswalk_map_config.is_enabled:
            logger.warning(f"CrosswalkMap '{crosswalk_map_config.name}' (ID: {mod.crosswalk_map_id}) is disabled. Skipping.")
            return

        # 2. Extract values from DICOM dataset based on map's match_columns
        incoming_match_values: Dict[str, Any] = {}
        can_match = True
        if not isinstance(crosswalk_map_config.match_columns, list):
             logger.error(f"Invalid match_columns definition (not a list) in CrosswalkMap ID {mod.crosswalk_map_id}. Cannot perform lookup.")
             return # Cannot proceed

        for match_map_item in crosswalk_map_config.match_columns:
            if not isinstance(match_map_item, dict):
                logger.error(f"Invalid item found in match_columns for CrosswalkMap ID {mod.crosswalk_map_id}: {match_map_item}. Skipping map.")
                can_match = False; break # Stop processing this map

            dicom_tag_str = match_map_item.get('dicom_tag')
            # Use column_name if provided, otherwise default to the DICOM tag string
            column_name = match_map_item.get('column_name', dicom_tag_str)

            if not dicom_tag_str or not column_name:
                logger.error(f"Missing 'dicom_tag' or cannot determine 'column_name' in match_columns item for Map ID {mod.crosswalk_map_id}: {match_map_item}. Skipping map.")
                can_match = False; break

            match_tag = parse_dicom_tag(dicom_tag_str)
            if not match_tag:
                logger.error(f"Invalid DICOM tag '{dicom_tag_str}' specified in match_columns for Map ID {mod.crosswalk_map_id}. Skipping map.")
                can_match = False; break

            # Get data element from dataset
            data_element = dataset.get(match_tag)
            if data_element is None or data_element.value is None or data_element.value == '':
                # If a required match tag is missing or empty, lookup cannot succeed
                logger.debug(f"Match tag '{dicom_tag_str}' ({match_tag}) not found or is empty in dataset for CrosswalkMap ID {mod.crosswalk_map_id}. Lookup cannot proceed.")
                can_match = False; break

            value_to_match = data_element.value
            # Handle MultiValue: typically use the first value for lookup? Or error?
            if isinstance(value_to_match, MultiValue):
                 if len(value_to_match) > 0 and value_to_match[0] is not None:
                     value_to_match = value_to_match[0]
                     logger.warning(f"Using first value ('{value_to_match}') of multi-value tag {dicom_tag_str} ({match_tag}) for crosswalk match lookup.")
                 else:
                     logger.debug(f"Match tag '{dicom_tag_str}' ({match_tag}) is multi-value but empty or contains only None. Lookup cannot proceed.")
                     can_match = False; break

            # Store the value using the designated column_name as the key
            incoming_match_values[column_name] = str(value_to_match) # Use string representation for lookup? Assume service handles types.

        if not can_match:
            logger.debug(f"Crosswalk lookup cannot proceed for Map ID {mod.crosswalk_map_id} due to missing/invalid match data.")
            return # Stop if matching prerequisites failed

        # 3. Perform the crosswalk lookup using the service
        logger.debug(f"Performing crosswalk lookup for Map ID {mod.crosswalk_map_id} with match values: {incoming_match_values}")
        replacement_data: Optional[Dict[str, Any]] = None
        try:
            # Assuming the service function handles the actual lookup logic (DB query, cache, etc.)
            replacement_data = crosswalk_service.get_crosswalk_value_sync(crosswalk_map_config, incoming_match_values)
            logger.debug(f"Crosswalk lookup for Map ID {mod.crosswalk_map_id} returned: {replacement_data}")
        except Exception as lookup_exc:
            logger.error(f"Error during crosswalk lookup service call for Map ID {mod.crosswalk_map_id}: {lookup_exc}", exc_info=True)
            return # Stop if lookup fails

        # 4. Apply the replacements if data was found
        if replacement_data and isinstance(replacement_data, dict):
            logger.info(f"Crosswalk data found for Map ID {mod.crosswalk_map_id}. Applying {len(replacement_data)} replacements.")
            for target_tag_str, replace_info in replacement_data.items():
                target_tag = parse_dicom_tag(target_tag_str)
                if not target_tag:
                    logger.warning(f"Invalid target DICOM tag '{target_tag_str}' found in replacement data for Map ID {mod.crosswalk_map_id}. Skipping this replacement.")
                    continue

                # Ensure replace_info has the expected structure (e.g., {'value': ..., 'vr': ...})
                if not isinstance(replace_info, dict):
                    logger.warning(f"Invalid replacement info format for target tag '{target_tag_str}' in Map ID {mod.crosswalk_map_id}: {replace_info}. Skipping.")
                    continue

                new_value = replace_info.get("value") # Could be None or empty string
                new_vr: Optional[str] = replace_info.get("vr") # VR might be optional

                # Log original value before modification
                original_target_element = deepcopy(dataset.get(target_tag, None))
                mod_desc = f"Crosswalk: Set {target_tag_str} ({target_tag}) via Map {mod.crosswalk_map_id}"

                # Determine the final VR
                final_vr = new_vr # Use VR from crosswalk if provided
                if not final_vr:
                    # If not provided, try to keep original VR if element exists
                    if original_target_element:
                        final_vr = original_target_element.VR
                        logger.debug(f"Crosswalk: Using original VR '{final_vr}' for target tag {target_tag_str} as none was provided in map.")
                    else:
                        # If element is new, try to guess VR from DICOM dictionary
                        try:
                            final_vr = dictionary_VR(target_tag)
                            logger.debug(f"Crosswalk: Guessed VR '{final_vr}' from dictionary for new target tag {target_tag_str}.")
                        except KeyError:
                            # Fallback if tag not in dictionary or VR unknown
                            final_vr = 'UN' # Or maybe 'LO' or 'SH' depending on expected data? 'UN' is safest.
                            logger.warning(f"Crosswalk: Cannot determine VR for new tag {target_tag_str} ({target_tag}). Defaulting to 'UN'.")

                # Process/Coerce the new value based on VR (optional, could be done by caller)
                # Basic type coercion attempt based on determined VR
                processed_value = new_value
                if new_value is not None:
                    try:
                        if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'):
                            processed_value = int(new_value)
                        elif final_vr in ('FL', 'FD', 'OD', 'OF'):
                            processed_value = float(new_value)
                        elif final_vr == 'DS':
                            # DS should be string, but Decimal() can validate format
                            try: Decimal(str(new_value)); processed_value = str(new_value)
                            except InvalidOperation: logger.warning(f"Crosswalk: Value '{new_value}' invalid for VR 'DS'. Using as string."); processed_value = str(new_value)
                        elif final_vr in ('DA', 'DT', 'TM'):
                            # Basic validation/formatting could go here, but pydicom often handles it.
                            processed_value = str(new_value) # Keep as string usually
                        else: # SH, LO, LT, PN, UI, etc. - usually strings
                            processed_value = str(new_value)
                    except (ValueError, TypeError) as conv_err:
                        logger.warning(f"Crosswalk: Value '{new_value}' could not be coerced for VR '{final_vr}': {conv_err}. Using original type or string.")
                        # Fallback to string if conversion fails? Or keep original type? Let's try keeping original type.
                        processed_value = new_value
                else: # new_value is None
                    processed_value = None # Assign None to clear the tag value

                # Apply the modification to the dataset
                try:
                    dataset[target_tag] = DataElement(target_tag, final_vr, processed_value)
                    logger.debug(f"Crosswalk: Set tag {target_tag_str} ({target_tag}) to value '{processed_value}' with VR '{final_vr}'.")
                    _add_original_attribute(dataset, original_target_element, mod_desc, source_identifier)
                except Exception as apply_err:
                    logger.error(f"Crosswalk: Failed to apply modification for target tag {target_tag_str} ({target_tag}): {apply_err}", exc_info=True)
                    # Continue to next replacement if one fails

        elif replacement_data is None:
            logger.info(f"Crosswalk lookup did not return any replacement data for Map ID {mod.crosswalk_map_id} using match values: {incoming_match_values}")
        else: # replacement_data is not None and not a dict
            logger.error(f"Crosswalk lookup for Map ID {mod.crosswalk_map_id} returned unexpected data type: {type(replacement_data)}. Expected dict or None.")

    except Exception as e:
        logger.error(f"Unexpected error during crosswalk modification processing for Map ID {mod.crosswalk_map_id}: {e}", exc_info=True)
    finally:
        if db_session:
            db_session.close()


# --- apply_modifications ---
def apply_modifications(dataset: pydicom.Dataset, modifications: List[TagModification], source_identifier: str):
    """Applies a list of tag modifications to a dataset."""
    if not modifications:
        return # Nothing to do

    # Define VR categories for easier checks
    # Based on DICOM Standard Part 5, Table 6.2-1 Value Representation
    STRING_LIKE_VRS = {'AE', 'AS', 'CS', 'DA', 'DS', 'DT', 'IS', 'LO', 'LT', 'PN', 'SH', 'ST', 'TM', 'UI', 'UR', 'UT'}
    NUMERIC_VRS = {'FL', 'FD', 'SL', 'SS', 'UL', 'US', 'DS', 'IS', 'OD', 'OF', 'OL', 'OV', 'SV', 'UV'} # Added more numeric/binary types

    for mod_union in modifications:
        # Determine the action and the specific modification model
        action = mod_union.action
        mod: Any = mod_union # The specific model instance (e.g., TagSetModification)

        try:
            # Handle CROSSWALK separately as it involves DB lookups etc.
            if action == ModifyActionEnum.CROSSWALK:
                 if isinstance(mod, TagCrosswalkModification):
                     _apply_crosswalk_modification(dataset, mod, source_identifier)
                 else:
                     logger.error(f"Mismatched modification type for CROSSWALK action: {type(mod)}. Skipping.")
                 continue # Move to next modification

            # --- Common logic for Tag-based modifications ---
            primary_tag_str: Optional[str] = None
            source_tag_str: Optional[str] = None      # For COPY/MOVE
            destination_tag_str: Optional[str] = None # For COPY/MOVE

            # Get the relevant tag strings based on action type
            if hasattr(mod, 'tag'): primary_tag_str = mod.tag
            if hasattr(mod, 'source_tag'): source_tag_str = mod.source_tag           # Used by COPY/MOVE as the primary tag
            if hasattr(mod, 'destination_tag'): destination_tag_str = mod.destination_tag # Used by COPY/MOVE

            log_tag_str = primary_tag_str or source_tag_str # Tag being acted upon or source tag
            tag: Optional[BaseTag] = None
            tag_str_repr: str = "N/A" # For logging

            if log_tag_str:
                tag = parse_dicom_tag(log_tag_str)
                if not tag:
                    logger.warning(f"Skipping modification: Invalid tag '{log_tag_str}' provided for action {action.value}.")
                    continue # Skip this modification if tag is invalid
                else:
                    # Get a readable representation for logs
                    tag_keyword = keyword_for_tag(tag) or '(Unknown Keyword)'
                    tag_str_repr = f"{tag_keyword} {tag}"
            elif action not in [ModifyActionEnum.CROSSWALK]: # Crosswalk handled above
                 logger.warning(f"Skipping modification: No tag specified for action {action.value}.")
                 continue # Cannot proceed without a tag for most actions

            # Get original element *before* modification for logging purposes
            original_element_for_log: Optional[DataElement] = None
            if tag: # Only try to get if tag was successfully parsed
                original_element_for_log = deepcopy(dataset.get(tag, None))

            modification_description = f"Action: {action.value}" # Default description

            # --- Apply modification based on action ---

            if action == ModifyActionEnum.DELETE:
                if not isinstance(mod, TagDeleteModification) or not tag: continue # Type check and ensure tag is valid
                modification_description = f"Deleted tag {tag_str_repr}"
                if tag in dataset:
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_str_repr}.")
                    _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                else:
                    logger.debug(f"Tag {tag_str_repr} not found in dataset. Cannot delete.")

            elif action == ModifyActionEnum.SET:
                 if not isinstance(mod, TagSetModification) or not tag: continue
                 new_value = mod.value
                 vr_from_mod = mod.vr # VR specified in the modification rule
                 final_vr = vr_from_mod # Use specified VR if available

                 modification_description = f"Set {tag_str_repr} to '{str(new_value)[:50]}...' (VR:{vr_from_mod or 'auto'})"

                 if not final_vr: # If VR not specified in rule
                      if original_element_for_log:
                          final_vr = original_element_for_log.VR # Keep original VR
                      else: # Tag is new, guess VR
                          try: final_vr = dictionary_VR(tag)
                          except KeyError: final_vr = 'UN'; logger.warning(f"Set: No VR specified and cannot guess VR for new tag {tag_str_repr}. Defaulting to 'UN'.")

                 # Basic Type Coercion based on FINAL VR
                 processed_value = new_value
                 if new_value is not None and final_vr in NUMERIC_VRS:
                     try:
                          if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'): processed_value = int(new_value)
                          elif final_vr in ('FL', 'FD', 'OD', 'OF'): processed_value = float(new_value)
                          elif final_vr == 'DS':
                              try: Decimal(str(new_value)); processed_value = str(new_value)
                              except InvalidOperation: logger.warning(f"Set: Value '{new_value}' invalid for VR 'DS'. Using raw string."); processed_value = str(new_value)
                          # Note: Oth er numeric types (OL, OV, SV, UV) might need byte handling - skipping coercion here.
                     except (ValueError, TypeError) as conv_err:
                          logger.warning(f"Set: Value '{new_value}' not coercible for VR '{final_vr}': {conv_err}. Using raw value.")
                          processed_value = new_value # Keep original if coercion fails
                 elif new_value is None:
                     processed_value = None # Allow setting tag to empty

                 # Apply the change
                 dataset[tag] = DataElement(tag, final_vr, processed_value)
                 logger.debug(f"Set tag {tag_str_repr} (VR: {final_vr}).")
                 _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)

            elif action in [ModifyActionEnum.PREPEND, ModifyActionEnum.SUFFIX]:
                 if not isinstance(mod, (TagPrependModification, TagSuffixModification)) or not tag: continue
                 text_to_add = mod.value
                 modification_description = f"{action.value} tag {tag_str_repr} with '{text_to_add}'"

                 if not original_element_for_log:
                     logger.warning(f"Cannot {action.value}: Tag {tag_str_repr} does not exist.")
                     continue

                 current_vr = original_element_for_log.VR
                 # Check if VR is suitable for string manipulation
                 if current_vr not in STRING_LIKE_VRS: # Allow IS/DS as they can be string-like
                     logger.warning(f"Cannot {action.value}: Tag {tag_str_repr} has non-string-like VR '{current_vr}'.")
                     continue

                 current_value = original_element_for_log.value
                 modified_value: Any

                 if isinstance(current_value, MultiValue):
                      # Apply to each item in the MultiValue list
                      if action == ModifyActionEnum.PREPEND:
                           modified_value = [text_to_add + str(item) for item in current_value if item is not None]
                      else: # SUFFIX
                           modified_value = [str(item) + text_to_add for item in current_value if item is not None]
                      logger.debug(f"Applied {action.value} to multi-value tag {tag_str_repr}.")
                 else:
                      # Apply to single value
                      original_str = str(current_value) if current_value is not None else ""
                      if action == ModifyActionEnum.PREPEND:
                           modified_value = text_to_add + original_str
                      else: # SUFFIX
                           modified_value = original_str + text_to_add
                      logger.debug(f"Applied {action.value} to single value tag {tag_str_repr}.")

                 # Update the dataset (using original VR)
                 dataset[tag] = DataElement(tag, current_vr, modified_value)
                 _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)

            elif action == ModifyActionEnum.REGEX_REPLACE:
                 if not isinstance(mod, TagRegexReplaceModification) or not tag: continue
                 pattern = mod.pattern
                 replacement = mod.replacement
                 modification_description = f"Regex replace {tag_str_repr} using pattern '{pattern}' with '{replacement}'"

                 if not original_element_for_log:
                     logger.warning(f"Cannot {action.value}: Tag {tag_str_repr} does not exist.")
                     continue

                 current_vr = original_element_for_log.VR
                 if current_vr not in STRING_LIKE_VRS:
                     logger.warning(f"Cannot {action.value}: Tag {tag_str_repr} has non-string-like VR '{current_vr}'.")
                     continue

                 try:
                      current_value = original_element_for_log.value
                      modified_value: Any

                      if isinstance(current_value, MultiValue):
                           # Apply regex to each item
                           modified_value = [re.sub(pattern, replacement, str(item)) for item in current_value if item is not None]
                           logger.debug(f"Applied {action.value} to multi-value tag {tag_str_repr} with pattern '{pattern}'.")
                      else:
                           # Apply regex to single value
                           original_str = str(current_value) if current_value is not None else ""
                           modified_value = re.sub(pattern, replacement, original_str)
                           logger.debug(f"Applied {action.value} to single value tag {tag_str_repr} with pattern '{pattern}'.")

                      # Update the dataset (using original VR)
                      dataset[tag] = DataElement(tag, current_vr, modified_value)
                      _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)

                 except re.error as regex_err:
                      logger.error(f"Error applying {action.value} regex for tag {tag_str_repr}: Invalid pattern '{pattern}': {regex_err}")
                 except Exception as e:
                      logger.error(f"Unexpected error during {action.value} for tag {tag_str_repr}: {e}", exc_info=True)


            elif action in [ModifyActionEnum.COPY, ModifyActionEnum.MOVE]:
                # Ensure correct modification type
                if not isinstance(mod, (TagCopyModification, TagMoveModification)): continue
                # Ensure source tag (parsed as 'tag') and destination tag string are valid
                if not tag or not destination_tag_str:
                    logger.warning(f"Skipping {action.value}: Invalid source ('{log_tag_str}') or destination ('{destination_tag_str}') tag.")
                    continue

                dest_tag = parse_dicom_tag(destination_tag_str)
                if not dest_tag:
                    logger.warning(f"Skipping {action.value}: Invalid destination tag '{destination_tag_str}'.")
                    continue

                dest_tag_keyword = keyword_for_tag(dest_tag) or '(Unknown Keyword)'
                dest_tag_str_repr = f"{dest_tag_keyword} {dest_tag}"

                if not original_element_for_log:
                    logger.warning(f"Cannot {action.value}: Source tag {tag_str_repr} not found.")
                    continue

                # Log the state of the destination tag *before* modification
                original_dest_element_before_mod: Optional[DataElement] = deepcopy(dataset.get(dest_tag, None))

                modification_description = f"{action.value} tag {tag_str_repr} to {dest_tag_str_repr}"

                # Determine VR for the destination tag
                dest_vr = mod.destination_vr or original_element_for_log.VR # Use specified VR or source VR

                # Create the new element for the destination
                # Use deepcopy for the value to avoid unexpected sharing if value is mutable
                new_element = DataElement(dest_tag, dest_vr, deepcopy(original_element_for_log.value))

                # Add/replace the destination tag in the dataset
                dataset[dest_tag] = new_element
                logger.debug(f"Tag {tag_str_repr} {action.value}d to {dest_tag_str_repr} (VR '{dest_vr}').")
                # Log the change to the destination tag
                _add_original_attribute(dataset, original_dest_element_before_mod, modification_description, source_identifier)

                # If MOVE, delete the original source tag
                if action == ModifyActionEnum.MOVE:
                    if tag in dataset: # Should exist based on earlier check, but double-check
                        move_delete_desc = f"Deleted source tag {tag_str_repr} as part of MOVE operation"
                        # Log the deletion of the source tag
                        _add_original_attribute(dataset, original_element_for_log, move_delete_desc, source_identifier)
                        del dataset[tag]
                        logger.debug(f"Deleted original source tag {tag_str_repr} after move.")
                    else:
                         # This case should theoretically not happen if logic is correct
                         logger.warning(f"MOVE action: Source tag {tag_str_repr} was expected but not found for deletion after copy.")

        except Exception as e:
            # Generic error handler for unexpected issues within a modification step
            tag_id_for_error = tag_str_repr if tag_str_repr != "N/A" else (getattr(mod, 'crosswalk_map_id', 'N/A') if action == ModifyActionEnum.CROSSWALK else 'Unknown Target')
            logger.error(f"Failed to apply modification ({action.value}) for target '{tag_id_for_error}': {e}", exc_info=True)
            # Continue to the next modification even if one fails


# --- Main Processing Function ---
def process_dicom_instance(
    original_ds: pydicom.Dataset,
    active_rulesets: List[RuleSet],
    source_identifier: str,
    association_info: Optional[Dict[str, str]] = None
) -> Tuple[Optional[pydicom.Dataset], List[str], List[Dict[str, Any]]]:
    """
    Processes a DICOM instance against rulesets, applies modifications, and determines destinations.

    Returns:
        Tuple containing:
        - The potentially modified pydicom.Dataset (or original if no mods applied).
          Returns None only if no rules match *and* trigger any action (mod or destination).
        - List of strings describing the applied rules (e.g., "RuleSetName/RuleName (ID:...)").
        - List of unique destination identifier dictionaries [{'id': int, 'name': str, 'backend_type': str}].
          Returns (original_ds, [], []) if no rules match or trigger.
    """
    logger.debug(f"Processing instance from source '{source_identifier}' against {len(active_rulesets)} active rulesets.")

    modified_ds: Optional[pydicom.Dataset] = None # Will hold the modified dataset if changes occur
    applied_rules_info: List[str] = []            # Tracks which rules were successfully applied
    destinations_to_process: List[Dict[str, Any]] = [] # Collects destination identifiers from matched rules
    modifications_applied_in_total = False      # Flag if any modification actually changed the dataset
    any_rule_matched_and_triggered = False      # Flag if at least one rule matched conditions and schedule

    current_time_utc = datetime.now(timezone.utc) # Get current time once for all schedule checks

    # Check AI dependencies availability status
    ai_enabled = ANYIO_AVAILABLE and ai_assist_service.gemini_model is not None
    if not ai_enabled:
        logger.debug("AI standardization features disabled (anyio library not found or Gemini model not configured).")

    # --- Iterate through RuleSets (sorted by priority externally) ---
    for ruleset in active_rulesets:
        logger.debug(f"Evaluating RuleSet '{ruleset.name}' (ID: {ruleset.id}, Priority: {ruleset.priority}, Mode: {ruleset.execution_mode.value})")
        rules_in_ruleset = getattr(ruleset, 'rules', [])
        if not rules_in_ruleset:
            logger.debug(f"RuleSet '{ruleset.name}' contains no rules. Skipping.")
            continue

        matched_rule_in_this_set = False # Track if a match occurs *within this ruleset* for FIRST_MATCH mode

        # --- Iterate through Rules within the RuleSet (sorted by priority externally) ---
        for rule in rules_in_ruleset:
            # 1. Check if Rule is Active and Schedule Allows
            is_rule_statically_active = rule.is_active
            rule_schedule = getattr(rule, 'schedule', None) # Get schedule if relationship loaded
            # Rule is triggerable if active AND (no schedule OR schedule is active now)
            is_schedule_currently_active = is_schedule_active(rule_schedule, current_time_utc) if rule.schedule_id else True
            is_rule_currently_triggerable = is_rule_statically_active and is_schedule_currently_active

            if not is_rule_currently_triggerable:
                # Log why it's not triggerable for clarity
                reason = []
                if not is_rule_statically_active: reason.append("globally disabled")
                if rule.schedule_id and not is_schedule_currently_active: reason.append("schedule inactive")
                logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) is not triggerable ({', '.join(reason)}). Skipping.")
                continue # Skip this rule

            # 2. Check Source Applicability
            # Rule applies if no sources listed OR if source_identifier is in the list
            rule_applicable_sources = getattr(rule, 'applicable_sources', []) # Should be a list or None/empty
            source_match = not rule_applicable_sources or \
                           (isinstance(rule_applicable_sources, list) and source_identifier in rule_applicable_sources)

            if not source_match:
                logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) skipped: Source '{source_identifier}' not in applicable sources {rule_applicable_sources}.")
                continue # Skip rule if source doesn't match

            logger.debug(f"Checking triggerable rule '{rule.name}' (ID: {rule.id}, Priority: {rule.priority}).")

            # 3. Validate Rule Components (robustness check)
            try:
                # Use TypeAdapter for validation against the expected schema types
                match_criteria_validated = TypeAdapter(List[MatchCriterion]).validate_python(rule.match_criteria or [])
                assoc_criteria_validated = TypeAdapter(Optional[List[AssociationMatchCriterion]]).validate_python(rule.association_criteria or None)
                modifications_validated = TypeAdapter(List[TagModification]).validate_python(rule.tag_modifications or [])
                ai_tags_to_standardize = TypeAdapter(Optional[List[str]]).validate_python(rule.ai_standardization_tags or None)
            except Exception as val_err:
                logger.error(f"Rule '{rule.name}' (ID: {rule.id}) definition validation error: {val_err}. Skipping rule.", exc_info=True)
                continue # Skip rule with invalid configuration

            # 4. Perform Matching
            try:
                # Use the modified dataset if it exists, otherwise the original
                dataset_to_match = modified_ds if modified_ds is not None else original_ds
                tag_match = check_match(dataset_to_match, match_criteria_validated)
                assoc_match = check_association_match(association_info, assoc_criteria_validated)
            except Exception as match_err:
                logger.error(f"Error during matching evaluation for rule '{rule.name}' (ID: {rule.id}): {match_err}", exc_info=True)
                continue # Skip rule if matching logic causes an error

            # 5. Rule Matched: Apply Actions
            if tag_match and assoc_match:
                logger.info(f"Rule '{ruleset.name}' / '{rule.name}' (ID: {rule.id}) MATCHED & TRIGGERED (Tags: {tag_match}, Assoc: {assoc_match}).")
                any_rule_matched_and_triggered = True # Mark that at least one rule did match
                matched_rule_in_this_set = True       # Mark match within this specific ruleset
                modifications_applied_in_this_rule = False # Track if *this rule* caused changes

                # Create a deep copy ONCE when the first modification is needed
                if modified_ds is None:
                    modified_ds = deepcopy(original_ds)
                    logger.debug("Created deep copy of dataset for modifications.")

                # --- 5a. Apply Standard Tag Modifications ---
                if modifications_validated:
                    num_mods = len(modifications_validated)
                    logger.debug(f"Applying {num_mods} standard tag modification(s) for rule '{rule.name}'...")
                    # Record state *before* applying this rule's mods? Difficult. Assume apply_modifications handles logging.
                    initial_dataset_state_json = modified_ds.to_json()
                    apply_modifications(modified_ds, modifications_validated, source_identifier)
                    final_dataset_state_json = modified_ds.to_json()
                    if initial_dataset_state_json != final_dataset_state_json:
                         modifications_applied_in_this_rule = True
                         logger.debug(f"Standard modifications resulted in changes for rule '{rule.name}'.")
                    else:
                         logger.debug(f"Standard modifications did not result in changes for rule '{rule.name}'.")
                else:
                     logger.debug(f"Rule '{rule.name}' matched, but has no standard tag modifications defined.")

                # --- 5b. Apply AI Tag Standardization (if enabled and configured) ---
                if ai_enabled and ai_tags_to_standardize:
                    logger.debug(f"Applying AI standardization for tags {ai_tags_to_standardize} via rule '{rule.name}'...")
                    for tag_str in ai_tags_to_standardize:
                        tag = parse_dicom_tag(tag_str)
                        if not tag:
                            logger.warning(f"AI Std: Invalid tag '{tag_str}' specified in rule '{rule.name}'. Skipping tag.")
                            continue

                        tag_keyword = keyword_for_tag(tag) or '(Unknown Keyword)'
                        tag_str_repr = f"{tag_keyword} {tag}"
                        current_element = modified_ds.get(tag, None) # Check in the potentially modified dataset

                        # Check if tag exists and has a non-empty value
                        if current_element is None or current_element.value is None or current_element.value == '':
                            logger.debug(f"AI Std: Tag {tag_str_repr} is missing or empty. Skipping standardization.")
                            continue

                        value_to_standardize = current_element.value
                        original_value_repr = repr(value_to_standardize) # For logging

                        # Handle MultiValue: Standardize the first value? Concatenate? Error? Let's use first non-null.
                        if isinstance(value_to_standardize, MultiValue):
                             first_valid_value = next((str(item) for item in value_to_standardize if item is not None and str(item).strip()), None)
                             if first_valid_value:
                                 value_to_standardize = first_valid_value
                                 logger.warning(f"AI Std: Using first non-empty value ('{value_to_standardize}') of MV tag {tag_str_repr} for standardization.")
                             else:
                                 logger.debug(f"AI Std: Tag {tag_str_repr} is MultiValue but contains no non-empty values. Skipping.")
                                 continue
                        else:
                             value_to_standardize = str(value_to_standardize)

                        # Skip if value is effectively empty after string conversion
                        if not value_to_standardize.strip():
                            logger.debug(f"AI Std: Tag {tag_str_repr} value becomes empty after string conversion. Skipping.")
                            continue

                        # Determine context (e.g., Tag Keyword)
                        context = keyword_for_tag(tag) or "Unknown DICOM Tag"

                        try:
                             logger.debug(f"AI Std: Calling AI service for {tag_str_repr} (Context: {context}) with value '{value_to_standardize}'")
                             # Run the async function from the sync worker thread using anyio bridge
                             standardized_value: Optional[str] = anyio.from_thread.run(
                                 ai_assist_service.standardize_vocabulary_gemini, value_to_standardize, context
                             )

                             if standardized_value and standardized_value != value_to_standardize:
                                 logger.info(f"AI Std: Tag {tag_str_repr} updated: '{value_to_standardize}' -> '{standardized_value}'")
                                 ai_mod_desc = f"AI Standardize: Set {tag_str_repr} via AI (Context: {context})"
                                 # Log the original state before AI modification
                                 _add_original_attribute(modified_ds, deepcopy(current_element), ai_mod_desc, source_identifier)

                                 # Determine VR (keep original if possible, guess otherwise)
                                 final_vr = current_element.VR
                                 if not final_vr or final_vr == "??": # Handle invalid VR from source
                                      try: final_vr = dictionary_VR(tag)
                                      except KeyError: final_vr = 'LO'; logger.warning(f"AI Std: Cannot guess VR for {tag_str_repr}. Defaulting to 'LO'.")

                                 # Update the dataset element
                                 modified_ds[tag] = DataElement(tag, final_vr, standardized_value)
                                 modifications_applied_in_this_rule = True # Mark change
                             elif standardized_value:
                                 logger.debug(f"AI Std: Tag {tag_str_repr} value '{value_to_standardize}' was not changed by AI.")
                             else: # AI service returned None or empty string
                                 logger.warning(f"AI Std: AI service failed to standardize or returned empty for tag {tag_str_repr} input '{value_to_standardize}'. Value not changed.")

                        except ImportError:
                            # This should have been caught by ai_enabled check, but as safety
                            logger.error("AI Std: Attempted AI call but 'anyio' is missing. Disabling future attempts.")
                            ai_enabled = False # Prevent further attempts in this run
                            break # Stop AI processing for this rule if dependency missing
                        except RuntimeError as rt_err:
                            # Handles errors like event loop not running if anyio setup fails
                            logger.error(f"AI Std: Runtime error during AI call for {tag_str_repr}: {rt_err}. Check async setup.", exc_info=True)
                        except Exception as ai_err:
                            logger.error(f"AI Std: Unexpected error during AI standardization for {tag_str_repr}: {ai_err}", exc_info=True)

                elif ai_tags_to_standardize and not ai_enabled:
                     # Rule requests AI but it's not available
                     logger.warning(f"Rule '{rule.name}' requests AI standardization, but the service is unavailable. Skipping AI mods for this rule.")

                # --- 5c. Collect Destinations ---
                # Use relationship attribute `destinations` which should be loaded list of StorageBackendConfig objects
                rule_destinations: List[StorageBackendConfig] = getattr(rule, 'destinations', [])
                if rule_destinations:
                     logger.debug(f"Collecting destinations for matched rule '{rule.name}'...")
                     for dest_config_obj in rule_destinations:
                          # Check if the specific destination is enabled
                          if dest_config_obj.is_enabled:
                              try:
                                  backend_type_value_from_model = dest_config_obj.backend_type
                                  resolved_backend_type_as_string: Optional[str] = None

                                  if isinstance(backend_type_value_from_model, str):
                                      # It was already a string in the model (e.g., "filesystem")
                                      resolved_backend_type_as_string = backend_type_value_from_model
                                  elif hasattr(backend_type_value_from_model, 'value'):
                                      # It's an enum-like object, get its .value
                                      value_of_enum = backend_type_value_from_model.value
                                      if isinstance(value_of_enum, str):
                                          resolved_backend_type_as_string = value_of_enum
                                      else:
                                          logger.error(f"Destination ID {dest_config_obj.id}: backend_type.value is not a string (type: {type(value_of_enum)}). Skipping.")
                                          continue # Skip this destination
                                  else:
                                      # It's neither a string nor an enum with .value. This is bad.
                                      logger.error(f"Destination ID {dest_config_obj.id}: backend_type is an unexpected type ({type(backend_type_value_from_model)}). Skipping.")
                                      continue # Skip this destination

                                  if resolved_backend_type_as_string is None: # Should be caught above, but defensive
                                      logger.error(f"Destination ID {dest_config_obj.id}: Failed to resolve backend_type to a string. Skipping.")
                                      continue

                                  dest_info_dict = {
                                      "id": dest_config_obj.id,
                                      "name": dest_config_obj.name,
                                      "backend_type": resolved_backend_type_as_string 
                                  }
                                  destinations_to_process.append(dest_info_dict)
                                  logger.debug(f"Added destination ID {dest_config_obj.id} ('{dest_config_obj.name}', Type: {resolved_backend_type_as_string}) from rule '{rule.name}'.") 
                              except AttributeError as attr_err:
                                  # Handle cases where destination object might be incomplete
                                  dest_id = getattr(dest_config_obj, 'id', 'Unknown ID')
                                  logger.error(f"Error accessing attributes for destination object (ID: {dest_id}) linked to rule '{rule.name}': {attr_err}.", exc_info=True)
                              except Exception as dest_err:
                                  dest_name = getattr(dest_config_obj, 'name', 'Unknown Name')
                                  logger.error(f"Unexpected error processing destination '{dest_name}' from rule '{rule.name}': {dest_err}", exc_info=True)
                          elif dest_config_obj:
                              logger.debug(f"Skipping disabled destination '{getattr(dest_config_obj, 'name', 'N/A')}' (ID: {getattr(dest_config_obj, 'id', 'N/A')}) from rule '{rule.name}'.")
                          # else: dest_config_obj is None (shouldn't happen with proper relationship loading)

                # --- Record rule application and check execution mode ---
                applied_rules_info.append(f"{ruleset.name}/{rule.name} (ID:{rule.id})")
                if modifications_applied_in_this_rule:
                    modifications_applied_in_total = True # Mark that *some* change happened overall

                # If RuleSet mode is FIRST_MATCH, stop processing rules in *this* ruleset
                if ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                    logger.debug(f"RuleSet '{ruleset.name}' mode is FIRST_MATCH. Stopping rule evaluation for this ruleset after match.")
                    break # Exit inner loop (rules)

            else: # Rule did not match (tag_match or assoc_match was False)
                logger.debug(f"Rule '{rule.name}' did not match conditions (Tags: {tag_match}, Assoc: {assoc_match}).")
            # --- End of single rule processing ---

        # --- After processing all rules in a ruleset ---
        # If a rule matched in this set AND mode was FIRST_MATCH, stop processing further rulesets
        if matched_rule_in_this_set and ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
            logger.debug(f"Stopping ruleset evaluation due to FIRST_MATCH mode in RuleSet '{ruleset.name}'.")
            break # Exit outer loop (rulesets)
    # --- End of ruleset loop ---


    # --- Final Steps ---

    if not any_rule_matched_and_triggered:
        logger.info(f"No rules matched or triggered for source '{source_identifier}'. Returning original dataset with no destinations.")
        # Return original dataset, empty applied rules, empty destinations
        return original_ds, [], []

    # --- Deduplicate Destinations ---
    # Use a set to track unique destination identifiers (based on ID should be sufficient)
    unique_dest_identifiers: List[Dict[str, Any]] = []
    seen_dest_ids = set()
    for dest_info_dict in destinations_to_process:
         dest_id = dest_info_dict.get("id")
         if dest_id is not None and dest_id not in seen_dest_ids:
             unique_dest_identifiers.append(dest_info_dict)
             seen_dest_ids.add(dest_id)
         elif dest_id is None:
             logger.warning(f"Found destination info without an ID: {dest_info_dict}. Skipping.") # Should not happen

    num_unique_dest = len(unique_dest_identifiers)
    logger.debug(f"Processing resulted in {num_unique_dest} unique destination(s) to be processed: {unique_dest_identifiers}")

    # Determine final dataset to return
    # Return the modified dataset if modifications were made, otherwise the original
    final_ds_to_return = modified_ds if modifications_applied_in_total else original_ds

    # Sanity check: Should not be None if any rule matched and triggered
    if final_ds_to_return is None:
        logger.error("Logic error: A rule matched but the final dataset is None. Falling back to original dataset.")
        final_ds_to_return = original_ds

    return final_ds_to_return, applied_rules_info, unique_dest_identifiers

# --- END OF FILE processing_logic.py ---
