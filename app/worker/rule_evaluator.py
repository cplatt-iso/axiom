# app/worker/rule_evaluator.py

import structlog
import re
from datetime import datetime, time # Keep time import
from typing import Optional, Any, List, Dict
from decimal import Decimal, InvalidOperation

import pydicom
from pydicom.tag import BaseTag
from pydicom.multival import MultiValue
from pydicom.dataset import Dataset
from pydicom.valuerep import DSfloat, IS, DSdecimal # For numeric comparisons

from app.schemas.rule import MatchOperation, MatchCriterion, AssociationMatchCriterion
from app.db.models import Rule, Schedule # For type hints
# Assuming parse_dicom_tag will be central, e.g., in processing_orchestrator or a dicom_util
# For now, let's assume it's accessible. If not, we'll copy/move it.
from app.worker.utils.dicom_utils import parse_dicom_tag

logger = structlog.get_logger(__name__) # Standard logging

# --- Schedule Checking Logic (Moved from old processing_logic.py) ---
def is_schedule_active(schedule: Optional[Schedule], current_time_utc: datetime) -> bool:
    """Checks if a schedule is active at the given UTC time."""
    if schedule is None or not schedule.is_enabled:
        return True # No schedule or disabled schedule means always active (rule's own is_active will gate it)

    if not isinstance(schedule.time_ranges, list):
        logger.warning(f"Schedule ID {schedule.id} ('{schedule.name}') has invalid time_ranges (not a list). Assuming inactive.")
        return False
    if not schedule.time_ranges: # Empty list of time ranges
        logger.debug(f"Schedule ID {schedule.id} ('{schedule.name}') has no time ranges defined. Assuming inactive.")
        return False

    # Convert current UTC time to the schedule's timezone if specified, otherwise assume UTC for schedule times
    # For simplicity now, assuming schedule times are meant to be compared against system's current time perception (UTC here)
    # A more robust solution would involve timezone awareness in schedule definitions.
    current_time_for_schedule = current_time_utc 
    current_day_str = current_time_for_schedule.strftime("%a") # e.g., 'Mon'
    current_time_obj = current_time_for_schedule.time()

    logger.debug(f"Checking schedule '{schedule.name}' (ID: {schedule.id}). Current effective time: {current_time_for_schedule.isoformat()} ({current_day_str} {current_time_obj})")

    for time_range in schedule.time_ranges:
        try:
            days = time_range.get("days") # List like ["Mon", "Tue"]
            start_time_str = time_range.get("start_time") # e.g., "09:00:00"
            end_time_str = time_range.get("end_time") # e.g., "17:00:00"

            if not isinstance(days, list) or not all(isinstance(d, str) for d in days) or \
               not isinstance(start_time_str, str) or not isinstance(end_time_str, str):
                logger.warning(f"Schedule {schedule.id}: Skipping invalid time range format: {time_range}")
                continue
            
            normalized_days = [day[:3].title() for day in days] # Ensure 'Mon', 'Tue' etc.

            if current_day_str not in normalized_days:
                continue

            start_time = time.fromisoformat(start_time_str)
            end_time = time.fromisoformat(end_time_str)

            if start_time <= end_time: # Normal range (e.g., 09:00 - 17:00)
                if start_time <= current_time_obj < end_time:
                    logger.debug(f"Schedule '{schedule.name}' ACTIVE: Time {current_time_obj} in {start_time_str}-{end_time_str} on {current_day_str}.")
                    return True
            else: # Overnight range (e.g., 22:00 - 06:00)
                if current_time_obj >= start_time or current_time_obj < end_time:
                    logger.debug(f"Schedule '{schedule.name}' ACTIVE (overnight): Time {current_time_obj} in {start_time_str}-{end_time_str} on {current_day_str}.")
                    return True
        except (ValueError, TypeError) as e:
            logger.error(f"Schedule {schedule.id}: Error parsing time range {time_range}: {e}", exc_info=True)
            continue
    
    logger.debug(f"Schedule '{schedule.name}' INACTIVE: No matching range for {current_day_str} {current_time_obj}.")
    return False


# --- Rule Applicability and Active State ---
def is_rule_triggerable(
    rule: Rule,
    source_identifier: str,
    current_time_utc: datetime,
    # schedules_map: Dict[int, Schedule] # Pass pre-fetched schedules for efficiency if needed
) -> bool:
    """
    Determines if a rule is currently active and should be evaluated.
    Checks rule.is_active, schedule, and applicable_sources.
    """
    if not rule.is_active:
        logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) is globally disabled. Skipping.")
        return False

    # Check schedule
    # Assumes rule.schedule relationship is eagerly loaded or efficiently fetched.
    # If rule.schedule_id is None, it implies no schedule, so it's always "schedule active".
    rule_schedule: Optional[Schedule] = getattr(rule, 'schedule', None)
    if rule.schedule_id and not is_schedule_active(rule_schedule, current_time_utc):
        logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) skipped: Schedule '{getattr(rule_schedule, 'name', 'N/A')}' is inactive.")
        return False

    # Check source applicability
    rule_applicable_sources: Optional[List[str]] = getattr(rule, 'applicable_sources', None)
    if rule_applicable_sources: # If list is present and not empty
        if not isinstance(rule_applicable_sources, list) or source_identifier not in rule_applicable_sources:
            logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) skipped: Source '{source_identifier}' not in applicable sources {rule_applicable_sources}.")
            return False
    
    # If all checks pass
    logger.debug(f"Rule '{rule.name}' (ID: {rule.id}, Priority: {rule.priority}) is triggerable for source '{source_identifier}'.")
    return True


# --- Matching Logic (Moved from old processing_logic.py) ---
def _values_equal(actual_val: Any, expected_val: Any) -> bool:
    if actual_val is None and expected_val is None: return True
    if actual_val is None or expected_val is None: return False
    try:
        if isinstance(actual_val, (DSfloat, IS, DSdecimal)): actual_decimal = actual_val.real
        else: actual_decimal = Decimal(str(actual_val))
        expected_decimal = Decimal(str(expected_val))
        return actual_decimal == expected_decimal
    except (ValueError, TypeError, InvalidOperation):
        pass # Fallback to string
    return str(actual_val) == str(expected_val)

def _compare_numeric(actual_val: Any, expected_val: Any, op: MatchOperation) -> bool:
    try:
        if isinstance(actual_val, (DSfloat, IS, DSdecimal)): actual_decimal = actual_val.real
        else: actual_decimal = Decimal(str(actual_val))
        expected_decimal = Decimal(str(expected_val))
        if op == MatchOperation.GREATER_THAN: return actual_decimal > expected_decimal
        if op == MatchOperation.LESS_THAN: return actual_decimal < expected_decimal
        if op == MatchOperation.GREATER_EQUAL: return actual_decimal >= expected_decimal
        if op == MatchOperation.LESS_EQUAL: return actual_decimal <= expected_decimal
        return False
    except (ValueError, TypeError, InvalidOperation):
        return False

def check_tag_criteria(dataset: pydicom.Dataset, criteria: List[MatchCriterion]) -> bool:
    if not criteria:
        return True

    logger.debug("Checking tag criteria", criteria=criteria, patient_id_in_dataset=str(dataset.get("00100020", "MISSING"))) # Log PatientID presence

    for criterion in criteria:
        if not hasattr(criterion, 'tag') or not hasattr(criterion, 'op'): # Basic sanity
            logger.warning(f"Skipping malformed tag criterion: {criterion}")
            return False # A malformed criterion should probably fail the match

        tag_str = criterion.tag
        op = criterion.op
        expected_value = criterion.value
        tag: Optional[BaseTag] = parse_dicom_tag(tag_str)

        if not tag:
            logger.warning(f"Tag criteria: Invalid tag key '{tag_str}' in criterion. Match fails.")
            return False

        actual_data_element = dataset.get(tag, None)

        if op == MatchOperation.EXISTS:
            if actual_data_element is None: return False
            else: continue
        elif op == MatchOperation.NOT_EXISTS:
            if actual_data_element is not None: return False
            else: continue
        
        if actual_data_element is None: # For ops other than EXISTS/NOT_EXISTS, missing tag is a fail
            return False

        actual_value = actual_data_element.value
        
        # Normalize actual_value to a list for consistent checking, handling MultiValue
        actual_value_list: List[Any]
        if isinstance(actual_value, MultiValue):
            actual_value_list = [item for item in actual_value if item is not None] # Filter out None within MV
            if not actual_value_list and actual_value: # MultiValue exists but all items were None
                 actual_value_list = [None] # Treat as a list containing a single None for comparison
        elif actual_value is not None:
            actual_value_list = [actual_value]
        else: # Tag exists but its value is None
            actual_value_list = [None]

        # If after normalization, the list is empty (e.g. an empty MV was provided and we didn't convert to [None])
        # and the operation isn't checking for existence, this might be an issue.
        # For now, if actual_value_list is empty, loops like `any(...)` will correctly yield False.

        match_found_for_this_criterion = False
        try:
            if op == MatchOperation.EQUALS:
                match_found_for_this_criterion = any(_values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.NOT_EQUALS:
                # All actual values must not be equal to the expected value.
                # If actual_value_list is [None], and expected is not None, this is true.
                # If actual_value_list is empty (e.g. from an empty MV), this is true.
                match_found_for_this_criterion = all(not _values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.CONTAINS:
                if not isinstance(expected_value, str): return False # Rule error
                match_found_for_this_criterion = any(expected_value in str(av) for av in actual_value_list if av is not None)
            elif op == MatchOperation.STARTS_WITH:
                if not isinstance(expected_value, str): return False
                match_found_for_this_criterion = any(str(av).startswith(expected_value) for av in actual_value_list if av is not None)
            elif op == MatchOperation.ENDS_WITH:
                if not isinstance(expected_value, str): return False
                match_found_for_this_criterion = any(str(av).endswith(expected_value) for av in actual_value_list if av is not None)
            elif op == MatchOperation.REGEX:
                if not isinstance(expected_value, str): return False
                try:
                    match_found_for_this_criterion = any(bool(re.search(expected_value, str(av))) for av in actual_value_list if av is not None)
                except re.error as regex_err:
                    logger.error(f"Invalid regex '{expected_value}' for tag {tag_str}: {regex_err}"); return False
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                match_found_for_this_criterion = any(_compare_numeric(av, expected_value, op) for av in actual_value_list if av is not None)
            elif op == MatchOperation.IN:
                if not isinstance(expected_value, list): return False
                # Check if any actual value is present in the expected list of values.
                # Optimize by creating a set of stringified expected values if expected_value list is large.
                # For simplicity and given typical use cases, direct iteration is fine.
                match_found_for_this_criterion = any(any(_values_equal(av, ev_item) for ev_item in expected_value) for av in actual_value_list)
            elif op == MatchOperation.NOT_IN:
                if not isinstance(expected_value, list): return False
                # All actual values must NOT be present in the expected list.
                match_found_for_this_criterion = all(all(not _values_equal(av, ev_item) for ev_item in expected_value) for av in actual_value_list)

            # IP ops are invalid here (validated at schema level but good to be defensive)
            elif op in [MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET]:
                 logger.warning(f"IP operator '{op.value}' misused on DICOM tag '{tag_str}'. Match fails."); return False
            else: # Unknown op
                 logger.warning(f"Unhandled tag match op '{op.value}' for tag {tag_str}. Match fails."); return False

            if not match_found_for_this_criterion:
                # logger.debug(f"Tag match fail: {tag_str} ('{actual_value}') failed {op.value} with '{expected_value}'")
                return False # This criterion failed
        except Exception as e:
            logger.error(f"Error evaluating tag criterion for {tag_str} op {op.value}: {e}", exc_info=True)
            return False # Fail on error
            
    return True # All criteria matched


def check_association_criteria(assoc_info: Optional[Dict[str, str]], criteria: Optional[List[AssociationMatchCriterion]]) -> bool:
    """Checks if association info matches all provided criteria."""
    if not criteria: return True
    if not assoc_info: return False # Criteria exist, but no info to check

    # logger.debug(f"Checking association criteria against: {assoc_info}")

    for criterion in criteria:
        if not hasattr(criterion, 'parameter') or not hasattr(criterion, 'op'): # Basic sanity
            logger.warning(f"Skipping malformed association criterion: {criterion}")
            return False # Malformed criterion fails the match

        param_enum_val = criterion.parameter # This is already the Literal from schema
        op = criterion.op
        expected_value = criterion.value
        
        # Map the schema's Literal parameter to the key in assoc_info dict
        # These keys ('calling_ae_title', etc.) are typically lowercase from how assoc_info is built
        param_key_map = {
            'CALLING_AE_TITLE': 'calling_ae_title',
            'CALLED_AE_TITLE': 'called_ae_title',
            'SOURCE_IP': 'source_ip',
        }
        lookup_key = param_key_map.get(param_enum_val)

        if not lookup_key: # Should not happen if schema validation is tight
            logger.error(f"Unknown association parameter '{param_enum_val}' in rule_evaluator. Match fails.")
            return False
            
        actual_value: Optional[str] = assoc_info.get(lookup_key)

        if op == MatchOperation.EXISTS:
            if lookup_key not in assoc_info: return False # Check key presence, not just if value is None
            else: continue
        elif op == MatchOperation.NOT_EXISTS:
            if lookup_key in assoc_info: return False
            else: continue

        if actual_value is None: # For other ops, missing actual_value is a fail
            return False

        match = False
        try:
            if op == MatchOperation.EQUALS: match = _values_equal(actual_value, expected_value)
            elif op == MatchOperation.NOT_EQUALS: match = not _values_equal(actual_value, expected_value)
            elif op == MatchOperation.CONTAINS:
                if not isinstance(expected_value, str): return False
                match = expected_value in str(actual_value)
            elif op == MatchOperation.STARTS_WITH:
                if not isinstance(expected_value, str): return False
                match = str(actual_value).startswith(expected_value)
            elif op == MatchOperation.ENDS_WITH:
                if not isinstance(expected_value, str): return False
                match = str(actual_value).endswith(expected_value)
            elif op == MatchOperation.REGEX:
                if not isinstance(expected_value, str): return False
                try: match = bool(re.search(expected_value, str(actual_value)))
                except re.error as regex_err:
                    logger.error(f"Invalid regex '{expected_value}' for assoc param {param_enum_val}: {regex_err}"); return False
            elif op == MatchOperation.IN:
                if not isinstance(expected_value, list): return False
                match = any(_values_equal(actual_value, ev_item) for ev_item in expected_value)
            elif op == MatchOperation.NOT_IN:
                if not isinstance(expected_value, list): return False
                match = not any(_values_equal(actual_value, ev_item) for ev_item in expected_value)
            
            # IP Address operations (assuming ipaddress module is available if these become active)
            # For now, these are validated by schema to only be used with SOURCE_IP.
            # Implementation of IP checks would go here.
            elif op == MatchOperation.IP_ADDRESS_EQUALS:
                # import ipaddress
                # try: match = ipaddress.ip_address(actual_value) == ipaddress.ip_address(str(expected_value))
                # except ValueError: match = False
                logger.warning(f"IP_ADDRESS_EQUALS NYI for assoc param '{param_enum_val}'. Match fails.")
                match = False
            elif op == MatchOperation.IP_ADDRESS_STARTS_WITH: # More like "is in network prefix"
                logger.warning(f"IP_ADDRESS_STARTS_WITH NYI for assoc param '{param_enum_val}'. Match fails.")
                match = False
            elif op == MatchOperation.IP_ADDRESS_IN_SUBNET:
                # import ipaddress
                # try: match = ipaddress.ip_address(actual_value) in ipaddress.ip_network(str(expected_value), strict=False)
                # except ValueError: match = False
                logger.warning(f"IP_ADDRESS_IN_SUBNET NYI for assoc param '{param_enum_val}'. Match fails.")
                match = False
            
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 logger.warning(f"Numeric op '{op.value}' misused on assoc param '{param_enum_val}'. Match fails."); return False
            else: # Unknown op
                 logger.warning(f"Unhandled assoc match op '{op.value}' for param {param_enum_val}. Match fails."); return False

            if not match:
                # logger.debug(f"Assoc match fail: Param '{param_enum_val}' ('{actual_value}') failed {op.value} with '{expected_value}'")
                return False
        except Exception as e:
            logger.error(f"Error evaluating assoc criterion for {param_enum_val} op {op.value}: {e}", exc_info=True)
            return False # Fail on error
            
    return True # All criteria matched
