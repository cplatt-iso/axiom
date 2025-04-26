# app/worker/processing_logic.py

import logging
import re
import json
from copy import deepcopy
from typing import Optional, Any, List, Dict, Tuple, Union, Literal
from datetime import date, time as dt_time, datetime, timezone
from decimal import Decimal
from pydantic import BaseModel, Field, TypeAdapter

import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, tag_for_keyword, repeater_has_keyword
from pydicom.valuerep import DSfloat, IS, DSdecimal, VR
from pydicom.dataset import DataElement, Dataset
from pydicom.multival import MultiValue
from pydicom.sequence import Sequence

from app.schemas.rule import (
    MatchOperation, ModifyAction as ModifyActionEnum, MatchCriterion, AssociationMatchCriterion,
    TagModification, TagSetModification, TagDeleteModification,
    TagPrependModification, TagSuffixModification, TagRegexReplaceModification,
    TagCopyModification, TagMoveModification
    # Crosswalk modification type is part of TagModification Union
)

from app.db.models import RuleSet, RuleSetExecutionMode, Rule, CrosswalkMap
from app.db.models.storage_backend_config import StorageBackendConfig
from app.services.storage_backends import StorageBackendError, get_storage_backend
from app.core.config import settings
from app import crud
from app.crosswalk import service as crosswalk_service

logger = logging.getLogger(__name__)

ORIGINAL_ATTRIBUTES_SEQUENCE_TAG = Tag(0x0400, 0x0550)
SOURCE_AE_TITLE_TAG = Tag(0x0002, 0x0016)
SOURCE_APPLICATION_ENTITY_TITLE_TAG = Tag(0x0002, 0x0016)
CALLED_AE_TITLE_TAG = Tag(0x0000, 0x0000) # Placeholder
CALLING_AE_TITLE_TAG = Tag(0x0000, 0x0000) # Placeholder
SOURCE_IP = Tag(0x0000, 0x0000) # Placeholder

# --- Helper functions ---
def parse_dicom_tag(tag_str: str) -> Optional[BaseTag]:
    if not isinstance(tag_str, str):
        logger.warning(f"Invalid type for tag string: {type(tag_str)}. Expected str.")
        return None
    tag_str = tag_str.strip()
    match_ge = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", tag_str)
    if match_ge:
        try:
            group1 = int(match_ge.group(1), 16)
            group2 = int(match_ge.group(2), 16)
            return Tag(group1, group2)
        except ValueError:
            pass # Continue to check keywords
    # Check if it's a potential keyword (alphanumeric, not a repeater group keyword)
    if re.match(r"^[a-zA-Z0-9]+$", tag_str) and not repeater_has_keyword(tag_str):
        try:
            tag_val = tag_for_keyword(tag_str)
            if tag_val:
                return Tag(tag_val)
        except (ValueError, TypeError):
             # Not a standard keyword or parsing failed
             pass

    logger.warning(f"Could not parse tag string/keyword: '{tag_str}'")
    return None

def _values_equal(actual_val: Any, expected_val: Any) -> bool:
    # Attempt numeric/decimal comparison first
    try:
        if isinstance(actual_val, (int, float, Decimal, DSfloat, IS, DSdecimal)) and isinstance(expected_val, (int, float, str, Decimal)):
            try:
                actual_decimal = Decimal(str(actual_val))
                expected_decimal = Decimal(str(expected_val))
                return actual_decimal == expected_decimal
            except (ValueError, TypeError):
                # If conversion fails, fall through to string comparison
                pass
    except (ValueError, TypeError):
        pass # Ignore conversion errors and fall through

    # Attempt date/time comparison
    try:
        if isinstance(actual_val, (date, datetime, dt_time)):
             # Simple string comparison might be sufficient for DICOM formats
             return str(actual_val) == str(expected_val)
    except Exception as e:
        # Log unexpected errors during date/time comparison
        logger.debug(f"Could not compare values temporally: {actual_val} vs {expected_val}. Error: {e}. Falling back to string comparison.")
        pass

    # Fallback to string comparison
    return str(actual_val) == str(expected_val)

def _compare_numeric(actual_val: Any, expected_val: Any, op: MatchOperation) -> bool:
     try:
         actual_decimal = Decimal(str(actual_val))
         expected_decimal = Decimal(str(expected_val))
         if op == MatchOperation.GREATER_THAN:
            return actual_decimal > expected_decimal
         if op == MatchOperation.LESS_THAN:
            return actual_decimal < expected_decimal
         if op == MatchOperation.GREATER_EQUAL:
            return actual_decimal >= expected_decimal
         if op == MatchOperation.LESS_EQUAL:
            return actual_decimal <= expected_decimal
         # Should not happen if called correctly, but return False for safety
         return False
     except (ValueError, TypeError, Exception):
         logger.debug(f"Cannot compare non-numeric values using Decimal: '{actual_val}', '{expected_val}'")
         return False

# --- Match checking functions ---
def check_match(dataset: pydicom.Dataset, criteria: List[MatchCriterion]) -> bool:
    if not criteria:
        return True # No criteria means match

    for criterion in criteria:
        # Basic validation of the criterion object itself
        if not isinstance(criterion, MatchCriterion):
            logger.warning(f"Skipping invalid criterion object: {criterion}")
            continue

        tag_str = criterion.tag
        op = criterion.op
        expected_value = criterion.value

        tag = parse_dicom_tag(tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key '{tag_str}' in match criteria {criterion.model_dump()}")
            return False # A single invalid tag means the whole match fails

        actual_data_element = dataset.get(tag, None)

        # Handle existence checks first
        if op == MatchOperation.EXISTS:
            if actual_data_element is None:
                logger.debug(f"Match fail: Tag {tag_str} ({tag}) does not exist (op: EXISTS)")
                return False
            else:
                continue # This criterion matched, check next
        elif op == MatchOperation.NOT_EXISTS:
            if actual_data_element is not None:
                logger.debug(f"Match fail: Tag {tag_str} ({tag}) exists (op: NOT_EXISTS)")
                return False
            else:
                continue # This criterion matched, check next

        # For other operators, the tag must exist
        if actual_data_element is None:
            logger.debug(f"Match fail: Tag {tag_str} ({tag}) does not exist (required for op: {op.value})")
            return False

        actual_value = actual_data_element.value
        # Handle multi-valued tags: check if *any* value matches
        is_multi_value = isinstance(actual_value, MultiValue)
        actual_value_list = list(actual_value) if is_multi_value else [actual_value]

        # Ensure list isn't empty if tag exists but value is None (shouldn't happen often with pydicom)
        if not actual_value_list and actual_value is not None:
             actual_value_list = [actual_value]
        elif not actual_value_list and actual_value is None:
             # Tag exists but has None value. How should operators handle this?
             # Most comparisons will likely fail. Let the checks proceed.
             logger.debug(f"Tag {tag_str} exists but has None value.")
             pass


        match = False # Assume no match initially for this criterion
        try:
            if op == MatchOperation.EQUALS:
                match = any(_values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.NOT_EQUALS:
                # For multi-value, 'not equals' means ALL values must not equal the expected value
                match = all(not _values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.CONTAINS:
                if not isinstance(expected_value, str):
                    logger.warning(f"'contains' requires string value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                    continue # Skip this criterion, maybe log error or fail rule? Failing rule seems safer. return False
                match = any(expected_value in str(av) for av in actual_value_list)
            elif op == MatchOperation.STARTS_WITH:
                if not isinstance(expected_value, str):
                    logger.warning(f"'startswith' requires string value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                    continue # Skip or fail rule? return False
                match = any(str(av).startswith(expected_value) for av in actual_value_list)
            elif op == MatchOperation.ENDS_WITH:
                if not isinstance(expected_value, str):
                    logger.warning(f"'endswith' requires string value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                    continue # Skip or fail rule? return False
                match = any(str(av).endswith(expected_value) for av in actual_value_list)
            elif op == MatchOperation.REGEX:
                 if not isinstance(expected_value, str):
                     logger.warning(f"'regex' requires string value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                     continue # Skip or fail rule? return False
                 match = any(bool(re.search(expected_value, str(av))) for av in actual_value_list)
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 match = any(_compare_numeric(av, expected_value, op) for av in actual_value_list)
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'in' requires list value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                     continue # Skip or fail rule? return False
                 match = any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'not_in' requires list value, got {type(expected_value)}. Skipping criterion for tag {tag_str}.")
                     continue # Skip or fail rule? return False
                 # For multi-value, 'not in' means NO actual value is in the expected list
                 match = not any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op in [MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET]:
                 # These should not be used here, but in association criteria
                logger.warning(f"IP matching operation '{op.value}' used incorrectly on DICOM tag '{tag_str}'. Rule will fail.")
                return False

            # If this specific criterion did not match, the whole rule fails
            if not match:
                 actual_repr = repr(actual_value_list[0]) if len(actual_value_list) == 1 else f"{len(actual_value_list)} values"
                 logger.debug(f"Match fail: Tag {tag_str} ('{actual_repr}') failed op '{op.value}' with value '{expected_value}'")
                 return False
        except Exception as e:
            # Log errors during comparison and fail the match
            logger.error(f"Error during matching for tag {tag_str} with op {op.value}: {e}", exc_info=True)
            return False

    # If loop completes without returning False, all criteria matched
    return True

def check_association_match(assoc_info: Optional[Dict[str, str]], criteria: Optional[List[AssociationMatchCriterion]]) -> bool:
    if not criteria:
        return True # No criteria means match
    if not assoc_info:
        logger.debug("Association criteria exist, but no association info provided. Match fails.")
        return False # Criteria exist but no info to match against

    logger.debug(f"Checking association criteria against: {assoc_info}")

    for criterion in criteria:
        if not isinstance(criterion, AssociationMatchCriterion):
            logger.warning(f"Skipping invalid association criterion object: {criterion}")
            continue

        param = criterion.parameter
        op = criterion.op
        expected_value = criterion.value

        actual_value: Optional[str] = None
        if param == "CALLING_AE_TITLE":
            actual_value = assoc_info.get('calling_ae_title')
        elif param == "CALLED_AE_TITLE":
            actual_value = assoc_info.get('called_ae_title')
        elif param == "SOURCE_IP":
            actual_value = assoc_info.get('source_ip')

        # If the parameter isn't in the association info, the criterion fails
        if actual_value is None:
            logger.debug(f"Assoc match fail: Parameter '{param}' not found in association info. Required for op '{op.value}'")
            return False

        match = False # Assume no match initially for this criterion
        try:
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
                     logger.warning(f"'regex' requires string value for assoc. param '{param}', got {type(expected_value)}.")
                     continue # Skip or fail? Let's fail the rule for bad config. return False
                 match = bool(re.search(expected_value, str(actual_value)))
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'in' requires list value for assoc. param '{param}', got {type(expected_value)}.")
                     continue # Fail rule return False
                 match = any(_values_equal(actual_value, ev) for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list):
                     logger.warning(f"'not_in' requires list value for assoc. param '{param}', got {type(expected_value)}.")
                     continue # Fail rule return False
                 match = not any(_values_equal(actual_value, ev) for ev in expected_value)
            elif op == MatchOperation.IP_ADDRESS_EQUALS:
                 logger.warning("IP_EQ matching not implemented yet.")
                 match = False # Fail until implemented
            elif op == MatchOperation.IP_ADDRESS_STARTS_WITH:
                 logger.warning("IP_STARTSWITH matching not implemented yet.")
                 match = False # Fail until implemented
            elif op == MatchOperation.IP_ADDRESS_IN_SUBNET:
                 logger.warning("IP_IN_SUBNET matching not implemented yet.")
                 match = False # Fail until implemented
            elif op in [MatchOperation.EXISTS, MatchOperation.NOT_EXISTS, MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 logger.warning(f"Unsupported operator '{op.value}' used for Association criteria on parameter '{param}'. Rule will fail.")
                 return False # These operators don't make sense for association strings/IPs
            else:
                 # Should be unreachable if enums are correct
                 logger.warning(f"Unknown operator '{op.value}' encountered for Association criteria. Rule will fail.")
                 return False

            # If this criterion did not match, the whole rule fails
            if not match:
                logger.debug(f"Assoc match fail: Parameter '{param}' ('{actual_value}') failed op '{op.value}' with value '{expected_value}'")
                return False
        except Exception as e:
            logger.error(f"Error during association matching for parameter {param} with op {op.value}: {e}", exc_info=True)
            return False # Fail rule on error

    # If loop completes, all association criteria matched
    logger.debug("All association criteria matched.")
    return True

# --- Original Attributes Sequence function ---
def _add_original_attribute(dataset: Dataset, original_element: Optional[DataElement], modification_description: str, source_identifier: str):
    """Adds or appends to the Original Attributes Sequence, truncating reason."""
    log_enabled = getattr(settings, 'LOG_ORIGINAL_ATTRIBUTES', False)
    if not log_enabled or original_element is None:
        return # Do nothing if disabled or no original element provided

    try:
        # Ensure OriginalAttributesSequence exists and is a valid sequence
        if ORIGINAL_ATTRIBUTES_SEQUENCE_TAG not in dataset:
            new_sequence = Sequence([])
            # Create the DataElement with correct VR
            dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG] = DataElement(
                ORIGINAL_ATTRIBUTES_SEQUENCE_TAG, 'SQ', new_sequence
            )
            logger.debug(f"Created OriginalAttributesSequence for {source_identifier}.")
        else:
            seq_element = dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG]
            # Validate existing sequence
            if not isinstance(seq_element, DataElement) or seq_element.VR != 'SQ' or not isinstance(seq_element.value, (Sequence, list)):
                 logger.error(f"Existing OriginalAttributesSequence (0x0400,0x0550) for source {source_identifier} is not a valid Sequence. Cannot append modification details.")
                 return # Skip appending to invalid sequence

        # Create the item to add to the OriginalAttributesSequence
        item = Dataset()
        # Add a deep copy of the original element that was modified/deleted
        item.add(deepcopy(original_element))

        # Create the nested ModifiedAttributesSequence
        # Ensure ModifiedAttributesSequence tag exists if needed, although pydicom might handle this
        # Check if it exists and is valid first? For simplicity, just create/overwrite.
        item.ModifiedAttributesSequence = Sequence([])

        # Create the item for the ModifiedAttributesSequence
        mod_item = Dataset()
        mod_item.AttributeModificationDateTime = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S.%f')
        mod_item.ModifyingSystem = source_identifier[:64] # LO VR limit 64
        # Truncate reason string to fit LO VR limit (64 chars)
        mod_item.ReasonForTheAttributeModification = modification_description[:64]

        # Append modification details
        item.ModifiedAttributesSequence.append(mod_item)

        # Append the whole item (original element + modification details) to the main sequence
        dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG].value.append(item)
        logger.debug(f"Appended modification details for tag {original_element.tag} to OriginalAttributesSequence.")

    except Exception as e:
        # Catch any unexpected errors during sequence manipulation
        logger.error(f"Failed to add item to OriginalAttributesSequence for tag {original_element.tag}: {e}", exc_info=True)

# --- Apply Modifications Function ---
def apply_modifications(dataset: pydicom.Dataset, modifications: List[TagModification], source_identifier: str):
    """
    Applies tag modifications to the dataset IN-PLACE based on a list of actions
    defined by TagModification schema objects (discriminated union).
    Logs changes to Original Attributes Sequence if enabled.
    Handles standard actions and the 'crosswalk' action.
    """
    if not modifications:
        return # No modifications to apply

    STRING_LIKE_VRS = {'AE', 'AS', 'CS', 'DA', 'DS', 'DT', 'IS', 'LO', 'LT', 'PN', 'SH', 'ST', 'TM', 'UI', 'UR', 'UT'}

    for mod_union in modifications:
        # Validate the modification object against the union schema again? Pydantic should have done this.
        # We rely on the type hint and discriminated union structure.
        action = mod_union.action
        mod: Any = mod_union # Use Any for easier attribute access, assuming validation passed

        db_session = None
        if action == ModifyActionEnum.CROSSWALK:
            # Import SessionLocal only when needed to avoid circular dependencies at module level
            from app.db.session import SessionLocal
            db_session = SessionLocal()

        try:
            # Determine primary tag (target or source) for logging/lookup and original element logging
            primary_tag_str: Optional[str] = None
            source_tag_str: Optional[str] = None # Specifically for copy/move source logging
            if hasattr(mod, 'tag'):
                primary_tag_str = mod.tag
            elif hasattr(mod, 'source_tag'):
                primary_tag_str = mod.source_tag
                source_tag_str = mod.source_tag

            # Parse the primary tag if it exists (needed for most actions)
            tag: Optional[BaseTag] = None
            if primary_tag_str:
                tag = parse_dicom_tag(primary_tag_str)
                if not tag:
                    logger.warning(f"Skipping modification: Invalid primary/source tag key '{primary_tag_str}' in action {action.value}: {mod.model_dump()}")
                    continue # Skip this modification entirely

            # --- Original Element Logging ---
            # Get the original element *before* any modification happens
            # For COPY/MOVE, we log the original state of BOTH source and destination
            original_element_for_log: Optional[DataElement] = None
            if tag: # If primary tag (target/source) exists
                original_element_for_log = deepcopy(dataset.get(tag, None))

            # Handle CROSSWALK action
            if action == ModifyActionEnum.CROSSWALK:
                logger.debug(f"Processing CROSSWALK modification using Map ID: {mod.crosswalk_map_id}")
                if not db_session:
                    logger.error("Database session required but not available for crosswalk lookup.")
                    continue # Skip this modification

                # 1. Get CrosswalkMap configuration
                crosswalk_map_config = crud.crud_crosswalk_map.get(db_session, id=mod.crosswalk_map_id)
                if not crosswalk_map_config:
                    logger.error(f"CrosswalkMap configuration ID {mod.crosswalk_map_id} not found. Skipping crosswalk.")
                    continue
                if not crosswalk_map_config.is_enabled:
                     logger.warning(f"CrosswalkMap '{crosswalk_map_config.name}' (ID: {mod.crosswalk_map_id}) is disabled. Skipping.")
                     continue

                # 2. Extract match values from the *incoming* dataset
                incoming_match_values: Dict[str, Any] = {}
                can_match = True
                for match_map in crosswalk_map_config.match_columns:
                    dicom_tag_str = match_map.get('dicom_tag')
                    col_name = match_map.get('column_name') # For logging
                    if not dicom_tag_str:
                        logger.error(f"Invalid match_columns config in Map ID {mod.crosswalk_map_id}: missing 'dicom_tag'. Config: {match_map}")
                        can_match = False
                        break
                    match_tag = parse_dicom_tag(dicom_tag_str)
                    if not match_tag:
                         logger.error(f"Invalid DICOM tag '{dicom_tag_str}' in match_columns for Map ID {mod.crosswalk_map_id}.")
                         can_match = False
                         break
                    data_element = dataset.get(match_tag)
                    # If tag doesn't exist or value is None, cannot match
                    if data_element is None or data_element.value is None:
                         logger.debug(f"Match tag '{dicom_tag_str}' (Col: {col_name}) not found or empty in dataset for Map ID {mod.crosswalk_map_id}. Cannot perform crosswalk.")
                         can_match = False
                         break
                    # Handle multi-value: use first value for lookup key generation
                    value_to_match = data_element.value
                    if isinstance(value_to_match, MultiValue):
                         if len(value_to_match) > 0:
                             value_to_match = value_to_match[0]
                             logger.warning(f"Using first value of multi-value tag {dicom_tag_str} for crosswalk matching.")
                         else:
                              logger.debug(f"Match tag '{dicom_tag_str}' is multi-value but empty. Cannot match.")
                              can_match = False
                              break
                    incoming_match_values[dicom_tag_str] = value_to_match

                if not can_match:
                    continue # Skip this crosswalk modification

                # 3. Perform lookup using sync version from service
                logger.debug(f"Performing crosswalk lookup for Map ID {mod.crosswalk_map_id} with values: {incoming_match_values}")
                replacement_data: Optional[Dict[str, Any]] = None
                try:
                    replacement_data = crosswalk_service.get_crosswalk_value_sync(crosswalk_map_config, incoming_match_values)
                    logger.debug(f"Crosswalk lookup result: {replacement_data}")
                except Exception as lookup_exc:
                    logger.error(f"Error during crosswalk lookup for Map ID {mod.crosswalk_map_id}: {lookup_exc}", exc_info=True)
                    # Continue to next modification if lookup fails? Or stop rule? Default: continue.

                # 4. Apply replacements if data found
                if replacement_data and isinstance(replacement_data, dict):
                    logger.info(f"Crosswalk data found for Map ID {mod.crosswalk_map_id}. Applying replacements.")
                    for target_tag_str, replace_info in replacement_data.items():
                        target_tag = parse_dicom_tag(target_tag_str)
                        if not target_tag:
                            logger.warning(f"Invalid target DICOM tag '{target_tag_str}' in replacement data for Map ID {mod.crosswalk_map_id}. Skipping.")
                            continue

                        new_value = replace_info.get("value")
                        new_vr = replace_info.get("vr")

                        # Log the original value of the tag we are about to replace
                        original_target_element = deepcopy(dataset.get(target_tag, None))
                        mod_desc = f"Crosswalk: Set tag {target_tag_str} via Map ID {mod.crosswalk_map_id}"

                        # Determine VR if not provided in the map
                        final_vr = new_vr
                        if not final_vr:
                            if original_target_element:
                                final_vr = original_target_element.VR
                            else:
                                try:
                                    final_vr = dictionary_VR(target_tag)
                                except KeyError:
                                    final_vr = 'UN' # Default to Unknown if not in dictionary
                                    logger.warning(f"Crosswalk: Could not determine VR for new tag {target_tag_str}. Defaulting to 'UN'. Specify VR in CrosswalkMap if needed.")

                        # Apply the 'set' logic using the crosswalked value
                        processed_value = new_value
                        if new_value is not None:
                            # Basic type coercion based on VR
                            try:
                                if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'):
                                    processed_value = [int(v) for v in new_value] if isinstance(new_value, list) else int(new_value)
                                elif final_vr in ('FL', 'FD', 'OD', 'OF'):
                                    processed_value = [float(v) for v in new_value] if isinstance(new_value, list) else float(new_value)
                                elif final_vr == 'DS':
                                    # DS can be string or list of strings representing numbers
                                    processed_value = [str(v) for v in new_value] if isinstance(new_value, list) else str(new_value)
                                # Add more conversions if needed (e.g., DA, TM, DT)
                            except (ValueError, TypeError) as conv_err:
                                 logger.warning(f"Crosswalk: Value '{new_value}' for tag {target_tag_str} could not be coerced for VR '{final_vr}': {conv_err}. Setting value as received from crosswalk.")
                                 processed_value = new_value # Use original value if coercion fails
                        else:
                            # If crosswalk lookup explicitly returned None for a source column
                            processed_value = None
                            logger.debug(f"Crosswalk: Setting tag {target_tag_str} ({target_tag}) to empty value (VR: {final_vr}) as crosswalk returned None.")

                        # Set the data element
                        dataset[target_tag] = DataElement(target_tag, final_vr, processed_value)
                        logger.debug(f"Crosswalk: Set tag {target_tag_str} ({target_tag}) (VR: {final_vr})")

                        # Log the original attribute
                        _add_original_attribute(dataset, original_target_element, mod_desc, source_identifier)

                else:
                    # Log if lookup returned no data (either cache miss with fail/log or DB miss)
                    logger.info(f"Crosswalk lookup did not return data for Map ID {mod.crosswalk_map_id} using match values: {incoming_match_values}")

                # Continue to the next modification in the list
                continue

            # --- Handle Other Actions ---
            # Ensure 'tag' is valid if required by the action
            if tag is None and action not in [ModifyActionEnum.COPY, ModifyActionEnum.MOVE]: # Copy/Move use source_tag parsed into tag
                 logger.warning(f"Skipping modification action {action.value}: Missing or invalid target tag.")
                 continue

            if action == ModifyActionEnum.DELETE:
                if tag in dataset:
                    modification_description = f"Deleted tag {tag_str}"
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_str} ({tag})")
                    # Log original element *before* deletion
                    _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                else:
                    logger.debug(f"Tag {tag_str} ({tag}) not found for deletion.")

            elif action == ModifyActionEnum.SET:
                 if not isinstance(mod, TagSetModification): continue # Type check
                 new_value = mod.value
                 vr = mod.vr
                 final_vr = vr
                 modification_description = f"Set tag {tag_str} to '{new_value}' (VR:{vr or 'auto'})"

                 if not final_vr:
                      if original_element_for_log: final_vr = original_element_for_log.VR
                      else:
                           try: final_vr = dictionary_VR(tag)
                           except KeyError: final_vr = 'UN'; logger.warning(f"Could not determine VR for new tag {tag_str} ({tag}) in SET. Defaulting to 'UN'.")

                 processed_value = new_value
                 # Basic type coercion
                 if new_value is not None:
                     try:
                          if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'): processed_value = [int(v) for v in new_value] if isinstance(new_value, list) else int(new_value)
                          elif final_vr in ('FL', 'FD', 'OD', 'OF'): processed_value = [float(v) for v in new_value] if isinstance(new_value, list) else float(new_value)
                          elif final_vr == 'DS': processed_value = [str(v) for v in new_value] if isinstance(new_value, list) else str(new_value)
                     except (ValueError, TypeError) as conv_err:
                         logger.warning(f"Value '{new_value}' for tag {tag_str} could not be coerced for VR '{final_vr}': {conv_err}. Setting as is.")
                         processed_value = new_value
                 else:
                     processed_value = None

                 dataset[tag] = DataElement(tag, final_vr, processed_value)
                 logger.debug(f"Set tag {tag_str} ({tag}) (VR: {final_vr})")
                 _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)

            elif action in [ModifyActionEnum.PREPEND, ModifyActionEnum.SUFFIX]:
                 if not isinstance(mod, (TagPrependModification, TagSuffixModification)): continue
                 modification_description = f"{action.value} tag {tag_str} with '{mod.value}'"
                 if not original_element_for_log:
                     logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) does not exist.")
                     continue
                 current_vr = original_element_for_log.VR
                 if current_vr not in STRING_LIKE_VRS:
                     logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) has non-string-like VR '{current_vr}'.")
                     continue

                 text_to_add = mod.value
                 current_value = original_element_for_log.value
                 if isinstance(current_value, MultiValue):
                      modified_list = []
                      for item in current_value:
                           item_str = str(item)
                           modified_item = text_to_add + item_str if action == ModifyActionEnum.PREPEND else item_str + text_to_add
                           modified_list.append(modified_item)
                      dataset[tag].value = modified_list
                      logger.debug(f"Applied {action.value} to all {len(modified_list)} items of multivalue tag {tag_str} ({tag}).")
                 else:
                      original_str = str(current_value) if current_value is not None else ""
                      modified_value = text_to_add + original_str if action == ModifyActionEnum.PREPEND else original_str + text_to_add
                      dataset[tag].value = modified_value
                      logger.debug(f"Applied {action.value} to tag {tag_str} ({tag}).")
                 _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)

            elif action == ModifyActionEnum.REGEX_REPLACE:
                 if not isinstance(mod, TagRegexReplaceModification): continue
                 modification_description = f"Regex replace tag {tag_str} using '{mod.pattern}' -> '{mod.replacement}'"
                 if not original_element_for_log:
                     logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) does not exist.")
                     continue
                 current_vr = original_element_for_log.VR
                 if current_vr not in STRING_LIKE_VRS:
                     logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) has non-string-like VR '{current_vr}'.")
                     continue

                 pattern = mod.pattern
                 replacement = mod.replacement
                 try:
                      current_value = original_element_for_log.value
                      if isinstance(current_value, MultiValue):
                           modified_list = [re.sub(pattern, replacement, str(item)) for item in current_value]
                           dataset[tag].value = modified_list
                           logger.debug(f"Applied {action.value} to multivalue tag {tag_str} ({tag}) using pattern '{pattern}'.")
                      else:
                           original_str = str(current_value) if current_value is not None else ""
                           modified_value = re.sub(pattern, replacement, original_str)
                           dataset[tag].value = modified_value
                           logger.debug(f"Applied {action.value} to tag {tag_str} ({tag}) using pattern '{pattern}'.")
                      _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                 except re.error as regex_err:
                     logger.error(f"Error applying {action.value} regex for tag {tag_str} ({tag}): {regex_err}", exc_info=True)

            elif action in [ModifyActionEnum.COPY, ModifyActionEnum.MOVE]:
                if not isinstance(mod, (TagCopyModification, TagMoveModification)): continue
                # 'tag' here represents the SOURCE tag from the check above
                source_tag = tag
                # We already logged the original source element using original_element_for_log

                dest_tag_str = getattr(mod, 'destination_tag', None)
                dest_tag = parse_dicom_tag(dest_tag_str) if dest_tag_str else None

                if not dest_tag:
                    logger.warning(f"Skipping {action.value}: Invalid destination tag '{dest_tag_str}'.")
                    continue

                source_element = dataset.get(source_tag) # Re-get in case it was modified? No, use original snapshot
                if original_element_for_log is None: # Check the snapshot
                    logger.warning(f"Cannot {action.value}: Source tag {source_tag_str} ({source_tag}) not found in dataset originally.")
                    continue

                # Log the original state of the *destination* tag before overwriting/creating
                original_dest_element_before_mod: Optional[DataElement] = deepcopy(dataset.get(dest_tag, None))

                modification_description = f"{action.value} tag {source_tag_str} to {dest_tag_str}"

                # Use VR from source unless overridden
                dest_vr = mod.destination_vr or original_element_for_log.VR
                # Create new element with deepcopied value from original source snapshot
                new_element = DataElement(dest_tag, dest_vr, deepcopy(original_element_for_log.value))
                dataset[dest_tag] = new_element
                logger.debug(f"Tag {source_tag_str} ({source_tag}) {action.value}d to {dest_tag_str} ({dest_tag}) with VR '{dest_vr}'")

                # Log the change to the destination tag (recording its previous state)
                _add_original_attribute(dataset, original_dest_element_before_mod, modification_description, source_identifier)

                if action == ModifyActionEnum.MOVE:
                    if source_tag in dataset:
                        # Log the deletion of the source tag (recording its state just before deletion)
                        _add_original_attribute(dataset, original_element_for_log, f"Deleted tag {source_tag_str} as part of move", source_identifier)
                        del dataset[source_tag]
                        logger.debug(f"Deleted original source tag {source_tag_str} ({source_tag}) after move.")

        except Exception as e:
            tag_id_str = str(tag) if tag else getattr(mod, 'crosswalk_map_id', 'N/A')
            logger.error(f"Failed to apply modification ({action.value}) for target {tag_id_str}: {e}", exc_info=True)
        finally:
            if db_session:
                db_session.close()

# --- process_dicom_instance function ---
def process_dicom_instance(
    original_ds: pydicom.Dataset,
    active_rulesets: List[RuleSet],
    source_identifier: str,
    association_info: Optional[Dict[str, str]] = None
) -> Tuple[Optional[pydicom.Dataset], List[str], List[Dict[str, Any]]]:

    logger.debug(f"Processing instance from source '{source_identifier}' against {len(active_rulesets)} rulesets.")
    modified_ds: Optional[pydicom.Dataset] = None
    applied_rules_info: List[str] = []
    destinations_to_process: List[Dict[str, Any]] = []
    any_rule_matched = False
    modifications_applied = False

    for ruleset in active_rulesets:
        logger.debug(f"Evaluating RuleSet '{ruleset.name}' (ID: {ruleset.id}, Priority: {ruleset.priority})")
        # Ensure rules are loaded (should be due to eager loading in CRUD)
        rules_to_evaluate = ruleset.rules or []
        if not rules_to_evaluate:
             logger.debug(f"RuleSet '{ruleset.name}' has no rules.")
             continue

        matched_rule_in_this_set = False
        # Rules should already be sorted by priority from the relationship definition
        for rule in rules_to_evaluate:
             if not rule.is_active:
                 logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) is inactive. Skipping.")
                 continue

             # Check applicable sources
             rule_sources = rule.applicable_sources
             source_match = False
             if not rule_sources: # Applies to all if null/empty
                 source_match = True
                 logger.debug(f"Rule '{rule.name}' applies to all sources (including '{source_identifier}').")
             elif isinstance(rule_sources, list) and source_identifier in rule_sources:
                 source_match = True
                 logger.debug(f"Rule '{rule.name}' applicable_sources ({rule_sources}) includes source '{source_identifier}'.")
             else:
                 logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) does not apply to source '{source_identifier}'. Skipping.")

             if not source_match:
                 continue

             logger.debug(f"Checking rule '{rule.name}' (ID: {rule.id}, Priority: {rule.priority}) against source '{source_identifier}'")
             try:
                 # --- Schema Validation ---
                 # Validate match criteria structure from DB
                 match_criteria_validated: List[MatchCriterion] = []
                 if rule.match_criteria:
                     try:
                         match_criteria_validated = TypeAdapter(List[MatchCriterion]).validate_python(rule.match_criteria)
                     except Exception as val_err:
                          logger.error(f"Rule '{rule.name}' (ID: {rule.id}): Invalid 'match_criteria' structure: {val_err}. Skipping rule.")
                          continue

                 # Validate association criteria structure from DB
                 assoc_criteria_validated: List[AssociationMatchCriterion] = []
                 if rule.association_criteria:
                     try:
                          assoc_criteria_validated = TypeAdapter(List[AssociationMatchCriterion]).validate_python(rule.association_criteria)
                     except Exception as val_err:
                          logger.error(f"Rule '{rule.name}' (ID: {rule.id}): Invalid 'association_criteria' structure: {val_err}. Skipping rule.")
                          continue

                 # Validate tag modifications structure from DB
                 modifications_validated: List[TagModification] = []
                 if rule.tag_modifications:
                      try:
                          # Use the union type adapter
                          modifications_validated = TypeAdapter(List[TagModification]).validate_python(rule.tag_modifications)
                      except Exception as val_err:
                          logger.error(f"Rule '{rule.name}' (ID: {rule.id}): Invalid 'tag_modifications' structure: {val_err}. Skipping modifications for this rule.")
                          # Decide: skip entire rule or just modifications? Skipping mods seems safer.
                          modifications_validated = [] # Process rule match, but skip modifications
                 # --- End Schema Validation ---


                 # Perform matching
                 tag_match = check_match(original_ds, match_criteria_validated)
                 assoc_match = check_association_match(association_info, assoc_criteria_validated)

                 # If BOTH match criteria pass
                 if tag_match and assoc_match:
                     logger.info(f"Rule '{ruleset.name}' / '{rule.name}' MATCHED (Tags: {tag_match}, Assoc: {assoc_match}).")
                     any_rule_matched = True
                     matched_rule_in_this_set = True

                     # Apply modifications if they exist and are valid
                     if modifications_validated:
                         # Create deep copy only if not already created and modifications exist
                         if modified_ds is None:
                             modified_ds = deepcopy(original_ds)
                             logger.debug("Created deep copy of dataset for modifications.")

                         if modified_ds is not None: # Ensure copy exists
                            logger.debug(f"Applying {len(modifications_validated)} modifications for rule '{rule.name}'...")
                            apply_modifications(modified_ds, modifications_validated, source_identifier)
                            modifications_applied = True
                     else:
                         logger.debug(f"Rule '{rule.name}' matched but had no valid modifications defined or they failed validation.")

                     # Add destinations from this matched rule
                     # Ensure destinations relationship is loaded (should be by selectinload)
                     rule_destinations: List[StorageBackendConfig] = rule.destinations or []
                     for dest_config_obj in rule_destinations:
                          if dest_config_obj.is_enabled:
                                # Extract config dict and add type for the factory function
                                backend_config_dict = dest_config_obj.config or {}
                                backend_config_dict['type'] = dest_config_obj.backend_type
                                destinations_to_process.append(backend_config_dict)
                                logger.debug(f"Added destination '{dest_config_obj.name}' (Type: {dest_config_obj.backend_type}) from rule '{rule.name}'.")
                          else:
                               logger.debug(f"Skipping disabled destination '{dest_config_obj.name}' from rule '{rule.name}'.")

                     # Record which rule was applied
                     applied_rules_info.append(f"{ruleset.name}/{rule.name} (ID:{rule.id})")

                     # If ruleset mode is FIRST_MATCH, stop processing rules in this set
                     if ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                         logger.debug(f"RuleSet '{ruleset.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                         break # Exit inner loop (rules)
                 else:
                     logger.debug(f"Rule '{rule.name}' did not match (Tags: {tag_match}, Assoc: {assoc_match}).")

             except Exception as rule_proc_exc:
                 logger.error(f"Error processing rule '{ruleset.name}/{rule.name}': {rule_proc_exc}", exc_info=True)
                 # Continue to the next rule even if one fails? Or stop the ruleset? Let's continue for now.

        # If FIRST_MATCH was triggered in the inner loop, break the outer loop too
        if matched_rule_in_this_set and ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
            logger.debug(f"FIRST_MATCH rule found in set '{ruleset.name}'. Stopping ruleset evaluation.")
            break # Exit outer loop (rulesets)

    # After evaluating all rulesets (or breaking early)
    if not any_rule_matched:
        logger.info(f"No applicable rules matched for source '{source_identifier}' and assoc info {association_info}. No modifications or destinations applied.")
        return None, [], []

    # Deduplicate destinations
    unique_dest_strings = set()
    unique_destination_configs = []
    for dest_config_dict in destinations_to_process:
        try:
            # Use canonical JSON string for comparison
            json_string = json.dumps(dest_config_dict, sort_keys=True, separators=(',', ':'))
            if json_string not in unique_dest_strings:
                unique_dest_strings.add(json_string)
                unique_destination_configs.append(dest_config_dict)
        except TypeError as e:
             # Fallback for non-serializable data - use string representation (less reliable)
             logger.warning(f"Could not serialize destination config for de-duplication: {dest_config_dict}. Error: {e}. Using string representation.")
             str_repr = str(dest_config_dict)
             if str_repr not in unique_dest_strings:
                  unique_dest_strings.add(str_repr)
                  unique_destination_configs.append(dest_config_dict)

    logger.debug(f"Found {len(unique_destination_configs)} unique destinations after processing rules.")

    # Return the dataset (original if no modifications applied, copied/modified otherwise)
    # Return applied rule info and unique destination configs
    return modified_ds if modifications_applied else original_ds, applied_rules_info, unique_destination_configs
