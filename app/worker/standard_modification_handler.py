# backend/app/worker/standard_modification_handler.py
import structlog
import re
from copy import deepcopy
from typing import Optional, Any, List, Dict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import pydicom
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, keyword_for_tag
from pydicom.dataset import Dataset  # MODIFIED: DataElement removed from here
# MODIFIED: DataElement imported directly
from pydicom.dataelem import DataElement
from pydicom.multival import MultiValue
from pydicom.sequence import Sequence

from app.schemas.rule import (
    ModifyAction as ModifyActionEnum,
    TagModification, TagSetModification, TagDeleteModification,
    TagPrependModification, TagSuffixModification, TagRegexReplaceModification,
    TagCopyModification, TagMoveModification, TagCrosswalkModification
)
from app.core.config import settings
from app import crud  # For crosswalk
from app.crosswalk import service as crosswalk_service  # For crosswalk
from app.db.models import CrosswalkMap  # For crosswalk type hint
from app.utils.config_helpers import get_config_value  # For dynamic configuration

from app.worker.utils.dicom_utils import parse_dicom_tag

logger = structlog.get_logger(__name__)
# --- START: New Tag Constants from Recommended Rewrite ---
# Original Attributes Sequence  (SQ) - Corrected outer sequence
OAS_TAG = Tag(0x0400, 0x0561)
# Modified Attributes Sequence  (SQ) - Nested sequence for original value
MAS_TAG = Tag(0x0400, 0x0550)
MOD_DT = Tag(0x0400, 0x0562)   # Attribute Modification DateTime (DT)
MOD_SYS = Tag(0x0400, 0x0563)   # Modifying System               (LO)
SRC_PREV = Tag(0x0400, 0x0564)   # Source of Previous Values       (LO)
REASON = Tag(0x0400, 0x0565)   # Reason for Attribute Mod.       (LO)
# --- END: New Tag Constants ---

# --- START: Recommended Full Rewrite of _add_original_attribute ---


def _add_original_attribute(
    ds: Dataset,  # Changed from 'dataset' to 'ds' to match proposal
    original_element: Optional[DataElement],  # Matched type hint from proposal
    description: str,  # Free text description (for logging only)
    source_id: str,   # Changed from 'source_identifier' - represents the modifying system
    db_session,  # Added database session for dynamic config
    source_of_previous_values: Optional[str] = None,  # Where original values came from (AE, institution, etc.)
    modified_tag: Optional[BaseTag] = None,  # Required for ADD case when original_element is None
):
    # Use dynamic configuration instead of static settings
    if not get_config_value(db_session, "LOG_ORIGINAL_ATTRIBUTES", True):
        return

    # never copy group-length elements (tag.elem ends in 0x0000)
    # or command-set elements (tag.group is 0x0000)
    if original_element and (
            original_element.tag.element == 0x0000 or original_element.tag.group == 0x0000):
        logger.debug(
            f"Skipping logging of group-length or command-set element: {original_element.tag}")
        return

    # get or create the outer Original Attributes Sequence (0x0400,0x0561)
    # pydicom < 2.1 way:
    if OAS_TAG not in ds:
        # Create a new DataElement for the sequence
        oas_element = DataElement(OAS_TAG, "SQ", Sequence([]))
        ds.add(oas_element)
        oas_sequence = oas_element.value
    else:
        oas_sequence = ds[OAS_TAG].value
        if not isinstance(oas_sequence, Sequence):
            logger.error(
                f"Existing OriginalAttributesSequence {OAS_TAG} is not a Sequence. Cannot append.")
            # Potentially re-initialize or raise error
            oas_element = DataElement(OAS_TAG, "SQ", Sequence([]))
            ds[OAS_TAG] = oas_element  # Replace if not a sequence
            oas_sequence = oas_element.value

    # build one item of Original Attributes Sequence
    item = Dataset()

    # Determine the reason code based on whether original element existed
    if original_element is not None:
        reason_code = "COERCE"  # Replaced an existing value
    else:
        reason_code = "ADD"     # Added a new value that wasn't there before

    # Modified Attributes Sequence (0x0400,0x0550) (exactly one item inside, containing the original element)
    # This sequence holds the attribute(s) as they were before modification.
    mas_item_content = Dataset()
    
    if original_element is not None:  
        # COERCE case: Add the original element as it was before modification
        mas_item_content.add(deepcopy(original_element))
    else:
        # ADD case (original_element is None): Create element with empty value to show it was previously missing
        # According to DICOM spec, Modified Attributes Sequence should contain the tag with empty value
        if modified_tag is None:
            raise ValueError("modified_tag is required when original_element is None (ADD case)")
        
        # Create an empty element for the tag that was added
        vr = dictionary_VR(modified_tag)
        if not vr:
            raise ValueError(f"Cannot determine VR for tag {modified_tag}")
        
        # Create empty value appropriate for the VR
        if vr in ['LO', 'SH', 'PN', 'ST', 'LT', 'UT', 'CS']:
            empty_value = ""
        elif vr in ['IS', 'DS']:
            empty_value = ""
        elif vr in ['DA', 'TM', 'DT']:
            empty_value = ""
        elif vr == 'SQ':
            empty_value = Sequence([])
        else:
            empty_value = ""  # Default to empty string
            
        empty_element = DataElement(modified_tag, vr, empty_value)
        mas_item_content.add(empty_element)
    
    # Create the Modified Attributes Sequence and add the item
    item.add_new(MAS_TAG, "SQ", Sequence([mas_item_content]))

    # audit attributes, added directly to the 'item' of OriginalAttributesSequence
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S.%f")[:26]
    item.add_new(MOD_DT, "DT", now)
    
    # ModifyingSystem (0x0400,0x0563) - The system that made the modification
    item.add_new(MOD_SYS, "LO", source_id[:64])

    # SourceOfPreviousValues (0x0400,0x0564) - Where the original values came from
    # This should be the sending AE, institution, or system that provided the original values
    # If not specified, try to determine from the dataset itself, otherwise use empty string (Type 2)
    prev_values_source = source_of_previous_values
    if not prev_values_source:
        # Try to determine source from DICOM headers
        if hasattr(ds, 'SourceApplicationEntityTitle') and ds.SourceApplicationEntityTitle:
            prev_values_source = str(ds.SourceApplicationEntityTitle)
        elif hasattr(ds, 'InstitutionName') and ds.InstitutionName and original_element and original_element.tag != Tag(0x0008, 0x0080):
            # Use institution name if available and we're not modifying the institution name itself
            prev_values_source = f"@{ds.InstitutionName}"
        else:
            prev_values_source = ""  # Unknown source
    
    item.add_new(SRC_PREV, "LO", prev_values_source[:64])

    # ReasonForTheAttributeModification (0x0400,0x0565) - CS VR with defined terms
    # COERCE = policy-driven replacement of existing value
    # ADD = adding value where none existed before  
    # CORRECT = fixing data entry/operator mistake
    item.add_new(REASON, "CS", reason_code)

    # Append the fully constructed item to the OriginalAttributesSequence
    oas_sequence.append(item)

    # Optional but recommended: Set InstanceCoercionDateTime (0008,0015) in main dataset
    # This indicates when the instance was coerced during storage
    if reason_code == "COERCE" and not hasattr(ds, 'InstanceCoercionDateTime'):
        ds.add_new(Tag(0x0008, 0x0015), "DT", now)

    log_tag_repr = str(
        original_element.tag) if original_element else "N/A (Element new or created)"
    logger.debug(
        f"Logged modification details for tag {log_tag_repr} to OriginalAttributesSequence "
        f"(Reason: {reason_code}, Source: {prev_values_source or 'Unknown'}).")


# ... (rest of the file remains the same) ...
def _apply_crosswalk_modification(dataset: pydicom.Dataset, mod: TagCrosswalkModification, source_identifier: str, db_session) -> bool:  # Added db_session
    """Applies crosswalk modifications. Returns True if dataset was changed."""
    # db_session is now passed in
    map_changed_dataset = False
    logger.debug(
        f"Processing CROSSWALK modification using Map ID: {mod.crosswalk_map_id}")

    crosswalk_map_config: Optional[CrosswalkMap] = crud.crud_crosswalk_map.get(
        db_session, id=mod.crosswalk_map_id)
    if not crosswalk_map_config:
        logger.error(
            f"CrosswalkMap ID {mod.crosswalk_map_id} not found. Skipping.")
        return False
    if not crosswalk_map_config.is_enabled:
        logger.warning(
            f"CrosswalkMap '{crosswalk_map_config.name}' (ID: {mod.crosswalk_map_id}) is disabled. Skipping.")
        return False

    incoming_match_values: Dict[str, Any] = {}
    can_match = True
    if not isinstance(crosswalk_map_config.match_columns, list):
        logger.error(
            f"Invalid match_columns in CrosswalkMap ID {mod.crosswalk_map_id}. Cannot lookup.")
        return False

    for match_map_item in crosswalk_map_config.match_columns:
        if not isinstance(match_map_item, dict):
            logger.error(
                f"Invalid item in match_columns for CrosswalkMap ID {mod.crosswalk_map_id}: {match_map_item}.")
            can_match = False
            break
        dicom_tag_str = match_map_item.get('dicom_tag')
        column_name = match_map_item.get('column_name', dicom_tag_str)
        if not dicom_tag_str or not column_name:
            logger.error(
                f"Missing 'dicom_tag' or 'column_name' in match_columns for Map ID {mod.crosswalk_map_id}: {match_map_item}.")
            can_match = False
            break
        match_tag = parse_dicom_tag(dicom_tag_str)
        if not match_tag:
            logger.error(
                f"Invalid DICOM tag '{dicom_tag_str}' in match_columns for Map ID {mod.crosswalk_map_id}.")
            can_match = False
            break
        data_element = dataset.get(match_tag)
        if data_element is None or data_element.value is None or data_element.value == '':
            logger.debug(
                f"Match tag '{dicom_tag_str}' not found or empty for CrosswalkMap ID {mod.crosswalk_map_id}. Lookup failed.")
            can_match = False
            break
        value_to_match = data_element.value
        if isinstance(value_to_match, MultiValue):
            if len(value_to_match) > 0 and value_to_match[0] is not None:
                value_to_match = value_to_match[0]
            else:
                logger.debug(
                    f"Match tag '{dicom_tag_str}' is multi-value but empty/None. Lookup failed.")
                can_match = False
                break
        incoming_match_values[column_name] = str(value_to_match)

    if not can_match:
        logger.debug(
            f"Crosswalk lookup cannot proceed for Map ID {mod.crosswalk_map_id}.")
        return False

    logger.debug(
        f"Performing crosswalk lookup for Map ID {mod.crosswalk_map_id} with: {incoming_match_values}")
    replacement_data: Optional[Dict[str, Any]] = None
    try:
        replacement_data = crosswalk_service.get_crosswalk_value_sync(
            crosswalk_map_config, incoming_match_values)  # Assumes this doesn't need async
        logger.debug(
            f"Crosswalk lookup for Map ID {mod.crosswalk_map_id} returned: {replacement_data}")
    except Exception as lookup_exc:
        logger.error(
            f"Error during crosswalk lookup for Map ID {mod.crosswalk_map_id}: {lookup_exc}",
            exc_info=True)
        return False

    if replacement_data and isinstance(replacement_data, dict):
        logger.info(
            f"Crosswalk data found for Map ID {mod.crosswalk_map_id}. Applying {len(replacement_data)} replacements.")
        for target_tag_str, replace_info in replacement_data.items():
            target_tag = parse_dicom_tag(target_tag_str)
            if not target_tag:
                logger.warning(
                    f"Invalid target DICOM tag '{target_tag_str}' in replacement data for Map ID {mod.crosswalk_map_id}.")
                continue
            if not isinstance(replace_info, dict):
                logger.warning(
                    f"Invalid replacement info for target '{target_tag_str}' in Map ID {mod.crosswalk_map_id}: {replace_info}.")
                continue

            new_value = replace_info.get("value")
            new_vr: Optional[str] = replace_info.get("vr")
            original_target_element = deepcopy(dataset.get(target_tag, None))
            mod_desc = f"Crosswalk: Set {target_tag_str} via Map {mod.crosswalk_map_id}"
            final_vr = new_vr
            if not final_vr:
                if original_target_element:
                    final_vr = original_target_element.VR
                else:
                    try:
                        final_vr = dictionary_VR(target_tag)
                    except KeyError:
                        final_vr = 'UN'
                        logger.warning(
                            f"Crosswalk: Unknown VR for new tag {target_tag_str}. Defaulting 'UN'.")

            # Basic assignment, further coercion could be added if needed
            processed_value = new_value
            # Add sophisticated type coercion here based on final_vr if required
            # For now, pydicom will handle some basic conversions based on VR.

            try:
                current_value_in_ds = dataset.get(target_tag, None)
                if current_value_in_ds is None or current_value_in_ds.value != processed_value or current_value_in_ds.VR != final_vr:
                    dataset[target_tag] = DataElement(
                        target_tag, final_vr, processed_value)
                    logger.debug(
                        f"Crosswalk: Set tag {target_tag_str} to '{str(processed_value)[:50]}...' VR '{final_vr}'.")
                    _add_original_attribute(
                        dataset, original_target_element, mod_desc, source_identifier, db_session, 
                        modified_tag=target_tag)
                    map_changed_dataset = True
                else:
                    logger.debug(
                        f"Crosswalk: Tag {target_tag_str} already has value '{str(processed_value)[:50]}...' and VR '{final_vr}'. No change made.")
            except Exception as apply_err:
                logger.error(
                    f"Crosswalk: Failed to apply mod for target {target_tag_str}: {apply_err}",
                    exc_info=True)
    elif replacement_data is None:
        logger.info(
            f"Crosswalk lookup found no data for Map ID {mod.crosswalk_map_id} with values: {incoming_match_values}")
    else:
        logger.error(
            f"Crosswalk lookup for Map ID {mod.crosswalk_map_id} bad return type: {type(replacement_data)}.")
    return map_changed_dataset


# Added db_session
def apply_standard_modifications(
        dataset: pydicom.Dataset,
        modifications: List[TagModification],
        source_identifier: str,
        db_session) -> bool:
    """
    Applies a list of standard tag modifications to a dataset.
    Returns True if any modification changed the dataset, False otherwise.
    """
    if not modifications:
        return False

    overall_dataset_changed = False

    STRING_LIKE_VRS = {
        'AE',
        'AS',
        'CS',
        'DA',
        'DS',
        'DT',
        'IS',
        'LO',
        'LT',
        'PN',
        'SH',
        'ST',
        'TM',
        'UI',
        'UR',
        'UT'}
    NUMERIC_VRS = {
        'FL',
        'FD',
        'SL',
        'SS',
        'UL',
        'US',
        'DS',
        'IS',
        'OD',
        'OF',
        'OL',
        'OV',
        'SV',
        'UV'}

    for mod_union in modifications:
        action = mod_union.action
        mod: Any = mod_union
        current_tag_changed_dataset = False
        
        # Initialize these to avoid unbound variable errors in exception handling
        tag_str_repr = "N/A"
        dest_tag_str_repr = "N/A"
        tag = None

        try:
            if action == ModifyActionEnum.CROSSWALK:
                if isinstance(mod, TagCrosswalkModification):
                    # Pass db_session to _apply_crosswalk_modification
                    if _apply_crosswalk_modification(
                            dataset, mod, source_identifier, db_session):
                        overall_dataset_changed = True
                else:
                    logger.error(
                        f"Mismatched type for CROSSWALK: {type(mod)}.")
                continue

            primary_tag_str: Optional[str] = None
            source_tag_str: Optional[str] = None
            destination_tag_str: Optional[str] = None

            if hasattr(mod, 'tag'):
                primary_tag_str = mod.tag
            if hasattr(mod, 'source_tag'):
                source_tag_str = mod.source_tag
            if hasattr(mod, 'destination_tag'):
                destination_tag_str = mod.destination_tag

            log_tag_str = primary_tag_str or source_tag_str
            tag: Optional[BaseTag] = None
            tag_str_repr: str = "N/A"

            if log_tag_str:
                tag = parse_dicom_tag(log_tag_str)
                if not tag:
                    logger.warning(
                        f"Skipping mod: Invalid tag '{log_tag_str}' for {action.value}.")
                    continue
                kw = keyword_for_tag(tag) or '(Unknown Keyword)'
                tag_str_repr = f"{kw} {tag}"
            elif action not in [ModifyActionEnum.CROSSWALK]:
                logger.warning(
                    f"Skipping mod: No tag for action {action.value}.")
                continue

            original_element_for_log: Optional[DataElement] = None
            if tag:
                original_element_for_log = deepcopy(dataset.get(tag, None))

            modification_description = f"Action: {action.value}"

            if action == ModifyActionEnum.DELETE:
                if not isinstance(mod, TagDeleteModification) or not tag:
                    continue
                modification_description = f"Deleted tag {tag_str_repr}"
                if tag in dataset:
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_str_repr}.")
                    _add_original_attribute(
                        dataset,
                        original_element_for_log,
                        modification_description,
                        source_identifier, db_session,
                        modified_tag=tag)
                    current_tag_changed_dataset = True
                else:
                    logger.debug(f"Tag {tag_str_repr} not found for DELETE.")

            elif action == ModifyActionEnum.SET:
                if not isinstance(mod, TagSetModification) or not tag:
                    continue
                new_value = mod.value
                vr_from_mod = mod.vr
                final_vr = vr_from_mod
                modification_description = f"Set {tag_str_repr} to '{str(new_value)[:50]}...' (VR:{vr_from_mod or 'auto'})"

                if not final_vr:
                    if original_element_for_log:
                        final_vr = original_element_for_log.VR
                    else:
                        try:
                            final_vr = dictionary_VR(tag)
                        except KeyError:
                            final_vr = 'UN'
                            logger.warning(
                                f"Set: No VR, cannot guess for new tag {tag_str_repr}. Default 'UN'.")

                processed_value = new_value
                # Basic Type Coercion (can be expanded)
                if new_value is not None and final_vr in NUMERIC_VRS:
                    try:
                        if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'):
                            processed_value = int(new_value)
                        elif final_vr in ('FL', 'FD', 'OD', 'OF'):
                            processed_value = float(new_value)
                        elif final_vr == 'DS':
                            try:
                                Decimal(str(new_value))
                                # Keep as string for DS
                                processed_value = str(new_value)
                            except InvalidOperation:
                                logger.warning(
                                    f"Set: Value '{new_value}' invalid for DS. Using raw str.")
                                processed_value = str(new_value)
                    except (ValueError, TypeError) as conv_err:
                        logger.warning(
                            f"Set: Value '{new_value}' not coercible for VR '{final_vr}': {conv_err}. Using raw.")
                elif new_value is None:  # Allow setting tag to empty
                    processed_value = None

                # Check if value or VR will actually change
                if original_element_for_log is None or \
                   original_element_for_log.value != processed_value or \
                   original_element_for_log.VR != final_vr:
                    dataset[tag] = DataElement(tag, final_vr, processed_value)
                    logger.debug(f"Set tag {tag_str_repr} (VR: {final_vr}).")
                    _add_original_attribute(
                        dataset,
                        original_element_for_log,
                        modification_description,
                        source_identifier, db_session,
                        modified_tag=tag)
                    current_tag_changed_dataset = True
                else:
                    logger.debug(
                        f"Set: Tag {tag_str_repr} value and VR unchanged. No action.")

            elif action in [ModifyActionEnum.PREPEND, ModifyActionEnum.SUFFIX]:
                if not isinstance(
                        mod, (TagPrependModification, TagSuffixModification)) or not tag:
                    continue
                text_to_add = mod.value
                modification_description = f"{action.value} tag {tag_str_repr} with '{text_to_add}'"

                if not original_element_for_log:
                    logger.warning(
                        f"Cannot {action.value}: Tag {tag_str_repr} does not exist.")
                    continue
                current_vr = original_element_for_log.VR
                if current_vr not in STRING_LIKE_VRS:
                    logger.warning(
                        f"Cannot {action.value}: Tag {tag_str_repr} has non-string VR '{current_vr}'.")
                    continue

                current_value = original_element_for_log.value
                modified_value: Any
                changed_during_op = False

                if isinstance(current_value, MultiValue):
                    new_multi_value = []
                    for item_val in current_value:  # Renamed to avoid conflict
                        item_str = str(
                            item_val) if item_val is not None else ""
                        if action == ModifyActionEnum.PREPEND:
                            new_item = text_to_add + item_str
                        else:
                            new_item = item_str + text_to_add
                        new_multi_value.append(new_item)
                        if new_item != item_str:
                            changed_during_op = True
                    modified_value = new_multi_value
                else:
                    original_str = str(
                        current_value) if current_value is not None else ""
                    if action == ModifyActionEnum.PREPEND:
                        modified_value = text_to_add + original_str
                    else:
                        modified_value = original_str + text_to_add
                    if modified_value != original_str:
                        changed_during_op = True

                if changed_during_op:
                    dataset[tag] = DataElement(tag, current_vr, modified_value)
                    _add_original_attribute(
                        dataset,
                        original_element_for_log,
                        modification_description,
                        source_identifier, db_session,
                        modified_tag=tag)
                    current_tag_changed_dataset = True
                    logger.debug(
                        f"Applied {action.value} to tag {tag_str_repr}.")
                else:
                    logger.debug(
                        f"{action.value}: Content of tag {tag_str_repr} unchanged by operation. No action.")

            elif action == ModifyActionEnum.REGEX_REPLACE:
                if not isinstance(mod, TagRegexReplaceModification) or not tag:
                    continue
                pattern = mod.pattern
                replacement = mod.replacement
                modification_description = f"Regex replace {tag_str_repr} pattern '{pattern}' with '{replacement}'"

                if not original_element_for_log:
                    logger.warning(
                        f"Cannot {action.value}: Tag {tag_str_repr} does not exist.")
                    continue
                current_vr = original_element_for_log.VR
                if current_vr not in STRING_LIKE_VRS:
                    logger.warning(
                        f"Cannot {action.value}: Tag {tag_str_repr} non-string VR '{current_vr}'.")
                    continue

                current_value = original_element_for_log.value
                modified_value: Any
                changed_during_op = False
                try:
                    if isinstance(current_value, MultiValue):
                        new_multi_value = []
                        for item_val in current_value:  # Renamed to avoid conflict
                            item_str = str(
                                item_val) if item_val is not None else ""
                            new_item = re.sub(pattern, replacement, item_str)
                            new_multi_value.append(new_item)
                            if new_item != item_str:
                                changed_during_op = True
                        modified_value = new_multi_value
                    else:
                        original_str = str(
                            current_value) if current_value is not None else ""
                        modified_value = re.sub(
                            pattern, replacement, original_str)
                        if modified_value != original_str:
                            changed_during_op = True

                    if changed_during_op:
                        dataset[tag] = DataElement(
                            tag, current_vr, modified_value)
                        _add_original_attribute(
                            dataset,
                            original_element_for_log,
                            modification_description,
                            source_identifier, db_session,
                            modified_tag=tag)
                        current_tag_changed_dataset = True
                        logger.debug(
                            f"Applied {action.value} to tag {tag_str_repr} with pattern '{pattern}'.")
                    else:
                        logger.debug(
                            f"RegexReplace: Content of {tag_str_repr} unchanged. No action.")
                except re.error as regex_err:
                    logger.error(
                        f"Error applying {action.value} for {tag_str_repr}: Invalid pattern '{pattern}': {regex_err}")
                except Exception as e:
                    logger.error(
                        f"Unexpected error {action.value} for {tag_str_repr}: {e}",
                        exc_info=True)

            elif action in [ModifyActionEnum.COPY, ModifyActionEnum.MOVE]:
                if not isinstance(
                        mod, (TagCopyModification, TagMoveModification)):
                    continue
                if not tag or not destination_tag_str:  # tag is source_tag here
                    logger.warning(
                        f"Skipping {action.value}: Invalid source ('{log_tag_str}') or dest ('{destination_tag_str}').")
                    continue
                dest_tag = parse_dicom_tag(destination_tag_str)
                if not dest_tag:
                    logger.warning(
                        f"Skipping {action.value}: Invalid dest tag '{destination_tag_str}'.")
                    continue

                dest_kw = keyword_for_tag(dest_tag) or '(Unknown Keyword)'
                dest_tag_str_repr = f"{dest_kw} {dest_tag}"

                # This is original_element of source_tag (named 'tag')
                if not original_element_for_log:
                    logger.warning(
                        f"Cannot {action.value}: Source tag {tag_str_repr} not found.")
                    continue

                original_dest_element_before_mod: Optional[DataElement] = deepcopy(
                    dataset.get(dest_tag, None))
                # For COPY/MOVE, the modification_description refers to the *destination* tag being changed.
                # The original_element passed to _add_original_attribute should
                # be the original state of the *destination* tag.
                modification_description = f"{action.value} from {tag_str_repr} to {dest_tag_str_repr}"
                # Use source VR if dest VR not specified
                dest_vr = mod.destination_vr or original_element_for_log.VR

                new_value_for_dest = deepcopy(original_element_for_log.value)

                # Check if destination tag will actually change
                if original_dest_element_before_mod is None or \
                   original_dest_element_before_mod.value != new_value_for_dest or \
                   original_dest_element_before_mod.VR != dest_vr:

                    new_element = DataElement(
                        dest_tag, dest_vr, new_value_for_dest)
                    dataset[dest_tag] = new_element
                    logger.debug(
                        f"Tag {tag_str_repr} {action.value.lower()}d to {dest_tag_str_repr} (VR '{dest_vr}').")
                    # Log the change to the destination tag, passing its
                    # original state
                    _add_original_attribute(
                        dataset,
                        original_dest_element_before_mod,
                        modification_description,
                        source_identifier, db_session,
                        modified_tag=dest_tag)
                    current_tag_changed_dataset = True  # The destination tag was changed/created
                else:
                    logger.debug(
                        f"{action.value}: Destination {dest_tag_str_repr} already has same value and VR. No change to destination.")

                if action == ModifyActionEnum.MOVE:
                    # If moving, the source tag is deleted. This is another modification.
                    # original_element_for_log here is the state of the
                    # *source* tag before deletion.
                    if tag in dataset:  # Source tag (named 'tag')
                        move_delete_desc = f"Deleted source {tag_str_repr} (part of MOVE to {dest_tag_str_repr})"
                        # Log the deletion of the source tag, passing its
                        # original state (which is original_element_for_log)
                        _add_original_attribute(
                            dataset,
                            original_element_for_log,
                            move_delete_desc,
                            source_identifier, db_session,
                            modified_tag=tag)
                        del dataset[tag]
                        logger.debug(
                            f"Deleted original source tag {tag_str_repr} after move.")
                        current_tag_changed_dataset = True  # Mark change because source was deleted
                    else:
                        logger.warning(
                            f"MOVE: Source tag {tag_str_repr} unexpectedly not found for deletion after copy.")

            if current_tag_changed_dataset:
                overall_dataset_changed = True

        except Exception as e:
            tag_id_for_error = tag_str_repr if tag_str_repr != "N/A" else (
                getattr(
                    mod,
                    'crosswalk_map_id',
                    'N/A') if action == ModifyActionEnum.CROSSWALK else 'Unknown Target')  # type: ignore[possibly-undefined]
            logger.error(
                f"Failed to apply modification ({action.value}) for target '{tag_id_for_error}': {e}",
                exc_info=True)

    return overall_dataset_changed
