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
from pydicom.dataset import DataElement, Dataset
from pydicom.multival import MultiValue
from pydicom.sequence import Sequence

from app.schemas.rule import (
    ModifyAction as ModifyActionEnum,
    TagModification, TagSetModification, TagDeleteModification,
    TagPrependModification, TagSuffixModification, TagRegexReplaceModification,
    TagCopyModification, TagMoveModification, TagCrosswalkModification
)
from app.core.config import settings
from app import crud # For crosswalk
from app.crosswalk import service as crosswalk_service # For crosswalk
from app.db.models import CrosswalkMap # For crosswalk type hint

from app.worker.utils.dicom_utils import parse_dicom_tag

logger = structlog.get_logger(__name__)

ORIGINAL_ATTRIBUTES_SEQUENCE_TAG = Tag(0x0400, 0x0550)
MODIFIED_ATTRIBUTES_SEQUENCE_TAG = Tag(0x0400, 0x0561)
SOURCE_MODIFICATION_ITEM_TAG = Tag(0x0400,0x0565) # Used in Original Attributes Sequence Item

def _add_original_attribute(dataset: Dataset, original_element: Optional[DataElement], modification_description: str, source_identifier: str):
    """Adds original attribute info to the Original Attributes Sequence."""
    log_enabled = getattr(settings, 'LOG_ORIGINAL_ATTRIBUTES', False)
    if not log_enabled:
        return

    try:
        if ORIGINAL_ATTRIBUTES_SEQUENCE_TAG not in dataset:
            dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG] = DataElement(
                ORIGINAL_ATTRIBUTES_SEQUENCE_TAG, 'SQ', Sequence([])
            )
        elif not isinstance(dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG].value, (Sequence, list)):
             logger.error(f"Existing OriginalAttributesSequence {ORIGINAL_ATTRIBUTES_SEQUENCE_TAG} is not a Sequence.")
             return

        item = Dataset()
        if original_element is not None:
            # If the original element was itself a sequence, we might need special handling
            # For now, assume it's a non-sequence element or a sequence that can be copied.
            # We store the Source Modification Item Sequence (0400,0565) within the Original Attributes Sequence Item.
            # This sequence contains the original DataElement.
            source_mod_item_ds = Dataset()
            source_mod_item_ds.add(deepcopy(original_element)) # Add the original element here

            source_mod_seq = Sequence([source_mod_item_ds])
            item.add_new(SOURCE_MODIFICATION_ITEM_TAG, 'SQ', source_mod_seq)


        # ModifiedAttributesSequence (0400,0561) part
        mod_attr_item = Dataset()
        mod_attr_item.add_new(Tag(0x0040, 0xA73A), "DT", datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S.%f')[:26]) # AttributeModificationDateTime
        mod_attr_item.add_new(Tag(0x0400, 0x0563), "LO", source_identifier[:64]) # ModifyingSystem
        mod_attr_item.add_new(Tag(0x0400, 0x0562), "LO", modification_description[:64]) # ReasonForTheAttributeModification

        item.add_new(MODIFIED_ATTRIBUTES_SEQUENCE_TAG, 'SQ', Sequence([mod_attr_item]))

        dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG].value.append(item)
        log_tag_repr = str(original_element.tag) if original_element else "N/A (Element new or deleted)"
        logger.debug(f"Logged modification details for tag {log_tag_repr} to OriginalAttributesSequence.")

    except Exception as e:
        log_tag_repr = str(original_element.tag) if original_element else "N/A"
        logger.error(f"Failed to add item to OriginalAttributesSequence for tag {log_tag_repr}: {e}", exc_info=True)


def _apply_crosswalk_modification(dataset: pydicom.Dataset, mod: TagCrosswalkModification, source_identifier: str, db_session) -> bool: # Added db_session
    """Applies crosswalk modifications. Returns True if dataset was changed."""
    # db_session is now passed in
    map_changed_dataset = False
    logger.debug(f"Processing CROSSWALK modification using Map ID: {mod.crosswalk_map_id}")

    crosswalk_map_config: Optional[CrosswalkMap] = crud.crud_crosswalk_map.get(db_session, id=mod.crosswalk_map_id)
    if not crosswalk_map_config:
        logger.error(f"CrosswalkMap ID {mod.crosswalk_map_id} not found. Skipping.")
        return False
    if not crosswalk_map_config.is_enabled:
        logger.warning(f"CrosswalkMap '{crosswalk_map_config.name}' (ID: {mod.crosswalk_map_id}) is disabled. Skipping.")
        return False

    incoming_match_values: Dict[str, Any] = {}
    can_match = True
    if not isinstance(crosswalk_map_config.match_columns, list):
         logger.error(f"Invalid match_columns in CrosswalkMap ID {mod.crosswalk_map_id}. Cannot lookup.")
         return False

    for match_map_item in crosswalk_map_config.match_columns:
        if not isinstance(match_map_item, dict):
            logger.error(f"Invalid item in match_columns for CrosswalkMap ID {mod.crosswalk_map_id}: {match_map_item}.")
            can_match = False; break
        dicom_tag_str = match_map_item.get('dicom_tag')
        column_name = match_map_item.get('column_name', dicom_tag_str)
        if not dicom_tag_str or not column_name:
            logger.error(f"Missing 'dicom_tag' or 'column_name' in match_columns for Map ID {mod.crosswalk_map_id}: {match_map_item}.")
            can_match = False; break
        match_tag = parse_dicom_tag(dicom_tag_str)
        if not match_tag:
            logger.error(f"Invalid DICOM tag '{dicom_tag_str}' in match_columns for Map ID {mod.crosswalk_map_id}.")
            can_match = False; break
        data_element = dataset.get(match_tag)
        if data_element is None or data_element.value is None or data_element.value == '':
            logger.debug(f"Match tag '{dicom_tag_str}' not found or empty for CrosswalkMap ID {mod.crosswalk_map_id}. Lookup failed.")
            can_match = False; break
        value_to_match = data_element.value
        if isinstance(value_to_match, MultiValue):
             if len(value_to_match) > 0 and value_to_match[0] is not None:
                 value_to_match = value_to_match[0]
             else:
                 logger.debug(f"Match tag '{dicom_tag_str}' is multi-value but empty/None. Lookup failed.")
                 can_match = False; break
        incoming_match_values[column_name] = str(value_to_match)

    if not can_match:
        logger.debug(f"Crosswalk lookup cannot proceed for Map ID {mod.crosswalk_map_id}.")
        return False

    logger.debug(f"Performing crosswalk lookup for Map ID {mod.crosswalk_map_id} with: {incoming_match_values}")
    replacement_data: Optional[Dict[str, Any]] = None
    try:
        replacement_data = crosswalk_service.get_crosswalk_value_sync(crosswalk_map_config, incoming_match_values) # Assumes this doesn't need async
        logger.debug(f"Crosswalk lookup for Map ID {mod.crosswalk_map_id} returned: {replacement_data}")
    except Exception as lookup_exc:
        logger.error(f"Error during crosswalk lookup for Map ID {mod.crosswalk_map_id}: {lookup_exc}", exc_info=True)
        return False

    if replacement_data and isinstance(replacement_data, dict):
        logger.info(f"Crosswalk data found for Map ID {mod.crosswalk_map_id}. Applying {len(replacement_data)} replacements.")
        for target_tag_str, replace_info in replacement_data.items():
            target_tag = parse_dicom_tag(target_tag_str)
            if not target_tag:
                logger.warning(f"Invalid target DICOM tag '{target_tag_str}' in replacement data for Map ID {mod.crosswalk_map_id}.")
                continue
            if not isinstance(replace_info, dict):
                logger.warning(f"Invalid replacement info for target '{target_tag_str}' in Map ID {mod.crosswalk_map_id}: {replace_info}.")
                continue

            new_value = replace_info.get("value")
            new_vr: Optional[str] = replace_info.get("vr")
            original_target_element = deepcopy(dataset.get(target_tag, None))
            mod_desc = f"Crosswalk: Set {target_tag_str} via Map {mod.crosswalk_map_id}"
            final_vr = new_vr
            if not final_vr:
                if original_target_element: final_vr = original_target_element.VR
                else:
                    try: final_vr = dictionary_VR(target_tag)
                    except KeyError: final_vr = 'UN'; logger.warning(f"Crosswalk: Unknown VR for new tag {target_tag_str}. Defaulting 'UN'.")

            processed_value = new_value # Basic assignment, further coercion could be added if needed
            # Add sophisticated type coercion here based on final_vr if required
            # For now, pydicom will handle some basic conversions based on VR.

            try:
                current_value_in_ds = dataset.get(target_tag, None)
                if current_value_in_ds is None or current_value_in_ds.value != processed_value or current_value_in_ds.VR != final_vr:
                    dataset[target_tag] = DataElement(target_tag, final_vr, processed_value)
                    logger.debug(f"Crosswalk: Set tag {target_tag_str} to '{str(processed_value)[:50]}...' VR '{final_vr}'.")
                    _add_original_attribute(dataset, original_target_element, mod_desc, source_identifier)
                    map_changed_dataset = True
                else:
                    logger.debug(f"Crosswalk: Tag {target_tag_str} already has value '{str(processed_value)[:50]}...' and VR '{final_vr}'. No change made.")
            except Exception as apply_err:
                logger.error(f"Crosswalk: Failed to apply mod for target {target_tag_str}: {apply_err}", exc_info=True)
    elif replacement_data is None:
        logger.info(f"Crosswalk lookup found no data for Map ID {mod.crosswalk_map_id} with values: {incoming_match_values}")
    else:
        logger.error(f"Crosswalk lookup for Map ID {mod.crosswalk_map_id} bad return type: {type(replacement_data)}.")
    return map_changed_dataset


def apply_standard_modifications(dataset: pydicom.Dataset, modifications: List[TagModification], source_identifier: str, db_session) -> bool: # Added db_session
    """
    Applies a list of standard tag modifications to a dataset.
    Returns True if any modification changed the dataset, False otherwise.
    """
    if not modifications:
        return False

    overall_dataset_changed = False
    
    STRING_LIKE_VRS = {'AE', 'AS', 'CS', 'DA', 'DS', 'DT', 'IS', 'LO', 'LT', 'PN', 'SH', 'ST', 'TM', 'UI', 'UR', 'UT'}
    NUMERIC_VRS = {'FL', 'FD', 'SL', 'SS', 'UL', 'US', 'DS', 'IS', 'OD', 'OF', 'OL', 'OV', 'SV', 'UV'}

    for mod_union in modifications:
        action = mod_union.action
        mod: Any = mod_union 
        current_tag_changed_dataset = False

        try:
            if action == ModifyActionEnum.CROSSWALK:
                if isinstance(mod, TagCrosswalkModification):
                    # Pass db_session to _apply_crosswalk_modification
                    if _apply_crosswalk_modification(dataset, mod, source_identifier, db_session):
                        overall_dataset_changed = True
                else:
                    logger.error(f"Mismatched type for CROSSWALK: {type(mod)}.")
                continue 

            primary_tag_str: Optional[str] = None
            source_tag_str: Optional[str] = None
            destination_tag_str: Optional[str] = None

            if hasattr(mod, 'tag'): primary_tag_str = mod.tag
            if hasattr(mod, 'source_tag'): source_tag_str = mod.source_tag
            if hasattr(mod, 'destination_tag'): destination_tag_str = mod.destination_tag

            log_tag_str = primary_tag_str or source_tag_str
            tag: Optional[BaseTag] = None
            tag_str_repr: str = "N/A"

            if log_tag_str:
                tag = parse_dicom_tag(log_tag_str)
                if not tag:
                    logger.warning(f"Skipping mod: Invalid tag '{log_tag_str}' for {action.value}.")
                    continue
                kw = keyword_for_tag(tag) or '(Unknown Keyword)'
                tag_str_repr = f"{kw} {tag}"
            elif action not in [ModifyActionEnum.CROSSWALK]:
                 logger.warning(f"Skipping mod: No tag for action {action.value}.")
                 continue

            original_element_for_log: Optional[DataElement] = None
            if tag: original_element_for_log = deepcopy(dataset.get(tag, None))
            
            modification_description = f"Action: {action.value}"

            if action == ModifyActionEnum.DELETE:
                if not isinstance(mod, TagDeleteModification) or not tag: continue
                modification_description = f"Deleted tag {tag_str_repr}"
                if tag in dataset:
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_str_repr}.")
                    _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                    current_tag_changed_dataset = True
                else:
                    logger.debug(f"Tag {tag_str_repr} not found for DELETE.")

            elif action == ModifyActionEnum.SET:
                if not isinstance(mod, TagSetModification) or not tag: continue
                new_value = mod.value
                vr_from_mod = mod.vr
                final_vr = vr_from_mod
                modification_description = f"Set {tag_str_repr} to '{str(new_value)[:50]}...' (VR:{vr_from_mod or 'auto'})"

                if not final_vr:
                    if original_element_for_log: final_vr = original_element_for_log.VR
                    else:
                        try: final_vr = dictionary_VR(tag)
                        except KeyError: final_vr = 'UN'; logger.warning(f"Set: No VR, cannot guess for new tag {tag_str_repr}. Default 'UN'.")
                
                processed_value = new_value
                # Basic Type Coercion (can be expanded)
                if new_value is not None and final_vr in NUMERIC_VRS:
                    try:
                        if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'): processed_value = int(new_value)
                        elif final_vr in ('FL', 'FD', 'OD', 'OF'): processed_value = float(new_value)
                        elif final_vr == 'DS':
                            try: Decimal(str(new_value)); processed_value = str(new_value) # Keep as string for DS
                            except InvalidOperation: logger.warning(f"Set: Value '{new_value}' invalid for DS. Using raw str."); processed_value = str(new_value)
                    except (ValueError, TypeError) as conv_err:
                        logger.warning(f"Set: Value '{new_value}' not coercible for VR '{final_vr}': {conv_err}. Using raw.")
                elif new_value is None: # Allow setting tag to empty
                    processed_value = None
                
                # Check if value or VR will actually change
                if original_element_for_log is None or \
                   original_element_for_log.value != processed_value or \
                   original_element_for_log.VR != final_vr:
                    dataset[tag] = DataElement(tag, final_vr, processed_value)
                    logger.debug(f"Set tag {tag_str_repr} (VR: {final_vr}).")
                    _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                    current_tag_changed_dataset = True
                else:
                    logger.debug(f"Set: Tag {tag_str_repr} value and VR unchanged. No action.")


            elif action in [ModifyActionEnum.PREPEND, ModifyActionEnum.SUFFIX]:
                if not isinstance(mod, (TagPrependModification, TagSuffixModification)) or not tag: continue
                text_to_add = mod.value
                modification_description = f"{action.value} tag {tag_str_repr} with '{text_to_add}'"

                if not original_element_for_log:
                    logger.warning(f"Cannot {action.value}: Tag {tag_str_repr} does not exist.")
                    continue
                current_vr = original_element_for_log.VR
                if current_vr not in STRING_LIKE_VRS:
                    logger.warning(f"Cannot {action.value}: Tag {tag_str_repr} has non-string VR '{current_vr}'.")
                    continue

                current_value = original_element_for_log.value
                modified_value: Any
                changed_during_op = False

                if isinstance(current_value, MultiValue):
                    new_multi_value = []
                    for item in current_value:
                        item_str = str(item) if item is not None else ""
                        if action == ModifyActionEnum.PREPEND: new_item = text_to_add + item_str
                        else: new_item = item_str + text_to_add
                        new_multi_value.append(new_item)
                        if new_item != item_str: changed_during_op = True
                    modified_value = new_multi_value
                else:
                    original_str = str(current_value) if current_value is not None else ""
                    if action == ModifyActionEnum.PREPEND: modified_value = text_to_add + original_str
                    else: modified_value = original_str + text_to_add
                    if modified_value != original_str: changed_during_op = True
                
                if changed_during_op:
                    dataset[tag] = DataElement(tag, current_vr, modified_value)
                    _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                    current_tag_changed_dataset = True
                    logger.debug(f"Applied {action.value} to tag {tag_str_repr}.")
                else:
                    logger.debug(f"{action.value}: Content of tag {tag_str_repr} unchanged by operation. No action.")


            elif action == ModifyActionEnum.REGEX_REPLACE:
                if not isinstance(mod, TagRegexReplaceModification) or not tag: continue
                pattern = mod.pattern
                replacement = mod.replacement
                modification_description = f"Regex replace {tag_str_repr} pattern '{pattern}' with '{replacement}'"

                if not original_element_for_log:
                    logger.warning(f"Cannot {action.value}: Tag {tag_str_repr} does not exist.")
                    continue
                current_vr = original_element_for_log.VR
                if current_vr not in STRING_LIKE_VRS:
                    logger.warning(f"Cannot {action.value}: Tag {tag_str_repr} non-string VR '{current_vr}'.")
                    continue
                
                current_value = original_element_for_log.value
                modified_value: Any
                changed_during_op = False
                try:
                    if isinstance(current_value, MultiValue):
                        new_multi_value = []
                        for item in current_value:
                            item_str = str(item) if item is not None else ""
                            new_item = re.sub(pattern, replacement, item_str)
                            new_multi_value.append(new_item)
                            if new_item != item_str: changed_during_op = True
                        modified_value = new_multi_value
                    else:
                        original_str = str(current_value) if current_value is not None else ""
                        modified_value = re.sub(pattern, replacement, original_str)
                        if modified_value != original_str: changed_during_op = True
                    
                    if changed_during_op:
                        dataset[tag] = DataElement(tag, current_vr, modified_value)
                        _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                        current_tag_changed_dataset = True
                        logger.debug(f"Applied {action.value} to tag {tag_str_repr} with pattern '{pattern}'.")
                    else:
                        logger.debug(f"RegexReplace: Content of {tag_str_repr} unchanged. No action.")
                except re.error as regex_err:
                    logger.error(f"Error applying {action.value} for {tag_str_repr}: Invalid pattern '{pattern}': {regex_err}")
                except Exception as e:
                    logger.error(f"Unexpected error {action.value} for {tag_str_repr}: {e}", exc_info=True)


            elif action in [ModifyActionEnum.COPY, ModifyActionEnum.MOVE]:
                if not isinstance(mod, (TagCopyModification, TagMoveModification)): continue
                if not tag or not destination_tag_str:
                    logger.warning(f"Skipping {action.value}: Invalid source ('{log_tag_str}') or dest ('{destination_tag_str}').")
                    continue
                dest_tag = parse_dicom_tag(destination_tag_str)
                if not dest_tag:
                    logger.warning(f"Skipping {action.value}: Invalid dest tag '{destination_tag_str}'.")
                    continue
                
                dest_kw = keyword_for_tag(dest_tag) or '(Unknown Keyword)'
                dest_tag_str_repr = f"{dest_kw} {dest_tag}"

                if not original_element_for_log: # This is original_element of source_tag
                    logger.warning(f"Cannot {action.value}: Source tag {tag_str_repr} not found.")
                    continue

                original_dest_element_before_mod: Optional[DataElement] = deepcopy(dataset.get(dest_tag, None))
                modification_description = f"{action.value} tag {tag_str_repr} to {dest_tag_str_repr}"
                dest_vr = mod.destination_vr or original_element_for_log.VR
                
                # Check if destination will change
                new_value_for_dest = deepcopy(original_element_for_log.value)
                if original_dest_element_before_mod is None or \
                   original_dest_element_before_mod.value != new_value_for_dest or \
                   original_dest_element_before_mod.VR != dest_vr:
                    
                    new_element = DataElement(dest_tag, dest_vr, new_value_for_dest)
                    dataset[dest_tag] = new_element
                    logger.debug(f"Tag {tag_str_repr} {action.value.lower()}d to {dest_tag_str_repr} (VR '{dest_vr}').")
                    _add_original_attribute(dataset, original_dest_element_before_mod, modification_description, source_identifier)
                    current_tag_changed_dataset = True # The destination tag was changed/created
                else:
                    logger.debug(f"{action.value}: Destination {dest_tag_str_repr} already has same value and VR. No change to destination.")

                if action == ModifyActionEnum.MOVE:
                    # The source tag deletion is a change itself if it existed
                    if tag in dataset: # Source tag
                        move_delete_desc = f"Deleted source {tag_str_repr} for MOVE to {dest_tag_str_repr}"
                        _add_original_attribute(dataset, original_element_for_log, move_delete_desc, source_identifier)
                        del dataset[tag] # original_element_for_log is the source tag's original state
                        logger.debug(f"Deleted original source tag {tag_str_repr} after move.")
                        # current_tag_changed_dataset = True # This is also a change
                        overall_dataset_changed = True # Explicitly mark overall change due to deletion
                    else: # Should not happen if original_element_for_log existed
                         logger.warning(f"MOVE: Source tag {tag_str_repr} unexpectedly not found for deletion after copy.")
            
            if current_tag_changed_dataset:
                overall_dataset_changed = True

        except Exception as e:
            tag_id_for_error = tag_str_repr if tag_str_repr != "N/A" else (getattr(mod, 'crosswalk_map_id', 'N/A') if action == ModifyActionEnum.CROSSWALK else 'Unknown Target')
            logger.error(f"Failed to apply modification ({action.value}) for target '{tag_id_for_error}': {e}", exc_info=True)
            
    return overall_dataset_changed
