# app/worker/processing_logic.py

import logging
import re
import json
from copy import deepcopy
from typing import Optional, Any, List, Dict, Tuple, Union, Literal
from datetime import date, time as dt_time, datetime, timezone, time
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
    TagCopyModification, TagMoveModification, TagCrosswalkModification
)
from app.db.models import RuleSet, RuleSetExecutionMode, Rule, CrosswalkMap, Schedule
from app.db.models.storage_backend_config import (
    StorageBackendConfig, FileSystemBackendConfig, CStoreBackendConfig,
    GcsBackendConfig, GoogleHealthcareBackendConfig, StowRsBackendConfig
)
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


def is_schedule_active(schedule: Optional[Schedule], current_time: datetime) -> bool:
    if schedule is None or not schedule.is_enabled:
        return True

    if not isinstance(schedule.time_ranges, list):
        logger.warning(f"Schedule ID {schedule.id} ('{schedule.name}') has invalid time_ranges. Assuming inactive.")
        return False

    if not schedule.time_ranges:
        logger.warning(f"Schedule ID {schedule.id} ('{schedule.name}') has no time ranges. Assuming inactive.")
        return False

    current_day_str = current_time.strftime("%a")
    current_time_obj = current_time.time()

    logger.debug(f"Checking schedule '{schedule.name}' ID: {schedule.id}. Current: {current_time.isoformat()}")

    for time_range in schedule.time_ranges:
        try:
            days = time_range.get("days")
            start_time_str = time_range.get("start_time")
            end_time_str = time_range.get("end_time")

            if not isinstance(days, list) or not start_time_str or not end_time_str:
                logger.warning(f"Schedule {schedule.id}: Skipping invalid time range: {time_range}")
                continue

            if current_day_str not in days:
                continue

            start_time = time.fromisoformat(start_time_str)
            end_time = time.fromisoformat(end_time_str)

            if start_time < end_time:
                if start_time <= current_time_obj < end_time:
                    logger.info(f"Schedule '{schedule.name}' ACTIVE: Time {current_time_obj} within {start_time_str}-{end_time_str} on {current_day_str}.")
                    return True
            else:
                if current_time_obj >= start_time or current_time_obj < end_time:
                    logger.info(f"Schedule '{schedule.name}' ACTIVE (overnight): Time {current_time_obj} within {start_time_str}-{end_time_str} on {current_day_str}.")
                    return True

        except (ValueError, TypeError) as e:
            logger.error(f"Schedule {schedule.id}: Error parsing time range {time_range}: {e}", exc_info=True)
            continue

    logger.debug(f"Schedule '{schedule.name}' INACTIVE: No matching range found.")
    return False


def parse_dicom_tag(tag_str: str) -> Optional[BaseTag]:
    if not isinstance(tag_str, str):
        logger.warning(f"Invalid type for tag string: {type(tag_str)}")
        return None
    tag_str = tag_str.strip()
    match_ge = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", tag_str)
    if match_ge:
        try:
            group1 = int(match_ge.group(1), 16)
            group2 = int(match_ge.group(2), 16)
            return Tag(group1, group2)
        except ValueError: pass
    if re.match(r"^[a-zA-Z0-9]+$", tag_str) and not repeater_has_keyword(tag_str):
        try:
            tag_val = tag_for_keyword(tag_str)
            if tag_val: return Tag(tag_val)
        except (ValueError, TypeError): pass
    logger.warning(f"Could not parse tag string/keyword: '{tag_str}'")
    return None


def _values_equal(actual_val: Any, expected_val: Any) -> bool:
    try:
        if isinstance(actual_val, (int, float, Decimal, DSfloat, IS, DSdecimal)) and isinstance(expected_val, (int, float, str, Decimal)):
            try:
                return Decimal(str(actual_val)) == Decimal(str(expected_val))
            except (ValueError, TypeError): pass
    except (ValueError, TypeError): pass
    try:
        if isinstance(actual_val, (date, datetime, dt_time)):
             return str(actual_val) == str(expected_val)
    except Exception as e: logger.debug(f"Could not compare temporally: {actual_val} vs {expected_val}. Error: {e}.")
    return str(actual_val) == str(expected_val)


def _compare_numeric(actual_val: Any, expected_val: Any, op: MatchOperation) -> bool:
     try:
         actual_decimal = Decimal(str(actual_val))
         expected_decimal = Decimal(str(expected_val))
         if op == MatchOperation.GREATER_THAN: return actual_decimal > expected_decimal
         if op == MatchOperation.LESS_THAN: return actual_decimal < expected_decimal
         if op == MatchOperation.GREATER_EQUAL: return actual_decimal >= expected_decimal
         if op == MatchOperation.LESS_EQUAL: return actual_decimal <= expected_decimal
         return False
     except (ValueError, TypeError, Exception):
         logger.debug(f"Cannot compare non-numeric: '{actual_val}', '{expected_val}'")
         return False


def check_match(dataset: pydicom.Dataset, criteria: List[MatchCriterion]) -> bool:
    if not criteria: return True
    for criterion in criteria:
        if not isinstance(criterion, MatchCriterion): logger.warning(f"Skipping invalid criterion: {criterion}"); continue
        tag_str = criterion.tag; op = criterion.op; expected_value = criterion.value
        tag = parse_dicom_tag(tag_str)
        if not tag: logger.warning(f"Skipping invalid tag key '{tag_str}'"); return False
        actual_data_element = dataset.get(tag, None)
        if op == MatchOperation.EXISTS:
            if actual_data_element is None: logger.debug(f"Match fail: Tag {tag_str} DNE"); return False
            else: continue
        elif op == MatchOperation.NOT_EXISTS:
            if actual_data_element is not None: logger.debug(f"Match fail: Tag {tag_str} exists"); return False
            else: continue
        if actual_data_element is None: logger.debug(f"Match fail: Tag {tag_str} DNE for op {op.value}"); return False
        actual_value = actual_data_element.value
        is_multi_value = isinstance(actual_value, MultiValue)
        actual_value_list = list(actual_value) if is_multi_value else [actual_value]
        if not actual_value_list and actual_value is not None: actual_value_list = [actual_value]
        elif not actual_value_list and actual_value is None: pass
        match = False
        try:
            if op == MatchOperation.EQUALS: match = any(_values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.NOT_EQUALS: match = all(not _values_equal(av, expected_value) for av in actual_value_list)
            elif op == MatchOperation.CONTAINS:
                if not isinstance(expected_value, str): logger.warning(f"'contains' needs string value for {tag_str}."); return False
                match = any(expected_value in str(av) for av in actual_value_list)
            elif op == MatchOperation.STARTS_WITH:
                if not isinstance(expected_value, str): logger.warning(f"'startswith' needs string value for {tag_str}."); return False
                match = any(str(av).startswith(expected_value) for av in actual_value_list)
            elif op == MatchOperation.ENDS_WITH:
                if not isinstance(expected_value, str): logger.warning(f"'endswith' needs string value for {tag_str}."); return False
                match = any(str(av).endswith(expected_value) for av in actual_value_list)
            elif op == MatchOperation.REGEX:
                 if not isinstance(expected_value, str): logger.warning(f"'regex' needs string value for {tag_str}."); return False
                 match = any(bool(re.search(expected_value, str(av))) for av in actual_value_list)
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 match = any(_compare_numeric(av, expected_value, op) for av in actual_value_list)
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list): logger.warning(f"'in' needs list value for {tag_str}."); return False
                 match = any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list): logger.warning(f"'not_in' needs list value for {tag_str}."); return False
                 match = not any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op in [MatchOperation.IP_ADDRESS_EQUALS, MatchOperation.IP_ADDRESS_STARTS_WITH, MatchOperation.IP_ADDRESS_IN_SUBNET]:
                 logger.warning(f"IP op '{op.value}' used incorrectly on DICOM tag '{tag_str}'."); return False
            if not match:
                 actual_repr = repr(actual_value_list[0]) if len(actual_value_list) == 1 else f"{len(actual_value_list)} values"
                 logger.debug(f"Match fail: Tag {tag_str} ('{actual_repr}') failed op '{op.value}' with value '{expected_value}'")
                 return False
        except Exception as e: logger.error(f"Error matching tag {tag_str} with op {op.value}: {e}", exc_info=True); return False
    return True


def check_association_match(assoc_info: Optional[Dict[str, str]], criteria: Optional[List[AssociationMatchCriterion]]) -> bool:
    if not criteria: return True
    if not assoc_info: logger.debug("Assoc criteria exist, but no assoc info provided."); return False
    logger.debug(f"Checking association criteria against: {assoc_info}")
    for criterion in criteria:
        if not isinstance(criterion, AssociationMatchCriterion): logger.warning(f"Skipping invalid assoc criterion: {criterion}"); continue
        param = criterion.parameter; op = criterion.op; expected_value = criterion.value
        actual_value: Optional[str] = None
        if param == "CALLING_AE_TITLE": actual_value = assoc_info.get('calling_ae_title')
        elif param == "CALLED_AE_TITLE": actual_value = assoc_info.get('called_ae_title')
        elif param == "SOURCE_IP": actual_value = assoc_info.get('source_ip')
        if actual_value is None: logger.debug(f"Assoc match fail: Param '{param}' not found."); return False
        match = False
        try:
            if op == MatchOperation.EQUALS: match = _values_equal(actual_value, expected_value)
            elif op == MatchOperation.NOT_EQUALS: match = not _values_equal(actual_value, expected_value)
            elif op == MatchOperation.CONTAINS: match = str(expected_value) in str(actual_value)
            elif op == MatchOperation.STARTS_WITH: match = str(actual_value).startswith(str(expected_value))
            elif op == MatchOperation.ENDS_WITH: match = str(actual_value).endswith(str(expected_value))
            elif op == MatchOperation.REGEX:
                 if not isinstance(expected_value, str): logger.warning(f"'regex' needs string value for assoc param '{param}'."); return False
                 match = bool(re.search(expected_value, str(actual_value)))
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list): logger.warning(f"'in' needs list value for assoc param '{param}'."); return False
                 match = any(_values_equal(actual_value, ev) for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list): logger.warning(f"'not_in' needs list value for assoc param '{param}'."); return False
                 match = not any(_values_equal(actual_value, ev) for ev in expected_value)
            elif op == MatchOperation.IP_ADDRESS_EQUALS: logger.warning("IP_EQ matching NYI."); match = False # TODO
            elif op == MatchOperation.IP_ADDRESS_STARTS_WITH: logger.warning("IP_STARTSWITH matching NYI."); match = False # TODO
            elif op == MatchOperation.IP_ADDRESS_IN_SUBNET: logger.warning("IP_IN_SUBNET matching NYI."); match = False # TODO
            elif op in [MatchOperation.EXISTS, MatchOperation.NOT_EXISTS, MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 logger.warning(f"Unsupported operator '{op.value}' for Assoc criteria on '{param}'."); return False
            else: logger.warning(f"Unknown operator '{op.value}' for Assoc criteria."); return False
            if not match: logger.debug(f"Assoc match fail: Param '{param}' ('{actual_value}') failed op '{op.value}' with '{expected_value}'"); return False
        except Exception as e: logger.error(f"Error assoc matching for param {param} op {op.value}: {e}", exc_info=True); return False
    logger.debug("All association criteria matched.")
    return True


def _add_original_attribute(dataset: Dataset, original_element: Optional[DataElement], modification_description: str, source_identifier: str):
    log_enabled = getattr(settings, 'LOG_ORIGINAL_ATTRIBUTES', False)
    if not log_enabled or original_element is None: return
    try:
        if ORIGINAL_ATTRIBUTES_SEQUENCE_TAG not in dataset:
            new_sequence = Sequence([])
            dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG] = DataElement(ORIGINAL_ATTRIBUTES_SEQUENCE_TAG, 'SQ', new_sequence)
            logger.debug(f"Created OriginalAttributesSequence for {source_identifier}.")
        else:
            seq_element = dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG]
            if not isinstance(seq_element, DataElement) or seq_element.VR != 'SQ' or not isinstance(seq_element.value, (Sequence, list)):
                 logger.error(f"Existing OriginalAttributesSequence tag {ORIGINAL_ATTRIBUTES_SEQUENCE_TAG} invalid for {source_identifier}. Cannot append.")
                 return
        item = Dataset()
        item.add(deepcopy(original_element))
        item.ModifiedAttributesSequence = Sequence([])
        mod_item = Dataset()
        mod_item.AttributeModificationDateTime = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S.%f')
        mod_item.ModifyingSystem = source_identifier[:64]
        mod_item.ReasonForTheAttributeModification = modification_description[:64]
        item.ModifiedAttributesSequence.append(mod_item)
        dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG].value.append(item)
        logger.debug(f"Appended modification details for tag {original_element.tag} to OriginalAttributesSequence.")
    except Exception as e:
        logger.error(f"Failed to add item to OriginalAttributesSequence for tag {original_element.tag}: {e}", exc_info=True)


def apply_modifications(dataset: pydicom.Dataset, modifications: List[TagModification], source_identifier: str):
    if not modifications: return
    STRING_LIKE_VRS = {'AE', 'AS', 'CS', 'DA', 'DS', 'DT', 'IS', 'LO', 'LT', 'PN', 'SH', 'ST', 'TM', 'UI', 'UR', 'UT'}
    for mod_union in modifications:
        action = mod_union.action; mod: Any = mod_union
        db_session = None
        if action == ModifyActionEnum.CROSSWALK:
            from app.db.session import SessionLocal
            db_session = SessionLocal()
        try:
            primary_tag_str: Optional[str] = None; source_tag_str: Optional[str] = None
            if hasattr(mod, 'tag'): primary_tag_str = mod.tag
            elif hasattr(mod, 'source_tag'): primary_tag_str = mod.source_tag; source_tag_str = mod.source_tag
            tag: Optional[BaseTag] = None; tag_str: str = "N/A"
            if primary_tag_str:
                tag = parse_dicom_tag(primary_tag_str)
                if not tag: logger.warning(f"Skipping modification: Invalid tag '{primary_tag_str}' in action {action.value}"); continue
                else: tag_str = f"({tag.group:04X},{tag.element:04X})"
            original_element_for_log: Optional[DataElement] = None
            if tag: original_element_for_log = deepcopy(dataset.get(tag, None))
            modification_description = f"Action {action.value}"
            if action == ModifyActionEnum.CROSSWALK:
                logger.debug(f"Processing CROSSWALK using Map ID: {mod.crosswalk_map_id}")
                if not db_session: logger.error("DB session needed for crosswalk."); continue
                crosswalk_map_config = crud.crud_crosswalk_map.get(db_session, id=mod.crosswalk_map_id)
                if not crosswalk_map_config: logger.error(f"CrosswalkMap ID {mod.crosswalk_map_id} not found."); continue
                if not crosswalk_map_config.is_enabled: logger.warning(f"CrosswalkMap '{crosswalk_map_config.name}' disabled."); continue
                incoming_match_values: Dict[str, Any] = {}; can_match = True
                for match_map in crosswalk_map_config.match_columns:
                    dicom_tag_str = match_map.get('dicom_tag'); col_name = match_map.get('column_name')
                    if not dicom_tag_str: logger.error(f"Invalid match_columns config in Map ID {mod.crosswalk_map_id}: missing 'dicom_tag'."); can_match = False; break
                    match_tag = parse_dicom_tag(dicom_tag_str)
                    if not match_tag: logger.error(f"Invalid DICOM tag '{dicom_tag_str}' in match_columns for Map ID {mod.crosswalk_map_id}."); can_match = False; break
                    data_element = dataset.get(match_tag)
                    if data_element is None or data_element.value is None: logger.debug(f"Match tag '{dicom_tag_str}' (Col: {col_name}) not found/empty for Map ID {mod.crosswalk_map_id}. Cannot crosswalk."); can_match = False; break
                    value_to_match = data_element.value
                    if isinstance(value_to_match, MultiValue):
                         if len(value_to_match) > 0: value_to_match = value_to_match[0]; logger.warning(f"Using first value of multi-value tag {dicom_tag_str} for crosswalk matching.")
                         else: logger.debug(f"Match tag '{dicom_tag_str}' multi-value but empty."); can_match = False; break
                    incoming_match_values[dicom_tag_str] = value_to_match
                if not can_match: continue
                logger.debug(f"Performing crosswalk lookup for Map ID {mod.crosswalk_map_id} with values: {incoming_match_values}")
                replacement_data: Optional[Dict[str, Any]] = None
                try:
                    replacement_data = crosswalk_service.get_crosswalk_value_sync(crosswalk_map_config, incoming_match_values)
                    logger.debug(f"Crosswalk lookup result: {replacement_data}")
                except Exception as lookup_exc: logger.error(f"Error during crosswalk lookup for Map ID {mod.crosswalk_map_id}: {lookup_exc}", exc_info=True)
                if replacement_data and isinstance(replacement_data, dict):
                    logger.info(f"Crosswalk data found for Map ID {mod.crosswalk_map_id}. Applying replacements.")
                    for target_tag_str, replace_info in replacement_data.items():
                        target_tag = parse_dicom_tag(target_tag_str)
                        if not target_tag: logger.warning(f"Invalid target tag '{target_tag_str}' in replacement data."); continue
                        new_value = replace_info.get("value"); new_vr = replace_info.get("vr")
                        original_target_element = deepcopy(dataset.get(target_tag, None))
                        mod_desc = f"Crosswalk: Set {target_tag_str} via Map {mod.crosswalk_map_id}"
                        final_vr = new_vr
                        if not final_vr:
                            if original_target_element: final_vr = original_target_element.VR
                            else:
                                try: final_vr = dictionary_VR(target_tag)
                                except KeyError: final_vr = 'UN'; logger.warning(f"Crosswalk: Cannot determine VR for {target_tag_str}. Defaulting to UN.")
                        processed_value = new_value
                        if new_value is not None:
                            try:
                                if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'): processed_value = [int(v) for v in new_value] if isinstance(new_value, list) else int(new_value)
                                elif final_vr in ('FL', 'FD', 'OD', 'OF'): processed_value = [float(v) for v in new_value] if isinstance(new_value, list) else float(new_value)
                                elif final_vr == 'DS': processed_value = [str(v) for v in new_value] if isinstance(new_value, list) else str(new_value)
                            except (ValueError, TypeError) as conv_err: logger.warning(f"Crosswalk: Value '{new_value}' for {target_tag_str} not coercible for VR '{final_vr}': {conv_err}."); processed_value = new_value
                        else: processed_value = None; logger.debug(f"Crosswalk: Setting {target_tag_str} to empty value.")
                        dataset[target_tag] = DataElement(target_tag, final_vr, processed_value)
                        logger.debug(f"Crosswalk: Set tag {target_tag_str} ({target_tag}) (VR: {final_vr})")
                        _add_original_attribute(dataset, original_target_element, mod_desc, source_identifier)
                else: logger.info(f"Crosswalk lookup did not return data for Map ID {mod.crosswalk_map_id} using values: {incoming_match_values}")
                continue
            if tag is None and action not in [ModifyActionEnum.COPY, ModifyActionEnum.MOVE]: logger.warning(f"Skipping {action.value}: Missing/invalid target tag."); continue
            if action == ModifyActionEnum.DELETE:
                modification_description = f"Deleted tag {tag_str}"
                if tag in dataset: del dataset[tag]; logger.debug(f"Deleted tag {tag_str} ({tag})"); _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                else: logger.debug(f"Tag {tag_str} ({tag}) not found for deletion.")
            elif action == ModifyActionEnum.SET:
                 if not isinstance(mod, TagSetModification): continue
                 new_value = mod.value; vr = mod.vr; final_vr = vr
                 modification_description = f"Set tag {tag_str} to '{new_value}' (VR:{vr or 'auto'})"
                 if not final_vr:
                      if original_element_for_log: final_vr = original_element_for_log.VR
                      else:
                           try: final_vr = dictionary_VR(tag)
                           except KeyError: final_vr = 'UN'; logger.warning(f"Cannot determine VR for new tag {tag_str} in SET. Defaulting to UN.")
                 processed_value = new_value
                 if new_value is not None:
                     try:
                          if final_vr in ('IS', 'SL', 'SS', 'UL', 'US'): processed_value = [int(v) for v in new_value] if isinstance(new_value, list) else int(new_value)
                          elif final_vr in ('FL', 'FD', 'OD', 'OF'): processed_value = [float(v) for v in new_value] if isinstance(new_value, list) else float(new_value)
                          elif final_vr == 'DS': processed_value = [str(v) for v in new_value] if isinstance(new_value, list) else str(new_value)
                     except (ValueError, TypeError) as conv_err: logger.warning(f"Value '{new_value}' for {tag_str} not coercible for VR '{final_vr}': {conv_err}."); processed_value = new_value
                 else: processed_value = None
                 dataset[tag] = DataElement(tag, final_vr, processed_value)
                 logger.debug(f"Set tag {tag_str} ({tag}) (VR: {final_vr})")
                 _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
            elif action in [ModifyActionEnum.PREPEND, ModifyActionEnum.SUFFIX]:
                 if not isinstance(mod, (TagPrependModification, TagSuffixModification)): continue
                 modification_description = f"{action.value} tag {tag_str} with '{mod.value}'"
                 if not original_element_for_log: logger.warning(f"Cannot {action.value}: Tag {tag_str} DNE."); continue
                 current_vr = original_element_for_log.VR
                 if current_vr not in STRING_LIKE_VRS: logger.warning(f"Cannot {action.value}: Tag {tag_str} non-string VR '{current_vr}'."); continue
                 text_to_add = mod.value; current_value = original_element_for_log.value
                 if isinstance(current_value, MultiValue):
                      modified_list = [(text_to_add + str(item) if action == ModifyActionEnum.PREPEND else str(item) + text_to_add) for item in current_value]
                      dataset[tag].value = modified_list; logger.debug(f"Applied {action.value} to multivalue tag {tag_str}.")
                 else:
                      original_str = str(current_value) if current_value is not None else ""
                      modified_value = text_to_add + original_str if action == ModifyActionEnum.PREPEND else original_str + text_to_add
                      dataset[tag].value = modified_value; logger.debug(f"Applied {action.value} to tag {tag_str}.")
                 _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
            elif action == ModifyActionEnum.REGEX_REPLACE:
                 if not isinstance(mod, TagRegexReplaceModification): continue
                 modification_description = f"Regex replace {tag_str} using '{mod.pattern}' -> '{mod.replacement}'"
                 if not original_element_for_log: logger.warning(f"Cannot {action.value}: Tag {tag_str} DNE."); continue
                 current_vr = original_element_for_log.VR
                 if current_vr not in STRING_LIKE_VRS: logger.warning(f"Cannot {action.value}: Tag {tag_str} non-string VR '{current_vr}'."); continue
                 pattern = mod.pattern; replacement = mod.replacement
                 try:
                      current_value = original_element_for_log.value
                      if isinstance(current_value, MultiValue):
                           modified_list = [re.sub(pattern, replacement, str(item)) for item in current_value]
                           dataset[tag].value = modified_list; logger.debug(f"Applied {action.value} to multivalue tag {tag_str} pattern '{pattern}'.")
                      else:
                           original_str = str(current_value) if current_value is not None else ""
                           modified_value = re.sub(pattern, replacement, original_str)
                           dataset[tag].value = modified_value; logger.debug(f"Applied {action.value} to tag {tag_str} pattern '{pattern}'.")
                      _add_original_attribute(dataset, original_element_for_log, modification_description, source_identifier)
                 except re.error as regex_err: logger.error(f"Error applying {action.value} regex for tag {tag_str}: {regex_err}", exc_info=True)
            elif action in [ModifyActionEnum.COPY, ModifyActionEnum.MOVE]:
                if not isinstance(mod, (TagCopyModification, TagMoveModification)): continue
                source_tag = tag # Already parsed
                dest_tag_str = getattr(mod, 'destination_tag', None)
                dest_tag = parse_dicom_tag(dest_tag_str) if dest_tag_str else None
                if not dest_tag: logger.warning(f"Skipping {action.value}: Invalid destination tag '{dest_tag_str}'."); continue
                if original_element_for_log is None: logger.warning(f"Cannot {action.value}: Source tag {tag_str} not found."); continue
                original_dest_element_before_mod: Optional[DataElement] = deepcopy(dataset.get(dest_tag, None))
                modification_description = f"{action.value} tag {tag_str} to {dest_tag_str}"
                dest_vr = mod.destination_vr or original_element_for_log.VR
                new_element = DataElement(dest_tag, dest_vr, deepcopy(original_element_for_log.value))
                dataset[dest_tag] = new_element
                logger.debug(f"Tag {tag_str} {action.value}d to {dest_tag_str} VR '{dest_vr}'")
                _add_original_attribute(dataset, original_dest_element_before_mod, modification_description, source_identifier)
                if action == ModifyActionEnum.MOVE:
                    if source_tag in dataset:
                        _add_original_attribute(dataset, original_element_for_log, f"Deleted tag {tag_str} as part of move", source_identifier)
                        del dataset[source_tag]
                        logger.debug(f"Deleted original source tag {tag_str} after move.")
        except Exception as e:
            tag_id_str = tag_str if tag_str != "N/A" else (getattr(mod, 'crosswalk_map_id', 'N/A') if action == ModifyActionEnum.CROSSWALK else 'Unknown')
            logger.error(f"Failed to apply modification ({action.value}) for target {tag_id_str}: {e}", exc_info=True)
        finally:
            if db_session: db_session.close()


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
    any_rule_matched_and_triggered = False
    modifications_applied = False
    current_time_utc = datetime.now(timezone.utc)

    for ruleset in active_rulesets:
        logger.debug(f"Evaluating RuleSet '{ruleset.name}' (ID: {ruleset.id}, Priority: {ruleset.priority})")
        rules_to_evaluate = getattr(ruleset, 'rules', [])
        if not rules_to_evaluate:
            logger.debug(f"RuleSet '{ruleset.name}' has no rules.")
            continue

        matched_rule_in_this_set = False
        for rule in rules_to_evaluate:
            is_rule_statically_active = rule.is_active
            is_schedule_currently_active = True
            rule_schedule = getattr(rule, 'schedule', None)

            if rule.schedule_id and rule_schedule:
                 is_schedule_currently_active = is_schedule_active(rule_schedule, current_time_utc)
            elif rule.schedule_id and not rule_schedule:
                 logger.error(f"Rule '{rule.name}' (ID: {rule.id}) has schedule_id {rule.schedule_id} but schedule data not loaded. Assuming inactive.")
                 is_schedule_currently_active = False

            is_rule_currently_triggerable = is_rule_statically_active and is_schedule_currently_active

            if not is_rule_currently_triggerable:
                logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) is NOT triggerable. (is_active: {is_rule_statically_active}, schedule_active: {is_schedule_currently_active}). Skipping.")
                continue

            rule_sources = rule.applicable_sources
            source_match = not rule_sources or (isinstance(rule_sources, list) and source_identifier in rule_sources)
            if not source_match: logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) does not apply to source '{source_identifier}'. Skipping."); continue

            logger.debug(f"Checking rule '{rule.name}' (ID: {rule.id}, Priority: {rule.priority}) - Rule is ACTIVE and Schedule allows.")

            try:
                match_criteria_validated: List[MatchCriterion] = []
                if rule.match_criteria and isinstance(rule.match_criteria, (list, dict)):
                    try:
                        criteria_data = rule.match_criteria
                        if isinstance(criteria_data, str): criteria_data = json.loads(criteria_data)
                        match_criteria_validated = TypeAdapter(List[MatchCriterion]).validate_python(criteria_data)
                    except Exception as val_err:
                        logger.error(f"Rule '{rule.name}' (ID: {rule.id}): Invalid 'match_criteria': {val_err}. Skipping rule.")
                        continue
                elif rule.match_criteria:
                     logger.warning(f"Rule '{rule.name}' (ID: {rule.id}): 'match_criteria' type ({type(rule.match_criteria)}). Skipping.")
                     continue

                assoc_criteria_validated: List[AssociationMatchCriterion] = []
                if rule.association_criteria and isinstance(rule.association_criteria, (list, dict)):
                    try:
                        assoc_data = rule.association_criteria
                        if isinstance(assoc_data, str): assoc_data = json.loads(assoc_data)
                        assoc_criteria_validated = TypeAdapter(List[AssociationMatchCriterion]).validate_python(assoc_data)
                    except Exception as val_err:
                        logger.error(f"Rule '{rule.name}' (ID: {rule.id}): Invalid 'association_criteria': {val_err}. Skipping rule.")
                        continue
                elif rule.association_criteria:
                     logger.warning(f"Rule '{rule.name}' (ID: {rule.id}): 'association_criteria' type ({type(rule.association_criteria)}). Skipping.")
                     continue

                modifications_validated: List[TagModification] = []
                if rule.tag_modifications and isinstance(rule.tag_modifications, (list, dict)):
                    try:
                        mods_data = rule.tag_modifications
                        if isinstance(mods_data, str): mods_data = json.loads(mods_data)
                        tag_mod_adapter = TypeAdapter(List[TagModification])
                        modifications_validated = tag_mod_adapter.validate_python(mods_data)
                    except Exception as val_err:
                        logger.error(f"Rule '{rule.name}' (ID: {rule.id}): Invalid 'tag_modifications': {val_err}. Skipping mods.")
                        modifications_validated = []
                elif rule.tag_modifications:
                     logger.warning(f"Rule '{rule.name}' (ID: {rule.id}): 'tag_modifications' type ({type(rule.tag_modifications)}). Skipping.")

                tag_match = check_match(original_ds, match_criteria_validated)
                assoc_match = check_association_match(association_info, assoc_criteria_validated)

                if tag_match and assoc_match:
                    logger.info(f"Rule '{ruleset.name}' / '{rule.name}' MATCHED & ACTIVE (Tags: {tag_match}, Assoc: {assoc_match}).")
                    any_rule_matched_and_triggered = True
                    matched_rule_in_this_set = True

                    if modifications_validated:
                        if modified_ds is None: modified_ds = deepcopy(original_ds); logger.debug("Created deep copy.")
                        if modified_ds is not None:
                           logger.debug(f"Applying {len(modifications_validated)} modifications for rule '{rule.name}'...")
                           apply_modifications(modified_ds, modifications_validated, source_identifier)
                           modifications_applied = True
                    else: logger.debug(f"Rule '{rule.name}' matched but no valid mods.")

                    rule_destinations: List[StorageBackendConfig] = getattr(rule, 'destinations', [])
                    for dest_config_obj in rule_destinations:
                         if dest_config_obj.is_enabled:
                              try:
                                  backend_type = dest_config_obj.backend_type
                                  backend_config_dict = {"type": backend_type, "name": dest_config_obj.name}

                                  if isinstance(dest_config_obj, FileSystemBackendConfig):
                                      if hasattr(dest_config_obj, 'path'): backend_config_dict["path"] = dest_config_obj.path
                                      else: raise AttributeError("'FileSystemBackendConfig' missing 'path'")
                                  elif isinstance(dest_config_obj, CStoreBackendConfig):
                                      cstore_keys = ["remote_ae_title", "host", "remote_port", "local_ae_title", "tls_enabled", "tls_ca_cert_secret_name", "tls_client_cert_secret_name", "tls_client_key_secret_name"]
                                      for key in cstore_keys:
                                          if hasattr(dest_config_obj, key): backend_config_dict[key] = getattr(dest_config_obj, key)
                                      if not all(k in backend_config_dict for k in ["host", "remote_port", "remote_ae_title"]):
                                          raise AttributeError("'CStoreBackendConfig' missing required host/port/ae_title")
                                  elif isinstance(dest_config_obj, GcsBackendConfig):
                                      gcs_keys = ["bucket", "prefix"]
                                      for key in gcs_keys:
                                           if hasattr(dest_config_obj, key): backend_config_dict[key] = getattr(dest_config_obj, key)
                                      if "bucket" not in backend_config_dict: raise AttributeError("'GcsBackendConfig' missing 'bucket'")
                                  elif isinstance(dest_config_obj, GoogleHealthcareBackendConfig):
                                      ghc_keys = ["gcp_project_id", "gcp_location", "gcp_dataset_id", "gcp_dicom_store_id"]
                                      for key in ghc_keys:
                                           if hasattr(dest_config_obj, key): backend_config_dict[key] = getattr(dest_config_obj, key)
                                      if not all(key in backend_config_dict for key in ghc_keys): raise AttributeError("'GoogleHealthcareBackendConfig' missing required fields")
                                  elif isinstance(dest_config_obj, StowRsBackendConfig):
                                       stow_keys = ["base_url"]
                                       for key in stow_keys:
                                            if hasattr(dest_config_obj, key): backend_config_dict[key] = getattr(dest_config_obj, key)
                                       if "base_url" not in backend_config_dict: raise AttributeError("'StowRsBackendConfig' missing 'base_url'")
                                  else:
                                       logger.error(f"Unknown backend type '{type(dest_config_obj).__name__}' for {dest_config_obj.name}")
                                       continue

                                  destinations_to_process.append(backend_config_dict)
                                  logger.debug(f"Added destination '{dest_config_obj.name}' (Type: {backend_type}) from rule '{rule.name}'. Config: {backend_config_dict}")

                              except AttributeError as attr_err:
                                   logger.error(f"Error processing destination '{dest_config_obj.name}' ID: {dest_config_obj.id}, Type: {getattr(dest_config_obj, 'backend_type', 'Unknown')}. Attr Error: {attr_err}.", exc_info=True)
                                   continue
                              except Exception as dest_err:
                                   logger.error(f"Unexpected error processing destination '{dest_config_obj.name}': {dest_err}", exc_info=True)
                                   continue
                         else:
                              logger.debug(f"Skipping disabled destination '{dest_config_obj.name}' from rule '{rule.name}'.")

                    applied_rules_info.append(f"{ruleset.name}/{rule.name} (ID:{rule.id})")

                    if ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                        logger.debug(f"FIRST_MATCH mode triggered for RuleSet ID: {ruleset.id}.")
                        break

                else:
                    logger.debug(f"Rule '{rule.name}' did not match (Tags: {tag_match}, Assoc: {assoc_match}).")

            except Exception as rule_proc_exc:
                logger.error(f"Error processing rule '{ruleset.name}/{rule.name}': {rule_proc_exc}", exc_info=True)

        if matched_rule_in_this_set and ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
            logger.debug(f"Breaking outer loop due to FIRST_MATCH in RuleSet ID: {ruleset.id}")
            break

    if not any_rule_matched_and_triggered:
        logger.info(f"No rules matched for source '{source_identifier}'.")
        return None, [], []

    unique_dest_strings = set()
    unique_destination_configs = []
    for dest_config_dict in destinations_to_process:
        try: frozen_config = frozenset(sorted(dest_config_dict.items())); unique_key = frozen_config
        except TypeError as e: logger.warning(f"Unhashable dest config: {dest_config_dict}. Error: {e}. Using string repr."); unique_key = json.dumps(dest_config_dict, sort_keys=True)
        if unique_key not in unique_dest_strings:
            unique_dest_strings.add(unique_key)
            unique_destination_configs.append(dest_config_dict)
    logger.debug(f"Found {len(unique_destination_configs)} unique destinations.")

    final_ds_to_return = modified_ds if modifications_applied else original_ds
    return final_ds_to_return, applied_rules_info, unique_destination_configs
