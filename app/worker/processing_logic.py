# app/worker/processing_logic.py

import logging
import re
import json
from copy import deepcopy
from typing import Optional, Any, List, Dict, Tuple, Union
from datetime import date, time as dt_time, datetime, timezone
from decimal import Decimal
from pydantic import TypeAdapter

import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.tag import Tag, BaseTag
from pydicom.datadict import dictionary_VR, tag_for_keyword, repeater_has_keyword
from pydicom.valuerep import DSfloat, IS, DSdecimal, VR
from pydicom.dataset import DataElement, Dataset
from pydicom.multival import MultiValue
from pydicom.sequence import Sequence

from app.schemas.rule import (
    MatchOperation, ModifyAction, MatchCriterion, AssociationMatchCriterion,
    TagModification, TagSetModification, TagDeleteModification,
    TagPrependModification, TagSuffixModification, TagRegexReplaceModification,
    TagCopyModification, TagMoveModification
)

from app.db.models.rule import RuleSet, RuleSetExecutionMode, Rule
from app.db.models.storage_backend_config import StorageBackendConfig
from app.services.storage_backends import StorageBackendError, get_storage_backend
from app.core.config import settings

logger = logging.getLogger(__name__)

ORIGINAL_ATTRIBUTES_SEQUENCE_TAG = Tag(0x0400, 0x0550)
SOURCE_AE_TITLE_TAG = Tag(0x0002, 0x0016)
SOURCE_APPLICATION_ENTITY_TITLE_TAG = Tag(0x0002, 0x0016)
CALLED_AE_TITLE_TAG = Tag(0x0000, 0x0000) # Placeholder, need actual tag if required
CALLING_AE_TITLE_TAG = Tag(0x0000, 0x0000) # Placeholder, need actual tag if required
SOURCE_IP = Tag(0x0000, 0x0000) # Placeholder, need actual tag if required

def parse_dicom_tag(tag_str: str) -> Optional[BaseTag]:
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
                 if not isinstance(expected_value, str): logger.warning(f"'regex' requires string value, got {type(expected_value)}. Skipping {tag_str}."); continue
                 match = any(bool(re.search(expected_value, str(av))) for av in actual_value_list)
            elif op in [MatchOperation.GREATER_THAN, MatchOperation.LESS_THAN, MatchOperation.GREATER_EQUAL, MatchOperation.LESS_EQUAL]:
                 match = any(_compare_numeric(av, expected_value, op) for av in actual_value_list)
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list): logger.warning(f"'in' requires list value, got {type(expected_value)}. Skipping {tag_str}."); continue
                 match = any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list): logger.warning(f"'not_in' requires list value, got {type(expected_value)}. Skipping {tag_str}."); continue
                 match = not any(_values_equal(av, ev) for av in actual_value_list for ev in expected_value)
            elif op in [MatchOperation.IP_EQ, MatchOperation.IP_STARTSWITH, MatchOperation.IP_IN_SUBNET]:
                logger.warning(f"IP matching operation '{op.value}' is not yet implemented for tag criteria. Rule will not match."); return False

            if not match:
                 actual_repr = repr(actual_value_list[0]) if len(actual_value_list) == 1 else f"{len(actual_value_list)} values"
                 logger.debug(f"Match fail: Tag {tag_str} ('{actual_repr}') failed op '{op.value}' with value '{expected_value}'")
                 return False
        except Exception as e:
            logger.error(f"Error during matching for tag {tag_str} with op {op.value}: {e}", exc_info=True)
            return False
    return True


def _add_original_attribute(dataset: Dataset, original_element: Optional[DataElement], modification_description: str, source_identifier: str):
    """Adds or appends to the Original Attributes Sequence."""
    # --- Check settings attribute safely ---
    log_enabled = getattr(settings, 'LOG_ORIGINAL_ATTRIBUTES', False)
    if not log_enabled or original_element is None:
    # --- End Check ---
        return

    try:
        if ORIGINAL_ATTRIBUTES_SEQUENCE_TAG not in dataset:
            new_sequence = Sequence([])
            dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG] = DataElement(
                ORIGINAL_ATTRIBUTES_SEQUENCE_TAG, 'SQ', new_sequence
            )
            logger.debug(f"Created OriginalAttributesSequence for {source_identifier}.")
        else:
            if not isinstance(dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG].value, Sequence):
                 logger.error("Existing OriginalAttributesSequence (0x0400,0x0550) is not a valid Sequence. Cannot append modification details.")
                 return

        item = Dataset()
        item.add(deepcopy(original_element))
        item.ModifiedAttributesSequence = Sequence([Dataset()])
        item.ModifiedAttributesSequence[0].AttributeModificationDateTime = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S.%f')
        item.ModifiedAttributesSequence[0].ModifyingSystem = source_identifier[:64]
        item.ModifiedAttributesSequence[0].ReasonForTheAttributeModification = modification_description[:64]

        dataset[ORIGINAL_ATTRIBUTES_SEQUENCE_TAG].value.append(item)
        logger.debug(f"Appended modification details for tag {original_element.tag} to OriginalAttributesSequence.")

    except Exception as e:
        logger.error(f"Failed to add item to OriginalAttributesSequence for tag {original_element.tag}: {e}", exc_info=True)


def apply_modifications(dataset: pydicom.Dataset, modifications: List[TagModification], source_identifier: str):
    """
    Applies tag modifications to the dataset IN-PLACE based on a list of actions
    defined by TagModification schema objects (discriminated union).
    Logs changes to Original Attributes Sequence if enabled.
    """
    if not modifications:
        return

    STRING_LIKE_VRS = {'AE', 'AS', 'CS', 'DA', 'DS', 'DT', 'IS', 'LO', 'LT', 'PN', 'SH', 'ST', 'TM', 'UI', 'UR', 'UT'}

    for mod in modifications:
        tag_str = getattr(mod, 'tag', None)
        source_tag_str = getattr(mod, 'source_tag', None)
        dest_tag_str = getattr(mod, 'destination_tag', None)
        action = mod.action

        primary_tag_str = tag_str if tag_str is not None else source_tag_str
        if primary_tag_str is None:
            logger.warning(f"Skipping modification: Missing target/source tag for action {action.value}: {mod.model_dump()}")
            continue

        tag = parse_dicom_tag(primary_tag_str)
        if not tag:
            logger.warning(f"Skipping invalid tag key '{primary_tag_str}' in modification: {mod.model_dump()}")
            continue

        source_tag = parse_dicom_tag(source_tag_str) if source_tag_str else None
        dest_tag = parse_dicom_tag(dest_tag_str) if dest_tag_str else None

        try:
            original_element_before_mod: Optional[DataElement] = deepcopy(dataset.get(tag, None))
            modification_description = ""

            if action == ModifyAction.DELETE:
                if tag in dataset:
                    modification_description = f"Deleted tag {tag}"
                    del dataset[tag]
                    logger.debug(f"Deleted tag {tag_str} ({tag})")
                    _add_original_attribute(dataset, original_element_before_mod, modification_description, source_identifier)
                else:
                    logger.debug(f"Tag {tag_str} ({tag}) not found for deletion.")
            elif action == ModifyAction.SET:
                 if not isinstance(mod, TagSetModification): continue
                 new_value = mod.value; vr = mod.vr; final_vr = vr
                 modification_description = f"Set tag {tag} to '{new_value}' (VR:{vr or 'auto'})"
                 if not final_vr:
                      if original_element_before_mod: final_vr = original_element_before_mod.VR; logger.debug(f"Using existing VR '{final_vr}' for SET on tag {tag_str} ({tag}).")
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
                 _add_original_attribute(dataset, original_element_before_mod, modification_description, source_identifier)
            elif action in [ModifyAction.PREPEND, ModifyAction.SUFFIX]:
                 if not isinstance(mod, (TagPrependModification, TagSuffixModification)): continue
                 modification_description = f"{action.value} tag {tag} with '{mod.value}'"
                 if not original_element_before_mod: logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) does not exist."); continue
                 current_vr = original_element_before_mod.VR
                 if current_vr not in STRING_LIKE_VRS: logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) has non-string-like VR '{current_vr}'."); continue
                 text_to_add = mod.value
                 current_value = original_element_before_mod.value
                 if isinstance(current_value, MultiValue):
                      modified_list = [(text_to_add + str(item) if action == ModifyAction.PREPEND else str(item) + text_to_add) for item in current_value]
                      dataset[tag].value = modified_list
                      logger.debug(f"Applied {action.value} to all {len(modified_list)} items of multivalue tag {tag_str} ({tag}).")
                 else:
                      original_str = str(current_value)
                      modified_value = text_to_add + original_str if action == ModifyAction.PREPEND else original_str + text_to_add
                      dataset[tag].value = modified_value
                      logger.debug(f"Applied {action.value} to tag {tag_str} ({tag}). New value: '{modified_value}'")
                 _add_original_attribute(dataset, original_element_before_mod, modification_description, source_identifier)
            elif action == ModifyAction.REGEX_REPLACE:
                 if not isinstance(mod, TagRegexReplaceModification): continue
                 modification_description = f"Regex replace tag {tag} using '{mod.pattern}' -> '{mod.replacement}'"
                 if not original_element_before_mod: logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) does not exist."); continue
                 current_vr = original_element_before_mod.VR
                 if current_vr not in STRING_LIKE_VRS: logger.warning(f"Cannot {action.value}: Tag {tag_str} ({tag}) has non-string-like VR '{current_vr}'."); continue
                 pattern = mod.pattern; replacement = mod.replacement
                 try:
                      current_value = original_element_before_mod.value
                      if isinstance(current_value, MultiValue):
                           modified_list = [re.sub(pattern, replacement, str(item)) for item in current_value]
                           dataset[tag].value = modified_list
                           logger.debug(f"Applied {action.value} to multivalue tag {tag_str} ({tag}) using pattern '{pattern}'.")
                      else:
                           original_str = str(current_value)
                           modified_value = re.sub(pattern, replacement, original_str)
                           dataset[tag].value = modified_value
                           logger.debug(f"Applied {action.value} to tag {tag_str} ({tag}) using pattern '{pattern}'. New value: '{modified_value}'")
                      _add_original_attribute(dataset, original_element_before_mod, modification_description, source_identifier)
                 except re.error as regex_err: logger.error(f"Error applying {action.value} regex for tag {tag_str} ({tag}): {regex_err}", exc_info=True)

            elif action in [ModifyAction.COPY, ModifyAction.MOVE]:
                if not isinstance(mod, (TagCopyModification, TagMoveModification)): continue
                if not source_tag or not dest_tag: logger.warning(f"Skipping {action.value}: Invalid source ({source_tag}) or destination ({dest_tag}) tag."); continue

                source_element = dataset.get(source_tag, None)
                if source_element is None:
                    logger.warning(f"Cannot {action.value}: Source tag {source_tag_str} ({source_tag}) not found in dataset.")
                    continue

                original_dest_element_before_mod: Optional[DataElement] = deepcopy(dataset.get(dest_tag, None))
                modification_description = f"{action.value} tag {source_tag} to {dest_tag}"

                dest_vr = mod.destination_vr or source_element.VR
                new_element = DataElement(dest_tag, dest_vr, deepcopy(source_element.value))
                dataset[dest_tag] = new_element
                logger.debug(f"Tag {source_tag_str} ({source_tag}) {action.value}d to {dest_tag_str} ({dest_tag}) with VR '{dest_vr}'")

                _add_original_attribute(dataset, original_dest_element_before_mod, modification_description, source_identifier)

                if action == ModifyAction.MOVE:
                    if source_tag in dataset:
                        original_source_element_before_delete: Optional[DataElement] = deepcopy(dataset.get(source_tag, None))
                        del dataset[source_tag]
                        logger.debug(f"Deleted original source tag {source_tag_str} ({source_tag}) after move.")
                        _add_original_attribute(dataset, original_source_element_before_delete, f"Deleted tag {source_tag} as part of move", source_identifier)

        except Exception as e:
            logger.error(f"Failed to apply modification ({action.value}) for tag {primary_tag_str} ({tag}): {e}", exc_info=True)


def check_association_match(assoc_info: Optional[Dict[str, str]], criteria: Optional[List[AssociationMatchCriterion]]) -> bool:
    if not criteria: return True
    if not assoc_info: return True

    logger.debug(f"Checking association criteria against: {assoc_info}")

    for criterion in criteria:
        if not isinstance(criterion, AssociationMatchCriterion):
             logger.warning(f"Skipping invalid association criterion object: {criterion}")
             continue

        param = criterion.parameter
        op = criterion.op
        expected_value = criterion.value

        actual_value: Optional[str] = None
        if param == "CALLING_AE_TITLE": actual_value = assoc_info.get('calling_ae_title')
        elif param == "CALLED_AE_TITLE": actual_value = assoc_info.get('called_ae_title')
        elif param == "SOURCE_IP": actual_value = assoc_info.get('source_ip')

        if actual_value is None:
             logger.debug(f"Assoc match fail: Parameter '{param}' not found in association info. Required for op '{op.value}'")
             return False

        match = False
        try:
            if op == MatchOperation.EQUALS: match = _values_equal(actual_value, expected_value)
            elif op == MatchOperation.NOT_EQUALS: match = not _values_equal(actual_value, expected_value)
            elif op == MatchOperation.CONTAINS: match = str(expected_value) in str(actual_value)
            elif op == MatchOperation.STARTS_WITH: match = str(actual_value).startswith(str(expected_value))
            elif op == MatchOperation.ENDS_WITH: match = str(actual_value).endswith(str(expected_value))
            elif op == MatchOperation.REGEX:
                 if not isinstance(expected_value, str): logger.warning(f"'regex' requires string value for assoc. param '{param}', got {type(expected_value)}."); continue
                 match = bool(re.search(expected_value, str(actual_value)))
            elif op == MatchOperation.IN:
                 if not isinstance(expected_value, list): logger.warning(f"'in' requires list value for assoc. param '{param}', got {type(expected_value)}."); continue
                 match = any(_values_equal(actual_value, ev) for ev in expected_value)
            elif op == MatchOperation.NOT_IN:
                 if not isinstance(expected_value, list): logger.warning(f"'not_in' requires list value for assoc. param '{param}', got {type(expected_value)}."); continue
                 match = not any(_values_equal(actual_value, ev) for ev in expected_value)
            elif op == MatchOperation.IP_EQ:
                 logger.warning("IP_EQ matching not implemented yet.")
                 match = False
            elif op == MatchOperation.IP_STARTSWITH:
                 logger.warning("IP_STARTSWITH matching not implemented yet.")
                 match = False
            elif op == MatchOperation.IP_IN_SUBNET:
                 logger.warning("IP_IN_SUBNET matching not implemented yet.")
                 match = False
            else:
                 logger.warning(f"Unsupported association operator '{op.value}'. Rule will not match.")
                 return False

            if not match:
                 logger.debug(f"Assoc match fail: Parameter '{param}' ('{actual_value}') failed op '{op.value}' with value '{expected_value}'")
                 return False
        except Exception as e:
            logger.error(f"Error during association matching for parameter {param} with op {op.value}: {e}", exc_info=True)
            return False

    logger.debug("All association criteria matched.")
    return True


def process_dicom_instance(
    original_ds: pydicom.Dataset,
    active_rulesets: List[RuleSet],
    source_identifier: str,
    association_info: Optional[Dict[str, str]] = None
) -> Tuple[Optional[pydicom.Dataset], List[str], List[Dict[str, Any]]]:

    logger.debug(f"Processing instance from source '{source_identifier}' against {len(active_rulesets)} rulesets.")
    modified_ds: Optional[pydicom.Dataset] = None
    applied_rules_info = []
    destinations_to_process: List[Dict[str, Any]] = []
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
             # --- Enhanced Source Matching Debug ---
             logger.debug(f"Rule '{rule.name}' applicable_sources: {rule_sources} (type: {type(rule_sources)}). Comparing with source_identifier: '{source_identifier}' (type: {type(source_identifier)})")
             if rule_sources and isinstance(rule_sources, list) and source_identifier not in rule_sources:
                  logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) does not apply to source '{source_identifier}'. Skipping.")
                  continue
             elif rule_sources is None or len(rule_sources) == 0:
                  logger.debug(f"Rule '{rule.name}' applicable_sources is null or empty, applies to all including '{source_identifier}'.")
             else:
                  # This case should only be reached if rule_sources exists and source_identifier IS in it
                  logger.debug(f"Rule '{rule.name}' applicable_sources ({rule_sources}) includes source '{source_identifier}'. Proceeding.")
             # --- End Enhanced Debug ---

             logger.debug(f"Checking rule '{rule.name}' (ID: {rule.id}, Priority: {rule.priority}) against source '{source_identifier}'")
             try:
                 match_criteria_schemas = [MatchCriterion(**crit) for crit in rule.match_criteria or []]
                 assoc_criteria_schemas = [AssociationMatchCriterion(**crit) for crit in rule.association_criteria or []]

                 tag_match = check_match(original_ds, match_criteria_schemas)
                 assoc_match = check_association_match(association_info, assoc_criteria_schemas)

                 if tag_match and assoc_match:
                     logger.info(f"Rule '{ruleset.name}' / '{rule.name}' MATCHED (Tags: {tag_match}, Assoc: {assoc_match}).")
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
                              apply_modifications(modified_ds, mod_schemas, source_identifier)
                              modifications_applied = True
                          else:
                              logger.debug(f"Rule '{rule.name}' matched but had no valid modifications defined.")

                     rule_destinations: List[StorageBackendConfig] = rule.destinations or []
                     for dest_config_obj in rule_destinations:
                          if dest_config_obj.is_enabled:
                                backend_config_dict = dest_config_obj.config or {}
                                backend_config_dict['type'] = dest_config_obj.backend_type
                                destinations_to_process.append(backend_config_dict)
                                logger.debug(f"Added destination '{dest_config_obj.name}' (Type: {dest_config_obj.backend_type}) from rule '{rule.name}'.")
                          else:
                               logger.debug(f"Skipping disabled destination '{dest_config_obj.name}' from rule '{rule.name}'.")

                     applied_rules_info.append(f"{ruleset.name}/{rule.name} (ID:{rule.id})")

                     if ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                         logger.debug(f"RuleSet '{ruleset.name}' mode is FIRST_MATCH. Stopping rule evaluation for this set.")
                         break
                 else:
                     logger.debug(f"Rule '{rule.name}' did not match (Tags: {tag_match}, Assoc: {assoc_match}).")

             except Exception as rule_proc_exc:
                 logger.error(f"Error processing rule '{ruleset.name}/{rule.name}': {rule_proc_exc}", exc_info=True)

        if matched_rule_in_this_set and ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
            logger.debug(f"FIRST_MATCH rule found in set '{ruleset.name}'. Stopping ruleset evaluation.")
            break

    if not any_rule_matched:
        logger.info(f"No applicable rules matched for source '{source_identifier}' and assoc info {association_info}. No modifications or destinations applied.")
        return None, [], []

    unique_dest_strings = set()
    unique_destination_configs = []
    for dest_config_dict in destinations_to_process:
        try:
            json_string = json.dumps(dest_config_dict, sort_keys=True, separators=(',', ':'))
            if json_string not in unique_dest_strings:
                unique_dest_strings.add(json_string)
                unique_destination_configs.append(dest_config_dict)
        except TypeError as e:
             logger.warning(f"Could not serialize destination config for de-duplication: {dest_config_dict}. Error: {e}. Skipping this instance.")

    logger.debug(f"Found {len(unique_destination_configs)} unique destinations after processing rules.")

    return modified_ds if modifications_applied else None, applied_rules_info, unique_destination_configs
