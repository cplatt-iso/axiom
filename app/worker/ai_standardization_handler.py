# app/worker/ai_standardization_handler.py

import structlog
from copy import deepcopy
from typing import Optional, List, Any

import pydicom
from pydicom.tag import BaseTag
from pydicom.datadict import keyword_for_tag, dictionary_VR
from pydicom.dataset import DataElement, Dataset
from pydicom.multival import MultiValue

# Import the SYNC wrapper function AND the initialization flag
from app.services.ai_assist_service import standardize_vocabulary_gemini_sync, VERTEX_AI_INITIALIZED_SUCCESSFULLY
from app.worker.standard_modification_handler import _add_original_attribute
from app.worker.utils.dicom_utils import parse_dicom_tag

logger = structlog.get_logger(__name__)

def apply_ai_standardization_for_rule(
    dataset: Dataset,
    ai_tags_to_standardize: Optional[List[str]],
    source_identifier: str,
    # No portal parameter
) -> bool:
    """
    Applies AI-based vocabulary standardization using the synchronous wrapper.

    Args:
        dataset: The pydicom.Dataset to modify.
        ai_tags_to_standardize: List of DICOM tag strings.
        source_identifier: Identifier for the DICOM instance source.

    Returns:
        True if any AI modification changed the dataset, False otherwise.
    """
    # --- CORRECTED CHECK: Use the imported VERTEX_AI_INITIALIZED_SUCCESSFULLY flag ---
    if not VERTEX_AI_INITIALIZED_SUCCESSFULLY:
        if ai_tags_to_standardize: # Only log if AI was actually requested
            logger.warning("AI Standardization: Vertex AI not successfully initialized (checked via flag). Skipping AI.")
        return False
    # --- END CORRECTED CHECK ---
        
    if not ai_tags_to_standardize:
        logger.debug("AI Standardization: No AI tags specified for standardization in this rule.")
        return False # Nothing to do

    dataset_changed_by_ai = False
    log_context = logger.bind(source_id=source_identifier, num_ai_tags=len(ai_tags_to_standardize))
    log_context.debug("AI Standardization (sync wrapper): Attempting for tags.")

    for tag_str in ai_tags_to_standardize:
        tag: Optional[BaseTag] = parse_dicom_tag(tag_str)
        if not tag:
            log_context.warning("AI Std (sync): Invalid tag in rule, skipping.", tag_string=tag_str)
            continue

        tag_keyword = keyword_for_tag(tag) or f"({tag.group:04X},{tag.element:04X})"
        tag_str_repr = f"{tag_keyword} {tag}"
        current_element: Optional[DataElement] = dataset.get(tag, None)
        
        # Bind tag-specific info for per-tag logging
        tag_log = log_context.bind(current_ai_tag=tag_str_repr)

        if current_element is None or current_element.value is None or current_element.value == '':
            tag_log.debug("AI Std (sync): Tag missing or empty, skipping.")
            continue

        value_to_standardize = current_element.value
        # original_value_for_log = deepcopy(value_to_standardize) # For comparison (used if change occurs)

        if isinstance(value_to_standardize, MultiValue):
            first_valid_value = next((str(item) for item in value_to_standardize if item is not None and str(item).strip()), None)
            if first_valid_value:
                value_to_standardize = first_valid_value
                tag_log.debug("AI Std (sync): Using first non-empty value of MV tag.", selected_value=value_to_standardize)
            else:
                tag_log.debug("AI Std (sync): Tag is MV with no non-empty string values. Skipping.")
                continue
        else:
            value_to_standardize = str(value_to_standardize)

        if not value_to_standardize.strip():
            tag_log.debug("AI Std (sync): Tag value effectively empty after string conversion. Skipping.")
            continue

        context_for_ai = tag_keyword if tag_keyword != f"({tag.group:04X},{tag.element:04X})" else "Unknown DICOM Tag"
        
        tag_log = tag_log.bind(ai_call_context=context_for_ai, ai_call_input_value=value_to_standardize)
        tag_log.debug("AI Std (sync): Calling synchronous AI service wrapper.")
        
        standardized_value: Optional[str] = None
        try:
            standardized_value = standardize_vocabulary_gemini_sync(
                value_to_standardize, 
                context_for_ai
            )
        except Exception as ai_err: 
            tag_log.error("AI Std (sync): Unexpected error during sync AI standardization call.", error_details=str(ai_err), exc_info=True)
            continue # Skip to next tag

        if standardized_value and standardized_value.strip() and standardized_value != value_to_standardize:
            tag_log.info("AI Std (sync): Tag value updated.", original_value=value_to_standardize, new_value=standardized_value)
            
            # Log original attribute before modification
            original_element_for_oa_log = deepcopy(current_element)
            ai_mod_desc = f"AI Standardize Sync: {tag_str_repr} (Context: {context_for_ai})"
            _add_original_attribute(dataset, original_element_for_oa_log, ai_mod_desc, source_identifier)

            final_vr = current_element.VR
            if not final_vr or final_vr == "??": 
                try: final_vr = dictionary_VR(tag)
                except KeyError: final_vr = 'LO'; tag_log.warning("AI Std (sync): Cannot guess VR. Defaulting 'LO'.")
            
            dataset[tag] = DataElement(tag, final_vr, standardized_value)
            dataset_changed_by_ai = True
        elif standardized_value: 
            tag_log.debug("AI Std (sync): Tag value not meaningfully changed by AI.", original_value=value_to_standardize, returned_value=standardized_value)
        else: 
            tag_log.warning("AI Std (sync): AI service (sync wrapper) failed or returned empty.")
            
    return dataset_changed_by_ai
