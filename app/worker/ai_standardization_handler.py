# app/worker/ai_standardization_handler.py

import structlog
from copy import deepcopy
from typing import Optional, List, Any

import pydicom
from pydicom.tag import BaseTag
from pydicom.datadict import keyword_for_tag, dictionary_VR
from pydicom.dataset import Dataset # MODIFIED: Removed DataElement from here
from pydicom.dataelem import DataElement # MODIFIED: Added direct import for DataElement
from pydicom.multival import MultiValue

from sqlalchemy.orm import Session

# Import the SYNC wrapper function AND the initialization flag
from app.services.ai_assist_service import standardize_vocabulary_gemini_sync, VERTEX_AI_INITIALIZED_SUCCESSFULLY
from app import crud # To access crud.crud_ai_prompt_config
from app.schemas.ai_prompt_config import AIPromptConfigRead # For type hinting
from app.worker.standard_modification_handler import _add_original_attribute
from app.worker.utils.dicom_utils import parse_dicom_tag

logger = structlog.get_logger(__name__)

def apply_ai_standardization_for_rule(
    db: Session, # <<< ADDED DB SESSION PARAMETER
    dataset: Dataset,
    # OLD: ai_tags_to_standardize: Optional[List[str]],
    ai_prompt_config_ids: Optional[List[int]], # <<< CHANGED: Now a list of AIPromptConfig IDs
    source_identifier: str,
) -> bool:
    """
    Applies AI-based vocabulary standardization using database-driven prompt configurations.

    Args:
        db: The SQLAlchemy database session.
        dataset: The pydicom.Dataset to modify.
        ai_prompt_config_ids: List of AIPromptConfig IDs from the matched rule.
        source_identifier: Identifier for the DICOM instance source.

    Returns:
        True if any AI modification changed the dataset, False otherwise.
    """
    if not VERTEX_AI_INITIALIZED_SUCCESSFULLY:
        if ai_prompt_config_ids: # Only log if AI was actually requested
            logger.warning("AI Standardization: Vertex AI not successfully initialized. Skipping AI.",
                           source_id=source_identifier,
                           rule_has_ai_configs=True if ai_prompt_config_ids else False)
        return False
        
    if not ai_prompt_config_ids:
        logger.debug("AI Standardization: No AI prompt configuration IDs specified in this rule.",
                       source_id=source_identifier)
        return False # Nothing to do

    dataset_changed_by_ai = False
    # Base log context for this operation
    base_log_context = logger.bind(
        source_id=source_identifier,
        num_ai_prompt_configs_in_rule=len(ai_prompt_config_ids)
    )
    base_log_context.debug("AI Standardization: Attempting for configured AI prompt IDs.")

    # --- ITERATE THROUGH PROMPT CONFIG IDs ---
    for config_id in ai_prompt_config_ids:
        log_ctx_per_config = base_log_context.bind(current_ai_prompt_config_id=config_id)

        # Fetch the AIPromptConfig object from the database
        prompt_config_db_obj = crud.crud_ai_prompt_config.get(db, id=config_id)
        
        if not prompt_config_db_obj:
            log_ctx_per_config.warning("AI Std: AIPromptConfig not found in DB, skipping.",
                                       searched_config_id=config_id)
            continue
        if not prompt_config_db_obj.is_enabled:
            log_ctx_per_config.info("AI Std: AIPromptConfig is disabled, skipping.",
                                        config_name=prompt_config_db_obj.name)
            continue
        # Convert DB model to Pydantic schema for the service (if service expects schema)
        # If service expects DB model, this step isn't needed.
        # Our ai_assist_service.standardize_vocabulary_gemini_sync expects AIPromptConfigRead schema.
        try:
            prompt_config_schema = AIPromptConfigRead.model_validate(prompt_config_db_obj)
        except Exception as e_val: # Catch Pydantic validation error
            log_ctx_per_config.error("AI Std: Failed to validate AIPromptConfig DB object to schema.",
                                     error_details=str(e_val), exc_info=True)
            continue
        
        # The target DICOM tag keyword is now part of the prompt_config_schema
        target_dicom_tag_keyword = prompt_config_schema.dicom_tag_keyword
        log_ctx_per_config = log_ctx_per_config.bind(
            target_dicom_tag_for_ai=target_dicom_tag_keyword,
            prompt_config_name=prompt_config_schema.name
        )

        # Parse the target DICOM tag string (keyword or GGGG,EEEE) from the config
        # The prompt_config_schema.dicom_tag_keyword should ideally be just the keyword.
        # If it can also be "GGGG,EEEE", parse_dicom_tag handles it.
        tag: Optional[BaseTag] = parse_dicom_tag(target_dicom_tag_keyword)
        if not tag:
            log_ctx_per_config.warning("AI Std: Invalid target DICOM tag keyword in AIPromptConfig, skipping.",
                                       invalid_tag_keyword=target_dicom_tag_keyword)
            continue

        # Use keyword_for_tag for consistent representation in logs if tag was GGGG,EEEE
        # If parse_dicom_tag returns a tag object, keyword_for_tag should work or return None.
        # If target_dicom_tag_keyword was already a keyword, this might be redundant but safe.
        display_tag_keyword = keyword_for_tag(tag) or target_dicom_tag_keyword # Fallback to original if keyword_for_tag fails
        tag_str_repr = f"{display_tag_keyword} {tag}" # e.g., "PatientName (0010,0010)"
        log_ctx_per_config = log_ctx_per_config.bind(current_ai_tag_str_repr=tag_str_repr)
        
        current_element: Optional[DataElement] = dataset.get(tag, None)
        
        if current_element is None or current_element.value is None or str(current_element.value).strip() == '':
            log_ctx_per_config.debug("AI Std: Target DICOM tag missing in dataset or has empty value, skipping.")
            continue

        value_to_standardize_raw = current_element.value
        
        # Handle MultiValue (VM > 1) tags: standardize the first non-empty string value
        if isinstance(value_to_standardize_raw, MultiValue):
            first_valid_value_mv = next(
                (str(item) for item in value_to_standardize_raw if item is not None and str(item).strip()), 
                None
            )
            if first_valid_value_mv:
                value_to_standardize_str = first_valid_value_mv
                log_ctx_per_config.debug("AI Std: Using first non-empty value of MultiValue tag for standardization.",
                                         selected_value_for_ai=value_to_standardize_str,
                                         original_mv_value=list(value_to_standardize_raw)) # Log original MV
            else:
                log_ctx_per_config.debug("AI Std: Target DICOM tag is MultiValue with no non-empty string values. Skipping.")
                continue
        else: # Single value
            value_to_standardize_str = str(value_to_standardize_raw)

        if not value_to_standardize_str.strip(): # Final check after potential MV extraction
            log_ctx_per_config.debug("AI Std: Effective value to standardize is empty after processing. Skipping.")
            continue

        # The "context" for AI is now encapsulated within the prompt_config_schema (prompt template, model params, etc.)
        log_ctx_per_config = log_ctx_per_config.bind(ai_call_input_value=value_to_standardize_str)
        log_ctx_per_config.debug("AI Std: Calling synchronous AI service wrapper with DB-driven config.")
        
        standardized_value_from_ai: Optional[str] = None
        try:
            # --- CALL THE UPDATED AI SERVICE ---
            standardized_value_from_ai = standardize_vocabulary_gemini_sync(
                input_value=value_to_standardize_str,
                prompt_config=prompt_config_schema # Pass the full Pydantic schema object
            )
        except Exception as ai_err: 
            log_ctx_per_config.error("AI Std: Unexpected error during sync AI standardization call.",
                                     error_details=str(ai_err), exc_info=True)
            continue # Skip to next prompt config ID

        if standardized_value_from_ai and \
           standardized_value_from_ai.strip() and \
           standardized_value_from_ai != value_to_standardize_str: # Check if meaningfully changed
            
            log_ctx_per_config.info("AI Std: Tag value updated by AI.",
                                    original_value=value_to_standardize_str,
                                    new_value=standardized_value_from_ai)
            
            # Log original attribute before modification
            original_element_for_oa_log = deepcopy(current_element)
            ai_mod_desc = (f"AI Standardize: '{prompt_config_schema.name}' for {tag_str_repr} "
                           f"(ID: {prompt_config_schema.id})")
            _add_original_attribute(dataset, original_element_for_oa_log, ai_mod_desc, source_identifier, db, modified_tag=tag)

            # Determine VR for the new DataElement
            # current_element.VR should be reliable if the element exists.
            # If for some reason it's not, or if creating a new tag (not this case), dictionary_VR is a fallback.
            final_vr = current_element.VR
            if not final_vr or final_vr == "??": # Should be rare if current_element exists
                try:
                    final_vr = dictionary_VR(tag)
                except KeyError: # If tag is private or unknown to pydicom's dict
                    final_vr = 'LO' # Sensible default for string data
                    log_ctx_per_config.warning("AI Std: Cannot determine VR from pydicom dictionary for existing tag. Defaulting to 'LO'.",
                                               tag_group=f"{tag.group:04X}", tag_element=f"{tag.element:04X}")
            
            # Create new DataElement and update the dataset
            # For MultiValue original tags, the AI standardizes ONE value. We are replacing the WHOLE tag
            # with this single standardized value. This is a design choice.
            # If the requirement was to, e.g., standardize only the first item of an MV tag
            # and keep the rest, the logic here would be more complex.
            # Current approach: AI standardizes one value, that one value becomes the new value of the tag.
            dataset[tag] = DataElement(tag, final_vr, standardized_value_from_ai)
            dataset_changed_by_ai = True
        elif standardized_value_from_ai: 
            log_ctx_per_config.debug("AI Std: Tag value not meaningfully changed by AI or AI returned original.",
                                     original_value=value_to_standardize_str,
                                     returned_value_from_ai=standardized_value_from_ai)
        else: 
            # This means AI service returned None or empty string
            log_ctx_per_config.warning("AI Std: AI service (sync wrapper) failed or returned empty/None.")
            
    return dataset_changed_by_ai
