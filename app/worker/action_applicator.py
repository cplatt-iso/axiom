# app/worker/action_applicator.py

import structlog
from typing import Optional, List, TYPE_CHECKING

from pydicom.dataset import Dataset # For type hinting
from pydantic import TypeAdapter # For validating rule components

from app.db.models import Rule # For type hinting
from app.schemas.rule import TagModification, AssociationMatchCriterion # For validation (though Assoc not used here)
from app.worker.standard_modification_handler import apply_standard_modifications
# Import the AI handler function (which now uses the sync wrapper)
from app.worker.ai_standardization_handler import apply_ai_standardization_for_rule
# No need to import ANYIO_AVAILABLE or ai_assist_service here for checks

if TYPE_CHECKING:
    from sqlalchemy.orm import Session # For db_session type hint

logger = structlog.get_logger(__name__)

def apply_actions_for_rule(
    dataset: Dataset,
    rule: Rule,
    source_identifier: str,
    db_session: 'Session', # Keep db_session for crosswalk AND NOW FOR AI HANDLER
) -> bool:
    """
    Applies all defined actions (standard modifications, AI standardization) for a matched rule.
    Uses the synchronous AI standardization handler which now requires db_session.

    Args:
        dataset: The pydicom.Dataset to modify (should be a deepcopy).
        rule: The matched Rule object.
        source_identifier: Identifier for the DICOM instance source.
        db_session: The active SQLAlchemy session for database operations (e.g., crosswalk, AI config fetching).

    Returns:
        True if any action resulted in a modification to the dataset, False otherwise.
    """
    overall_dataset_changed_by_this_rule = False
    rule_name_for_log = f"'{ruleset_name_for_log(rule)}/{rule.name}' (ID: {rule.id})" # Helper for logs

    # --- 1. Validate Rule Components for Actions ---
    try:
        modifications_validated = TypeAdapter(List[TagModification]).validate_python(rule.tag_modifications or [])
        # --- CHANGED: Validate rule.ai_prompt_config_ids (List[int]) instead of rule.ai_standardization_tags (List[str]) ---
        ai_prompt_config_ids_validated = TypeAdapter(Optional[List[int]]).validate_python(rule.ai_prompt_config_ids or None)
    except Exception as val_err:
        logger.error(f"Rule {rule_name_for_log} action components validation error. Skipping actions.",
                     error_details=str(val_err), exc_info=True)
        return False

    # --- 2. Apply Standard Tag Modifications ---
    if modifications_validated:
        logger.debug(f"Applying {len(modifications_validated)} standard tag modification(s) for rule {rule_name_for_log}.")
        if apply_standard_modifications(dataset, modifications_validated, source_identifier, db_session):
            overall_dataset_changed_by_this_rule = True
            logger.debug(f"Standard modifications resulted in changes for rule {rule_name_for_log}.")
        else:
            logger.debug(f"Standard modifications did not result in changes for rule {rule_name_for_log}.")
    else:
        logger.debug(f"Rule {rule_name_for_log} has no standard tag modifications defined.")

    # --- 3. Apply AI Tag Standardization (using sync handler) ---
    # --- CHANGED: Condition now uses ai_prompt_config_ids_validated ---
    if ai_prompt_config_ids_validated:
        logger.debug(f"Attempting AI standardization for rule {rule_name_for_log} using {len(ai_prompt_config_ids_validated)} prompt config ID(s).",
                     prompt_config_ids=ai_prompt_config_ids_validated) # Log the IDs

        # --- CHANGED: Call apply_ai_standardization_for_rule with db_session and validated IDs ---
        if apply_ai_standardization_for_rule(
            db=db_session, # Pass the db_session
            dataset=dataset,
            ai_prompt_config_ids=ai_prompt_config_ids_validated, # Pass the list of int IDs
            source_identifier=source_identifier
        ):
            overall_dataset_changed_by_this_rule = True
            logger.debug(f"AI standardization resulted in changes for rule {rule_name_for_log}.")
        else:
            logger.debug(f"AI standardization did not result in changes for rule {rule_name_for_log}.")
    # Log if the list was explicitly present but empty (after validation, it would be an empty list)
    elif rule.ai_prompt_config_ids is not None and not rule.ai_prompt_config_ids: # Check original field if it was an empty list
         logger.debug(f"Rule {rule_name_for_log} has an empty list for ai_prompt_config_ids.")
    # else: rule.ai_prompt_config_ids was null (None), no need to log anything specific here.


    # --- Final Logging ---
    if overall_dataset_changed_by_this_rule:
        logger.info(f"Actions for rule {rule_name_for_log} resulted in dataset modifications.")
    else:
        # Log only if actions were defined but made no change
        if modifications_validated or ai_prompt_config_ids_validated: # Use the validated list of IDs
             logger.info(f"Defined actions for rule {rule_name_for_log} did not result in dataset modifications.")
        else:
             logger.debug(f"No actions defined or applied for rule {rule_name_for_log}.")

    return overall_dataset_changed_by_this_rule

def ruleset_name_for_log(rule: Rule) -> str:
    """Helper to safely get ruleset name for logging."""
    if rule.ruleset:
        return rule.ruleset.name
    # Fallback if ruleset is not loaded or available (should ideally always be for a Rule object from DB)
    # Could happen if 'rule' object is constructed manually for testing without full relationships.
    logger.warning("Rule object does not have ruleset information for logging.", rule_id=rule.id, rule_name=rule.name)
    return "UnknownRuleset"
