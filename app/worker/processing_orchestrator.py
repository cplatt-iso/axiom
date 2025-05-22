# app/worker/processing_orchestrator.py

import structlog
from copy import deepcopy
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING

import pydicom # For pydicom.Dataset and deepcopy
from pydantic import TypeAdapter # For validation

# --- App Imports ---
from app.db.models import RuleSet, Rule, RuleSetExecutionMode # Make sure Rule is imported if not already
# Import schemas for validation if needed, though individual modules might handle their parts
from app.schemas.rule import Rule as RuleSchema # Ensure Pydantic Rule schema is imported
from app.schemas import MatchCriterion, AssociationMatchCriterion # For type validation

# --- Worker Sub-Module Imports ---
from app.worker.rule_evaluator import is_rule_triggerable, check_tag_criteria, check_association_criteria
from app.worker.action_applicator import apply_actions_for_rule, ruleset_name_for_log
from app.worker.destination_handler import collect_destinations_for_rule, finalize_and_deduplicate_destinations
# parse_dicom_tag needs a home. For now, assuming it might be needed here too, or in a future dicom_utils
from app.worker.utils.dicom_utils import parse_dicom_tag

if TYPE_CHECKING:
    import anyio.abc # For type hinting BlockingPortal
    from sqlalchemy.orm import Session # For db_session type hint

logger = structlog.get_logger(__name__) # Ensure logger is initialized

def process_instance_against_rules(
    original_ds: pydicom.Dataset,
    active_rulesets: List[RuleSet], # This is List[app.db.models.RuleSet]
    source_identifier: str,
    db_session: 'Session', 
    association_info: Optional[Dict[str, str]] = None,
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None
) -> Tuple[Optional[pydicom.Dataset], List[str], List[Dict[str, Any]]]:
    
    logger.info("Entered process_instance_against_rules", num_active_rulesets_received=len(active_rulesets))
    """
    Orchestrates the processing of a DICOM instance against active rulesets.
    It evaluates rules, applies modifications, and collects destinations.

    Args:
        original_ds: The original pydicom.Dataset.
        active_rulesets: A list of active RuleSet objects, ordered by priority.
        source_identifier: Identifier for the source of the DICOM instance.
        db_session: The active SQLAlchemy session.
        association_info: Optional dictionary containing association details.
        ai_portal: Optional anyio.BlockingPortal for AI service calls.

    Returns:
        Tuple containing:
        - The potentially modified pydicom.Dataset. If no rules match or trigger any
          modifications/destinations, the original_ds is returned.
        - List of strings describing the applied rules (e.g., "RuleSetName/RuleName (ID:...)").
        - List of unique destination identifier dictionaries.
    """
    log = logger.bind(
        source_identifier=source_identifier,
        num_active_rulesets=len(active_rulesets)
    )
    log.debug("Starting DICOM instance processing orchestration.")

    modified_ds: Optional[pydicom.Dataset] = None # Holds the dataset if/when it's modified
    applied_rules_info: List[str] = []
    all_collected_destinations: List[Dict[str, Any]] = []
    
    modifications_applied_overall = False # Tracks if any rule made any change
    any_rule_matched_and_triggered_actions = False # Tracks if any rule matched and led to actions (mods or destinations)

    current_time_utc = datetime.now(timezone.utc)

    # --- Iterate through RuleSets (assumed to be pre-sorted by priority) ---
    for ruleset in active_rulesets:
        log.debug(f"Evaluating RuleSet '{ruleset.name}' (ID: {ruleset.id}, Prio: {ruleset.priority}, Mode: {ruleset.execution_mode.value})")
        
        rules_in_this_ruleset = getattr(ruleset, 'rules', [])
        if not rules_in_this_ruleset: # Should be pre-sorted by priority by SQLAlchemy query
            log.debug(f"RuleSet '{ruleset.name}' has no rules. Skipping.")
            continue

        matched_and_actioned_in_this_ruleset = False # For FIRST_MATCH logic within a ruleset

        # --- Iterate through Rules within the RuleSet ---
        for rule in rules_in_this_ruleset: # rule here is an ORM model (app.db.models.Rule)
            # ADD THIS LOGGING BLOCK START
            rule_details_for_log = {
                "rule_id": getattr(rule, 'id', 'N/A'),
                "rule_name": getattr(rule, 'name', 'N/A'),
                "is_active": getattr(rule, 'is_active', 'N/A'),
                "applicable_sources": getattr(rule, 'applicable_sources', 'N/A'),
                "schedule_id": getattr(rule, 'schedule_id', 'N/A'),
                "match_criteria": getattr(rule, 'match_criteria', 'N/A')
            }
            log.debug(
                "Orchestrator: About to evaluate rule.",
                **rule_details_for_log
            )
            # ADD THIS LOGGING BLOCK END

            # 1. Check if Rule is Active, Schedule Allows, and Source Applies
            if not is_rule_triggerable(rule, source_identifier, current_time_utc):
                # Detailed logging is handled by is_rule_triggerable
                continue

            # 2. Validate Rule Structure (critical parts for matching)
            # While individual modules re-validate their specific parts, a high-level check here can be good.
            # For now, we assume the rule object from DB is mostly okay and rely on sub-module validation.
            # Example full validation (can be heavy if done for every rule):
            # try:
            #     ValidatedRuleSchema.model_validate(rule) # If you have a Pydantic schema for the DB model
            # except ValidationError as e:
            #     log.error(f"Rule '{rule.name}' (ID: {rule.id}) data is invalid: {e}. Skipping.")
            #     continue
            
            match_criteria_for_rule = TypeAdapter(List[MatchCriterion]).validate_python(rule.match_criteria or [])
            assoc_criteria_for_rule = TypeAdapter(Optional[List[AssociationMatchCriterion]]).validate_python(rule.association_criteria or None)


            # 3. Perform Matching
            dataset_to_evaluate_against = modified_ds if modified_ds is not None else original_ds
            
            try:
                tag_conditions_met = check_tag_criteria(dataset_to_evaluate_against, match_criteria_for_rule)
                assoc_conditions_met = check_association_criteria(association_info, assoc_criteria_for_rule)
            except Exception as match_err:
                log.error(f"Error during matching for rule '{ruleset_name_for_log(rule)}/{rule.name}' (ID: {rule.id}): {match_err}", exc_info=True)
                continue # Skip to next rule on matching error

            # 4. If Rule Matched, Apply Actions and Collect Destinations
            if tag_conditions_met and assoc_conditions_met:
                log.info(f"MATCHED: Rule '{ruleset_name_for_log(rule)}/{rule.name}' (ID: {rule.id}). "
                         f"(Tags: {tag_conditions_met}, Assoc: {assoc_conditions_met})")
                
                any_rule_matched_and_triggered_actions = True # Mark that something happened
                
                # Ensure modified_ds is a deepcopy if we haven't made one yet and actions might occur
                if modified_ds is None: # Check before applying actions
                    # Only deepcopy if there are modifications or AI tags or destinations to process.
                    # A rule might match but have no actions/destinations.
                    if rule.tag_modifications or rule.ai_standardization_tags or rule.destinations:
                        log.debug("First potential modification/action; creating deepcopy of dataset.")
                        modified_ds = deepcopy(original_ds)
                    else:
                        log.debug(f"Rule '{rule.name}' matched but has no modifications, AI tags, or destinations. No dataset copy needed yet.")
                
                # Dataset to pass to actions; use modified_ds if modified_ds is not None else original_ds (if no copy was made)
                dataset_for_actions = modified_ds if modified_ds is not None else original_ds

                # 4a. Apply Actions (Standard modifications + AI Standardization)
                rule_changed_dataset = False
                if rule.tag_modifications or rule.ai_standardization_tags: # Only call if actions are defined
                    rule_changed_dataset = apply_actions_for_rule(
                        dataset_for_actions, rule, source_identifier, db_session
                    )
                    if rule_changed_dataset:
                        modifications_applied_overall = True
                
                # 4b. Collect Destinations
                # Convert ORM rule to Pydantic RuleSchema before passing
                try:
                    # Pydantic v2 style validation from ORM model
                    rule_pydantic_schema = RuleSchema.model_validate(rule)
                except Exception as e_val:
                    log.error(f"Failed to validate/convert ORM rule ID {rule.id} ('{rule.name}') to Pydantic schema for destination collection.", error=str(e_val), exc_info=True)
                    destinations_from_this_rule = []
                else:
                    destinations_from_this_rule = collect_destinations_for_rule(rule_pydantic_schema)
                
                if destinations_from_this_rule:
                    all_collected_destinations.extend(destinations_from_this_rule)

                # If this rule led to any change or defined destinations, it's considered "actioned"
                if rule_changed_dataset or destinations_from_this_rule:
                    applied_rules_info.append(f"{ruleset_name_for_log(rule)}/{rule.name} (ID:{rule.id})")
                    matched_and_actioned_in_this_ruleset = True 

                # Handle RuleSet Execution Mode
                if ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH and matched_and_actioned_in_this_ruleset:
                    log.debug(f"RuleSet '{ruleset.name}' is FIRST_MATCH and a rule was actioned. "
                              "Stopping further rule evaluation in this ruleset.")
                    break # Exit rule loop for this ruleset
            else:
                log.debug(f"Rule '{ruleset_name_for_log(rule)}/{rule.name}' (ID: {rule.id}) did not meet all match conditions.")
            # --- End of single rule processing ---

        # After processing all rules in a ruleset, check if we need to stop for FIRST_MATCH ruleset
        if ruleset.execution_mode == RuleSetExecutionMode.FIRST_MATCH and matched_and_actioned_in_this_ruleset:
            log.debug(f"Stopping further ruleset evaluation due to FIRST_MATCH mode action in RuleSet '{ruleset.name}'.")
            break # Exit ruleset loop
    # --- End of ruleset loop ---

    # --- Finalization ---
    if not any_rule_matched_and_triggered_actions:
        log.info(f"No rules matched or triggered any actions for source '{source_identifier}'.")
        # Return original dataset, no applied rules, no destinations
        return original_ds, [], []

    # Deduplicate collected destinations
    unique_destinations = finalize_and_deduplicate_destinations(all_collected_destinations)

    final_dataset_to_return = modified_ds if modifications_applied_overall else original_ds
    
    # Sanity check, should not happen if any_rule_matched_and_triggered_actions is True
    # and modifications_applied_overall correctly tracks changes or modified_ds was created.
    if final_dataset_to_return is None:
        log.error("CRITICAL: Orchestrator logic error - final_dataset_to_return is None despite actions. Defaulting to original_ds.")
        final_dataset_to_return = original_ds

    log.info(f"Instance processing complete. Applied rules: {len(applied_rules_info)}. Unique destinations: {len(unique_destinations)}.")
    return final_dataset_to_return, applied_rules_info, unique_destinations
