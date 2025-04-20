# app/api/api_v1/endpoints/rules.py

from typing import List, Optional, Any, Dict # Added Dict
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
import pydicom # <-- Import pydicom
from pydicom.errors import InvalidDicomError # <-- Import error type
import logging # <-- Import logging
from copy import deepcopy # <-- Import deepcopy
from pydantic import TypeAdapter

# --- Corrected Model Import ---
# Assuming models are accessible directly under app.db.models
from app.db import models # Use models from app.db

from app.api import deps # Import the updated deps module

# Existing imports
from app import crud, schemas # Ensure these have correct __init__.py
# Import specific processing schemas
from app.schemas import JsonProcessRequest, JsonProcessResponse

# --- Import worker helpers (temporary, ideally refactor into a service) ---
# NOTE: Be mindful of potential circular dependencies if tasks.py imports from API files
# It's better to move these helpers to a dedicated 'rules_engine' service module.
try:
    # Use the new processing_logic module instead of tasks.py directly
    from app.worker.processing_logic import check_match, apply_modifications, parse_dicom_tag
    from app.db.models.rule import RuleSetExecutionMode # Import enum from model definition
except ImportError as e:
    logging.getLogger(__name__).error(f"Could not import helpers from app.worker.processing_logic or models: {e}. JSON processing endpoint will fail.")
    # Define dummy functions to allow startup, but log error
    def check_match(*args, **kwargs): raise NotImplementedError("Rule matching logic not available")
    def apply_modifications(*args, **kwargs): raise NotImplementedError("Tag modification logic not available")
    RuleSetExecutionMode = None # Placeholder
# --- End Worker Helper Import ---


logger = logging.getLogger(__name__) # Initialize logger for this module
router = APIRouter()


# <------------------------------------------------------->
# <---         RuleSet and Rule CRUD Endpoints         --->
# <--- (Keep existing CRUD endpoints as they are above) --->
# <------------------------------------------------------->

# --- RuleSet Endpoints --- (Existing code) ---
@router.post( "/rulesets", response_model=schemas.RuleSet, status_code=status.HTTP_201_CREATED, summary="Create a new RuleSet", tags=["RuleSets"],)
def create_ruleset_endpoint(*, db: Session = Depends(deps.get_db), ruleset_in: schemas.RuleSetCreate, current_user: models.User = Depends(deps.get_current_active_user),) -> Any:
    # Ensure role check or appropriate permission check here if needed
    logger.info(f"User '{current_user.email}' attempting to create ruleset '{ruleset_in.name}'.")
    try: created_ruleset = crud.ruleset.create(db=db, ruleset=ruleset_in)
    except ValueError as e: raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    logger.info(f"RuleSet '{created_ruleset.name}' (ID: {created_ruleset.id}) created successfully.")
    return created_ruleset

@router.get( "/rulesets", response_model=List[schemas.RuleSet], summary="Retrieve multiple RuleSets", tags=["RuleSets"],)
def read_rulesets_endpoint( db: Session = Depends(deps.get_db), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.debug(f"User '{current_user.email}' reading rulesets (skip={skip}, limit={limit}).")
    rulesets = crud.ruleset.get_multi(db=db, skip=skip, limit=limit)
    # Note: get_multi in your crud file loads rules eagerly, consider a summary schema if performance becomes an issue
    logger.debug(f"Retrieved {len(rulesets)} rulesets.")
    return rulesets

@router.get( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Retrieve a specific RuleSet by ID", tags=["RuleSets"],)
def read_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.debug(f"User '{current_user.email}' reading ruleset ID: {ruleset_id}.")
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id) # get loads rules eagerly
    if db_ruleset is None:
        logger.warning(f"RuleSet ID {ruleset_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    logger.debug(f"Successfully retrieved RuleSet ID: {ruleset_id}, Name: '{db_ruleset.name}'.")
    return db_ruleset

@router.put( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Update a RuleSet", tags=["RuleSets"],)
def update_ruleset_endpoint( *, db: Session = Depends(deps.get_db), ruleset_id: int, ruleset_in: schemas.RuleSetUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.info(f"User '{current_user.email}' attempting to update ruleset ID: {ruleset_id}.")
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id) # Use the one that loads rules if needed? No, update doesn't need rules.
    if db_ruleset is None:
        logger.warning(f"Update failed: RuleSet ID {ruleset_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    try:
        updated_ruleset = crud.ruleset.update(db=db, ruleset_id=ruleset_id, ruleset_update=ruleset_in)
    except ValueError as e:
        logger.warning(f"Update conflict for ruleset ID {ruleset_id}: {e}")
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    logger.info(f"RuleSet ID: {ruleset_id} updated successfully by user '{current_user.email}'.")
    return updated_ruleset

@router.delete( "/rulesets/{ruleset_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a RuleSet", tags=["RuleSets"],)
def delete_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    logger.info(f"User '{current_user.email}' attempting to delete ruleset ID: {ruleset_id}.")
    deleted = crud.ruleset.delete(db=db, ruleset_id=ruleset_id)
    if not deleted:
        logger.warning(f"Delete failed: RuleSet ID {ruleset_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    logger.info(f"RuleSet ID: {ruleset_id} deleted successfully by user '{current_user.email}'.")
    return None

# --- Rule Endpoints --- (Existing code) ---
@router.post( "/rules", response_model=schemas.Rule, status_code=status.HTTP_201_CREATED, summary="Create a new Rule for a RuleSet", tags=["Rules"],)
def create_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_in: schemas.RuleCreate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.info(f"User '{current_user.email}' attempting to create rule '{rule_in.name}' for ruleset ID: {rule_in.ruleset_id}.")
    try: created_rule = crud.rule.create(db=db, rule=rule_in)
    except ValueError as e: # Catch ruleset not found error from CRUD
        logger.warning(f"Rule creation failed for user '{current_user.email}': {e}")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    logger.info(f"Rule '{created_rule.name}' (ID: {created_rule.id}) created successfully in ruleset ID: {created_rule.ruleset_id}.")
    return created_rule

@router.get( "/rules", response_model=List[schemas.Rule], summary="Retrieve multiple Rules (optionally filter by RuleSet)", tags=["Rules"],)
def read_rules_endpoint( db: Session = Depends(deps.get_db), ruleset_id: Optional[int] = Query(None), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.debug(f"User '{current_user.email}' reading rules (ruleset_id={ruleset_id}, skip={skip}, limit={limit}).")
    if ruleset_id is not None:
         # Check if ruleset exists first to provide a clearer 404 for the ruleset itself
         db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id) # Simple get is fine here
         if not db_ruleset:
             logger.warning(f"Attempt to read rules for non-existent RuleSet ID: {ruleset_id} by user '{current_user.email}'.")
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"RuleSet with id {ruleset_id} not found.")

         rules = crud.rule.get_multi_by_ruleset(db=db, ruleset_id=ruleset_id, skip=skip, limit=limit)
         logger.debug(f"Retrieved {len(rules)} rules for ruleset ID: {ruleset_id}.")
         return rules
    else:
         # If allowing fetch of all rules is desired, implement here.
         # Otherwise, keep restriction:
         logger.warning(f"User '{current_user.email}' attempted to read rules without specifying ruleset_id.")
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filtering by ruleset_id is required.")
    # return rules # This line was unreachable due to the else block raising exception

@router.get( "/rules/{rule_id}", response_model=schemas.Rule, summary="Retrieve a specific Rule by ID", tags=["Rules"],)
def read_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.debug(f"User '{current_user.email}' reading rule ID: {rule_id}.")
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        logger.warning(f"Rule ID {rule_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    logger.debug(f"Successfully retrieved Rule ID: {rule_id}, Name: '{db_rule.name}'.")
    return db_rule

@router.put( "/rules/{rule_id}", response_model=schemas.Rule, summary="Update a Rule", tags=["Rules"],)
def update_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_id: int, rule_in: schemas.RuleUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.info(f"User '{current_user.email}' attempting to update rule ID: {rule_id}.")
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        logger.warning(f"Update failed: Rule ID {rule_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    updated_rule = crud.rule.update(db=db, rule_id=rule_id, rule_update=rule_in)
    logger.info(f"Rule ID: {rule_id} updated successfully by user '{current_user.email}'.")
    return updated_rule

@router.delete( "/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a Rule", tags=["Rules"],)
def delete_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    logger.info(f"User '{current_user.email}' attempting to delete rule ID: {rule_id}.")
    deleted = crud.rule.delete(db=db, rule_id=rule_id)
    if not deleted:
        logger.warning(f"Delete failed: Rule ID {rule_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    logger.info(f"Rule ID: {rule_id} deleted successfully by user '{current_user.email}'.")
    return None


# <------------------------------------------------------->
# <---          NEW JSON Processing Endpoint           --->
# <------------------------------------------------------->

@router.post(
    "/process-json", # Endpoint path: /api/v1/rules-engine/process-json
    response_model=schemas.JsonProcessResponse,
    summary="Process DICOM JSON Header with Rules",
    tags=["Processing"], # Add a new tag for organization
)
def process_dicom_json(
    *,
    db: Session = Depends(deps.get_db),
    request_data: schemas.JsonProcessRequest,
    current_user: models.User = Depends(deps.get_current_active_user), # Secure endpoint
) -> Any:
    """
    Accepts a DICOM header in JSON format, applies configured rules,
    and returns the morphed JSON header.

    **Note:** This endpoint performs synchronous processing and does not
    use the Celery queue. It's intended for testing or scenarios where
    immediate feedback on JSON morphing is needed. Destinations are ignored.
    """
    logger.info(f"Received JSON processing request from user {current_user.email} "
                f"for source '{request_data.source_identifier}' "
                f"(RuleSet ID: {request_data.ruleset_id or 'Active'})")

    original_json = request_data.dicom_json
    errors: List[str] = []
    warnings: List[str] = [] # Optional: collect warnings
    applied_ruleset_id: Optional[int] = None
    applied_rule_ids: List[int] = []
    morphed_ds: Optional[pydicom.Dataset] = None # Initialize as Optional
    ds: Optional[pydicom.Dataset] = None # Initialize original dataset as Optional

    # 1. Convert JSON to pydicom Dataset
    try:
        # Use pydicom's helper, requires keys like "00100010"
        ds = pydicom.dataset.Dataset.from_json(original_json) # Explicitly avoid loading bulk data refs
        if not hasattr(ds, 'file_meta'):
             file_meta = pydicom.dataset.FileMetaDataset()
             file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian
             # Try getting SOP Class/Instance from main dataset if available
             sop_class_uid_tag = '00080016'
             sop_instance_uid_tag = '00080018'
             if sop_class_uid_tag in ds:
                 file_meta.MediaStorageSOPClassUID = ds[sop_class_uid_tag].value
             else:
                 warnings.append("SOPClassUID (0008,0016) missing in input JSON for file_meta.")
                 file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2" # Default to CT Image Storage? Or fail?

             if sop_instance_uid_tag in ds:
                 file_meta.MediaStorageSOPInstanceUID = ds[sop_instance_uid_tag].value
             else:
                 warnings.append("SOPInstanceUID (0008,0018) missing in input JSON for file_meta. Generating one.")
                 file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid()

             file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
             file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
             ds.file_meta = file_meta
             logger.debug("Added default file_meta to dataset created from JSON.")

        morphed_ds = deepcopy(ds) # Work on a copy for modifications
        logger.debug("Successfully converted input JSON to pydicom Dataset.")

    except Exception as e:
        logger.error(f"Error converting input JSON to pydicom Dataset: {e}", exc_info=True)
        errors.append(f"Invalid input JSON format or structure: {e}")
        # Return error immediately
        return schemas.JsonProcessResponse(
            original_json=original_json,
            morphed_json={},
            source_identifier=request_data.source_identifier,
            applied_ruleset_id=None,
            applied_rule_ids=[],
            errors=errors,
            warnings=warnings
        )

    # 2. Fetch Rules based on request (specific ruleset or active ones)
    rulesets_to_evaluate: List[models.RuleSet] = []
    try:
        if request_data.ruleset_id:
            logger.debug(f"Fetching specific RuleSet ID: {request_data.ruleset_id}")
            ruleset = crud.ruleset.get(db=db, ruleset_id=request_data.ruleset_id) # get should load rules
            if ruleset:
                if ruleset.is_active:
                    rulesets_to_evaluate.append(ruleset)
                else:
                     warnings.append(f"Specified RuleSet ID {request_data.ruleset_id} is inactive.")
            else:
                 errors.append(f"Specified RuleSet ID {request_data.ruleset_id} not found.")
        else:
            logger.debug("Fetching active, ordered RuleSets...")
            rulesets_to_evaluate = crud.ruleset.get_active_ordered(db=db) # get_active_ordered loads rules

        if not rulesets_to_evaluate and not errors and request_data.ruleset_id is None:
            warnings.append("No active rulesets found for processing.")
            logger.info("No active rulesets found for processing.")

    except Exception as db_exc:
        logger.error(f"Database error fetching rulesets: {db_exc}", exc_info=True)
        errors.append(f"Database error fetching rules: {db_exc}")
        # Return error immediately if DB fails
        return schemas.JsonProcessResponse(
            original_json=original_json,
            morphed_json={},
            source_identifier=request_data.source_identifier,
            applied_ruleset_id=None,
            applied_rule_ids=[],
            errors=errors,
            warnings=warnings
        )


    # 3. Apply Rules (Similar logic to Celery task, refactor needed)
    if RuleSetExecutionMode is None: # Check if import failed
         logger.critical("RuleSetExecutionMode import failed! Cannot proceed.") # Log critical error
         # Avoid raising HTTP 500 directly, add to errors list for consistent response
         errors.append("Rule processing logic not available due to import error.")
         # Fall through to return response with error

    any_rule_matched_and_applied = False
    modifications_made = False # Track if any modification was actually applied
    effective_source = request_data.source_identifier

    # Only proceed if RuleSetExecutionMode is available and no critical errors yet
    if RuleSetExecutionMode is not None and not errors:
        logger.debug(f"Starting evaluation of {len(rulesets_to_evaluate)} rulesets...")

        for ruleset_obj in rulesets_to_evaluate:
            try: # Wrap entire ruleset processing in try/except
                logger.debug(f"--- Evaluating RuleSet ID: {ruleset_obj.id}, Name: '{ruleset_obj.name}' ---") # Log ruleset start

                # Access rules safely
                rules_list = getattr(ruleset_obj, 'rules', None) # Use getattr for safety
                if rules_list is None:
                    logger.warning(f"RuleSet ID: {ruleset_obj.id} has no 'rules' attribute or relationship is not loaded.")
                    continue # Skip this ruleset if rules cannot be accessed
                if not rules_list:
                     logger.debug(f"RuleSet ID: {ruleset_obj.id} has no rules.")
                     continue

                logger.debug(f"RuleSet ID: {ruleset_obj.id} has {len(rules_list)} rules.")

                matched_rule_in_this_set = False
                for rule_obj in rules_list: # Use the potentially loaded list
                    try: # Wrap single rule processing
                        logger.debug(f"Processing Rule ID: {rule_obj.id}, Name: '{rule_obj.name}', Active: {rule_obj.is_active}") # Log rule start
                        if not rule_obj.is_active:
                            logger.debug(f"Rule ID: {rule_obj.id} is inactive.")
                            continue

                        # Check source applicability
                        rule_sources = rule_obj.applicable_sources
                        logger.debug(f"Rule ID: {rule_obj.id} sources: {rule_sources}")
                        if rule_sources and effective_source not in rule_sources:
                            logger.debug(f"Skipping rule ID: {rule_obj.id} - does not apply to source '{effective_source}'.")
                            continue

                        logger.debug(f"Checking match criteria for Rule ID: {rule_obj.id}")
                        criteria_list = rule_obj.match_criteria # This is likely stored as JSONB/JSON in the DB
                        # Convert DB JSON criteria to Pydantic models for check_match
                        match_criteria_models: List[schemas.MatchCriterion] = []
                        if isinstance(criteria_list, list):
                             try:
                                 match_criteria_models = [schemas.MatchCriterion(**crit) for crit in criteria_list]
                             except Exception as val_err:
                                 logger.warning(f"Rule ID: {rule_obj.id} - Failed to validate match_criteria: {val_err}. Skipping match for this rule.")
                                 continue # Skip match if criteria structure is invalid
                        elif criteria_list:
                            logger.warning(f"Rule ID: {rule_obj.id} match_criteria is not a list ({type(criteria_list)}). Skipping match.")
                            continue # Skip match if criteria is not a list

                        # Perform matching using the validated models
                        is_match = check_match(ds, match_criteria_models) # Use original 'ds' for matching
                        logger.debug(f"Rule ID: {rule_obj.id} match result: {is_match}")

                        if is_match:
                            logger.info(f"Rule '{ruleset_obj.name}' / '{rule_obj.name}' (ID: {rule_obj.id}) MATCHED.")
                            any_rule_matched_and_applied = True
                            matched_rule_in_this_set = True
                            applied_rule_ids.append(rule_obj.id)
                            if applied_ruleset_id is None: applied_ruleset_id = ruleset_obj.id

                            # Apply Modifications to the copy 'morphed_ds'
                            modifications_list = rule_obj.tag_modifications # This is likely stored as JSONB/JSON
                            if isinstance(modifications_list, list) and modifications_list:
                                # Validate and Apply Modifications
                                try:
                                     # Validate DB JSON mods against Pydantic schemas
                                     # mod_schemas = [schemas.TagModification.model_validate(mod) for mod in modifications_list]
                                     # mod_schemas = [pydantic.parse_obj_as(schemas.TagModification, mod) for mod in modifications_list] # Pydantic v1 style
                                     ta_tag_modification = TypeAdapter(schemas.TagModification) # Create adapter for the Union type
                                     mod_schemas = [ta_tag_modification.validate_python(mod) for mod in modifications_list]

                                     if mod_schemas:
                                         logger.debug(f"Applying {len(mod_schemas)} modifications for rule ID: {rule_obj.id}...")
                                         apply_modifications(morphed_ds, mod_schemas) # Modify the copy using validated models
                                         modifications_made = True # Mark that state changed
                                     else:
                                         logger.debug(f"Rule ID: {rule_obj.id} had modifications list, but none validated.")
                                except Exception as mod_val_err:
                                     logger.error(f"Rule ID: {rule_obj.id} - Failed to validate/apply tag_modifications: {mod_val_err}", exc_info=True)
                                     errors.append(f"Error validating modifications for Rule ID {rule_obj.id}: {mod_val_err}")
                                     # Decide whether to continue applying other rules
                            else:
                                 logger.debug(f"Rule ID: {rule_obj.id} matched but has no modifications defined.")

                            # Destinations are ignored in this JSON processing endpoint

                            # Check execution mode
                            ruleset_mode = ruleset_obj.execution_mode
                            logger.debug(f"Rule ID: {rule_obj.id} matched in RuleSet ID: {ruleset_obj.id} with mode: {ruleset_mode}")
                            if ruleset_mode == RuleSetExecutionMode.FIRST_MATCH:
                                logger.debug(f"FIRST_MATCH mode triggered. Stopping rule processing for RuleSet ID: {ruleset_obj.id}.")
                                break # Stop processing rules in this ruleset
                        # --- End if is_match ---

                    except Exception as single_rule_exc:
                        logger.error(f"!!! Exception processing SINGLE RULE ID: {getattr(rule_obj, 'id', 'N/A')} !!!", exc_info=True)
                        errors.append(f"Internal error processing rule ID {getattr(rule_obj, 'id', 'N/A')}")
                        # Continue to next rule? Or break? Depending on desired robustness. Let's continue.

                # --- End inner loop (rules) ---

                # Check again for FIRST_MATCH break after inner loop finishes or breaks
                if matched_rule_in_this_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                     logger.debug(f"Breaking outer loop due to FIRST_MATCH in RuleSet ID: {ruleset_obj.id}")
                     break # Stop processing further rulesets

            except Exception as ruleset_exc:
                 logger.error(f"!!! Exception processing RULESET ID: {getattr(ruleset_obj, 'id', 'N/A')} !!!", exc_info=True)
                 errors.append(f"Internal error processing ruleset ID {getattr(ruleset_obj, 'id', 'N/A')}")
                 # Continue to next ruleset if possible? If the error was accessing ruleset_obj itself, this might fail.

        # --- End outer loop (rulesets) ---
        logger.debug("Finished evaluating rulesets.")


    # 4. Convert Morphed Dataset back to JSON
    morphed_json: Dict[str, Any] = {} # Default to empty dict
    try:
        # Use the morphed dataset if modifications were made, otherwise use the original
        dataset_to_convert = morphed_ds if modifications_made else ds
        if dataset_to_convert:
            # Use to_json_dict for a dictionary representation suitable for pydicom 1.x/2.x
            morphed_json = dataset_to_convert.to_json_dict()
            logger.debug("Successfully converted final dataset back to JSON dict.")
        else:
            # This case shouldn't happen if conversion succeeded initially, but handle defensively
            logger.warning("No dataset available (original or morphed) to convert back to JSON.")
            morphed_json = original_json # Fallback to original input json if no dataset exists

    except Exception as e:
        logger.error(f"Error converting final dataset back to JSON: {e}", exc_info=True)
        errors.append(f"Error serializing result: {e}")
        # Decide what to return - maybe original json with errors?
        morphed_json = original_json # Return original if result couldn't be serialized

    # 5. Return Response
    response = schemas.JsonProcessResponse(
        original_json=original_json, # Always return the original input
        morphed_json=morphed_json, # Return the result of morphing (or original on error)
        source_identifier=effective_source,
        applied_ruleset_id=applied_ruleset_id,
        applied_rule_ids=applied_rule_ids,
        errors=errors,
        warnings=warnings
    )
    logger.info(f"Finished JSON processing request. Matched Rules: {len(applied_rule_ids)}. Errors: {len(errors)}. Warnings: {len(warnings)}")
    return response
