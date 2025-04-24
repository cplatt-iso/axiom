# app/api/api_v1/endpoints/rules.py

from typing import List, Optional, Any, Dict
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError
import logging
from copy import deepcopy
from pydantic import TypeAdapter

from app.db import models

from app.api import deps

from app import crud, schemas
from app.schemas import JsonProcessRequest, JsonProcessResponse

try:
    from app.worker.processing_logic import check_match, apply_modifications, parse_dicom_tag
    from app.db.models.rule import RuleSetExecutionMode
except ImportError as e:
    logging.getLogger(__name__).error(f"Could not import helpers from app.worker.processing_logic or models: {e}. JSON processing endpoint will fail.")
    def check_match(*args, **kwargs): raise NotImplementedError("Rule matching logic not available")
    def apply_modifications(*args, **kwargs): raise NotImplementedError("Tag modification logic not available")
    RuleSetExecutionMode = None


logger = logging.getLogger(__name__)
router = APIRouter()


# <------------------------------------------------------->
# <---         RuleSet and Rule CRUD Endpoints         --->
# <------------------------------------------------------->

# --- RuleSet Endpoints --- (No changes needed here) ---
@router.post( "/rulesets", response_model=schemas.RuleSet, status_code=status.HTTP_201_CREATED, summary="Create a new RuleSet", tags=["RuleSets"],)
def create_ruleset_endpoint(*, db: Session = Depends(deps.get_db), ruleset_in: schemas.RuleSetCreate, current_user: models.User = Depends(deps.get_current_active_user),) -> Any:
    logger.info(f"User '{current_user.email}' attempting to create ruleset '{ruleset_in.name}'.")
    try: created_ruleset = crud.ruleset.create(db=db, ruleset=ruleset_in)
    except ValueError as e: raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    logger.info(f"RuleSet '{created_ruleset.name}' (ID: {created_ruleset.id}) created successfully.")
    return created_ruleset

@router.get( "/rulesets", response_model=List[schemas.RuleSet], summary="Retrieve multiple RuleSets", tags=["RuleSets"],)
def read_rulesets_endpoint( db: Session = Depends(deps.get_db), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.debug(f"User '{current_user.email}' reading rulesets (skip={skip}, limit={limit}).")
    rulesets = crud.ruleset.get_multi(db=db, skip=skip, limit=limit)
    logger.debug(f"Retrieved {len(rulesets)} rulesets.")
    return rulesets

@router.get( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Retrieve a specific RuleSet by ID", tags=["RuleSets"],)
def read_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.debug(f"User '{current_user.email}' reading ruleset ID: {ruleset_id}.")
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id)
    if db_ruleset is None:
        logger.warning(f"RuleSet ID {ruleset_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    logger.debug(f"Successfully retrieved RuleSet ID: {ruleset_id}, Name: '{db_ruleset.name}'.")
    return db_ruleset

@router.put( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Update a RuleSet", tags=["RuleSets"],)
def update_ruleset_endpoint( *, db: Session = Depends(deps.get_db), ruleset_id: int, ruleset_in: schemas.RuleSetUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    logger.info(f"User '{current_user.email}' attempting to update ruleset ID: {ruleset_id}.")
    # Use the simple get here, no need to load rules/destinations for ruleset update
    db_ruleset = db.query(models.RuleSet).filter(models.RuleSet.id == ruleset_id).first()
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

# --- Rule Endpoints --- (UPDATED) ---
@router.post( "/rules", response_model=schemas.Rule, status_code=status.HTTP_201_CREATED, summary="Create a new Rule for a RuleSet", tags=["Rules"],)
def create_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_in: schemas.RuleCreate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    """
    Creates a new Rule within a specified RuleSet.
    Accepts a list of `destination_ids` to link StorageBackendConfigs.
    """
    logger.info(f"User '{current_user.email}' attempting to create rule '{rule_in.name}' for ruleset ID: {rule_in.ruleset_id}.")
    try:
        # CRUD create now handles destination ID lookup and association
        created_rule = crud.rule.create(db=db, rule=rule_in)
    except ValueError as e: # Catch ruleset/destination not found errors from CRUD
        logger.warning(f"Rule creation failed for user '{current_user.email}': {e}")
        # Determine appropriate status code based on error message
        if "RuleSet with id" in str(e):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
        elif "destination IDs not found" in str(e):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) # Bad request if destination IDs are invalid
        else:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) # Generic bad request
    logger.info(f"Rule '{created_rule.name}' (ID: {created_rule.id}) created successfully in ruleset ID: {created_rule.ruleset_id}.")
    # The returned rule object will have the destinations populated due to ORM mode and eager loading
    return created_rule

@router.get( "/rules", response_model=List[schemas.Rule], summary="Retrieve multiple Rules (optionally filter by RuleSet)", tags=["Rules"],)
def read_rules_endpoint( db: Session = Depends(deps.get_db), ruleset_id: Optional[int] = Query(None), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    """
    Retrieves Rules. Filtering by `ruleset_id` is required.
    Returns rules with their associated destinations.
    """
    logger.debug(f"User '{current_user.email}' reading rules (ruleset_id={ruleset_id}, skip={skip}, limit={limit}).")
    if ruleset_id is not None:
         # Check if ruleset exists first
         db_ruleset = db.query(models.RuleSet).filter(models.RuleSet.id == ruleset_id).first()
         if not db_ruleset:
             logger.warning(f"Attempt to read rules for non-existent RuleSet ID: {ruleset_id} by user '{current_user.email}'.")
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"RuleSet with id {ruleset_id} not found.")

         # CRUD method now loads destinations eagerly
         rules = crud.rule.get_multi_by_ruleset(db=db, ruleset_id=ruleset_id, skip=skip, limit=limit)
         logger.debug(f"Retrieved {len(rules)} rules for ruleset ID: {ruleset_id}.")
         return rules
    else:
         logger.warning(f"User '{current_user.email}' attempted to read rules without specifying ruleset_id.")
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filtering by ruleset_id is required.")

@router.get( "/rules/{rule_id}", response_model=schemas.Rule, summary="Retrieve a specific Rule by ID", tags=["Rules"],)
def read_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    """
    Retrieves a specific Rule by its ID, including its associated destinations.
    """
    logger.debug(f"User '{current_user.email}' reading rule ID: {rule_id}.")
    # CRUD method now loads destinations eagerly
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        logger.warning(f"Rule ID {rule_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    logger.debug(f"Successfully retrieved Rule ID: {rule_id}, Name: '{db_rule.name}'.")
    return db_rule

@router.put( "/rules/{rule_id}", response_model=schemas.Rule, summary="Update a Rule", tags=["Rules"],)
def update_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_id: int, rule_in: schemas.RuleUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    """
    Updates an existing Rule.
    Accepts a list of `destination_ids` to replace the existing destinations.
    """
    logger.info(f"User '{current_user.email}' attempting to update rule ID: {rule_id}.")
    # CRUD get loads the rule with destinations
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        logger.warning(f"Update failed: Rule ID {rule_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")

    try:
        # CRUD update now handles destination ID lookup and association update
        updated_rule = crud.rule.update(db=db, rule_id=rule_id, rule_update=rule_in)
    except ValueError as e: # Catch destination not found errors from CRUD
        logger.warning(f"Rule update failed for user '{current_user.email}': {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

    logger.info(f"Rule ID: {rule_id} updated successfully by user '{current_user.email}'.")
    # The returned rule object will have the updated destinations populated
    return updated_rule

@router.delete( "/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a Rule", tags=["Rules"],)
def delete_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    """Deletes a specific Rule by its ID."""
    logger.info(f"User '{current_user.email}' attempting to delete rule ID: {rule_id}.")
    deleted = crud.rule.delete(db=db, rule_id=rule_id)
    if not deleted:
        logger.warning(f"Delete failed: Rule ID {rule_id} not found for user '{current_user.email}'.")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    logger.info(f"Rule ID: {rule_id} deleted successfully by user '{current_user.email}'.")
    return None


# --- JSON Processing Endpoint --- (No changes needed here) ---
@router.post(
    "/process-json",
    response_model=schemas.JsonProcessResponse,
    summary="Process DICOM JSON Header with Rules",
    tags=["Processing"],
)
def process_dicom_json(
    *,
    db: Session = Depends(deps.get_db),
    request_data: schemas.JsonProcessRequest,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Accepts a DICOM header in JSON format, applies configured rules,
    and returns the morphed JSON header. Destinations are ignored.
    """
    # ... (existing logic remains the same, as it doesn't interact with destination persistence) ...
    logger.info(f"Received JSON processing request from user {current_user.email} "
                f"for source '{request_data.source_identifier}' "
                f"(RuleSet ID: {request_data.ruleset_id or 'Active'})")

    original_json = request_data.dicom_json
    errors: List[str] = []
    warnings: List[str] = []
    applied_ruleset_id: Optional[int] = None
    applied_rule_ids: List[int] = []
    morphed_ds: Optional[pydicom.Dataset] = None
    ds: Optional[pydicom.Dataset] = None

    # 1. Convert JSON to pydicom Dataset
    try:
        ds = pydicom.dataset.Dataset.from_json(original_json)
        if not hasattr(ds, 'file_meta'):
             file_meta = pydicom.dataset.FileMetaDataset()
             file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian
             sop_class_uid_tag = '00080016'; sop_instance_uid_tag = '00080018'
             if sop_class_uid_tag in ds: file_meta.MediaStorageSOPClassUID = ds[sop_class_uid_tag].value
             else: warnings.append("SOPClassUID missing."); file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2"
             if sop_instance_uid_tag in ds: file_meta.MediaStorageSOPInstanceUID = ds[sop_instance_uid_tag].value
             else: warnings.append("SOPInstanceUID missing."); file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid()
             file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
             file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
             ds.file_meta = file_meta
             logger.debug("Added default file_meta to dataset created from JSON.")
        morphed_ds = deepcopy(ds)
        logger.debug("Successfully converted input JSON to pydicom Dataset.")
    except Exception as e:
        logger.error(f"Error converting input JSON to pydicom Dataset: {e}", exc_info=True)
        errors.append(f"Invalid input JSON format or structure: {e}")
        return schemas.JsonProcessResponse(original_json=original_json, morphed_json={}, source_identifier=request_data.source_identifier, applied_ruleset_id=None, applied_rule_ids=[], errors=errors, warnings=warnings)

    # 2. Fetch Rules
    rulesets_to_evaluate: List[models.RuleSet] = []
    try:
        if request_data.ruleset_id:
            logger.debug(f"Fetching specific RuleSet ID: {request_data.ruleset_id}")
            ruleset = crud.ruleset.get(db=db, ruleset_id=request_data.ruleset_id)
            if ruleset:
                if ruleset.is_active: rulesets_to_evaluate.append(ruleset)
                else: warnings.append(f"Specified RuleSet ID {request_data.ruleset_id} is inactive.")
            else: errors.append(f"Specified RuleSet ID {request_data.ruleset_id} not found.")
        else:
            logger.debug("Fetching active, ordered RuleSets...")
            rulesets_to_evaluate = crud.ruleset.get_active_ordered(db=db)
        if not rulesets_to_evaluate and not errors and request_data.ruleset_id is None:
            warnings.append("No active rulesets found for processing.")
    except Exception as db_exc:
        logger.error(f"Database error fetching rulesets: {db_exc}", exc_info=True)
        errors.append(f"Database error fetching rules: {db_exc}")
        return schemas.JsonProcessResponse(original_json=original_json, morphed_json={}, source_identifier=request_data.source_identifier, applied_ruleset_id=None, applied_rule_ids=[], errors=errors, warnings=warnings)

    # 3. Apply Rules
    if RuleSetExecutionMode is None:
         logger.critical("RuleSetExecutionMode import failed! Cannot proceed.")
         errors.append("Rule processing logic not available due to import error.")
    elif not errors: # Only proceed if no critical errors so far
        any_rule_matched_and_applied = False
        modifications_made = False
        effective_source = request_data.source_identifier
        logger.debug(f"Starting evaluation of {len(rulesets_to_evaluate)} rulesets...")
        for ruleset_obj in rulesets_to_evaluate:
            try:
                logger.debug(f"--- Evaluating RuleSet ID: {ruleset_obj.id}, Name: '{ruleset_obj.name}' ---")
                rules_list = getattr(ruleset_obj, 'rules', None)
                if rules_list is None: logger.warning(f"RuleSet ID: {ruleset_obj.id} has no 'rules' attribute."); continue
                if not rules_list: logger.debug(f"RuleSet ID: {ruleset_obj.id} has no rules."); continue
                logger.debug(f"RuleSet ID: {ruleset_obj.id} has {len(rules_list)} rules.")
                matched_rule_in_this_set = False
                for rule_obj in rules_list:
                    try:
                        logger.debug(f"Processing Rule ID: {rule_obj.id}, Name: '{rule_obj.name}', Active: {rule_obj.is_active}")
                        if not rule_obj.is_active: logger.debug(f"Rule ID: {rule_obj.id} is inactive."); continue
                        rule_sources = rule_obj.applicable_sources
                        if rule_sources and effective_source not in rule_sources: logger.debug(f"Skipping rule ID: {rule_obj.id} - does not apply to source '{effective_source}'."); continue
                        logger.debug(f"Checking match criteria for Rule ID: {rule_obj.id}")
                        criteria_list = rule_obj.match_criteria
                        match_criteria_models: List[schemas.MatchCriterion] = []
                        if isinstance(criteria_list, list):
                             try: match_criteria_models = [schemas.MatchCriterion(**crit) for crit in criteria_list]
                             except Exception as val_err: logger.warning(f"Rule ID: {rule_obj.id} - Failed to validate match_criteria: {val_err}. Skipping match."); continue
                        elif criteria_list: logger.warning(f"Rule ID: {rule_obj.id} match_criteria is not a list ({type(criteria_list)}). Skipping match."); continue
                        is_match = check_match(ds, match_criteria_models)
                        logger.debug(f"Rule ID: {rule_obj.id} match result: {is_match}")
                        if is_match:
                            logger.info(f"Rule '{ruleset_obj.name}' / '{rule_obj.name}' (ID: {rule_obj.id}) MATCHED.")
                            any_rule_matched_and_applied = True
                            matched_rule_in_this_set = True
                            applied_rule_ids.append(rule_obj.id)
                            if applied_ruleset_id is None: applied_ruleset_id = ruleset_obj.id
                            modifications_list = rule_obj.tag_modifications
                            if isinstance(modifications_list, list) and modifications_list:
                                try:
                                     ta_tag_modification = TypeAdapter(schemas.TagModification)
                                     mod_schemas = [ta_tag_modification.validate_python(mod) for mod in modifications_list]
                                     if mod_schemas:
                                         logger.debug(f"Applying {len(mod_schemas)} modifications for rule ID: {rule_obj.id}...")
                                         apply_modifications(morphed_ds, mod_schemas)
                                         modifications_made = True
                                     else: logger.debug(f"Rule ID: {rule_obj.id} had modifications list, but none validated.")
                                except Exception as mod_val_err: logger.error(f"Rule ID: {rule_obj.id} - Failed to validate/apply tag_modifications: {mod_val_err}", exc_info=True); errors.append(f"Error validating modifications for Rule ID {rule_obj.id}: {mod_val_err}")
                            else: logger.debug(f"Rule ID: {rule_obj.id} matched but has no modifications defined.")
                            ruleset_mode = ruleset_obj.execution_mode
                            if ruleset_mode == RuleSetExecutionMode.FIRST_MATCH: logger.debug(f"FIRST_MATCH mode triggered. Stopping rule processing for RuleSet ID: {ruleset_obj.id}."); break
                    except Exception as single_rule_exc: logger.error(f"!!! Exception processing SINGLE RULE ID: {getattr(rule_obj, 'id', 'N/A')} !!!", exc_info=True); errors.append(f"Internal error processing rule ID {getattr(rule_obj, 'id', 'N/A')}")
                if matched_rule_in_this_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH: logger.debug(f"Breaking outer loop due to FIRST_MATCH in RuleSet ID: {ruleset_obj.id}"); break
            except Exception as ruleset_exc: logger.error(f"!!! Exception processing RULESET ID: {getattr(ruleset_obj, 'id', 'N/A')} !!!", exc_info=True); errors.append(f"Internal error processing ruleset ID {getattr(ruleset_obj, 'id', 'N/A')}")
        logger.debug("Finished evaluating rulesets.")

    # 4. Convert Morphed Dataset back to JSON
    morphed_json: Dict[str, Any] = {}
    try:
        dataset_to_convert = morphed_ds if modifications_made else ds
        if dataset_to_convert: morphed_json = dataset_to_convert.to_json_dict()
        else: logger.warning("No dataset available to convert back to JSON."); morphed_json = original_json
    except Exception as e:
        logger.error(f"Error converting final dataset back to JSON: {e}", exc_info=True)
        errors.append(f"Error serializing result: {e}")
        morphed_json = original_json

    # 5. Return Response
    response = schemas.JsonProcessResponse(
        original_json=original_json,
        morphed_json=morphed_json,
        source_identifier=effective_source,
        applied_ruleset_id=applied_ruleset_id,
        applied_rule_ids=applied_rule_ids,
        errors=errors,
        warnings=warnings
    )
    logger.info(f"Finished JSON processing request. Matched Rules: {len(applied_rule_ids)}. Errors: {len(errors)}. Warnings: {len(warnings)}")
    return response
