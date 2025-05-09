# app/api/api_v1/endpoints/rules.py

from typing import List, Optional, Any, Dict
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body # Added Body
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError
# Use structlog consistently
import structlog # type: ignore
logger = structlog.get_logger(__name__)
from copy import deepcopy
from pydantic import TypeAdapter
import json # For potential string parsing if needed, but from_json handles dicts

from app.db import models
from app.api import deps
from app import crud, schemas # Import top-level schemas
# Import specific schemas used in this endpoint
from app.schemas.rule import ( 
    RuleSet, RuleSetCreate, RuleSetUpdate, Rule, RuleCreate, RuleUpdate, 
    MatchCriterion, AssociationMatchCriterion, TagModification, RuleSetExecutionMode)
                        
# --- CORRECTED: Import helpers from their new locations ---
try:
    from app.worker.utils.dicom_utils import parse_dicom_tag
    from app.worker.rule_evaluator import check_tag_criteria # Renamed from check_match
    from app.worker.standard_modification_handler import apply_standard_modifications # Renamed from apply_modifications
    WORKER_HELPERS_AVAILABLE = True
except ImportError as import_err:
    logger.error(
        "Failed to import refactored worker helpers (parse_dicom_tag, check_tag_criteria, apply_standard_modifications). "
        "JSON processing endpoint will likely fail.", 
        error_details=str(import_err), exc_info=True
    )
    WORKER_HELPERS_AVAILABLE = False
    # Define dummy functions if import fails to allow server startup, but endpoint will fail
    def parse_dicom_tag(*args, **kwargs): raise NotImplementedError("Worker helper parse_dicom_tag not imported.")
    def check_tag_criteria(*args, **kwargs): raise NotImplementedError("Worker helper check_tag_criteria not imported.")
    def apply_standard_modifications(*args, **kwargs): raise NotImplementedError("Worker helper apply_standard_modifications not imported.")
# --- END CORRECTION ---

# Import RuleSetExecutionMode enum if needed (it is used below)
# from app.db.models.rule import RuleSetExecutionMode # Import from models if defined there, or schemas if defined there

router = APIRouter()

# <------------------------------------------------------->
# <---         RuleSet and Rule CRUD Endpoints         --->
# <---      (UNCHANGED - Keeping existing CRUD code)   --->
# <------------------------------------------------------->

# --- RuleSet Endpoints ---
@router.post( "/rulesets", response_model=schemas.RuleSet, status_code=status.HTTP_201_CREATED, summary="Create a new RuleSet", tags=["RuleSets"],)
def create_ruleset_endpoint(*, db: Session = Depends(deps.get_db), ruleset_in: schemas.RuleSetCreate, current_user: models.User = Depends(deps.get_current_active_user),) -> Any:
    log = logger.bind(user_email=current_user.email, ruleset_name=ruleset_in.name)
    log.info("Attempting to create ruleset")
    try: 
        # Assuming crud.ruleset exists based on your crud/__init__.py
        created_ruleset = crud.ruleset.create(db=db, ruleset=ruleset_in) 
    except ValueError as e:
        log.warning("RuleSet creation conflict", error=str(e))
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    log.info("RuleSet created successfully", ruleset_id=created_ruleset.id)
    return created_ruleset

@router.get( "/rulesets", response_model=List[schemas.RuleSet], summary="Retrieve multiple RuleSets", tags=["RuleSets"],)
def read_rulesets_endpoint( db: Session = Depends(deps.get_db), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, skip=skip, limit=limit)
    log.debug("Reading rulesets")
    rulesets = crud.ruleset.get_multi(db=db, skip=skip, limit=limit) # Assuming crud.ruleset access
    log.debug("Retrieved rulesets", count=len(rulesets))
    return rulesets

@router.get( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Retrieve a specific RuleSet by ID", tags=["RuleSets"],)
def read_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, ruleset_id=ruleset_id)
    log.debug("Reading ruleset")
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id) # Assuming crud.ruleset access
    if db_ruleset is None:
        log.warning("RuleSet not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    log.debug("Successfully retrieved RuleSet", ruleset_name=db_ruleset.name)
    return db_ruleset

@router.put( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Update a RuleSet", tags=["RuleSets"],)
def update_ruleset_endpoint( *, db: Session = Depends(deps.get_db), ruleset_id: int, ruleset_in: schemas.RuleSetUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, ruleset_id=ruleset_id)
    log.info("Attempting to update ruleset")
    # No need to query first, crud.update should handle not found
    try:
        updated_ruleset = crud.ruleset.update(db=db, ruleset_id=ruleset_id, ruleset_update=ruleset_in) # Assuming crud.ruleset access
        if updated_ruleset is None: # crud.update might return None if not found
             log.warning("Update failed: RuleSet not found by CRUD operation.")
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    except ValueError as e: # crud.update might raise ValueError for conflicts (like duplicate name)
        log.warning("Update conflict for ruleset", error=str(e))
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    log.info("RuleSet updated successfully")
    return updated_ruleset

@router.delete( "/rulesets/{ruleset_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a RuleSet", tags=["RuleSets"],)
def delete_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    log = logger.bind(user_email=current_user.email, ruleset_id=ruleset_id)
    log.info("Attempting to delete ruleset")
    deleted_ruleset = crud.ruleset.delete(db=db, ruleset_id=ruleset_id) # Assuming crud.ruleset access
    if deleted_ruleset is None: # crud.delete might return None if not found
        log.warning("Delete failed: RuleSet not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    log.info("RuleSet deleted successfully")
    return None

# --- Rule Endpoints ---
@router.post( "/rules", response_model=schemas.Rule, status_code=status.HTTP_201_CREATED, summary="Create a new Rule for a RuleSet", tags=["Rules"],)
def create_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_in: schemas.RuleCreate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, rule_name=rule_in.name, ruleset_id=rule_in.ruleset_id)
    log.info("Attempting to create rule")
    try:
        created_rule = crud.rule.create(db=db, rule=rule_in) # Assuming crud.rule access
    except ValueError as e: # Catch specific errors from CRUD layer
        log.warning("Rule creation failed", error=str(e))
        # Determine appropriate status code based on error message (crude but effective)
        if "RuleSet with id" in str(e) and "not found" in str(e): 
            status_code = status.HTTP_404_NOT_FOUND
        elif "destination IDs not found" in str(e) or "Schedule ID not found" in str(e): 
            status_code = status.HTTP_400_BAD_REQUEST # Bad foreign key reference
        else:
            status_code = status.HTTP_400_BAD_REQUEST # Default bad request
        raise HTTPException(status_code=status_code, detail=str(e))
    log.info("Rule created successfully", rule_id=created_rule.id)
    return created_rule

@router.get( "/rules", response_model=List[schemas.Rule], summary="Retrieve multiple Rules (optionally filter by RuleSet)", tags=["Rules"],)
def read_rules_endpoint( db: Session = Depends(deps.get_db), ruleset_id: Optional[int] = Query(None), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, ruleset_id=ruleset_id, skip=skip, limit=limit)
    log.debug("Reading rules")
    if ruleset_id is not None:
         # Verify ruleset exists first
         db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id) # Assuming crud.ruleset access
         if not db_ruleset:
             log.warning("Attempt to read rules for non-existent RuleSet ID", target_ruleset_id=ruleset_id)
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"RuleSet with id {ruleset_id} not found.")
         rules = crud.rule.get_multi_by_ruleset(db=db, ruleset_id=ruleset_id, skip=skip, limit=limit) # Assuming crud.rule access
         log.debug("Retrieved rules for ruleset", count=len(rules))
         return rules
    else:
         # Allow fetching all rules? Or enforce filtering? Current code requires ruleset_id.
         # Let's keep the requirement for now as per original logic provided.
         log.warning("Attempted to read rules without specifying ruleset_id.")
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filtering by ruleset_id is required for fetching rules via this endpoint.")

@router.get( "/rules/{rule_id}", response_model=schemas.Rule, summary="Retrieve a specific Rule by ID", tags=["Rules"],)
def read_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, rule_id=rule_id)
    log.debug("Reading rule")
    db_rule = crud.rule.get(db=db, rule_id=rule_id) # Assuming crud.rule access
    if db_rule is None:
        log.warning("Rule not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    log.debug("Successfully retrieved Rule", rule_name=db_rule.name)
    return db_rule

@router.put( "/rules/{rule_id}", response_model=schemas.Rule, summary="Update a Rule", tags=["Rules"],)
def update_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_id: int, rule_in: schemas.RuleUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, rule_id=rule_id)
    log.info("Attempting to update rule")
    # Let crud.rule.update handle checking existence
    try:
        updated_rule = crud.rule.update(db=db, rule_id=rule_id, rule_update=rule_in) # Assuming crud.rule access
        if updated_rule is None: # If CRUD returns None on not found
            log.warning("Update failed: Rule not found by CRUD operation.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    except ValueError as e: # Catch specific validation errors from CRUD
        log.warning("Rule update failed", error=str(e))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    log.info("Rule updated successfully")
    return updated_rule

@router.delete( "/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a Rule", tags=["Rules"],)
def delete_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    log = logger.bind(user_email=current_user.email, rule_id=rule_id)
    log.info("Attempting to delete rule")
    deleted_rule = crud.rule.delete(db=db, rule_id=rule_id) # Assuming crud.rule access
    if deleted_rule is None: # If CRUD returns None on not found
        log.warning("Delete failed: Rule not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    log.info("Rule deleted successfully")
    return None


# --- JSON Processing Endpoint --- (CORRECTED) ---
@router.post(
    "/process-json",
    response_model=schemas.JsonProcessResponse,
    summary="Process DICOM JSON Header with Rules",
    tags=["Processing"],
)
def process_dicom_json(
    *,
    db: Session = Depends(deps.get_db), # Get DB session via dependency
    request_data: schemas.JsonProcessRequest = Body(...), # Use Body for request payload
    current_user: models.User = Depends(deps.get_current_active_user), # Authentication
) -> Any:
    """
    Accepts a DICOM header in JSON format, applies configured rules (ignoring schedule),
    and returns the potentially morphed JSON header. Destinations are ignored.
    Requires worker helper functions to be available.
    """
    log = logger.bind(user_email=current_user.email, request_source_identifier=request_data.source_identifier, request_ruleset_id=request_data.ruleset_id)
    log.info("Received JSON processing request")

    # Check if worker helpers were imported successfully
    if not WORKER_HELPERS_AVAILABLE:
        log.error("Cannot process JSON request because essential worker helper functions failed to import.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Internal configuration error: Processing functions unavailable.")

    original_json = request_data.dicom_json
    errors: List[str] = []
    warnings: List[str] = []
    applied_ruleset_id: Optional[int] = None
    applied_rule_ids: List[int] = []
    morphed_ds: Optional[pydicom.Dataset] = None
    ds: Optional[pydicom.Dataset] = None

    json_api_source_identifier = request_data.source_identifier or "JSON_API_Source" 
    log = log.bind(effective_source_identifier=json_api_source_identifier)

    # 1. Convert JSON to pydicom Dataset
    try:
        ds = pydicom.dataset.Dataset.from_json(original_json)
        if not hasattr(ds, 'file_meta'):
             # Add minimal file_meta (same logic as before)
             file_meta = pydicom.dataset.FileMetaDataset()
             file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian 
             sop_class_uid_tag = '00080016'; sop_instance_uid_tag = '00080018'
             if sop_class_uid_tag in ds and hasattr(ds[sop_class_uid_tag], 'value') and ds[sop_class_uid_tag].value: file_meta.MediaStorageSOPClassUID = ds[sop_class_uid_tag].value[0] 
             else: warnings.append("SOPClassUID missing/empty in JSON."); file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2" 
             if sop_instance_uid_tag in ds and hasattr(ds[sop_instance_uid_tag], 'value') and ds[sop_instance_uid_tag].value: file_meta.MediaStorageSOPInstanceUID = ds[sop_instance_uid_tag].value[0] 
             else: warnings.append("SOPInstanceUID missing/empty in JSON."); file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid() 
             file_meta.ImplementationClassUID = settings.PYDICOM_IMPLEMENTATION_UID # Use setting
             file_meta.ImplementationVersionName = settings.IMPLEMENTATION_VERSION_NAME # Use setting
             ds.file_meta = file_meta
             log.debug("Added default file_meta to dataset created from JSON.")
        morphed_ds = deepcopy(ds) 
        log.debug("Successfully converted input JSON to pydicom Dataset.")
    except Exception as e:
        log.error("Error converting input JSON to pydicom Dataset", error_details=str(e), exc_info=True)
        errors.append(f"Invalid input JSON format or structure: {e}")
        return schemas.JsonProcessResponse(original_json=original_json, morphed_json={}, source_identifier=json_api_source_identifier, applied_ruleset_id=None, applied_rule_ids=[], errors=errors, warnings=warnings)

    # 2. Fetch Rules
    rulesets_to_evaluate: List[models.RuleSet] = []
    try:
        if request_data.ruleset_id:
            log.debug("Fetching specific RuleSet", requested_ruleset_id=request_data.ruleset_id)
            ruleset = crud.ruleset.get(db=db, ruleset_id=request_data.ruleset_id) 
            if ruleset:
                # Ignore ruleset.is_active for this endpoint - allow testing inactive rulesets if specified by ID
                rulesets_to_evaluate.append(ruleset)
                if not ruleset.is_active: warnings.append(f"Processing with specified RuleSet ID {request_data.ruleset_id} which is normally inactive.")
            else: errors.append(f"Specified RuleSet ID {request_data.ruleset_id} not found.")
        else:
            log.debug("Fetching active, ordered RuleSets...")
            rulesets_to_evaluate = crud.ruleset.get_active_ordered(db=db) 
        if not rulesets_to_evaluate and not errors: # Only warn if no rulesets found AND no prior errors
            warnings.append("No applicable rulesets found for processing (specify an ID to test inactive ones).")
    except Exception as db_exc:
        log.error("Database error fetching rulesets", error_details=str(db_exc), exc_info=True)
        errors.append(f"Database error fetching rules: {db_exc}")
        return schemas.JsonProcessResponse(original_json=original_json, morphed_json={}, source_identifier=json_api_source_identifier, applied_ruleset_id=None, applied_rule_ids=[], errors=errors, warnings=warnings)

    # 3. Apply Rules
    if not errors: 
        any_rule_matched_and_applied = False
        modifications_made_in_total = False
        log.debug("Starting evaluation of rulesets", ruleset_count=len(rulesets_to_evaluate))
        for ruleset_obj in rulesets_to_evaluate:
            ruleset_log = log.bind(ruleset_id=ruleset_obj.id, ruleset_name=ruleset_obj.name)
            try:
                ruleset_log.debug("--- Evaluating RuleSet ---")
                # Ensure rules relationship is loaded (should be by get/get_active_ordered)
                rules_list = getattr(ruleset_obj, 'rules', [])
                if not rules_list: ruleset_log.debug("RuleSet has no rules."); continue

                ruleset_log.debug("Rules in set", rule_count=len(rules_list))
                matched_rule_in_this_set = False
                for rule_obj in rules_list:
                    rule_log = ruleset_log.bind(rule_id=rule_obj.id, rule_name=rule_obj.name)
                    try:
                        # Ignore rule_obj.is_active and schedule for this endpoint - allow testing inactive/unscheduled rules
                        rule_log.debug("Processing Rule for JSON endpoint (is_active/schedule ignored)")
                        
                        # Check source match
                        rule_sources = rule_obj.applicable_sources
                        source_match = not rule_sources or (isinstance(rule_sources, list) and json_api_source_identifier in rule_sources)
                        if not source_match: 
                            rule_log.debug("Skipping rule - does not apply to source", rule_sources=rule_sources, api_source=json_api_source_identifier); 
                            continue

                        # Validate/parse criteria & modifications
                        try:
                             match_criteria_models = TypeAdapter(List[MatchCriterion]).validate_python(rule_obj.match_criteria or [])
                             # Ignore association criteria for JSON API
                             mod_schemas = TypeAdapter(List[TagModification]).validate_python(rule_obj.tag_modifications or [])
                        except Exception as val_err:
                             rule_log.error("Rule skipped due to invalid criteria/modification format", validation_error=str(val_err))
                             errors.append(f"Invalid definition for Rule ID {rule_obj.id}: {val_err}")
                             continue 

                        # Perform matching (using check_tag_criteria)
                        rule_log.debug("Checking match criteria", criteria_count=len(match_criteria_models))
                        is_match = check_tag_criteria(morphed_ds, match_criteria_models) # Check against potentially modified DS from previous rule in set
                        rule_log.debug("Match result", is_match=is_match)

                        if is_match:
                            rule_log.info("Rule MATCHED.")
                            any_rule_matched_and_applied = True
                            matched_rule_in_this_set = True
                            if rule_obj.id not in applied_rule_ids: applied_rule_ids.append(rule_obj.id) # Add only once
                            if applied_ruleset_id is None: applied_ruleset_id = ruleset_obj.id

                            # Apply modifications (if any)
                            if mod_schemas:
                                rule_log.debug("Applying modifications...", modification_count=len(mod_schemas))
                                try:
                                    # --- CORRECTED CALL - Pass db session ---
                                    modifications_made_by_this_rule = apply_standard_modifications(
                                        morphed_ds, 
                                        mod_schemas, 
                                        json_api_source_identifier,
                                        db # Pass the db session
                                    )
                                    if modifications_made_by_this_rule:
                                        modifications_made_in_total = True
                                    # --- END CORRECTION ---
                                except Exception as mod_apply_err:
                                     rule_log.error("Failed to apply tag_modifications", error_details=str(mod_apply_err), exc_info=True)
                                     errors.append(f"Error applying modifications for Rule ID {rule_obj.id}: {mod_apply_err}")
                            else:
                                rule_log.debug("Rule matched but has no modifications defined.")

                            # Handle FIRST_MATCH mode for the ruleset
                            ruleset_mode = ruleset_obj.execution_mode
                            if ruleset_mode == RuleSetExecutionMode.FIRST_MATCH:
                                ruleset_log.debug("FIRST_MATCH mode triggered. Stopping rule processing for this RuleSet.")
                                break 

                    except Exception as single_rule_exc:
                         rule_log.error("Exception processing SINGLE RULE for JSON API", error_details=str(single_rule_exc), exc_info=True)
                         errors.append(f"Internal error processing rule ID {rule_obj.id}")

                # Handle FIRST_MATCH mode for the outer loop
                if matched_rule_in_this_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                    log.debug("Breaking ruleset loop due to FIRST_MATCH mode.", matched_ruleset_id=ruleset_obj.id)
                    break

            except Exception as ruleset_exc:
                 ruleset_log.error("Exception processing RULESET for JSON API", error_details=str(ruleset_exc), exc_info=True)
                 errors.append(f"Internal error processing ruleset ID {ruleset_obj.id}")
        log.debug("Finished evaluating rulesets for JSON API.")

    # 4. Convert Morphed Dataset back to JSON
    morphed_json: Dict[str, Any] = {}
    try:
        # Use the dataset that was modified throughout the loop
        if morphed_ds:
             morphed_json = morphed_ds.to_json_dict()
             log.debug("Converted final dataset back to JSON")
        else:
             # This case means the initial conversion failed, error already logged
             log.warning("No dataset available (original or morphed) to convert back to JSON.")
             morphed_json = {} # Keep it empty if initial conversion failed
    except Exception as e:
        log.error("Error converting final dataset back to JSON", error_details=str(e), exc_info=True)
        errors.append(f"Error serializing result: {e}")
        morphed_json = {} 

    # 5. Return Response
    response = schemas.JsonProcessResponse(
        original_json=original_json,
        morphed_json=morphed_json,
        source_identifier=json_api_source_identifier, 
        applied_ruleset_id=applied_ruleset_id,
        applied_rule_ids=applied_rule_ids,
        errors=errors,
        warnings=warnings
    )
    log.info("Finished JSON processing request.", matched_rule_count=len(applied_rule_ids), error_count=len(errors), warning_count=len(warnings))
    return response
