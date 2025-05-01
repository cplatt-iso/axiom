# app/api/api_v1/endpoints/rules.py

from typing import List, Optional, Any, Dict
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError
# Use structlog if available, otherwise fallback
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
from copy import deepcopy
from pydantic import TypeAdapter
import json # Import json for potential string parsing

from app.db import models
from app.api import deps
from app import crud, schemas
from app.schemas import JsonProcessRequest, JsonProcessResponse

# --- Import helpers, handle potential ImportErrors ---
try:
    from app.worker.processing_logic import check_match, apply_modifications, parse_dicom_tag
    from app.db.models.rule import RuleSetExecutionMode
    PROCESSING_LOGIC_AVAILABLE = True
except ImportError as e:
    logger.error(f"Could not import helpers from app.worker.processing_logic or models: {e}. JSON processing endpoint may fail or be disabled.")
    # Define dummy functions if import fails
    def check_match(*args, **kwargs): raise NotImplementedError("Rule matching logic not available")
    def apply_modifications(*args, **kwargs): raise NotImplementedError("Tag modification logic not available")
    RuleSetExecutionMode = None # type: ignore
    PROCESSING_LOGIC_AVAILABLE = False

router = APIRouter()

# <------------------------------------------------------->
# <---         RuleSet and Rule CRUD Endpoints         --->
# <---      (UNCHANGED - Keep existing CRUD code)      --->
# <------------------------------------------------------->

# --- RuleSet Endpoints ---
@router.post( "/rulesets", response_model=schemas.RuleSet, status_code=status.HTTP_201_CREATED, summary="Create a new RuleSet", tags=["RuleSets"],)
def create_ruleset_endpoint(*, db: Session = Depends(deps.get_db), ruleset_in: schemas.RuleSetCreate, current_user: models.User = Depends(deps.get_current_active_user),) -> Any:
    log = logger.bind(user_email=current_user.email, ruleset_name=ruleset_in.name)
    log.info("Attempting to create ruleset")
    try: created_ruleset = crud.ruleset.create(db=db, ruleset=ruleset_in)
    except ValueError as e:
        log.warning("RuleSet creation conflict", error=str(e))
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    log.info("RuleSet created successfully", ruleset_id=created_ruleset.id)
    return created_ruleset

@router.get( "/rulesets", response_model=List[schemas.RuleSet], summary="Retrieve multiple RuleSets", tags=["RuleSets"],)
def read_rulesets_endpoint( db: Session = Depends(deps.get_db), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, skip=skip, limit=limit)
    log.debug("Reading rulesets")
    rulesets = crud.ruleset.get_multi(db=db, skip=skip, limit=limit)
    log.debug("Retrieved rulesets", count=len(rulesets))
    return rulesets

@router.get( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Retrieve a specific RuleSet by ID", tags=["RuleSets"],)
def read_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, ruleset_id=ruleset_id)
    log.debug("Reading ruleset")
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id)
    if db_ruleset is None:
        log.warning("RuleSet not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    log.debug("Successfully retrieved RuleSet", ruleset_name=db_ruleset.name)
    return db_ruleset

@router.put( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Update a RuleSet", tags=["RuleSets"],)
def update_ruleset_endpoint( *, db: Session = Depends(deps.get_db), ruleset_id: int, ruleset_in: schemas.RuleSetUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, ruleset_id=ruleset_id)
    log.info("Attempting to update ruleset")
    db_ruleset = db.query(models.RuleSet).filter(models.RuleSet.id == ruleset_id).first()
    if db_ruleset is None:
        log.warning("Update failed: RuleSet not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    try:
        updated_ruleset = crud.ruleset.update(db=db, ruleset_id=ruleset_id, ruleset_update=ruleset_in)
    except ValueError as e:
        log.warning("Update conflict for ruleset", error=str(e))
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    log.info("RuleSet updated successfully")
    return updated_ruleset

@router.delete( "/rulesets/{ruleset_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a RuleSet", tags=["RuleSets"],)
def delete_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    log = logger.bind(user_email=current_user.email, ruleset_id=ruleset_id)
    log.info("Attempting to delete ruleset")
    deleted = crud.ruleset.delete(db=db, ruleset_id=ruleset_id)
    if not deleted:
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
        created_rule = crud.rule.create(db=db, rule=rule_in)
    except ValueError as e:
        log.warning("Rule creation failed", error=str(e))
        status_code = status.HTTP_400_BAD_REQUEST
        if "RuleSet with id" in str(e): status_code = status.HTTP_404_NOT_FOUND
        elif "destination IDs not found" in str(e): status_code = status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=status_code, detail=str(e))
    log.info("Rule created successfully", rule_id=created_rule.id)
    return created_rule

@router.get( "/rules", response_model=List[schemas.Rule], summary="Retrieve multiple Rules (optionally filter by RuleSet)", tags=["Rules"],)
def read_rules_endpoint( db: Session = Depends(deps.get_db), ruleset_id: Optional[int] = Query(None), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, ruleset_id=ruleset_id, skip=skip, limit=limit)
    log.debug("Reading rules")
    if ruleset_id is not None:
         db_ruleset = db.query(models.RuleSet).filter(models.RuleSet.id == ruleset_id).first()
         if not db_ruleset:
             log.warning("Attempt to read rules for non-existent RuleSet ID")
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"RuleSet with id {ruleset_id} not found.")
         rules = crud.rule.get_multi_by_ruleset(db=db, ruleset_id=ruleset_id, skip=skip, limit=limit)
         log.debug("Retrieved rules for ruleset", count=len(rules))
         return rules
    else:
         log.warning("Attempted to read rules without specifying ruleset_id.")
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filtering by ruleset_id is required.")

@router.get( "/rules/{rule_id}", response_model=schemas.Rule, summary="Retrieve a specific Rule by ID", tags=["Rules"],)
def read_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, rule_id=rule_id)
    log.debug("Reading rule")
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        log.warning("Rule not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    log.debug("Successfully retrieved Rule", rule_name=db_rule.name)
    return db_rule

@router.put( "/rules/{rule_id}", response_model=schemas.Rule, summary="Update a Rule", tags=["Rules"],)
def update_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_id: int, rule_in: schemas.RuleUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    log = logger.bind(user_email=current_user.email, rule_id=rule_id)
    log.info("Attempting to update rule")
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        log.warning("Update failed: Rule not found")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    try:
        updated_rule = crud.rule.update(db=db, rule_id=rule_id, rule_update=rule_in)
    except ValueError as e:
        log.warning("Rule update failed", error=str(e))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    log.info("Rule updated successfully")
    return updated_rule

@router.delete( "/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a Rule", tags=["Rules"],)
def delete_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    log = logger.bind(user_email=current_user.email, rule_id=rule_id)
    log.info("Attempting to delete rule")
    deleted = crud.rule.delete(db=db, rule_id=rule_id)
    if not deleted:
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
    db: Session = Depends(deps.get_db),
    request_data: schemas.JsonProcessRequest,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Accepts a DICOM header in JSON format, applies configured rules,
    and returns the morphed JSON header. Destinations are ignored.
    """
    log = logger.bind(user_email=current_user.email, request_source_identifier=request_data.source_identifier, request_ruleset_id=request_data.ruleset_id)
    log.info("Received JSON processing request")

    if not PROCESSING_LOGIC_AVAILABLE:
        log.error("Cannot process JSON request because processing logic failed to import.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Rule processing logic not available.")

    original_json = request_data.dicom_json
    errors: List[str] = []
    warnings: List[str] = []
    applied_ruleset_id: Optional[int] = None
    applied_rule_ids: List[int] = []
    morphed_ds: Optional[pydicom.Dataset] = None
    ds: Optional[pydicom.Dataset] = None

    # Define a default source identifier for JSON API calls
    # This is needed for apply_modifications, especially crosswalk
    json_api_source_identifier = request_data.source_identifier or "JSON_API_Source" # Use provided or default
    log = log.bind(effective_source_identifier=json_api_source_identifier)

    # 1. Convert JSON to pydicom Dataset
    try:
        ds = pydicom.dataset.Dataset.from_json(original_json)
        # Ensure file_meta exists (needed by some logic/output formats)
        if not hasattr(ds, 'file_meta'):
             file_meta = pydicom.dataset.FileMetaDataset()
             file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian # Default
             sop_class_uid_tag = '00080016'; sop_instance_uid_tag = '00080018'
             if sop_class_uid_tag in ds and ds[sop_class_uid_tag].value: file_meta.MediaStorageSOPClassUID = ds[sop_class_uid_tag].value[0] # Assume single value
             else: warnings.append("SOPClassUID missing from JSON."); file_meta.MediaStorageSOPClassUID = "1.2.840.10008.5.1.4.1.1.2" # Default CT
             if sop_instance_uid_tag in ds and ds[sop_instance_uid_tag].value: file_meta.MediaStorageSOPInstanceUID = ds[sop_instance_uid_tag].value[0] # Assume single value
             else: warnings.append("SOPInstanceUID missing from JSON."); file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid() # Generate one if missing
             file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
             file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
             ds.file_meta = file_meta
             log.debug("Added default file_meta to dataset created from JSON.")
        morphed_ds = deepcopy(ds) # Start with a copy to modify
        log.debug("Successfully converted input JSON to pydicom Dataset.")
    except Exception as e:
        log.error("Error converting input JSON to pydicom Dataset", error=str(e), exc_info=True)
        errors.append(f"Invalid input JSON format or structure: {e}")
        # Return early if conversion fails
        return schemas.JsonProcessResponse(original_json=original_json, morphed_json={}, source_identifier=json_api_source_identifier, applied_ruleset_id=None, applied_rule_ids=[], errors=errors, warnings=warnings)

    # 2. Fetch Rules
    rulesets_to_evaluate: List[models.RuleSet] = []
    try:
        if request_data.ruleset_id:
            log.debug("Fetching specific RuleSet", requested_ruleset_id=request_data.ruleset_id)
            ruleset = crud.ruleset.get(db=db, ruleset_id=request_data.ruleset_id) # get should load relationships
            if ruleset:
                if ruleset.is_active: rulesets_to_evaluate.append(ruleset)
                else: warnings.append(f"Specified RuleSet ID {request_data.ruleset_id} is inactive.")
            else: errors.append(f"Specified RuleSet ID {request_data.ruleset_id} not found.")
        else:
            log.debug("Fetching active, ordered RuleSets...")
            rulesets_to_evaluate = crud.ruleset.get_active_ordered(db=db) # get_active_ordered should load relationships
        if not rulesets_to_evaluate and not errors and request_data.ruleset_id is None:
            warnings.append("No active rulesets found for processing.")
    except Exception as db_exc:
        log.error("Database error fetching rulesets", error=str(db_exc), exc_info=True)
        errors.append(f"Database error fetching rules: {db_exc}")
        # Return early if DB fails
        return schemas.JsonProcessResponse(original_json=original_json, morphed_json={}, source_identifier=json_api_source_identifier, applied_ruleset_id=None, applied_rule_ids=[], errors=errors, warnings=warnings)

    # 3. Apply Rules
    if not errors: # Only proceed if no critical errors so far
        any_rule_matched_and_applied = False
        modifications_made = False
        log.debug("Starting evaluation of rulesets", ruleset_count=len(rulesets_to_evaluate))
        for ruleset_obj in rulesets_to_evaluate:
            ruleset_log = log.bind(ruleset_id=ruleset_obj.id, ruleset_name=ruleset_obj.name)
            try:
                ruleset_log.debug("--- Evaluating RuleSet ---")
                rules_list = getattr(ruleset_obj, 'rules', [])
                if not rules_list: ruleset_log.debug("RuleSet has no rules."); continue

                ruleset_log.debug("Rules in set", rule_count=len(rules_list))
                matched_rule_in_this_set = False
                for rule_obj in rules_list:
                    rule_log = ruleset_log.bind(rule_id=rule_obj.id, rule_name=rule_obj.name)
                    try:
                        rule_log.debug("Processing Rule", rule_is_active=rule_obj.is_active)
                        if not rule_obj.is_active: rule_log.debug("Rule is inactive."); continue

                        # Check source match
                        rule_sources = rule_obj.applicable_sources
                        source_match = not rule_sources or (isinstance(rule_sources, list) and json_api_source_identifier in rule_sources)
                        if not source_match: rule_log.debug("Skipping rule - does not apply to source", rule_sources=rule_sources); continue

                        # Validate/parse criteria & modifications (using TypeAdapter for robustness)
                        try:
                             match_criteria_models = TypeAdapter(List[schemas.MatchCriterion]).validate_python(rule_obj.match_criteria or [])
                             # JSON endpoint doesn't use association criteria, but parse anyway if present for validation
                             assoc_criteria_models = TypeAdapter(List[schemas.AssociationMatchCriterion]).validate_python(rule_obj.association_criteria or [])
                             tag_mod_adapter = TypeAdapter(List[schemas.TagModification])
                             mod_schemas = tag_mod_adapter.validate_python(rule_obj.tag_modifications or [])
                        except Exception as val_err:
                             rule_log.error("Rule skipped due to invalid criteria/modification format", validation_error=str(val_err), exc_info=False)
                             errors.append(f"Invalid definition for Rule ID {rule_obj.id}: {val_err}")
                             continue # Skip this rule if definition is invalid

                        # Perform matching (only tag criteria relevant for JSON API)
                        rule_log.debug("Checking match criteria", criteria_count=len(match_criteria_models))
                        is_match = check_match(ds, match_criteria_models)
                        rule_log.debug("Match result", is_match=is_match)

                        if is_match:
                            rule_log.info("Rule MATCHED.")
                            any_rule_matched_and_applied = True
                            matched_rule_in_this_set = True
                            applied_rule_ids.append(rule_obj.id)
                            if applied_ruleset_id is None: applied_ruleset_id = ruleset_obj.id

                            # Apply modifications (if any)
                            if mod_schemas:
                                rule_log.debug("Applying modifications...", modification_count=len(mod_schemas))
                                try:
                                    # --- CORRECTED CALL ---
                                    # Pass the default source identifier
                                    apply_modifications(morphed_ds, mod_schemas, json_api_source_identifier)
                                    # --- END CORRECTION ---
                                    modifications_made = True
                                except Exception as mod_apply_err:
                                     rule_log.error("Failed to apply tag_modifications", error=str(mod_apply_err), exc_info=True)
                                     errors.append(f"Error applying modifications for Rule ID {rule_obj.id}: {mod_apply_err}")
                            else:
                                rule_log.debug("Rule matched but has no modifications defined.")

                            # Handle FIRST_MATCH mode for the ruleset
                            ruleset_mode = ruleset_obj.execution_mode
                            if ruleset_mode == RuleSetExecutionMode.FIRST_MATCH:
                                ruleset_log.debug("FIRST_MATCH mode triggered. Stopping rule processing for this RuleSet.")
                                break # Stop processing rules in THIS ruleset

                    except Exception as single_rule_exc:
                         rule_log.error("!!! Exception processing SINGLE RULE !!!", error=str(single_rule_exc), exc_info=True)
                         errors.append(f"Internal error processing rule ID {rule_obj.id}")

                # Handle FIRST_MATCH mode for the outer loop (stop processing further rulesets)
                if matched_rule_in_this_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                    log.debug("Breaking ruleset loop due to FIRST_MATCH mode.", matched_ruleset_id=ruleset_obj.id)
                    break

            except Exception as ruleset_exc:
                 ruleset_log.error("!!! Exception processing RULESET !!!", error=str(ruleset_exc), exc_info=True)
                 errors.append(f"Internal error processing ruleset ID {ruleset_obj.id}")
        log.debug("Finished evaluating rulesets.")

    # 4. Convert Morphed Dataset back to JSON
    morphed_json: Dict[str, Any] = {}
    try:
        # Convert the dataset that was potentially modified
        dataset_to_convert = morphed_ds if modifications_made else ds
        if dataset_to_convert:
             morphed_json = dataset_to_convert.to_json_dict()
             log.debug("Converted final dataset back to JSON")
        else:
             # Should not happen if initial conversion succeeded
             log.warning("No dataset available (original or morphed) to convert back to JSON.")
             morphed_json = original_json # Fallback to original if something went wrong
    except Exception as e:
        log.error("Error converting final dataset back to JSON", error=str(e), exc_info=True)
        errors.append(f"Error serializing result: {e}")
        morphed_json = {} # Return empty dict on serialization error

    # 5. Return Response
    response = schemas.JsonProcessResponse(
        original_json=original_json,
        morphed_json=morphed_json,
        source_identifier=json_api_source_identifier, # Return the identifier used
        applied_ruleset_id=applied_ruleset_id,
        applied_rule_ids=applied_rule_ids,
        errors=errors,
        warnings=warnings
    )
    log.info("Finished JSON processing request.", matched_rule_count=len(applied_rule_ids), error_count=len(errors), warning_count=len(warnings))
    return response
