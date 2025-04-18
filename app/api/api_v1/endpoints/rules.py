# app/api/api_v1/endpoints/rules.py

from typing import List, Optional, Any, Dict # Added Dict
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
import pydicom # <-- Import pydicom
from pydicom.errors import InvalidDicomError # <-- Import error type
import logging # <-- Import logging
from copy import deepcopy # <-- Import deepcopy

# --- Corrected Model Import ---
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
    from app.worker.tasks import check_match, apply_modifications, parse_dicom_tag # Import necessary helpers
    from app.db.models import RuleSetExecutionMode # Import enum if needed directly
except ImportError as e:
    logging.getLogger(__name__).error(f"Could not import helpers from app.worker.tasks: {e}. JSON processing endpoint will fail.")
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
    try: created_ruleset = crud.ruleset.create(db=db, ruleset=ruleset_in)
    except ValueError as e: raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return created_ruleset

@router.get( "/rulesets", response_model=List[schemas.RuleSet], summary="Retrieve multiple RuleSets", tags=["RuleSets"],)
def read_rulesets_endpoint( db: Session = Depends(deps.get_db), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=200), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    rulesets = crud.ruleset.get_multi(db=db, skip=skip, limit=limit)
    return rulesets

@router.get( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Retrieve a specific RuleSet by ID", tags=["RuleSets"],)
def read_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id)
    if db_ruleset is None: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    return db_ruleset

@router.put( "/rulesets/{ruleset_id}", response_model=schemas.RuleSet, summary="Update a RuleSet", tags=["RuleSets"],)
def update_ruleset_endpoint( *, db: Session = Depends(deps.get_db), ruleset_id: int, ruleset_in: schemas.RuleSetUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id)
    if db_ruleset is None: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    try: updated_ruleset = crud.ruleset.update(db=db, ruleset_id=ruleset_id, ruleset_update=ruleset_in)
    except ValueError as e: raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return updated_ruleset

@router.delete( "/rulesets/{ruleset_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a RuleSet", tags=["RuleSets"],)
def delete_ruleset_endpoint( ruleset_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    deleted = crud.ruleset.delete(db=db, ruleset_id=ruleset_id)
    if not deleted: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    return None

# --- Rule Endpoints --- (Existing code) ---
@router.post( "/rules", response_model=schemas.Rule, status_code=status.HTTP_201_CREATED, summary="Create a new Rule for a RuleSet", tags=["Rules"],)
def create_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_in: schemas.RuleCreate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    try: created_rule = crud.rule.create(db=db, rule=rule_in)
    except ValueError as e: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return created_rule

@router.get( "/rules", response_model=List[schemas.Rule], summary="Retrieve multiple Rules (optionally filter by RuleSet)", tags=["Rules"],)
def read_rules_endpoint( db: Session = Depends(deps.get_db), ruleset_id: Optional[int] = Query(None), skip: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    if ruleset_id is not None:
         rules = crud.rule.get_multi_by_ruleset(db=db, ruleset_id=ruleset_id, skip=skip, limit=limit)
         if not rules and crud.ruleset.get(db=db, ruleset_id=ruleset_id) is None: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"RuleSet with id {ruleset_id} not found.")
    else: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filtering by ruleset_id is required.") # Keep this restriction
    return rules

@router.get( "/rules/{rule_id}", response_model=schemas.Rule, summary="Retrieve a specific Rule by ID", tags=["Rules"],)
def read_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    return db_rule

@router.put( "/rules/{rule_id}", response_model=schemas.Rule, summary="Update a Rule", tags=["Rules"],)
def update_rule_endpoint( *, db: Session = Depends(deps.get_db), rule_id: int, rule_in: schemas.RuleUpdate, current_user: models.User = Depends(deps.get_current_active_user), ) -> Any:
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    updated_rule = crud.rule.update(db=db, rule_id=rule_id, rule_update=rule_in)
    return updated_rule

@router.delete( "/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a Rule", tags=["Rules"],)
def delete_rule_endpoint( rule_id: int, db: Session = Depends(deps.get_db), current_user: models.User = Depends(deps.get_current_active_user), ) -> None:
    deleted = crud.rule.delete(db=db, rule_id=rule_id)
    if not deleted: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
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
    """
    logger.info(f"Received JSON processing request from user {current_user.email} "
                f"for source '{request_data.source_identifier}' "
                f"(RuleSet ID: {request_data.ruleset_id or 'Active'})")

    original_json = request_data.dicom_json
    errors: List[str] = []
    warnings: List[str] = [] # Optional: collect warnings
    applied_ruleset_id: Optional[int] = None
    applied_rule_ids: List[int] = []
    morphed_ds = None

    # 1. Convert JSON to pydicom Dataset
    try:
        # Use pydicom's helper, assumes JSON format matches pydicom's output
        # Important: Ensure the JSON structure from the client matches!
        # It expects keys like "00100010": {"vr": "PN", "Value": [...] }
        ds = pydicom.dataset.Dataset.from_json(original_json) # Don't load bulk data
        # Need a file_meta, create a default one if missing?
        if not hasattr(ds, 'file_meta'):
             # Create basic file meta - needed for some operations and consistency
             file_meta = pydicom.dataset.FileMetaDataset()
             file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian # Default TS
             # Add MediaStorageSOPClassUID and InstanceUID if available in json?
             if '00020002' in ds: file_meta.MediaStorageSOPClassUID = ds['00020002'].value
             if '00080018' in ds: file_meta.MediaStorageSOPInstanceUID = ds['00080018'].value # SOP Instance UID
             else: file_meta.MediaStorageSOPInstanceUID = pydicom.uid.generate_uid() # Generate if missing
             file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID # Identify pydicom
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
            morphed_json={}, # Or return original_json? Empty dict indicates failure.
            source_identifier=request_data.source_identifier,
            applied_ruleset_id=None,
            applied_rule_ids=[],
            errors=errors
        )

    # 2. Fetch Rules based on request (specific ruleset or active ones)
    rulesets_to_evaluate: List[models.RuleSet] = []
    if request_data.ruleset_id:
        logger.debug(f"Fetching specific RuleSet ID: {request_data.ruleset_id}")
        ruleset = crud.ruleset.get(db=db, ruleset_id=request_data.ruleset_id)
        if ruleset and ruleset.is_active: # Optionally only use active ones even if specified?
            rulesets_to_evaluate.append(ruleset)
        elif ruleset:
             warnings.append(f"Specified RuleSet ID {request_data.ruleset_id} is inactive.")
        else:
             errors.append(f"Specified RuleSet ID {request_data.ruleset_id} not found.")
    else:
        logger.debug("Fetching active, ordered RuleSets...")
        rulesets_to_evaluate = crud.ruleset.get_active_ordered(db=db)

    if not rulesets_to_evaluate and not errors:
        warnings.append("No applicable rulesets found or specified for processing.")

    # 3. Apply Rules (Similar logic to Celery task, refactor needed)
    if RuleSetExecutionMode is None: # Check if import failed
         raise HTTPException(status_code=500, detail="Rule processing logic not available due to import error.")

    any_rule_matched_and_applied = False
    effective_source = request_data.source_identifier

    for ruleset_obj in rulesets_to_evaluate:
        logger.debug(f"Evaluating RuleSet '{ruleset_obj.name}' (ID: {ruleset_obj.id}) for source '{effective_source}'")
        if not ruleset_obj.rules: continue

        matched_rule_in_this_set = False
        for rule_obj in ruleset_obj.rules: # Assumes rules are ordered by priority
            if not rule_obj.is_active: continue

            # Check source applicability
            rule_sources = rule_obj.applicable_sources
            if rule_sources and effective_source not in rule_sources:
                logger.debug(f"Skipping rule '{rule_obj.name}' (ID: {rule_obj.id}): does not apply to source '{effective_source}'.")
                continue

            logger.debug(f"Checking rule '{rule_obj.name}' (ID: {rule_obj.id})")
            try:
                criteria_list = rule_obj.match_criteria
                if isinstance(criteria_list, list) and check_match(ds, criteria_list): # Use original 'ds' for matching
                    logger.info(f"Rule '{ruleset_obj.name}' / '{rule_obj.name}' MATCHED.")
                    any_rule_matched_and_applied = True
                    matched_rule_in_this_set = True
                    applied_rule_ids.append(rule_obj.id)
                    if applied_ruleset_id is None: applied_ruleset_id = ruleset_obj.id # Capture first matched ruleset ID

                    # Apply Modifications to the copy 'morphed_ds'
                    modifications_list = rule_obj.tag_modifications
                    if isinstance(modifications_list, list) and modifications_list:
                        logger.debug(f"Applying {len(modifications_list)} modifications for rule '{rule_obj.name}'...")
                        apply_modifications(morphed_ds, modifications_list) # Modify the copy
                    # Destinations are ignored in this JSON processing endpoint

                    if ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
                        logger.debug(f"RuleSet '{ruleset_obj.name}' mode is FIRST_MATCH. Stopping.")
                        break # Stop processing rules in this ruleset

            except Exception as rule_exc:
                logger.error(f"Error processing rule '{ruleset_obj.name}/{rule_obj.name}': {rule_exc}", exc_info=True)
                errors.append(f"Error during rule '{rule_obj.name}': {rule_exc}")

        if matched_rule_in_this_set and ruleset_obj.execution_mode == RuleSetExecutionMode.FIRST_MATCH:
            break # Stop processing further rulesets

    # 4. Convert Morphed Dataset back to JSON
    try:
        # Use to_json_dict for a dictionary representation
        morphed_json = morphed_ds.to_json_dict() if morphed_ds else {}
        logger.debug("Successfully converted morphed dataset back to JSON dict.")
    except Exception as e:
        logger.error(f"Error converting morphed dataset back to JSON: {e}", exc_info=True)
        errors.append(f"Error serializing result: {e}")
        # Decide what to return - maybe original json with errors?
        morphed_json = original_json # Return original if result couldn't be serialized

    # 5. Return Response
    response = schemas.JsonProcessResponse(
        original_json=original_json,
        morphed_json=morphed_json,
        source_identifier=effective_source,
        applied_ruleset_id=applied_ruleset_id,
        applied_rule_ids=applied_rule_ids,
        errors=errors
        # Add warnings=warnings if implemented
    )
    logger.info(f"Finished JSON processing request. Matched Rules: {len(applied_rule_ids)}. Errors: {len(errors)}.")
    return response
