# app/api/api_v1/endpoints/rules.py

from typing import List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

# --- Corrected Model Import ---
from app.db import models # Use models from app.db
# --- End Correction ---

from app.api import deps # Import the updated deps module

# Existing imports
from app import crud, schemas # Ensure these have correct __init__.py
# from app.db.session import get_db # Now using deps.get_db
# from app.schemas.rule import Rule, RuleCreate, RuleUpdate, RuleSet, RuleSetCreate, RuleSetUpdate, RuleSetSummary # Loaded via schemas
# from app.crud.crud_rule import ruleset as crud_ruleset, rule as crud_rule # Loaded via crud


router = APIRouter()

# --- RuleSet Endpoints ---

@router.post(
    "/rulesets",
    response_model=schemas.RuleSet, # Use schemas.RuleSet
    status_code=status.HTTP_201_CREATED,
    summary="Create a new RuleSet",
    tags=["RuleSets"],
)
def create_ruleset_endpoint(
    *,
    db: Session = Depends(deps.get_db),
    ruleset_in: schemas.RuleSetCreate, # Use schemas.RuleSetCreate
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> Any:
    """ Create a new RuleSet. Requires authentication. """
    try:
        created_ruleset = crud.ruleset.create(db=db, ruleset=ruleset_in)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return created_ruleset

@router.get(
    "/rulesets",
    response_model=List[schemas.RuleSet], # Use schemas.RuleSet
    summary="Retrieve multiple RuleSets",
    tags=["RuleSets"],
)
def read_rulesets_endpoint(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=200, description="Number of items to return"),
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> Any:
    """ Retrieve a list of RuleSets. Requires authentication. """
    rulesets = crud.ruleset.get_multi(db=db, skip=skip, limit=limit)
    return rulesets

@router.get(
    "/rulesets/{ruleset_id}",
    response_model=schemas.RuleSet, # Use schemas.RuleSet
    summary="Retrieve a specific RuleSet by ID",
    tags=["RuleSets"],
)
def read_ruleset_endpoint(
    ruleset_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> Any:
    """ Get details of a specific RuleSet by its ID. Requires authentication. """
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id)
    if db_ruleset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    return db_ruleset

@router.put(
    "/rulesets/{ruleset_id}",
    response_model=schemas.RuleSet, # Use schemas.RuleSet
    summary="Update a RuleSet",
    tags=["RuleSets"],
)
def update_ruleset_endpoint(
    *,
    db: Session = Depends(deps.get_db),
    ruleset_id: int,
    ruleset_in: schemas.RuleSetUpdate, # Use schemas.RuleSetUpdate
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> Any:
    """ Update an existing RuleSet. Requires authentication. """
    db_ruleset = crud.ruleset.get(db=db, ruleset_id=ruleset_id)
    if db_ruleset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    try:
        updated_ruleset = crud.ruleset.update(db=db, ruleset_id=ruleset_id, ruleset_update=ruleset_in)
    except ValueError as e:
         raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return updated_ruleset

@router.delete(
    "/rulesets/{ruleset_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a RuleSet",
    tags=["RuleSets"],
)
def delete_ruleset_endpoint(
    ruleset_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User (or superuser dep later)
) -> None:
    """ Delete a RuleSet and its associated Rules. Requires authentication. """
    deleted = crud.ruleset.delete(db=db, ruleset_id=ruleset_id)
    if not deleted:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    return None

# --- Rule Endpoints ---

@router.post(
    "/rules",
    response_model=schemas.Rule, # Use schemas.Rule
    status_code=status.HTTP_201_CREATED,
    summary="Create a new Rule for a RuleSet",
    tags=["Rules"],
)
def create_rule_endpoint(
    *,
    db: Session = Depends(deps.get_db),
    rule_in: schemas.RuleCreate, # Use schemas.RuleCreate
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> Any:
    """ Create a new Rule associated with an existing RuleSet. Requires authentication. """
    try:
        created_rule = crud.rule.create(db=db, rule=rule_in)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return created_rule

@router.get(
    "/rules",
    response_model=List[schemas.Rule], # Use schemas.Rule
    summary="Retrieve multiple Rules (optionally filter by RuleSet)",
    tags=["Rules"],
)
def read_rules_endpoint(
    db: Session = Depends(deps.get_db),
    ruleset_id: Optional[int] = Query(None, description="Filter by RuleSet ID"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> Any:
    """ Retrieve a list of Rules. Can be filtered by the parent RuleSet ID. Requires authentication. """
    if ruleset_id is not None:
         rules = crud.rule.get_multi_by_ruleset(db=db, ruleset_id=ruleset_id, skip=skip, limit=limit)
         if not rules and crud.ruleset.get(db=db, ruleset_id=ruleset_id) is None:
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"RuleSet with id {ruleset_id} not found.")
    else:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Listing all rules without filtering by ruleset_id is not currently supported.")
    return rules

@router.get(
    "/rules/{rule_id}",
    response_model=schemas.Rule, # Use schemas.Rule
    summary="Retrieve a specific Rule by ID",
    tags=["Rules"],
)
def read_rule_endpoint(
    rule_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> Any:
    """ Get details of a specific Rule by its ID. Requires authentication. """
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    return db_rule

@router.put(
    "/rules/{rule_id}",
    response_model=schemas.Rule, # Use schemas.Rule
    summary="Update a Rule",
    tags=["Rules"],
)
def update_rule_endpoint(
    *,
    db: Session = Depends(deps.get_db),
    rule_id: int,
    rule_in: schemas.RuleUpdate, # Use schemas.RuleUpdate
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> Any:
    """ Update an existing Rule. Requires authentication. """
    db_rule = crud.rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    updated_rule = crud.rule.update(db=db, rule_id=rule_id, rule_update=rule_in)
    return updated_rule

@router.delete(
    "/rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a Rule",
    tags=["Rules"],
)
def delete_rule_endpoint(
    rule_id: int,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user), # Use models.User
) -> None:
    """ Delete a specific Rule. Requires authentication. """
    deleted = crud.rule.delete(db=db, rule_id=rule_id)
    if not deleted:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    return None
