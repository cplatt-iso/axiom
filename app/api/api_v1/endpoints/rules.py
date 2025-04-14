# app/api/api_v1/endpoints/rules.py

from typing import List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query

from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.rule import (
    Rule, RuleCreate, RuleUpdate,
    RuleSet, RuleSetCreate, RuleSetUpdate, RuleSetSummary
)
from app.crud.crud_rule import ruleset as crud_ruleset, rule as crud_rule


router = APIRouter()

# --- RuleSet Endpoints ---

@router.post(
    "/rulesets",
    response_model=RuleSet,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new RuleSet",
    tags=["RuleSets"],
)
def create_ruleset_endpoint(
    *,
    db: Session = Depends(get_db),
    ruleset_in: RuleSetCreate,
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Add Authentication
) -> Any:
    """
    Create a new RuleSet. Requires authentication later.
    """
    # TODO: Add permission checks for current_user
    try:
        created_ruleset = crud_ruleset.create(db=db, ruleset=ruleset_in)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    return created_ruleset


@router.get(
    "/rulesets",
    response_model=List[RuleSet], # Or use RuleSetSummary for less data
    summary="Retrieve multiple RuleSets",
    tags=["RuleSets"],
)
def read_rulesets_endpoint(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=200, description="Number of items to return"),
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Auth
) -> Any:
    """
    Retrieve a list of RuleSets, ordered by priority then name.
    Supports pagination.
    """
    # TODO: Add permission checks
    rulesets = crud_ruleset.get_multi(db=db, skip=skip, limit=limit)
    # If using Summary:
    # summaries = [RuleSetSummary(..., rule_count=len(rs.rules)) for rs in rulesets]
    # return summaries
    return rulesets


@router.get(
    "/rulesets/{ruleset_id}",
    response_model=RuleSet,
    summary="Retrieve a specific RuleSet by ID",
    tags=["RuleSets"],
)
def read_ruleset_endpoint(
    ruleset_id: int,
    db: Session = Depends(get_db),
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Auth
) -> Any:
    """
    Get details of a specific RuleSet by its ID, including its rules.
    """
    db_ruleset = crud_ruleset.get(db=db, ruleset_id=ruleset_id)
    if db_ruleset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    # TODO: Add permission checks
    return db_ruleset


@router.put(
    "/rulesets/{ruleset_id}",
    response_model=RuleSet,
    summary="Update a RuleSet",
    tags=["RuleSets"],
)
def update_ruleset_endpoint(
    *,
    db: Session = Depends(get_db),
    ruleset_id: int,
    ruleset_in: RuleSetUpdate,
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Auth
) -> Any:
    """
    Update an existing RuleSet.
    """
    db_ruleset = crud_ruleset.get(db=db, ruleset_id=ruleset_id) # Check exists first
    if db_ruleset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    # TODO: Add permission checks
    try:
        updated_ruleset = crud_ruleset.update(db=db, ruleset_id=ruleset_id, ruleset_update=ruleset_in)
    except ValueError as e: # Catch potential unique name conflicts
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
    db: Session = Depends(get_db),
    # current_user: models.User = Depends(deps.get_current_active_superuser), # TODO: Auth + Permissions
) -> None:
    """
    Delete a RuleSet and its associated Rules. Requires admin privileges later.
    """
    deleted = crud_ruleset.delete(db=db, ruleset_id=ruleset_id)
    if not deleted:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RuleSet not found")
    return None


# --- Rule Endpoints ---

@router.post(
    "/rules",
    response_model=Rule,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new Rule for a RuleSet",
    tags=["Rules"],
)
def create_rule_endpoint(
    *,
    db: Session = Depends(get_db),
    rule_in: RuleCreate,
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Auth
) -> Any:
    """
    Create a new Rule associated with an existing RuleSet.
    """
    # TODO: Add permission checks
    try:
        created_rule = crud_rule.create(db=db, rule=rule_in)
    except ValueError as e: # Catches if ruleset_id doesn't exist
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    return created_rule


@router.get(
    "/rules",
    response_model=List[Rule],
    summary="Retrieve multiple Rules (optionally filter by RuleSet)",
    tags=["Rules"],
)
def read_rules_endpoint(
    db: Session = Depends(get_db),
    ruleset_id: Optional[int] = Query(None, description="Filter by RuleSet ID"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Auth
) -> Any:
    """
    Retrieve a list of Rules. Can be filtered by the parent RuleSet ID.
    """
    # TODO: Add permission checks
    if ruleset_id is not None:
         rules = crud_rule.get_multi_by_ruleset(db=db, ruleset_id=ruleset_id, skip=skip, limit=limit)
    else:
         # TODO: Implement crud_rule.get_multi(db, skip, limit) if needed
         # For now, getting rules without a ruleset filter isn't implemented
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Listing all rules without filtering by ruleset_id is not currently supported.")
         # Alternative: Query all rules across all rulesets (potentially large)
         # rules = db.query(models.Rule).order_by(models.Rule.id).offset(skip).limit(limit).all()
    return rules


@router.get(
    "/rules/{rule_id}",
    response_model=Rule,
    summary="Retrieve a specific Rule by ID",
    tags=["Rules"],
)
def read_rule_endpoint(
    rule_id: int,
    db: Session = Depends(get_db),
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Auth
) -> Any:
    """
    Get details of a specific Rule by its ID.
    """
    db_rule = crud_rule.get(db=db, rule_id=rule_id)
    if db_rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    # TODO: Add permission checks
    return db_rule


@router.put(
    "/rules/{rule_id}",
    response_model=Rule,
    summary="Update a Rule",
    tags=["Rules"],
)
def update_rule_endpoint(
    *,
    db: Session = Depends(get_db),
    rule_id: int,
    rule_in: RuleUpdate,
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Auth
) -> Any:
    """
    Update an existing Rule.
    """
    db_rule = crud_rule.get(db=db, rule_id=rule_id) # Check exists
    if db_rule is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    # TODO: Add permission checks
    updated_rule = crud_rule.update(db=db, rule_id=rule_id, rule_update=rule_in)
    return updated_rule


@router.delete(
    "/rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a Rule",
    tags=["Rules"],
)
def delete_rule_endpoint(
    rule_id: int,
    db: Session = Depends(get_db),
    # current_user: models.User = Depends(deps.get_current_active_user), # TODO: Auth
) -> None:
    """
    Delete a specific Rule. Requires relevant permissions later.
    """
    deleted = crud_rule.delete(db=db, rule_id=rule_id)
    if not deleted:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rule not found")
    return None
