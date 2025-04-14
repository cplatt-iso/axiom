# app/crud/crud_rule.py

from sqlalchemy.orm import Session, joinedload, selectinload, contains_eager
from sqlalchemy import asc, desc, func, update as sql_update, delete as sql_delete
from sqlalchemy.exc import IntegrityError # To catch DB errors
from typing import List, Optional

from app.db.models import RuleSet, Rule, RuleSetExecutionMode
from app.schemas.rule import RuleSetCreate, RuleSetUpdate, RuleCreate, RuleUpdate


# --- RuleSet CRUD ---

def get_ruleset(db: Session, ruleset_id: int) -> Optional[RuleSet]:
    """Gets a single RuleSet by ID, eagerly loading its rules."""
    return (
        db.query(RuleSet)
        .options(selectinload(RuleSet.rules)) # Use selectinload for rules
        .filter(RuleSet.id == ruleset_id)
        .first()
    )

def get_rulesets(db: Session, skip: int = 0, limit: int = 100) -> List[RuleSet]:
    """Gets a list of RuleSets with pagination, eagerly loading rules."""
    return (
        db.query(RuleSet)
        .options(selectinload(RuleSet.rules)) # Use selectinload for rules
        .order_by(asc(RuleSet.priority), asc(RuleSet.name)) # Order by priority, then name
        .offset(skip)
        .limit(limit)
        .all()
    )

def get_active_rulesets_ordered(db: Session) -> list[RuleSet]:
    """
    Retrieves all active RuleSets, ordered by priority (ascending).
    Eagerly loads associated rules, also ordered by their priority.
    (Modified to ensure rules are ordered too)
    """
    return (
        db.query(RuleSet)
        .filter(RuleSet.is_active == True)
        .options(
            selectinload(RuleSet.rules).joinedload(Rule.ruleset, innerjoin=False) # Ensure nested load if needed, but selectinload preferred
            # or use contains_eager if joining explicitly later
        )
        .order_by(asc(RuleSet.priority))
        # Ordering of rules within the relationship happens in the model definition's order_by
        .all()
    )


def create_ruleset(db: Session, ruleset: RuleSetCreate) -> RuleSet:
    """Creates a new RuleSet."""
    try:
        db_ruleset = RuleSet(**ruleset.model_dump()) # Use model_dump() in Pydantic v2
        db.add(db_ruleset)
        db.commit()
        db.refresh(db_ruleset)
        return db_ruleset
    except IntegrityError as e:
        db.rollback()
        # Check if it's a unique constraint violation (e.g., name)
        if "unique constraint" in str(e).lower():
             raise ValueError(f"RuleSet with name '{ruleset.name}' already exists.")
        else:
             raise # Re-raise other integrity errors

def update_ruleset(db: Session, ruleset_id: int, ruleset_update: RuleSetUpdate) -> Optional[RuleSet]:
    """Updates an existing RuleSet."""
    db_ruleset = get_ruleset(db, ruleset_id)
    if not db_ruleset:
        return None

    update_data = ruleset_update.model_dump(exclude_unset=True) # Get only fields that were set
    if not update_data:
         return db_ruleset # Nothing to update

    try:
        # Iterate over fields in the update data and set them on the model instance
        for key, value in update_data.items():
            setattr(db_ruleset, key, value)

        # No need for explicit `db.add` as the object is already in the session
        db.commit()
        db.refresh(db_ruleset)
        return db_ruleset
    except IntegrityError as e:
         db.rollback()
         if "unique constraint" in str(e).lower() and 'name' in update_data:
             raise ValueError(f"RuleSet with name '{update_data['name']}' already exists.")
         else:
             raise

def delete_ruleset(db: Session, ruleset_id: int) -> bool:
    """Deletes a RuleSet and its associated Rules (due to cascade)."""
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == ruleset_id).first()
    if db_ruleset:
        db.delete(db_ruleset)
        db.commit()
        return True
    return False


# --- Rule CRUD ---

def get_rule(db: Session, rule_id: int) -> Optional[Rule]:
    """Gets a single Rule by ID."""
    return db.query(Rule).filter(Rule.id == rule_id).first()

def get_rules_by_ruleset(db: Session, ruleset_id: int, skip: int = 0, limit: int = 100) -> List[Rule]:
    """Gets rules belonging to a specific RuleSet."""
    return (
        db.query(Rule)
        .filter(Rule.ruleset_id == ruleset_id)
        .order_by(asc(Rule.priority))
        .offset(skip)
        .limit(limit)
        .all()
        )


def create_rule(db: Session, rule: RuleCreate) -> Rule:
    """Creates a new Rule for a given RuleSet ID."""
    # Verify ruleset exists first
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == rule.ruleset_id).first()
    if not db_ruleset:
        raise ValueError(f"RuleSet with id {rule.ruleset_id} does not exist.")

    db_rule = Rule(**rule.model_dump())
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule


def update_rule(db: Session, rule_id: int, rule_update: RuleUpdate) -> Optional[Rule]:
    """Updates an existing Rule."""
    db_rule = get_rule(db, rule_id)
    if not db_rule:
        return None

    update_data = rule_update.model_dump(exclude_unset=True)
    if not update_data:
        return db_rule # Nothing to update

    for key, value in update_data.items():
        setattr(db_rule, key, value)

    db.commit()
    db.refresh(db_rule)
    return db_rule


def delete_rule(db: Session, rule_id: int) -> bool:
    """Deletes a Rule."""
    db_rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if db_rule:
        db.delete(db_rule)
        db.commit()
        return True
    return False


# --- CRUD Object instances --- (optional, but common pattern)
class RuleSetCRUDMethods:
    def get(self, db: Session, ruleset_id: int) -> Optional[RuleSet]:
        return get_ruleset(db, ruleset_id)
    def get_multi(self, db: Session, skip: int = 0, limit: int = 100) -> List[RuleSet]:
        return get_rulesets(db, skip=skip, limit=limit)
    def get_active_ordered(self, db: Session) -> list[RuleSet]:
        return get_active_rulesets_ordered(db)
    def create(self, db: Session, ruleset: RuleSetCreate) -> RuleSet:
        return create_ruleset(db, ruleset)
    def update(self, db: Session, ruleset_id: int, ruleset_update: RuleSetUpdate) -> Optional[RuleSet]:
        return update_ruleset(db, ruleset_id, ruleset_update)
    def delete(self, db: Session, ruleset_id: int) -> bool:
        return delete_ruleset(db, ruleset_id)

class RuleCRUDMethods:
    def get(self, db: Session, rule_id: int) -> Optional[Rule]:
        return get_rule(db, rule_id)
    def get_multi_by_ruleset(self, db: Session, ruleset_id: int, skip: int = 0, limit: int = 100) -> List[Rule]:
        return get_rules_by_ruleset(db, ruleset_id, skip=skip, limit=limit)
    def create(self, db: Session, rule: RuleCreate) -> Rule:
        return create_rule(db, rule)
    def update(self, db: Session, rule_id: int, rule_update: RuleUpdate) -> Optional[Rule]:
        return update_rule(db, rule_id, rule_update)
    def delete(self, db: Session, rule_id: int) -> bool:
        return delete_rule(db, rule_id)


ruleset = RuleSetCRUDMethods()
rule = RuleCRUDMethods()
