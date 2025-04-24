# app/crud/crud_rule.py

from sqlalchemy.orm import Session, joinedload, selectinload, contains_eager
from sqlalchemy import asc, desc, func, update as sql_update, delete as sql_delete, select # Import select
from sqlalchemy.exc import IntegrityError # To catch DB errors
from typing import List, Optional

# --- ADDED: Import StorageBackendConfig model ---
from app.db.models import RuleSet, Rule, RuleSetExecutionMode, StorageBackendConfig
# --- END ADDED ---
from app.schemas.rule import RuleSetCreate, RuleSetUpdate, RuleCreate, RuleUpdate


# --- RuleSet CRUD ---

def get_ruleset(db: Session, ruleset_id: int) -> Optional[RuleSet]:
    """Gets a single RuleSet by ID, eagerly loading its rules and their destinations."""
    return (
        db.query(RuleSet)
        .options(
            selectinload(RuleSet.rules) # Load rules for the ruleset
            .selectinload(Rule.destinations) # Then load destinations for each rule
        )
        .filter(RuleSet.id == ruleset_id)
        .first()
    )

def get_rulesets(db: Session, skip: int = 0, limit: int = 100) -> List[RuleSet]:
    """Gets a list of RuleSets with pagination, eagerly loading rules and their destinations."""
    return (
        db.query(RuleSet)
        .options(
            selectinload(RuleSet.rules) # Load rules for each ruleset
            .selectinload(Rule.destinations) # Then load destinations for each rule
        )
        .order_by(asc(RuleSet.priority), asc(RuleSet.name)) # Order by priority, then name
        .offset(skip)
        .limit(limit)
        .all()
    )

def get_active_rulesets_ordered(db: Session) -> list[RuleSet]:
    """
    Retrieves all active RuleSets, ordered by priority (ascending).
    Eagerly loads associated rules (ordered by priority) and their destinations.
    """
    return (
        db.query(RuleSet)
        .filter(RuleSet.is_active == True)
        .options(
            selectinload(RuleSet.rules) # Load rules for each ruleset
            .selectinload(Rule.destinations) # Then load destinations for each rule
        )
        .order_by(asc(RuleSet.priority))
        # Rule ordering is handled by the relationship definition in the model
        .all()
    )


def create_ruleset(db: Session, ruleset: RuleSetCreate) -> RuleSet:
    """Creates a new RuleSet."""
    try:
        db_ruleset = RuleSet(**ruleset.model_dump())
        db.add(db_ruleset)
        db.commit()
        db.refresh(db_ruleset)
        return db_ruleset
    except IntegrityError as e:
        db.rollback()
        if "unique constraint" in str(e).lower():
             raise ValueError(f"RuleSet with name '{ruleset.name}' already exists.")
        else:
             raise

def update_ruleset(db: Session, ruleset_id: int, ruleset_update: RuleSetUpdate) -> Optional[RuleSet]:
    """Updates an existing RuleSet."""
    # Use the simple get here, no need to load rules/destinations for ruleset update
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == ruleset_id).first()
    if not db_ruleset:
        return None

    update_data = ruleset_update.model_dump(exclude_unset=True)
    if not update_data:
         return db_ruleset

    try:
        for key, value in update_data.items():
            setattr(db_ruleset, key, value)
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
    """Gets a single Rule by ID, eagerly loading its destinations."""
    return (
        db.query(Rule)
        .options(selectinload(Rule.destinations)) # Eager load destinations
        .filter(Rule.id == rule_id)
        .first()
    )

def get_rules_by_ruleset(db: Session, ruleset_id: int, skip: int = 0, limit: int = 100) -> List[Rule]:
    """Gets rules belonging to a specific RuleSet, eagerly loading destinations."""
    return (
        db.query(Rule)
        .filter(Rule.ruleset_id == ruleset_id)
        .options(selectinload(Rule.destinations)) # Eager load destinations
        .order_by(asc(Rule.priority))
        .offset(skip)
        .limit(limit)
        .all()
        )


def create_rule(db: Session, rule: RuleCreate) -> Rule:
    """Creates a new Rule for a given RuleSet ID, associating destinations."""
    # Verify ruleset exists first
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == rule.ruleset_id).first()
    if not db_ruleset:
        raise ValueError(f"RuleSet with id {rule.ruleset_id} does not exist.")

    # Separate destination IDs from other rule data
    destination_ids = rule.destination_ids or []
    rule_data = rule.model_dump(exclude={'destination_ids'})

    db_rule = Rule(**rule_data)

    # Fetch and associate destination objects
    if destination_ids:
        destinations = db.execute(
            select(StorageBackendConfig).where(StorageBackendConfig.id.in_(destination_ids))
        ).scalars().all()
        if len(destinations) != len(destination_ids):
             # Find missing IDs for a better error message
             found_ids = {d.id for d in destinations}
             missing_ids = [id for id in destination_ids if id not in found_ids]
             raise ValueError(f"One or more destination IDs not found: {missing_ids}")
        db_rule.destinations = destinations # Assign the list of DB objects

    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    # Eager load destinations after refresh if needed, although selectinload on get might be sufficient
    # db.refresh(db_rule, attribute_names=['destinations'])
    return db_rule


def update_rule(db: Session, rule_id: int, rule_update: RuleUpdate) -> Optional[Rule]:
    """Updates an existing Rule, including its associated destinations."""
    # Fetch the rule with its current destinations eagerly loaded
    db_rule = get_rule(db, rule_id)
    if not db_rule:
        return None

    # Separate destination IDs from other update data
    destination_ids = rule_update.destination_ids
    update_data = rule_update.model_dump(exclude={'destination_ids'}, exclude_unset=True)

    # Update standard fields
    if update_data:
        for key, value in update_data.items():
            setattr(db_rule, key, value)

    # Update destinations relationship if destination_ids is provided (even if empty list)
    if destination_ids is not None:
        if not destination_ids: # Empty list means remove all destinations
            db_rule.destinations = []
        else:
            # Fetch the new set of destination objects
            destinations = db.execute(
                select(StorageBackendConfig).where(StorageBackendConfig.id.in_(destination_ids))
            ).scalars().all()
            if len(destinations) != len(destination_ids):
                found_ids = {d.id for d in destinations}
                missing_ids = [id for id in destination_ids if id not in found_ids]
                raise ValueError(f"One or more destination IDs not found during update: {missing_ids}")
            # Replace the entire list of destinations
            db_rule.destinations = destinations

    # Only commit if there were actual changes
    if update_data or destination_ids is not None:
        db.commit()
        db.refresh(db_rule)
        # Eager load destinations after refresh if needed
        # db.refresh(db_rule, attribute_names=['destinations'])

    return db_rule


def delete_rule(db: Session, rule_id: int) -> bool:
    """Deletes a Rule."""
    db_rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if db_rule:
        db.delete(db_rule)
        db.commit()
        return True
    return False


# --- CRUD Object instances ---
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
