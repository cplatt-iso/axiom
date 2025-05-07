# filename: app/crud/crud_rule.py

from sqlalchemy.orm import Session, joinedload, selectinload, with_polymorphic
from sqlalchemy import asc, desc, func, update as sql_update, delete as sql_delete, select
from sqlalchemy.exc import IntegrityError # To catch DB errors
from typing import List, Optional

from app.db.models import RuleSet, Rule, RuleSetExecutionMode
from app.db.models.storage_backend_config import StorageBackendConfig
from app.schemas.rule import RuleSetCreate, RuleSetUpdate, RuleCreate, RuleUpdate

# --- RuleSet CRUD Functions ---
# These were missing in the previous version you showed, re-adding them from your reference.

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
        if "unique constraint" in str(e).lower() and "name" in ruleset.model_dump(): # Check if name was part of the input
             raise ValueError(f"RuleSet with name '{ruleset.name}' already exists.")
        # Consider logging the original error e before raising a generic or specific one
        # logger.error(f"Database integrity error during ruleset creation: {e}", exc_info=True)
        raise # Re-raise the original error or a more specific one

def update_ruleset(db: Session, ruleset_id: int, ruleset_update: RuleSetUpdate) -> Optional[RuleSet]:
    """Updates an existing RuleSet."""
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == ruleset_id).first()
    if not db_ruleset:
        return None

    update_data = ruleset_update.model_dump(exclude_unset=True)
    if not update_data: # No actual data provided for update
         return db_ruleset # Return the object as is

    try:
        for key, value in update_data.items():
            setattr(db_ruleset, key, value)
        # db.add(db_ruleset) # Not strictly necessary if only updating fields on an existing tracked object
        db.commit()
        db.refresh(db_ruleset)
        return db_ruleset
    except IntegrityError as e:
         db.rollback()
         # Check if the error is due to a unique constraint on 'name' if 'name' was being updated
         if "unique constraint" in str(e).lower() and 'name' in update_data:
             raise ValueError(f"Cannot update RuleSet: name '{update_data['name']}' already exists.")
         # logger.error(f"Database integrity error during ruleset update for ID {ruleset_id}: {e}", exc_info=True)
         raise

def delete_ruleset(db: Session, ruleset_id: int) -> bool:
    """Deletes a RuleSet and its associated Rules (due to cascade ondelete in Rule model)."""
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == ruleset_id).first()
    if db_ruleset:
        db.delete(db_ruleset)
        db.commit()
        return True
    return False


# --- Rule CRUD Functions ---
# These functions were present in the "broken" version you sent and seem mostly okay.
# Keeping them as they were, with minor consistency adjustments if any.

def get_rule(db: Session, rule_id: int) -> Optional[Rule]:
    """Gets a single Rule by ID, eagerly loading its destinations."""
    return (
        db.query(Rule)
        .options(selectinload(Rule.destinations))
        .filter(Rule.id == rule_id)
        .first()
    )

def get_rules_by_ruleset(db: Session, ruleset_id: int, skip: int = 0, limit: int = 100) -> List[Rule]:
    """Gets rules belonging to a specific RuleSet, eagerly loading destinations."""
    return (
        db.query(Rule)
        .filter(Rule.ruleset_id == ruleset_id)
        .options(selectinload(Rule.destinations))
        .order_by(asc(Rule.priority))
        .offset(skip)
        .limit(limit)
        .all()
        )

def create_rule(db: Session, rule: RuleCreate) -> Rule:
    """Creates a new Rule for a given RuleSet ID, associating destinations."""
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == rule.ruleset_id).first()
    if not db_ruleset:
        raise ValueError(f"RuleSet with id {rule.ruleset_id} does not exist.")

    destination_ids = rule.destination_ids or []
    rule_data = rule.model_dump(exclude={'destination_ids'})
    db_rule = Rule(**rule_data)

    if destination_ids:
        # Ensure unique destination IDs before querying to avoid issues
        unique_destination_ids = list(set(destination_ids))
        destinations = db.execute(
            select(StorageBackendConfig).where(StorageBackendConfig.id.in_(unique_destination_ids))
        ).scalars().all()

        if len(destinations) != len(unique_destination_ids):
             found_ids = {d.id for d in destinations}
             missing_ids = [id_ for id_ in unique_destination_ids if id_ not in found_ids]
             raise ValueError(f"One or more destination IDs not found: {missing_ids}")
        db_rule.destinations = destinations
    else:
        db_rule.destinations = [] # Explicitly set to empty list if no IDs

    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    # Ensure relationships are fresh, especially after manual list appends.
    # selectinload on subsequent gets might be preferred over manual refresh here.
    # db.refresh(db_rule, attribute_names=['destinations']) 
    return db_rule


def update_rule(db: Session, rule_id: int, rule_update: RuleUpdate) -> Optional[Rule]:
    """Updates an existing Rule, including its associated destinations."""
    db_rule = (
        db.query(Rule)
        .options(selectinload(Rule.destinations)) # Load current destinations
        .filter(Rule.id == rule_id)
        .first()
    )
    if not db_rule:
        return None

    destination_ids = rule_update.destination_ids # This can be None, an empty list, or list of IDs
    update_data = rule_update.model_dump(exclude={'destination_ids'}, exclude_unset=True)

    if update_data:
        for key, value in update_data.items():
            setattr(db_rule, key, value)

    if destination_ids is not None: # If destination_ids is part of the update payload
        if not destination_ids: # An empty list means clear all destinations
            db_rule.destinations = []
        else:
            unique_destination_ids = list(set(destination_ids))
            destinations = db.execute(
                select(StorageBackendConfig).where(StorageBackendConfig.id.in_(unique_destination_ids))
            ).scalars().all()
            if len(destinations) != len(unique_destination_ids):
                found_ids = {d.id for d in destinations}
                missing_ids = [id_ for id_ in unique_destination_ids if id_ not in found_ids]
                raise ValueError(f"Destination IDs not found during update: {missing_ids}")
            db_rule.destinations = destinations # Assign the new list

    # Only commit if there were actual changes from update_data or if destination_ids was provided
    if update_data or destination_ids is not None:
        try:
            db.commit()
            db.refresh(db_rule)
            # Refreshing relationships explicitly might be good practice
            # db.refresh(db_rule, attribute_names=['destinations'])
            # For response consistency, re-fetch using the get_rule function
            # which handles eager loading as defined for reads.
            return get_rule(db, rule_id) 
        except IntegrityError as e:
             db.rollback()
             # logger.error(f"Database integrity error during rule update for ID {rule_id}: {e}", exc_info=True)
             raise
    else:
         # No changes were made to rule fields or destinations
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
# These classes will now correctly call the globally defined functions.

class RuleSetCRUDMethods:
    def get(self, db: Session, ruleset_id: int) -> Optional[RuleSet]: return get_ruleset(db, ruleset_id)
    def get_multi(self, db: Session, skip: int = 0, limit: int = 100) -> List[RuleSet]: return get_rulesets(db, skip=skip, limit=limit)
    def get_active_ordered(self, db: Session) -> list[RuleSet]: return get_active_rulesets_ordered(db)
    def create(self, db: Session, ruleset: RuleSetCreate) -> RuleSet: return create_ruleset(db, ruleset) # Now calls defined function
    def update(self, db: Session, ruleset_id: int, ruleset_update: RuleSetUpdate) -> Optional[RuleSet]: return update_ruleset(db, ruleset_id, ruleset_update) # Now calls defined function
    def delete(self, db: Session, ruleset_id: int) -> bool: return delete_ruleset(db, ruleset_id) # Now calls defined function

class RuleCRUDMethods:
    def get(self, db: Session, rule_id: int) -> Optional[Rule]: return get_rule(db, rule_id)
    def get_multi_by_ruleset(self, db: Session, ruleset_id: int, skip: int = 0, limit: int = 100) -> List[Rule]: return get_rules_by_ruleset(db, ruleset_id, skip=skip, limit=limit)
    def create(self, db: Session, rule: RuleCreate) -> Rule: return create_rule(db, rule)
    def update(self, db: Session, rule_id: int, rule_update: RuleUpdate) -> Optional[Rule]: return update_rule(db, rule_id, rule_update)
    def delete(self, db: Session, rule_id: int) -> bool: return delete_rule(db, rule_id)


ruleset = RuleSetCRUDMethods()
rule = RuleCRUDMethods()
