# backend/app/crud/crud_rule.py

from sqlalchemy.orm import Session, selectinload
from sqlalchemy import asc, desc, func, update as sql_update, delete as sql_delete, select
from sqlalchemy.exc import IntegrityError 
from typing import List, Optional

# Assuming your model imports are structured like this:
from app.db.models import RuleSet, Rule # RuleSetExecutionMode might not be directly used in this file
from app.db.models.storage_backend_config import StorageBackendConfig
from app.schemas.rule import RuleSetCreate, RuleSetUpdate, RuleCreate, RuleUpdate

# --- RuleSet CRUD Functions ---

def get_ruleset(db: Session, ruleset_id: int) -> Optional[RuleSet]:
    return (
        db.query(RuleSet)
        .options(
            selectinload(RuleSet.rules)
            .selectinload(Rule.destinations)
        )
        .filter(RuleSet.id == ruleset_id)
        .first()
    )

def get_rulesets(db: Session, skip: int = 0, limit: int = 100) -> List[RuleSet]:
    return (
        db.query(RuleSet)
        .options(
            selectinload(RuleSet.rules)
            .selectinload(Rule.destinations) 
        )
        .order_by(asc(RuleSet.priority), asc(RuleSet.name))
        .offset(skip)
        .limit(limit)
        .all()
    )

def get_active_rulesets_ordered(db: Session) -> list[RuleSet]:
    return (
        db.query(RuleSet)
        .filter(RuleSet.is_active == True)
        .options(
            selectinload(RuleSet.rules)
            .selectinload(Rule.destinations)
        )
        .order_by(asc(RuleSet.priority))
        .all()
    )

def create_ruleset(db: Session, ruleset: RuleSetCreate) -> RuleSet:
    try:
        db_ruleset = RuleSet(**ruleset.model_dump())
        db.add(db_ruleset)
        db.commit()
        db.refresh(db_ruleset)
        return db_ruleset
    except IntegrityError as e:
        db.rollback()
        if "unique constraint" in str(e).lower() and "name" in ruleset.model_dump():
             raise ValueError(f"RuleSet with name '{ruleset.name}' already exists.")
        raise 

def update_ruleset(db: Session, ruleset_id: int, ruleset_update: RuleSetUpdate) -> Optional[RuleSet]:
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
             raise ValueError(f"Cannot update RuleSet: name '{update_data['name']}' already exists.")
         raise

def delete_ruleset(db: Session, ruleset_id: int) -> bool:
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == ruleset_id).first()
    if db_ruleset:
        db.delete(db_ruleset)
        db.commit()
        return True
    return False


# --- Rule CRUD Functions ---

def get_rule(db: Session, rule_id: int) -> Optional[Rule]:
    return (
        db.query(Rule)
        .options(selectinload(Rule.destinations))
        .filter(Rule.id == rule_id)
        .first()
    )

def get_rules_by_ruleset(db: Session, ruleset_id: int, skip: int = 0, limit: int = 100) -> List[Rule]:
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
    db_ruleset = db.query(RuleSet).filter(RuleSet.id == rule.ruleset_id).first()
    if not db_ruleset:
        raise ValueError(f"RuleSet with id {rule.ruleset_id} does not exist.")

    destination_ids = rule.destination_ids or []
    rule_data = rule.model_dump(exclude={'destination_ids'})
    db_rule = Rule(**rule_data)

    if destination_ids:
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
        db_rule.destinations = []

    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule


def update_rule(db: Session, rule_id: int, rule_update: RuleUpdate) -> Optional[Rule]:
    db_rule = (
        db.query(Rule)
        .options(selectinload(Rule.destinations))
        .filter(Rule.id == rule_id)
        .first()
    )
    if not db_rule:
        return None

    destination_ids = rule_update.destination_ids 
    update_data = rule_update.model_dump(exclude={'destination_ids'}, exclude_unset=True)

    if update_data:
        for key, value in update_data.items():
            setattr(db_rule, key, value)

    if destination_ids is not None: 
        if not destination_ids: 
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
            db_rule.destinations = destinations

    if update_data or destination_ids is not None:
        try:
            db.commit()
            db.refresh(db_rule)
            return get_rule(db, rule_id) 
        except IntegrityError as e:
             db.rollback()
             raise
    else:
         return db_rule


def delete_rule(db: Session, rule_id: int) -> bool:
    db_rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if db_rule:
        db.delete(db_rule)
        db.commit()
        return True
    return False

# --- NEW FUNCTION ---
def get_rules_by_ai_prompt_config_id(db: Session, *, config_id: int) -> List[Rule]:
    """
    Retrieves all rules that have the given config_id in their
    ai_prompt_config_ids list.
    Assumes Rule.ai_prompt_config_ids is a JSONB column storing an array of integers (PostgreSQL).
    """
    # This query uses jsonb_path_exists for PostgreSQL.
    # It checks if a path exists in the JSONB document (Rule.ai_prompt_config_ids)
    # where an array element ('@') equals the provided 'config_id' (passed as '$id').
    statement = select(Rule).filter(
        func.jsonb_path_exists(
            Rule.ai_prompt_config_ids,
            '$[*] ? (@ == $id)',  # JSONPath: any array element equal to variable $id
            func.jsonb_build_object('id', config_id)  # Variables: {"id": <config_id_value>}
        )
    )
    return db.execute(statement).scalars().all()


# --- CRUD Object instances ---

class RuleSetCRUDMethods:
    def get(self, db: Session, ruleset_id: int) -> Optional[RuleSet]: return get_ruleset(db, ruleset_id)
    def get_multi(self, db: Session, skip: int = 0, limit: int = 100) -> List[RuleSet]: return get_rulesets(db, skip=skip, limit=limit)
    def get_active_ordered(self, db: Session) -> list[RuleSet]: return get_active_rulesets_ordered(db)
    def create(self, db: Session, ruleset: RuleSetCreate) -> RuleSet: return create_ruleset(db, ruleset)
    def update(self, db: Session, ruleset_id: int, ruleset_update: RuleSetUpdate) -> Optional[RuleSet]: return update_ruleset(db, ruleset_id, ruleset_update)
    def delete(self, db: Session, ruleset_id: int) -> bool: return delete_ruleset(db, ruleset_id)

class RuleCRUDMethods:
    def get(self, db: Session, rule_id: int) -> Optional[Rule]: return get_rule(db, rule_id)
    def get_multi_by_ruleset(self, db: Session, ruleset_id: int, skip: int = 0, limit: int = 100) -> List[Rule]: return get_rules_by_ruleset(db, ruleset_id, skip=skip, limit=limit)
    def create(self, db: Session, rule: RuleCreate) -> Rule: return create_rule(db, rule)
    def update(self, db: Session, rule_id: int, rule_update: RuleUpdate) -> Optional[Rule]: return update_rule(db, rule_id, rule_update)
    def delete(self, db: Session, rule_id: int) -> bool: return delete_rule(db, rule_id)
    # Add the new method here so it's accessible via `crud.crud_rule.get_rules_by_ai_prompt_config_id`
    def get_rules_by_ai_prompt_config_id(self, db: Session, *, config_id: int) -> List[Rule]:
        return get_rules_by_ai_prompt_config_id(db, config_id=config_id) # Calls the top-level function

# Instantiate the CRUD objects
ruleset = RuleSetCRUDMethods()
rule = RuleCRUDMethods() # This instance will now have the new method