# app/crud/crud_rule.py

from sqlalchemy.orm import Session, joinedload, selectinload, with_polymorphic
from sqlalchemy import asc, desc, func, update as sql_update, delete as sql_delete, select
from sqlalchemy.exc import IntegrityError
from typing import List, Optional

from app.db.models import RuleSet, Rule, RuleSetExecutionMode
from app.db.models.storage_backend_config import StorageBackendConfig
from app.schemas.rule import RuleSetCreate, RuleSetUpdate, RuleCreate, RuleUpdate


def get_ruleset(db: Session, ruleset_id: int) -> Optional[RuleSet]:
    return (
        db.query(RuleSet)
        .options(
            selectinload(RuleSet.rules).options(
                selectinload(Rule.destinations)
            )
        )
        .filter(RuleSet.id == ruleset_id)
        .first()
    )

def get_rulesets(db: Session, skip: int = 0, limit: int = 100) -> List[RuleSet]:
    return (
        db.query(RuleSet)
        .options(
            selectinload(RuleSet.rules).options(
                selectinload(Rule.destinations)
            )
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
            selectinload(RuleSet.rules).options(
                selectinload(Rule.destinations)
            )
        )
        .order_by(asc(RuleSet.priority))
        .all()
    )


def get_rule(db: Session, rule_id: int) -> Optional[Rule]:
    return (
        db.query(Rule)
        .options(
            selectinload(Rule.destinations)
        )
        .filter(Rule.id == rule_id)
        .first()
    )

def get_rules_by_ruleset(db: Session, ruleset_id: int, skip: int = 0, limit: int = 100) -> List[Rule]:
    return (
        db.query(Rule)
        .filter(Rule.ruleset_id == ruleset_id)
        .options(
            selectinload(Rule.destinations)
        )
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
        destinations = db.execute(
            select(StorageBackendConfig).where(StorageBackendConfig.id.in_(destination_ids))
        ).scalars().all()
        if len(destinations) != len(set(destination_ids)):
             found_ids = {d.id for d in destinations}
             missing_ids = [id for id in destination_ids if id not in found_ids]
             raise ValueError(f"One or more destination IDs not found: {missing_ids}")
        db_rule.destinations = destinations
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    # Refresh relationships after commit if needed
    db.refresh(db_rule, attribute_names=['destinations'])
    return db_rule


def update_rule(db: Session, rule_id: int, rule_update: RuleUpdate) -> Optional[Rule]:
    # Fetch rule with destinations (using plain selectinload now)
    db_rule = (
        db.query(Rule)
        .options(selectinload(Rule.destinations))
        .filter(Rule.id == rule_id)
        .first()
    )
    if not db_rule: return None

    destination_ids = rule_update.destination_ids
    update_data = rule_update.model_dump(exclude={'destination_ids'}, exclude_unset=True)

    if update_data:
        for key, value in update_data.items():
            setattr(db_rule, key, value)

    if destination_ids is not None:
        if not destination_ids:
            db_rule.destinations = []
        else:
            destinations = db.execute(
                select(StorageBackendConfig).where(StorageBackendConfig.id.in_(destination_ids))
            ).scalars().all()
            if len(destinations) != len(set(destination_ids)):
                found_ids = {d.id for d in destinations}
                missing_ids = [id for id in destination_ids if id not in found_ids]
                raise ValueError(f"Dest IDs not found during update: {missing_ids}")
            db_rule.destinations = destinations

    if update_data or destination_ids is not None:
        try:
            db.commit()
            db.refresh(db_rule)
            # Re-fetch to ensure response model gets fully loaded data
            # This is often safer after relationship manipulation + commit
            return get_rule(db, rule_id)
        except IntegrityError as e:
             db.rollback()
             raise
    else:
         # No changes made, return the originally fetched object
         return db_rule


def delete_rule(db: Session, rule_id: int) -> bool:
    db_rule = db.query(Rule).filter(Rule.id == rule_id).first()
    if db_rule:
        db.delete(db_rule)
        db.commit()
        return True
    return False


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


ruleset = RuleSetCRUDMethods()
rule = RuleCRUDMethods()
