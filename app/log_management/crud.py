"""
CRUD operations for log management
"""

from sqlalchemy.orm import Session
from typing import List, Optional
from app.log_management import models, schemas


# Retention Policy CRUD
def get_retention_policy(db: Session, policy_id: int) -> Optional[models.LogRetentionPolicy]:
    return db.query(models.LogRetentionPolicy).filter(models.LogRetentionPolicy.id == policy_id).first()


def get_retention_policies(
    db: Session, 
    skip: int = 0, 
    limit: int = 100,
    active_only: bool = True
) -> List[models.LogRetentionPolicy]:
    query = db.query(models.LogRetentionPolicy)
    if active_only:
        query = query.filter(models.LogRetentionPolicy.is_active == True)
    return query.offset(skip).limit(limit).all()


def get_retention_policies_count(db: Session, active_only: bool = True) -> int:
    query = db.query(models.LogRetentionPolicy)
    if active_only:
        query = query.filter(models.LogRetentionPolicy.is_active == True)
    return query.count()


def create_retention_policy(
    db: Session, 
    policy: schemas.LogRetentionPolicyCreate
) -> models.LogRetentionPolicy:
    db_policy = models.LogRetentionPolicy(**policy.dict())
    db.add(db_policy)
    db.commit()
    db.refresh(db_policy)
    return db_policy


def update_retention_policy(
    db: Session,
    policy_id: int,
    policy: schemas.LogRetentionPolicyUpdate
) -> Optional[models.LogRetentionPolicy]:
    db_policy = get_retention_policy(db, policy_id)
    if not db_policy:
        return None
        
    update_data = policy.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_policy, field, value)
    
    db.commit()
    db.refresh(db_policy)
    return db_policy


def delete_retention_policy(db: Session, policy_id: int) -> bool:
    db_policy = get_retention_policy(db, policy_id)
    if not db_policy:
        return False
    
    db.delete(db_policy)
    db.commit()
    return True


# Archival Rule CRUD
def get_archival_rule(db: Session, rule_id: int) -> Optional[models.LogArchivalRule]:
    return db.query(models.LogArchivalRule).filter(models.LogArchivalRule.id == rule_id).first()


def get_archival_rules(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    active_only: bool = True
) -> List[models.LogArchivalRule]:
    query = db.query(models.LogArchivalRule)
    if active_only:
        query = query.filter(models.LogArchivalRule.is_active == True)
    return query.offset(skip).limit(limit).all()


def get_archival_rules_count(db: Session, active_only: bool = True) -> int:
    query = db.query(models.LogArchivalRule)
    if active_only:
        query = query.filter(models.LogArchivalRule.is_active == True)
    return query.count()


def create_archival_rule(
    db: Session,
    rule: schemas.LogArchivalRuleCreate
) -> models.LogArchivalRule:
    db_rule = models.LogArchivalRule(**rule.dict())
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule


def update_archival_rule(
    db: Session,
    rule_id: int,
    rule: schemas.LogArchivalRuleUpdate
) -> Optional[models.LogArchivalRule]:
    db_rule = get_archival_rule(db, rule_id)
    if not db_rule:
        return None
        
    update_data = rule.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_rule, field, value)
    
    db.commit()
    db.refresh(db_rule)
    return db_rule


def delete_archival_rule(db: Session, rule_id: int) -> bool:
    db_rule = get_archival_rule(db, rule_id)
    if not db_rule:
        return False
    
    db.delete(db_rule)
    db.commit()
    return True


# Analytics Config CRUD
def get_analytics_config(db: Session, config_id: int) -> Optional[models.LogAnalyticsConfig]:
    return db.query(models.LogAnalyticsConfig).filter(models.LogAnalyticsConfig.id == config_id).first()


def get_analytics_configs(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    active_only: bool = True
) -> List[models.LogAnalyticsConfig]:
    query = db.query(models.LogAnalyticsConfig)
    if active_only:
        query = query.filter(models.LogAnalyticsConfig.is_active == True)
    return query.offset(skip).limit(limit).all()


def create_analytics_config(
    db: Session,
    config: schemas.LogAnalyticsConfigCreate
) -> models.LogAnalyticsConfig:
    db_config = models.LogAnalyticsConfig(**config.dict())
    db.add(db_config)
    db.commit()
    db.refresh(db_config)
    return db_config


def update_analytics_config(
    db: Session,
    config_id: int,
    config: schemas.LogAnalyticsConfigUpdate
) -> Optional[models.LogAnalyticsConfig]:
    db_config = get_analytics_config(db, config_id)
    if not db_config:
        return None
        
    update_data = config.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_config, field, value)
    
    db.commit()
    db.refresh(db_config)
    return db_config


def delete_analytics_config(db: Session, config_id: int) -> bool:
    db_config = get_analytics_config(db, config_id)
    if not db_config:
        return False
    
    db.delete(db_config)
    db.commit()
    return True
