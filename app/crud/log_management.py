"""
CRUD operations for log management models.

Provides database operations for log retention policies, archival rules,
and analytics configurations.
"""

from typing import List, Optional
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import and_

from ..log_management.models import (
    LogRetentionPolicy,
    LogArchivalRule,
    LogAnalyticsConfig,
)
from ..schemas.log_management import (
    LogRetentionPolicyCreate,
    LogRetentionPolicyUpdate,
    LogArchivalRuleCreate,
    LogArchivalRuleUpdate,
    LogAnalyticsConfigCreate,
    LogAnalyticsConfigUpdate,
)


# Log Retention Policy CRUD
def create_retention_policy(
    db: Session, policy: LogRetentionPolicyCreate
) -> LogRetentionPolicy:
    """Create a new log retention policy."""
    db_policy = LogRetentionPolicy(
        name=policy.name,
        description=policy.description,
        service_pattern=policy.service_pattern,
        log_level_filter=policy.log_level_filter,
        tier=policy.tier,
        hot_days=policy.hot_days,
        warm_days=policy.warm_days,
        cold_days=policy.cold_days,
        delete_days=policy.delete_days,
        max_index_size_gb=policy.max_index_size_gb,
        max_index_age_days=policy.max_index_age_days,
        storage_class_hot=policy.storage_class_hot,
        storage_class_warm=policy.storage_class_warm,
        storage_class_cold=policy.storage_class_cold,
        is_active=policy.is_active,
        updated_at=datetime.now(timezone.utc),
    )
    
    db.add(db_policy)
    db.commit()
    db.refresh(db_policy)
    return db_policy


def get_retention_policy(db: Session, policy_id: int) -> Optional[LogRetentionPolicy]:
    """Get a retention policy by ID."""
    return db.query(LogRetentionPolicy).filter(
        LogRetentionPolicy.id == policy_id
    ).first()


def get_retention_policy_by_name(
    db: Session, name: str
) -> Optional[LogRetentionPolicy]:
    """Get a retention policy by name."""
    return db.query(LogRetentionPolicy).filter(
        LogRetentionPolicy.name == name
    ).first()


def get_retention_policies(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False
) -> List[LogRetentionPolicy]:
    """Get retention policies with optional filtering."""
    query = db.query(LogRetentionPolicy)
    
    if active_only:
        query = query.filter(LogRetentionPolicy.is_active == True)
    
    return query.offset(skip).limit(limit).all()


def update_retention_policy(
    db: Session,
    policy_id: int,
    policy_update: LogRetentionPolicyUpdate
) -> Optional[LogRetentionPolicy]:
    """Update a retention policy."""
    db_policy = get_retention_policy(db, policy_id)
    if not db_policy:
        return None
    
    # Update only provided fields
    update_data = policy_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_policy, field, value)
    
    db_policy.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(db_policy)
    return db_policy


def delete_retention_policy(db: Session, policy_id: int) -> bool:
    """Delete a retention policy."""
    db_policy = get_retention_policy(db, policy_id)
    if not db_policy:
        return False
    
    db.delete(db_policy)
    db.commit()
    return True


# Log Archival Rule CRUD
def create_archival_rule(
    db: Session, rule: LogArchivalRuleCreate
) -> LogArchivalRule:
    """Create a new log archival rule."""
    db_rule = LogArchivalRule(
        name=rule.name,
        description=rule.description,
        service_pattern=rule.service_pattern,
        age_threshold_days=rule.age_threshold_days,
        storage_backend=rule.storage_backend,
        storage_bucket=rule.storage_bucket,
        storage_path_prefix=rule.storage_path_prefix,
        compression=rule.compression,
        format_type=rule.format_type,
        retention_days=rule.retention_days,
        delete_after_archive=rule.delete_after_archive,
        is_active=rule.is_active,
        cron_schedule=rule.cron_schedule,
        updated_at=datetime.now(timezone.utc),
    )
    
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule


def get_archival_rule(db: Session, rule_id: int) -> Optional[LogArchivalRule]:
    """Get an archival rule by ID."""
    return db.query(LogArchivalRule).filter(
        LogArchivalRule.id == rule_id
    ).first()


def get_archival_rule_by_name(
    db: Session, name: str
) -> Optional[LogArchivalRule]:
    """Get an archival rule by name."""
    return db.query(LogArchivalRule).filter(
        LogArchivalRule.name == name
    ).first()


def get_archival_rules(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False
) -> List[LogArchivalRule]:
    """Get archival rules with optional filtering."""
    query = db.query(LogArchivalRule)
    
    if active_only:
        query = query.filter(LogArchivalRule.is_active == True)
    
    return query.offset(skip).limit(limit).all()


def update_archival_rule(
    db: Session,
    rule_id: int,
    rule_update: LogArchivalRuleUpdate
) -> Optional[LogArchivalRule]:
    """Update an archival rule."""
    db_rule = get_archival_rule(db, rule_id)
    if not db_rule:
        return None
    
    # Update only provided fields
    update_data = rule_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_rule, field, value)
    
    db_rule.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(db_rule)
    return db_rule


def delete_archival_rule(db: Session, rule_id: int) -> bool:
    """Delete an archival rule."""
    db_rule = get_archival_rule(db, rule_id)
    if not db_rule:
        return False
    
    db.delete(db_rule)
    db.commit()
    return True


# Log Analytics Config CRUD
def create_analytics_config(
    db: Session, config: LogAnalyticsConfigCreate
) -> LogAnalyticsConfig:
    """Create a new log analytics configuration."""
    db_config = LogAnalyticsConfig(
        name=config.name,
        description=config.description,
        query_pattern=config.query_pattern,
        aggregation_window=config.aggregation_window,
        alert_threshold=config.alert_threshold,
        alert_operator=config.alert_operator,
        notification_channels=config.notification_channels,
        is_active=config.is_active,
        updated_at=datetime.now(timezone.utc),
    )
    
    db.add(db_config)
    db.commit()
    db.refresh(db_config)
    return db_config


def get_analytics_config(db: Session, config_id: int) -> Optional[LogAnalyticsConfig]:
    """Get an analytics configuration by ID."""
    return db.query(LogAnalyticsConfig).filter(
        LogAnalyticsConfig.id == config_id
    ).first()


def get_analytics_config_by_name(
    db: Session, name: str
) -> Optional[LogAnalyticsConfig]:
    """Get an analytics configuration by name."""
    return db.query(LogAnalyticsConfig).filter(
        LogAnalyticsConfig.name == name
    ).first()


def get_analytics_configs(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False
) -> List[LogAnalyticsConfig]:
    """Get analytics configurations with optional filtering."""
    query = db.query(LogAnalyticsConfig)
    
    if active_only:
        query = query.filter(LogAnalyticsConfig.is_active == True)
    
    return query.offset(skip).limit(limit).all()


def update_analytics_config(
    db: Session,
    config_id: int,
    config_update: LogAnalyticsConfigUpdate
) -> Optional[LogAnalyticsConfig]:
    """Update an analytics configuration."""
    db_config = get_analytics_config(db, config_id)
    if not db_config:
        return None
    
    # Update only provided fields
    update_data = config_update.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_config, field, value)
    
    db_config.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(db_config)
    return db_config


def delete_analytics_config(db: Session, config_id: int) -> bool:
    """Delete an analytics configuration."""
    db_config = get_analytics_config(db, config_id)
    if not db_config:
        return False
    
    db.delete(db_config)
    db.commit()
    return True


# Utility functions for complex queries
def get_policies_for_service(
    db: Session, service_pattern: str
) -> List[LogRetentionPolicy]:
    """Get all active retention policies that match a service pattern."""
    return db.query(LogRetentionPolicy).filter(
        and_(
            LogRetentionPolicy.is_active == True,
            LogRetentionPolicy.service_pattern.like(f"%{service_pattern}%")
        )
    ).all()


def get_expired_archival_rules(
    db: Session, current_time: datetime
) -> List[LogArchivalRule]:
    """Get archival rules that should be executed based on their schedule."""
    # This would need more complex logic to parse cron schedules
    # For now, return active rules that haven't run recently
    return db.query(LogArchivalRule).filter(
        and_(
            LogArchivalRule.is_active == True,
            LogArchivalRule.last_run.is_(None)
        )
    ).all()


def update_archival_rule_last_run(
    db: Session, rule_id: int, run_time: datetime
) -> bool:
    """Update the last run timestamp for an archival rule."""
    db_rule = get_archival_rule(db, rule_id)
    if not db_rule:
        return False
    
    db_rule.last_run = run_time
    db_rule.updated_at = datetime.now(timezone.utc)
    db.commit()
    return True
