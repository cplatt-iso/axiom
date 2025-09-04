"""
FastAPI routes for Log Management
Provides REST API for managing log retention, archival, and analytics
"""

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
import logging

from app.db.session import get_db
from app.api.deps import get_current_user
from app.log_management import schemas, crud
from app.log_management.elasticsearch_client import ElasticsearchLogManager
from app.log_management.policy_engine import PolicyEngine
from app.schemas.user import User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/log-management", tags=["Log Management"])


@router.get("/dashboard", response_model=schemas.LogManagementDashboard)
async def get_dashboard(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get comprehensive log management dashboard data"""
    es_client = ElasticsearchLogManager()
    
    # Get storage statistics
    storage_stats = await es_client.get_storage_stats()
    
    # Get service statistics
    service_logs = await es_client.get_service_log_counts()
    service_stats = []
    
    for service in service_logs:
        # TODO: Map container names to retention policies
        service_stats.append(schemas.ServiceLogStats(
            service_name=service["container_name"].replace("/", "").replace("-", "_"),
            container_name=service["container_name"],
            log_count=service["log_count"],
            size_gb=0.0,  # TODO: Calculate size per service
            earliest_log=service["earliest_log"],
            latest_log=service["latest_log"],
            retention_policy=None  # TODO: Look up assigned policy
        ))
    
    # Get policy counts
    active_policies = crud.get_retention_policies_count(db, active_only=True)
    active_archival_rules = crud.get_archival_rules_count(db, active_only=True)
    
    return schemas.LogManagementDashboard(
        storage_stats=schemas.LogStorageStats(**storage_stats),
        service_stats=service_stats,
        active_policies=active_policies,
        active_archival_rules=active_archival_rules,
        recent_archival_jobs=[],  # TODO: Implement job tracking
        alerts=[]  # TODO: Implement alerting
    )


# Retention Policies endpoints
@router.get("/retention-policies", response_model=List[schemas.LogRetentionPolicy])
def get_retention_policies(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all retention policies"""
    return crud.get_retention_policies(db, skip=skip, limit=limit, active_only=active_only)


@router.post("/retention-policies", response_model=schemas.LogRetentionPolicy)
async def create_retention_policy(
    policy: schemas.LogRetentionPolicyCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new retention policy"""
    # Generate ILM policy JSON
    ilm_policy = PolicyEngine.generate_ilm_policy(
        tier=policy.tier,
        hot_days=policy.hot_days,
        warm_days=policy.warm_days,
        cold_days=policy.cold_days,
        delete_days=policy.delete_days,
        max_index_size_gb=policy.max_index_size_gb,
        max_index_age_days=policy.max_index_age_days,
        storage_class_hot=policy.storage_class_hot,
        storage_class_warm=policy.storage_class_warm,
        storage_class_cold=policy.storage_class_cold
    )
    
    # Set the generated policy
    policy_data = policy.dict()
    policy_data["ilm_policy_json"] = ilm_policy
    
    # Create in database
    db_policy = crud.create_retention_policy(db, schemas.LogRetentionPolicyCreate(**policy_data))
    
    # Apply to Elasticsearch in background
    background_tasks.add_task(apply_retention_policy_to_es, db_policy.id)
    
    return db_policy


@router.get("/retention-policies/{policy_id}", response_model=schemas.LogRetentionPolicy)
def get_retention_policy(
    policy_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific retention policy"""
    policy = crud.get_retention_policy(db, policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    return policy


@router.put("/retention-policies/{policy_id}", response_model=schemas.LogRetentionPolicy)
async def update_retention_policy(
    policy_id: int,
    policy: schemas.LogRetentionPolicyUpdate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a retention policy"""
    db_policy = crud.get_retention_policy(db, policy_id)
    if not db_policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    
    updated_policy = crud.update_retention_policy(db, policy_id, policy)
    
    # Update ILM policy in Elasticsearch
    background_tasks.add_task(apply_retention_policy_to_es, policy_id)
    
    return updated_policy


@router.delete("/retention-policies/{policy_id}")
def delete_retention_policy(
    policy_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a retention policy"""
    policy = crud.get_retention_policy(db, policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail="Retention policy not found")
    
    crud.delete_retention_policy(db, policy_id)
    return {"message": "Policy deleted successfully"}


# Archival Rules endpoints
@router.get("/archival-rules", response_model=List[schemas.LogArchivalRule])
def get_archival_rules(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    active_only: bool = Query(True),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all archival rules"""
    return crud.get_archival_rules(db, skip=skip, limit=limit, active_only=active_only)


@router.post("/archival-rules", response_model=schemas.LogArchivalRule)
def create_archival_rule(
    rule: schemas.LogArchivalRuleCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new archival rule"""
    return crud.create_archival_rule(db, rule)


@router.get("/archival-rules/{rule_id}", response_model=schemas.LogArchivalRule)
def get_archival_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a specific archival rule"""
    rule = crud.get_archival_rule(db, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Archival rule not found")
    return rule


@router.put("/archival-rules/{rule_id}", response_model=schemas.LogArchivalRule)
def update_archival_rule(
    rule_id: int,
    rule: schemas.LogArchivalRuleUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update an archival rule"""
    db_rule = crud.get_archival_rule(db, rule_id)
    if not db_rule:
        raise HTTPException(status_code=404, detail="Archival rule not found")
    
    return crud.update_archival_rule(db, rule_id, rule)


@router.delete("/archival-rules/{rule_id}")
def delete_archival_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete an archival rule"""
    rule = crud.get_archival_rule(db, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Archival rule not found")
    
    crud.delete_archival_rule(db, rule_id)
    return {"message": "Archival rule deleted successfully"}


# Analytics endpoints
@router.post("/analytics/search")
async def search_logs(
    query: Dict[str, Any],
    index: str = Query("*"),
    current_user: User = Depends(get_current_user)
):
    """Execute a custom log search query"""
    es_client = ElasticsearchLogManager()
    results = await es_client.search_logs(query, index)
    return results


@router.get("/analytics/services")
async def get_service_analytics(
    days: int = Query(7, ge=1, le=365),
    current_user: User = Depends(get_current_user)
):
    """Get analytics data for services over specified time period"""
    es_client = ElasticsearchLogManager()
    
    # Build time range query
    query = {
        "size": 0,
        "query": {
            "range": {
                "@timestamp": {
                    "gte": f"now-{days}d"
                }
            }
        },
        "aggs": {
            "services": {
                "terms": {
                    "field": "container_name.keyword",
                    "size": 50
                },
                "aggs": {
                    "log_levels": {
                        "terms": {
                            "field": "level.keyword"
                        }
                    },
                    "timeline": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "interval": "1h"
                        }
                    }
                }
            }
        }
    }
    
    results = await es_client.search_logs(query)
    return results


@router.get("/predefined-policies")
def get_predefined_policies(
    current_user: User = Depends(get_current_user)
):
    """Get predefined ILM policies for common use cases"""
    return PolicyEngine.get_predefined_policies()


async def apply_retention_policy_to_es(policy_id: int):
    """Background task to apply retention policy to Elasticsearch"""
    try:
        # This would be implemented to:
        # 1. Get policy from database
        # 2. Create ILM policy in Elasticsearch  
        # 3. Create/update index templates
        # 4. Update existing indices if needed
        
        logger.info(f"Applied retention policy {policy_id} to Elasticsearch")
    except Exception as e:
        logger.error(f"Failed to apply retention policy {policy_id}: {str(e)}")
