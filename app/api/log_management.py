"""
Log Management API endpoints for managing retention policies and archival rules.

Provides CRUD operations for log retention policies, archival rules,
and analytics configurations with medical imaging compliance.
"""

from typing import List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from ..db.session import get_db
from ..crud import log_management as crud
from ..schemas.log_management import (
    LogRetentionPolicyCreate,
    LogRetentionPolicyUpdate,
    LogRetentionPolicyResponse,
    LogArchivalRuleCreate,
    LogArchivalRuleUpdate,
    LogArchivalRuleResponse,
    LogAnalyticsConfigCreate,
    LogAnalyticsConfigUpdate,
    LogAnalyticsConfigResponse,
)
from ..services.elasticsearch_manager import ElasticsearchManager
from ..log_management.policy_engine import PolicyEngine

router = APIRouter(prefix="/log-management", tags=["Log Management"])

# Initialize services
elasticsearch_manager = ElasticsearchManager()
policy_engine = PolicyEngine()


@router.get("/health")
async def health_check():
    """Check log management service health and Elasticsearch connectivity."""
    try:
        # Check Elasticsearch connection
        es_status = await elasticsearch_manager.health_check()
        
        # Check policy engine
        policy_count = len(policy_engine.generate_medical_imaging_policies())
        
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elasticsearch": es_status,
            "policy_templates": policy_count,
            "version": "1.0.0"
        }
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )


# Retention Policies Endpoints
@router.post("/retention-policies", response_model=LogRetentionPolicyResponse)
async def create_retention_policy(
    policy: LogRetentionPolicyCreate,
    db: Session = Depends(get_db)
):
    """Create a new log retention policy."""
    try:
        # Create in database
        db_policy = crud.create_retention_policy(db=db, policy=policy)
        
        # Apply to Elasticsearch if active
        if db_policy.is_active:
            ilm_policy = policy_engine.generate_ilm_policy_from_db(db_policy)
            await elasticsearch_manager.create_ilm_policy(
                policy_name=f"axiom-{db_policy.name.lower().replace(' ', '-')}",
                policy=ilm_policy
            )
        
        return db_policy
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create retention policy: {str(e)}"
        )


@router.get("/retention-policies", response_model=List[LogRetentionPolicyResponse])
async def list_retention_policies(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False,
    db: Session = Depends(get_db)
):
    """List all retention policies with optional filtering."""
    policies = crud.get_retention_policies(
        db=db, skip=skip, limit=limit, active_only=active_only
    )
    return policies


@router.get("/retention-policies/{policy_id}", response_model=LogRetentionPolicyResponse)
async def get_retention_policy(
    policy_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific retention policy by ID."""
    policy = crud.get_retention_policy(db=db, policy_id=policy_id)
    if not policy:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Retention policy not found"
        )
    return policy


@router.put("/retention-policies/{policy_id}", response_model=LogRetentionPolicyResponse)
async def update_retention_policy(
    policy_id: int,
    policy_update: LogRetentionPolicyUpdate,
    db: Session = Depends(get_db)
):
    """Update an existing retention policy."""
    try:
        policy = crud.update_retention_policy(
            db=db, policy_id=policy_id, policy_update=policy_update
        )
        if not policy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Retention policy not found"
            )
        
        # Update Elasticsearch ILM policy if active
        if policy.is_active:
            ilm_policy = policy_engine.generate_ilm_policy_from_db(policy)
            await elasticsearch_manager.update_ilm_policy(
                policy_name=f"axiom-{policy.name.lower().replace(' ', '-')}",
                policy=ilm_policy
            )
        
        return policy
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to update retention policy: {str(e)}"
        )


@router.delete("/retention-policies/{policy_id}")
async def delete_retention_policy(
    policy_id: int,
    db: Session = Depends(get_db)
):
    """Delete a retention policy."""
    try:
        policy = crud.get_retention_policy(db=db, policy_id=policy_id)
        if not policy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Retention policy not found"
            )
        
        # Remove from Elasticsearch first
        policy_name = f"axiom-{policy.name.lower().replace(' ', '-')}"
        await elasticsearch_manager.delete_ilm_policy(policy_name)
        
        # Delete from database
        success = crud.delete_retention_policy(db=db, policy_id=policy_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to delete retention policy"
            )
        
        return {"message": "Retention policy deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to delete retention policy: {str(e)}"
        )


# Archival Rules Endpoints
@router.post("/archival-rules", response_model=LogArchivalRuleResponse)
async def create_archival_rule(
    rule: LogArchivalRuleCreate,
    db: Session = Depends(get_db)
):
    """Create a new log archival rule."""
    db_rule = crud.create_archival_rule(db=db, rule=rule)
    return db_rule


@router.get("/archival-rules", response_model=List[LogArchivalRuleResponse])
async def list_archival_rules(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False,
    db: Session = Depends(get_db)
):
    """List all archival rules."""
    rules = crud.get_archival_rules(
        db=db, skip=skip, limit=limit, active_only=active_only
    )
    return rules


@router.get("/archival-rules/{rule_id}", response_model=LogArchivalRuleResponse)
async def get_archival_rule(
    rule_id: int,
    db: Session = Depends(get_db)
):
    """Get a specific archival rule by ID."""
    rule = crud.get_archival_rule(db=db, rule_id=rule_id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Archival rule not found"
        )
    return rule


@router.put("/archival-rules/{rule_id}", response_model=LogArchivalRuleResponse)
async def update_archival_rule(
    rule_id: int,
    rule_update: LogArchivalRuleUpdate,
    db: Session = Depends(get_db)
):
    """Update an existing archival rule."""
    rule = crud.update_archival_rule(
        db=db, rule_id=rule_id, rule_update=rule_update
    )
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Archival rule not found"
        )
    return rule


@router.delete("/archival-rules/{rule_id}")
async def delete_archival_rule(
    rule_id: int,
    db: Session = Depends(get_db)
):
    """Delete an archival rule."""
    success = crud.delete_archival_rule(db=db, rule_id=rule_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Archival rule not found"
        )
    return {"message": "Archival rule deleted successfully"}


# Elasticsearch Management Endpoints
@router.post("/elasticsearch/sync-policies")
async def sync_policies_to_elasticsearch(
    db: Session = Depends(get_db)
):
    """Synchronize all active retention policies to Elasticsearch ILM policies."""
    try:
        policies = crud.get_retention_policies(db=db, active_only=True)
        synced_policies = []
        
        for policy in policies:
            policy_name = f"axiom-{policy.name.lower().replace(' ', '-')}"
            ilm_policy = policy_engine.generate_ilm_policy_from_db(policy)
            
            await elasticsearch_manager.create_ilm_policy(
                policy_name=policy_name,
                policy=ilm_policy
            )
            
            synced_policies.append({
                "database_id": policy.id,
                "database_name": policy.name,
                "elasticsearch_policy_name": policy_name,
                "tier": policy.tier.value
            })
        
        return {
            "message": f"Synchronized {len(synced_policies)} policies to Elasticsearch",
            "policies": synced_policies
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync policies: {str(e)}"
        )


@router.get("/elasticsearch/ilm-policies")
async def list_elasticsearch_policies():
    """List all ILM policies currently in Elasticsearch."""
    try:
        policies = await elasticsearch_manager.list_ilm_policy_names()
        return {"policies": policies}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch Elasticsearch policies: {str(e)}"
        )


@router.get("/elasticsearch/indices")
async def list_elasticsearch_indices():
    """List all indices in Elasticsearch with metadata."""
    try:
        indices = await elasticsearch_manager.list_indices()
        return {"indices": indices}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch Elasticsearch indices: {str(e)}"
        )


@router.post("/elasticsearch/apply-templates")
async def apply_index_templates(
    db: Session = Depends(get_db)
):
    """Apply index templates based on active retention policies."""
    try:
        policies = crud.get_retention_policies(db=db, active_only=True)
        applied_templates = []
        
        for policy in policies:
            # Clean template name - no commas or special chars allowed
            template_name = f"axiom-{policy.name.lower().replace(' ', '-').replace('&', 'and')}-template"
            policy_name = f"axiom-{policy.name.lower().replace(' ', '-').replace('&', 'and')}"
            
            # Split service pattern into individual patterns for index_patterns array
            index_patterns = [pattern.strip() for pattern in policy.service_pattern.split(',')]
            
            # Set priority based on specificity - more specific patterns get higher priority
            # Critical/specific patterns = 200, operational = 150, debug/wildcard = 10
            if policy.tier.value == "critical":
                priority = 200
            elif policy.tier.value == "operational":
                priority = 150
            else:  # debug or wildcard patterns
                priority = 10
            
            template = policy_engine.generate_index_template(
                template_name=template_name,
                index_pattern=index_patterns,  # Pass as list
                ilm_policy_name=policy_name,
                priority=priority
            )
            
            await elasticsearch_manager.create_index_template(
                template_name=template_name,
                template=template
            )
            
            applied_templates.append({
                "template_name": template_name,
                "index_patterns": index_patterns,
                "ilm_policy": policy_name,
                "priority": priority
            })
        
        return {
            "message": f"Applied {len(applied_templates)} index templates",
            "templates": applied_templates
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to apply templates: {str(e)}"
        )


@router.get("/statistics")
async def get_log_statistics(db: Session = Depends(get_db)):
    """Get comprehensive log management statistics and analytics."""
    try:
        # Get basic Elasticsearch cluster statistics
        es_stats = await elasticsearch_manager.get_cluster_stats()
        
        # Get basic index statistics for axiom patterns
        axiom_indices = await elasticsearch_manager.get_indices_stats(
            index_pattern="axiom*"
        )
        
        # Fetch database policies for enhanced analytics
        db_policies = crud.get_retention_policies(db)
        
        # Get enhanced analytics (Phase 2 implementation with policy correlation)
        enhanced_analytics = await elasticsearch_manager.get_enhanced_analytics(db_policies)
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "version": "2.1.0",  # Updated version with Phase 2 policy correlation
            
            # Basic cluster information
            "elasticsearch": es_stats,
            "axiom_indices": axiom_indices,
            
            # Enhanced analytics for UI dashboard (Phase 2)
            "analytics": {
                "storage_distribution": enhanced_analytics["storage_distribution"],
                "log_levels": enhanced_analytics["log_level_distribution"],
                "ingestion_trends": enhanced_analytics["ingestion_trends"],
                "retention_compliance": enhanced_analytics["retention_compliance"],
                "service_breakdown": enhanced_analytics["service_breakdown"],
                "policy_efficiency": enhanced_analytics["policy_efficiency"]  # NEW Phase 2C
            },
            
            # Summary metrics for quick dashboard display
            "summary": {
                "total_storage_tb": round(enhanced_analytics["storage_distribution"]["total_storage_gb"] / 1024, 2),
                "daily_ingestion_gb": enhanced_analytics["ingestion_trends"]["average_daily_storage_gb"],
                "compliance_percentage": enhanced_analytics["retention_compliance"]["compliance_percentage"],
                "total_services": len(enhanced_analytics["service_breakdown"]["services"]),
                "most_active_service": enhanced_analytics["service_breakdown"]["services"][0]["service"] if enhanced_analytics["service_breakdown"]["services"] else "none"
            }
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get statistics: {str(e)}"
        )


@router.get("/analytics/dashboard")
async def get_dashboard_analytics(db: Session = Depends(get_db)):
    """Get detailed analytics specifically formatted for dashboard UI."""
    try:
        # Fetch database policies to include in efficiency analysis
        db_policies = crud.get_retention_policies(db)
        
        enhanced_analytics = await elasticsearch_manager.get_enhanced_analytics(db_policies)
        
        # Format data specifically for dashboard consumption
        dashboard_data = {
            "timestamp": enhanced_analytics["timestamp"],
            
            # Storage metrics formatted for charts
            "storage": {
                "total_tb": round(enhanced_analytics["storage_distribution"]["total_storage_gb"] / 1024, 2),
                "by_tier": {
                    "hot": {
                        "size_tb": round(enhanced_analytics["storage_distribution"]["by_tier"]["hot"]["size_gb"] / 1024, 2),
                        "percentage": enhanced_analytics["storage_distribution"]["by_tier"]["hot"]["percentage"]
                    },
                    "warm": {
                        "size_tb": round(enhanced_analytics["storage_distribution"]["by_tier"]["warm"]["size_gb"] / 1024, 2), 
                        "percentage": enhanced_analytics["storage_distribution"]["by_tier"]["warm"]["percentage"]
                    },
                    "cold": {
                        "size_tb": round(enhanced_analytics["storage_distribution"]["by_tier"]["cold"]["size_gb"] / 1024, 2),
                        "percentage": enhanced_analytics["storage_distribution"]["by_tier"]["cold"]["percentage"]
                    },
                    "archived": {
                        "size_tb": round(enhanced_analytics["storage_distribution"]["by_tier"]["archived"]["size_gb"] / 1024, 2),
                        "percentage": enhanced_analytics["storage_distribution"]["by_tier"]["archived"]["percentage"]
                    }
                }
            },
            
            # Log level distribution for pie charts
            "log_levels": enhanced_analytics["log_level_distribution"]["distribution"],
            
            # Ingestion trends for time series charts
            "ingestion": {
                "daily_average_gb": enhanced_analytics["ingestion_trends"]["average_daily_storage_gb"],
                "trends": enhanced_analytics["ingestion_trends"]["last_30_days"],
                "total_documents_30d": enhanced_analytics["ingestion_trends"]["total_documents_30d"]
            },
            
            # Compliance metrics for progress indicators
            "compliance": {
                "percentage": enhanced_analytics["retention_compliance"]["compliance_percentage"],
                "compliant_indices": enhanced_analytics["retention_compliance"]["compliant_indices"],
                "non_compliant_indices": enhanced_analytics["retention_compliance"]["non_compliant_indices"],
                "total_indices": enhanced_analytics["retention_compliance"]["total_indices"]
            },
            
            # Top services for bar charts
            "services": {
                "top_10": enhanced_analytics["service_breakdown"]["services"][:10],
                "total_services": len(enhanced_analytics["service_breakdown"]["services"])
            },
            
            # Policy efficiency metrics (NEW Phase 2C)
            "policy_efficiency": {
                "coverage_percentage": enhanced_analytics["policy_efficiency"]["coverage_percentage"],
                "efficiency_score": enhanced_analytics["policy_efficiency"]["efficiency_score"],
                "uncovered_indices_count": enhanced_analytics["policy_efficiency"]["total_uncovered_count"],
                "uncovered_storage_gb": enhanced_analytics["policy_efficiency"]["uncovered_storage_gb"],
                "policy_count": len(enhanced_analytics["policy_efficiency"]["policy_details"]),
                "recommendations": enhanced_analytics["policy_efficiency"]["recommendations"][:3]  # Top 3 for UI
            }
        }
        
        return dashboard_data
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get dashboard analytics: {str(e)}"
        )


@router.get("/analytics/retention-efficiency")
async def get_retention_efficiency(db: Session = Depends(get_db)):
    """Get detailed retention policy efficiency metrics with real correlation analysis."""
    try:
        # Fetch database policies for real correlation analysis
        db_policies = crud.get_retention_policies(db)
        
        enhanced_analytics = await elasticsearch_manager.get_enhanced_analytics(db_policies)
        
        policy_efficiency_data = enhanced_analytics["policy_efficiency"]
        
        # Detailed efficiency breakdown by policy
        policy_breakdown = []
        for policy_name, policy_data in policy_efficiency_data["policy_details"].items():
            efficiency_score = 0
            if policy_data["pattern_matches"] > 0:
                # Score based on coverage and storage managed
                coverage_factor = min(policy_data["pattern_matches"] / 10, 1.0)  # Max score at 10+ matches
                storage_factor = min(policy_data["covered_storage_gb"] / 50, 1.0)  # Max score at 50GB+
                efficiency_score = round((coverage_factor + storage_factor) * 50, 1)
            
            policy_breakdown.append({
                "policy_name": policy_name,
                "patterns": policy_data["patterns"],
                "tier": policy_data["tier"],
                "retention_days": policy_data["retention_days"],
                "covered_indices": policy_data["pattern_matches"],
                "covered_storage_gb": policy_data["covered_storage_gb"],
                "covered_documents": policy_data["covered_documents"],
                "efficiency_score": efficiency_score,
                "sample_indices": [idx["name"] for idx in policy_data["covered_indices"][:3]]  # Top 3 examples
            })
        
        return {
            "timestamp": enhanced_analytics["timestamp"],
            "overall_efficiency": {
                "coverage_percentage": policy_efficiency_data["coverage_percentage"],
                "total_policies": len(policy_breakdown),
                "active_policies": len([p for p in policy_breakdown if p["covered_indices"] > 0]),
                "uncovered_indices": policy_efficiency_data["total_uncovered_count"],
                "uncovered_storage_gb": policy_efficiency_data["uncovered_storage_gb"]
            },
            "policy_breakdown": sorted(policy_breakdown, key=lambda x: x["efficiency_score"], reverse=True),
            "uncovered_samples": policy_efficiency_data["uncovered_indices"][:5],  # Top 5 largest
            "recommendations": policy_efficiency_data["recommendations"],
            "improvement_opportunities": [
                {
                    "type": "coverage_gap",
                    "description": f"{policy_efficiency_data['total_uncovered_count']} indices without policy coverage",
                    "impact": f"{policy_efficiency_data['uncovered_storage_gb']} GB at risk",
                    "priority": "high" if policy_efficiency_data["uncovered_storage_gb"] > 10 else "medium"
                },
                {
                    "type": "pattern_optimization", 
                    "description": f"Review patterns for {len([p for p in policy_breakdown if p['covered_indices'] == 0])} inactive policies",
                    "impact": "Better automation coverage",
                    "priority": "medium"
                }
            ]
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get retention efficiency: {str(e)}"
        )
