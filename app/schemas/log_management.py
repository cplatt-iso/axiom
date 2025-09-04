"""
Pydantic schemas for log management API endpoints.

Defines request and response models for log retention policies,
archival rules, and analytics configurations.
"""

from typing import Optional, List
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, ConfigDict

from ..log_management.models import RetentionTier


# Base schemas for common fields
class LogRetentionPolicyBase(BaseModel):
    """Base schema for log retention policies."""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    service_pattern: str = Field(..., min_length=1, max_length=200)
    log_level_filter: Optional[str] = Field(None, max_length=20)
    tier: RetentionTier
    hot_days: int = Field(..., ge=1, le=365)
    warm_days: int = Field(..., ge=1, le=1095)
    cold_days: int = Field(..., ge=1, le=2555)
    delete_days: int = Field(..., ge=1, le=3650)
    max_index_size_gb: int = Field(..., ge=1, le=10000)
    max_index_age_days: int = Field(..., ge=1, le=3650)
    storage_class_hot: str = Field(..., max_length=50)
    storage_class_warm: str = Field(..., max_length=50)
    storage_class_cold: str = Field(..., max_length=50)
    is_active: bool = True


class LogRetentionPolicyCreate(LogRetentionPolicyBase):
    """Schema for creating a log retention policy."""
    pass


class LogRetentionPolicyUpdate(BaseModel):
    """Schema for updating a log retention policy."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    service_pattern: Optional[str] = Field(None, min_length=1, max_length=200)
    log_level_filter: Optional[str] = Field(None, max_length=20)
    tier: Optional[RetentionTier] = None
    hot_days: Optional[int] = Field(None, ge=1, le=365)
    warm_days: Optional[int] = Field(None, ge=1, le=1095)
    cold_days: Optional[int] = Field(None, ge=1, le=2555)
    delete_days: Optional[int] = Field(None, ge=1, le=3650)
    max_index_size_gb: Optional[int] = Field(None, ge=1, le=10000)
    max_index_age_days: Optional[int] = Field(None, ge=1, le=3650)
    storage_class_hot: Optional[str] = Field(None, max_length=50)
    storage_class_warm: Optional[str] = Field(None, max_length=50)
    storage_class_cold: Optional[str] = Field(None, max_length=50)
    is_active: Optional[bool] = None


class LogRetentionPolicyResponse(LogRetentionPolicyBase):
    """Schema for log retention policy responses."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Archival rule schemas
class LogArchivalRuleBase(BaseModel):
    """Base schema for log archival rules."""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    service_pattern: str = Field(..., min_length=1, max_length=200)
    age_threshold_days: int = Field(..., ge=1, le=3650)
    storage_backend: str = Field(..., max_length=20)
    storage_bucket: str = Field(..., min_length=1, max_length=200)
    storage_path_prefix: str = Field(..., max_length=500)
    compression: str = Field(..., max_length=10)
    format_type: str = Field(..., max_length=10)
    retention_days: int = Field(..., ge=1, le=10950)  # Up to 30 years
    delete_after_archive: bool = True
    is_active: bool = True
    cron_schedule: str = Field(..., min_length=1, max_length=100)


class LogArchivalRuleCreate(LogArchivalRuleBase):
    """Schema for creating a log archival rule."""
    pass


class LogArchivalRuleUpdate(BaseModel):
    """Schema for updating a log archival rule."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    service_pattern: Optional[str] = Field(None, min_length=1, max_length=200)
    age_threshold_days: Optional[int] = Field(None, ge=1, le=3650)
    storage_backend: Optional[str] = Field(None, max_length=20)
    storage_bucket: Optional[str] = Field(None, min_length=1, max_length=200)
    storage_path_prefix: Optional[str] = Field(None, max_length=500)
    compression: Optional[str] = Field(None, max_length=10)
    format_type: Optional[str] = Field(None, max_length=10)
    retention_days: Optional[int] = Field(None, ge=1, le=10950)
    delete_after_archive: Optional[bool] = None
    is_active: Optional[bool] = None
    cron_schedule: Optional[str] = Field(None, min_length=1, max_length=100)


class LogArchivalRuleResponse(LogArchivalRuleBase):
    """Schema for log archival rule responses."""
    id: int
    last_run: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Analytics configuration schemas
class AlertOperator(str, Enum):
    """Alert threshold operators."""
    GT = "gt"  # greater than
    GTE = "gte"  # greater than or equal
    LT = "lt"  # less than
    LTE = "lte"  # less than or equal
    EQ = "eq"  # equal to
    NE = "ne"  # not equal to


class LogAnalyticsConfigBase(BaseModel):
    """Base schema for log analytics configurations."""
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    query_pattern: str = Field(..., min_length=1, max_length=1000)
    aggregation_window: str = Field(..., min_length=1, max_length=20)
    alert_threshold: float = Field(..., ge=0)
    alert_operator: AlertOperator = AlertOperator.GT
    notification_channels: List[str] = Field(default_factory=list)
    is_active: bool = True


class LogAnalyticsConfigCreate(LogAnalyticsConfigBase):
    """Schema for creating a log analytics configuration."""
    pass


class LogAnalyticsConfigUpdate(BaseModel):
    """Schema for updating a log analytics configuration."""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    query_pattern: Optional[str] = Field(None, min_length=1, max_length=1000)
    aggregation_window: Optional[str] = Field(None, min_length=1, max_length=20)
    alert_threshold: Optional[float] = Field(None, ge=0)
    alert_operator: Optional[AlertOperator] = None
    notification_channels: Optional[List[str]] = None
    is_active: Optional[bool] = None


class LogAnalyticsConfigResponse(LogAnalyticsConfigBase):
    """Schema for log analytics configuration responses."""
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


# Elasticsearch integration schemas
class ElasticsearchPolicySync(BaseModel):
    """Schema for Elasticsearch policy synchronization results."""
    database_id: int
    database_name: str
    elasticsearch_policy_name: str
    tier: str
    status: str = "synced"


class ElasticsearchIndexInfo(BaseModel):
    """Schema for Elasticsearch index information."""
    name: str
    status: str
    health: str
    docs_count: int
    store_size: str
    creation_date: Optional[datetime] = None


class ElasticsearchClusterStats(BaseModel):
    """Schema for Elasticsearch cluster statistics."""
    cluster_name: str
    status: str
    nodes: int
    indices_count: int
    docs_count: int
    store_size_bytes: int
    
    
# Statistics and monitoring schemas
class LogManagementStatistics(BaseModel):
    """Schema for comprehensive log management statistics."""
    timestamp: datetime
    total_retention_policies: int
    active_retention_policies: int
    total_archival_rules: int
    active_archival_rules: int
    elasticsearch_indices: int
    total_log_volume_gb: float
    oldest_log_date: Optional[datetime] = None
    newest_log_date: Optional[datetime] = None


# Health check schemas
class ServiceHealthCheck(BaseModel):
    """Schema for individual service health status."""
    service: str
    status: str
    timestamp: datetime
    details: Optional[dict] = None


class LogManagementHealthResponse(BaseModel):
    """Schema for log management service health response."""
    status: str
    timestamp: datetime
    elasticsearch: ServiceHealthCheck
    policy_templates: int
    version: str
