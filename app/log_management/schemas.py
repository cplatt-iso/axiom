"""
Pydantic schemas for Log Management API
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from app.log_management.models import RetentionTier


class LogRetentionPolicyBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    service_pattern: str = Field(..., min_length=1, max_length=200)
    log_level_filter: Optional[str] = Field(None, pattern="^(ERROR|WARN|INFO|DEBUG)$")
    tier: RetentionTier
    hot_days: int = Field(default=30, ge=1, le=365)
    warm_days: int = Field(default=180, ge=30, le=1095)
    cold_days: int = Field(default=365, ge=180, le=3650)
    delete_days: int = Field(default=2555, ge=365, le=7300)  # 20 years max
    max_index_size_gb: int = Field(default=10, ge=1, le=100)
    max_index_age_days: int = Field(default=30, ge=1, le=365)
    storage_class_hot: str = Field(default="fast-ssd", max_length=50)
    storage_class_warm: str = Field(default="standard", max_length=50) 
    storage_class_cold: str = Field(default="cold-storage", max_length=50)
    compliance_required: bool = False
    ilm_policy_json: Optional[Dict[str, Any]] = None

    @validator('delete_days')
    def delete_must_be_greater_than_cold(cls, v, values):
        if 'cold_days' in values and v <= values['cold_days']:
            raise ValueError('delete_days must be greater than cold_days')
        return v

    @validator('cold_days')
    def cold_must_be_greater_than_warm(cls, v, values):
        if 'warm_days' in values and v <= values['warm_days']:
            raise ValueError('cold_days must be greater than warm_days')
        return v

    @validator('warm_days')
    def warm_must_be_greater_than_hot(cls, v, values):
        if 'hot_days' in values and v <= values['hot_days']:
            raise ValueError('warm_days must be greater than hot_days')
        return v


class LogRetentionPolicyCreate(LogRetentionPolicyBase):
    pass


class LogRetentionPolicyUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    service_pattern: Optional[str] = Field(None, min_length=1, max_length=200)
    log_level_filter: Optional[str] = Field(None, pattern="^(ERROR|WARN|INFO|DEBUG)$")
    tier: Optional[RetentionTier] = None
    hot_days: Optional[int] = Field(None, ge=1, le=365)
    warm_days: Optional[int] = Field(None, ge=30, le=1095)
    cold_days: Optional[int] = Field(None, ge=180, le=3650)
    delete_days: Optional[int] = Field(None, ge=365, le=7300)
    max_index_size_gb: Optional[int] = Field(None, ge=1, le=100)
    max_index_age_days: Optional[int] = Field(None, ge=1, le=365)
    storage_class_hot: Optional[str] = Field(None, max_length=50)
    storage_class_warm: Optional[str] = Field(None, max_length=50)
    storage_class_cold: Optional[str] = Field(None, max_length=50)
    compliance_required: Optional[bool] = None
    is_active: Optional[bool] = None
    ilm_policy_json: Optional[Dict[str, Any]] = None


class LogRetentionPolicy(LogRetentionPolicyBase):
    id: int
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True


class LogArchivalRuleBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    source_pattern: str = Field(..., min_length=1, max_length=200)
    archive_after_days: int = Field(..., ge=1, le=7300)
    destination_type: str = Field(..., pattern="^(s3|gcs|local|parquet)$")
    destination_config: Dict[str, Any]
    compression: str = Field(default="gzip", pattern="^(gzip|lz4|snappy|none)$")
    format: str = Field(default="json", pattern="^(json|parquet|avro)$")
    k8s_job_template: Optional[Dict[str, Any]] = None
    k8s_schedule: str = Field(default="0 2 * * *", max_length=100)


class LogArchivalRuleCreate(LogArchivalRuleBase):
    pass


class LogArchivalRuleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    source_pattern: Optional[str] = Field(None, min_length=1, max_length=200)
    archive_after_days: Optional[int] = Field(None, ge=1, le=7300)
    destination_type: Optional[str] = Field(None, pattern="^(s3|gcs|local|parquet)$")
    destination_config: Optional[Dict[str, Any]] = None
    compression: Optional[str] = Field(None, pattern="^(gzip|lz4|snappy|none)$")
    format: Optional[str] = Field(None, pattern="^(json|parquet|avro)$")
    k8s_job_template: Optional[Dict[str, Any]] = None
    k8s_schedule: Optional[str] = Field(None, max_length=100)
    is_active: Optional[bool] = None


class LogArchivalRule(LogArchivalRuleBase):
    id: int
    is_active: bool
    last_run: Optional[datetime]
    last_success: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True


class LogAnalyticsConfigBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    analytics_type: str = Field(..., pattern="^(dashboard|alert|report)$")
    query_pattern: str = Field(..., min_length=1, max_length=200)
    query_config: Dict[str, Any]
    schedule: Optional[str] = Field(None, max_length=100)
    output_config: Optional[Dict[str, Any]] = None
    k8s_cronjob_template: Optional[Dict[str, Any]] = None


class LogAnalyticsConfigCreate(LogAnalyticsConfigBase):
    pass


class LogAnalyticsConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    analytics_type: Optional[str] = Field(None, pattern="^(dashboard|alert|report)$")
    query_pattern: Optional[str] = Field(None, min_length=1, max_length=200)
    query_config: Optional[Dict[str, Any]] = None
    schedule: Optional[str] = Field(None, max_length=100)
    output_config: Optional[Dict[str, Any]] = None
    k8s_cronjob_template: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None


class LogAnalyticsConfig(LogAnalyticsConfigBase):
    id: int
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True


# Response schemas for UI
class LogStorageStats(BaseModel):
    total_indices: int
    total_size_gb: float
    hot_size_gb: float
    warm_size_gb: float
    cold_size_gb: float
    oldest_log_date: Optional[datetime]
    newest_log_date: Optional[datetime]


class ServiceLogStats(BaseModel):
    service_name: str
    container_name: str
    log_count: int
    size_gb: float
    earliest_log: Optional[datetime]
    latest_log: Optional[datetime]
    retention_policy: Optional[str]


class LogManagementDashboard(BaseModel):
    storage_stats: LogStorageStats
    service_stats: List[ServiceLogStats]
    active_policies: int
    active_archival_rules: int
    recent_archival_jobs: List[Dict[str, Any]]
    alerts: List[Dict[str, Any]]
