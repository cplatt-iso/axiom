from pydantic import BaseModel, Field, field_validator
from typing import Dict, Any, Optional, List
from datetime import datetime
from app.db.models.maintenance import (
    MaintenanceTaskType,
    MaintenanceTaskStatus,
    DeletionStrategy,
    CleanupMode
)


# Base schemas for MaintenanceConfig
class MaintenanceConfigBase(BaseModel):
    task_type: MaintenanceTaskType
    name: str = Field(..., min_length=1, max_length=255, description="Human-readable name for this config")
    description: Optional[str] = Field(None, description="Description of what this config does")
    enabled: bool = Field(True, description="Whether this maintenance task is enabled")
    monitor_interval_seconds: int = Field(300, ge=60, le=86400, description="How often to check in seconds")
    config: Dict[str, Any] = Field(..., description="Task-specific configuration parameters")

    model_config = {"from_attributes": True}


class MaintenanceConfigCreate(MaintenanceConfigBase):
    pass


class MaintenanceConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    enabled: Optional[bool] = None
    monitor_interval_seconds: Optional[int] = Field(None, ge=60, le=86400)
    config: Optional[Dict[str, Any]] = None

    model_config = {"from_attributes": True}


class MaintenanceConfigRead(MaintenanceConfigBase):
    id: int
    created_at: datetime
    updated_at: datetime
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None


# Base schemas for MaintenanceTask
class MaintenanceTaskBase(BaseModel):
    config_id: int
    task_type: MaintenanceTaskType
    status: MaintenanceTaskStatus = MaintenanceTaskStatus.IDLE

    model_config = {"from_attributes": True}


class MaintenanceTaskCreate(MaintenanceTaskBase):
    pass


class MaintenanceTaskUpdate(BaseModel):
    status: Optional[MaintenanceTaskStatus] = None
    files_processed: Optional[int] = Field(None, ge=0)
    bytes_freed: Optional[int] = Field(None, ge=0)
    errors_encountered: Optional[int] = Field(None, ge=0)
    execution_details: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    model_config = {"from_attributes": True}


class MaintenanceTaskRead(MaintenanceTaskBase):
    id: int
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    files_processed: int = 0
    bytes_freed: int = 0
    errors_encountered: int = 0
    execution_details: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


# Data Cleaner specific schemas
class DataCleanerConfigBase(BaseModel):
    maintenance_config_id: int
    target_directory: str = Field(..., min_length=1, max_length=500)
    cleanup_mode: CleanupMode
    deletion_strategy: DeletionStrategy = DeletionStrategy.OLDEST_FIRST
    high_water_mark_bytes: Optional[int] = Field(None, ge=0)
    target_free_space_percentage: Optional[float] = Field(None, ge=1.0, le=99.0)
    minimum_free_space_gb: float = Field(1.0, ge=0.1)
    dry_run: bool = Field(False, description="Log what would be deleted but don't actually delete")
    minimum_file_age_hours: int = Field(1, ge=0)
    file_pattern: Optional[str] = Field(None, max_length=255)
    exclude_pattern: Optional[str] = Field(None, max_length=255)
    batch_size: int = Field(100, ge=1, le=10000)
    max_files_per_run: int = Field(10000, ge=1)

    @field_validator('high_water_mark_bytes')
    @classmethod
    def validate_high_water_mark(cls, v, values):
        if hasattr(values, 'data') and 'cleanup_mode' in values.data:
            if values.data['cleanup_mode'] == CleanupMode.HIGH_WATER_MARK and v is None:
                raise ValueError('high_water_mark_bytes is required when cleanup_mode is HIGH_WATER_MARK')
        return v

    @field_validator('target_free_space_percentage')
    @classmethod
    def validate_free_space_percentage(cls, v, values):
        if hasattr(values, 'data') and 'cleanup_mode' in values.data:
            if values.data['cleanup_mode'] == CleanupMode.FREE_SPACE_PERCENTAGE and v is None:
                raise ValueError('target_free_space_percentage is required when cleanup_mode is FREE_SPACE_PERCENTAGE')
        return v

    model_config = {"from_attributes": True}


class DataCleanerConfigCreate(DataCleanerConfigBase):
    pass


class DataCleanerConfigUpdate(BaseModel):
    target_directory: Optional[str] = Field(None, min_length=1, max_length=500)
    cleanup_mode: Optional[CleanupMode] = None
    deletion_strategy: Optional[DeletionStrategy] = None
    high_water_mark_bytes: Optional[int] = Field(None, ge=0)
    target_free_space_percentage: Optional[float] = Field(None, ge=1.0, le=99.0)
    minimum_free_space_gb: Optional[float] = Field(None, ge=0.1)
    dry_run: Optional[bool] = None
    minimum_file_age_hours: Optional[int] = Field(None, ge=0)
    file_pattern: Optional[str] = Field(None, max_length=255)
    exclude_pattern: Optional[str] = Field(None, max_length=255)
    batch_size: Optional[int] = Field(None, ge=1, le=10000)
    max_files_per_run: Optional[int] = Field(None, ge=1)

    model_config = {"from_attributes": True}


class DataCleanerConfigRead(DataCleanerConfigBase):
    id: int
    created_at: datetime
    updated_at: datetime


# Combined schemas for easier API usage
class MaintenanceConfigWithDataCleaner(MaintenanceConfigRead):
    """Complete maintenance config with data cleaner details if applicable"""
    data_cleaner_config: Optional[DataCleanerConfigRead] = None


class MaintenanceTaskWithDetails(MaintenanceTaskRead):
    """Task execution with related config information"""
    config: Optional[MaintenanceConfigRead] = None


# Request/Response schemas for API endpoints
class MaintenanceStatusResponse(BaseModel):
    """Overall maintenance system status"""
    total_configs: int
    active_configs: int
    running_tasks: int
    last_24h_completions: int
    last_24h_failures: int
    configs: List[MaintenanceConfigRead]


class TaskExecutionRequest(BaseModel):
    """Request to manually trigger a task execution"""
    config_id: int
    force_run: bool = Field(False, description="Run even if another task is already running")


class TaskExecutionResponse(BaseModel):
    """Response from triggering a task execution"""
    task_id: int
    message: str
    started: bool
