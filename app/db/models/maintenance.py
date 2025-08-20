from sqlalchemy import Column, String, Text, Integer, BigInteger, Float, Boolean, JSON, DateTime, Enum as SQLAEnum
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base
from enum import Enum
from typing import Dict, Any, Optional
from datetime import datetime


class MaintenanceTaskType(str, Enum):
    """Types of maintenance tasks available"""
    DATA_CLEANER = "data_cleaner"
    # Future tasks can be added here
    # LOG_CLEANER = "log_cleaner"
    # CACHE_CLEANER = "cache_cleaner"


class MaintenanceTaskStatus(str, Enum):
    """Status of maintenance tasks"""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DISABLED = "disabled"


class DeletionStrategy(str, Enum):
    """Strategies for deleting files"""
    OLDEST_FIRST = "oldest_first"  # Based on study date from directory structure
    FIFO = "fifo"  # First in, first out (file creation time)
    LIFO = "lifo"  # Last in, first out (file creation time)


class CleanupMode(str, Enum):
    """Cleanup modes for data cleaner"""
    HIGH_WATER_MARK = "high_water_mark"  # Don't exceed X bytes
    KEEP_EMPTY = "keep_empty"  # Delete everything constantly
    FREE_SPACE_PERCENTAGE = "free_space_percentage"  # Maintain X% free space


class MaintenanceConfig(Base):
    """Configuration settings for maintenance tasks"""

    task_type: Mapped[MaintenanceTaskType] = mapped_column(SQLAEnum(MaintenanceTaskType), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, comment="Human-readable name for this config")
    description: Mapped[str] = mapped_column(Text, nullable=True, comment="Description of what this config does")
    
    # Core settings
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    monitor_interval_seconds: Mapped[int] = mapped_column(Integer, default=300, nullable=False, comment="How often to check in seconds")
    
    # Task-specific configuration as JSON
    config: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=False, comment="Task-specific configuration parameters")
    
    # Tracking
    last_run: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_run: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class MaintenanceTask(Base):
    """Individual maintenance task execution records"""
    
    config_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True, comment="References maintenance_configs.id")
    task_type: Mapped[MaintenanceTaskType] = mapped_column(SQLAEnum(MaintenanceTaskType), nullable=False, index=True)
    
    status: Mapped[MaintenanceTaskStatus] = mapped_column(SQLAEnum(MaintenanceTaskStatus), default=MaintenanceTaskStatus.IDLE, nullable=False, index=True)
    
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    
    # Results and logging
    files_processed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    bytes_freed: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False, comment="Bytes freed in this run")
    errors_encountered: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    
    # Detailed results as JSON
    execution_details: Mapped[Dict[str, Any]] = mapped_column(JSON, nullable=True, comment="Detailed execution results and logs")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True, comment="Error message if task failed")


class DataCleanerConfig(Base):
    """Specialized configuration for data cleaner tasks"""
    
    maintenance_config_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True, comment="References maintenance_configs.id")
    
    # Directory to monitor
    target_directory: Mapped[str] = mapped_column(String(500), nullable=False, comment="Directory path to monitor and clean")
    
    # Cleanup behavior
    cleanup_mode: Mapped[CleanupMode] = mapped_column(SQLAEnum(CleanupMode), nullable=False)
    deletion_strategy: Mapped[DeletionStrategy] = mapped_column(SQLAEnum(DeletionStrategy), default=DeletionStrategy.OLDEST_FIRST, nullable=False)
    
    # High water mark settings (bytes)
    high_water_mark_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True, comment="Maximum bytes to keep (for high_water_mark mode)")
    
    # Free space settings (percentage)
    target_free_space_percentage: Mapped[Optional[float]] = mapped_column(Float, nullable=True, comment="Target free space percentage (for free_space_percentage mode)")
    
    # Safety settings
    minimum_free_space_gb: Mapped[float] = mapped_column(Float, default=1.0, nullable=False, comment="Minimum free space to maintain in GB")
    dry_run: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, comment="If true, log what would be deleted but don't actually delete")
    
    # File age settings
    minimum_file_age_hours: Mapped[int] = mapped_column(Integer, default=1, nullable=False, comment="Don't delete files newer than this many hours")
    
    # Pattern matching
    file_pattern: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, comment="Glob pattern for files to consider (e.g., '*.dcm')")
    exclude_pattern: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, comment="Glob pattern for files to exclude")
    
    # Performance settings
    batch_size: Mapped[int] = mapped_column(Integer, default=100, nullable=False, comment="Number of files to process in each batch")
    max_files_per_run: Mapped[int] = mapped_column(Integer, default=10000, nullable=False, comment="Maximum files to process in a single run")
