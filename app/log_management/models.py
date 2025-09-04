"""
Log Management Database Models
Supports both Docker Compose and Kubernetes deployments
"""

from sqlalchemy import String, Text, Boolean, DateTime, JSON, Enum, Integer
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from app.db.base import Base
import enum
from datetime import datetime
from typing import Optional


class RetentionTier(enum.Enum):
    CRITICAL = "critical"     # 7+ years - DICOM transactions, audit logs
    OPERATIONAL = "operational"  # 1-2 years - system health, integrations  
    DEBUG = "debug"          # 30-90 days - verbose logs, metrics


class LogRetentionPolicy(Base):
    __tablename__ = "log_retention_policies"  # pyright: ignore
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Policy targeting
    service_pattern: Mapped[str] = mapped_column(String(200), nullable=False)  # e.g., "axiom.dcm4che-*"
    log_level_filter: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)   # ERROR, WARN, INFO, DEBUG
    
    # Retention settings
    tier: Mapped[RetentionTier] = mapped_column(Enum(RetentionTier), nullable=False)
    hot_days: Mapped[int] = mapped_column(Integer, default=30)        # Fast SSD storage
    warm_days: Mapped[int] = mapped_column(Integer, default=180)      # Slower storage
    cold_days: Mapped[int] = mapped_column(Integer, default=365)      # Very slow/cheap storage
    delete_days: Mapped[int] = mapped_column(Integer, default=2555)   # 7 years default
    
    # Index lifecycle settings
    max_index_size_gb: Mapped[int] = mapped_column(Integer, default=10)
    max_index_age_days: Mapped[int] = mapped_column(Integer, default=30)
    
    # Kubernetes storage classes (optional)
    storage_class_hot: Mapped[str] = mapped_column(String(50), default="fast-ssd")
    storage_class_warm: Mapped[str] = mapped_column(String(50), default="standard")
    storage_class_cold: Mapped[str] = mapped_column(String(50), default="cold-storage")
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    # Note: created_at and updated_at are inherited from Base class


class LogArchivalRule(Base):
    """
    Rules for archiving logs to external storage (S3, GCS, etc.)
    """
    __tablename__ = "log_archival_rules"  # pyright: ignore
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Selection criteria
    service_pattern: Mapped[str] = mapped_column(String(200), nullable=False)
    age_threshold_days: Mapped[int] = mapped_column(Integer, default=365)  # Archive after 1 year
    
    # Storage destination
    storage_backend: Mapped[str] = mapped_column(String(20), default="gcs")  # gcs, s3, azure
    storage_bucket: Mapped[str] = mapped_column(String(200), nullable=False)
    storage_path_prefix: Mapped[str] = mapped_column(String(500), default="axiom-logs/")
    
    # Archive format and compression
    compression: Mapped[str] = mapped_column(String(10), default="gzip")  # gzip, lz4, snappy
    format_type: Mapped[str] = mapped_column(String(10), default="ndjson")  # ndjson, parquet
    
    # Lifecycle settings
    retention_days: Mapped[int] = mapped_column(Integer, default=2555)  # 7 years in archive
    delete_after_archive: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Status and scheduling
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    cron_schedule: Mapped[str] = mapped_column(String(100), default="0 2 * * *")  # Daily at 2 AM
    last_run: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Note: created_at and updated_at are inherited from Base class


class LogAnalyticsConfig(Base):
    """
    Configuration for log analytics and monitoring dashboards
    """
    __tablename__ = "log_analytics_configs"  # pyright: ignore
    
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Dashboard configuration
    dashboard_type: Mapped[str] = mapped_column(String(20), default="kibana")  # kibana, grafana, custom
    index_pattern: Mapped[str] = mapped_column(String(200), default="axiom-flow-*")
    
    # Query settings
    default_time_range: Mapped[str] = mapped_column(String(20), default="24h")
    refresh_interval: Mapped[str] = mapped_column(String(10), default="30s")
    
    # Alert configuration
    alerts_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    alert_rules: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    notification_channels: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    
    # HIPAA/Compliance settings
    data_retention_policy: Mapped[str] = mapped_column(String(50), default="7years")
    anonymization_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    audit_logging_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    # Note: created_at and updated_at are inherited from Base class
