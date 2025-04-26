# app/db/models/crosswalk.py
import enum
from typing import Optional, Dict, Any, List
from datetime import datetime

from sqlalchemy import String, Integer, Boolean, Text, JSON, DateTime, Enum as DBEnum, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from pydantic import SecretStr # Import SecretStr

from app.db.base import Base

class CrosswalkDbType(str, enum.Enum):
    POSTGRES = "POSTGRES"
    MYSQL = "MYSQL"
    MSSQL = "MSSQL"
    # ORACLE = "ORACLE" # Deferred

class CrosswalkSyncStatus(str, enum.Enum):
    PENDING = "PENDING"
    SYNCING = "SYNCING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"

class CrosswalkDataSource(Base):
    """Configuration for connecting to an external crosswalk data source."""
    __tablename__ = "crosswalk_data_sources"

    name: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    db_type: Mapped[CrosswalkDbType] = mapped_column(DBEnum(CrosswalkDbType, name="crosswalk_db_type_enum", create_type=False), nullable=False)

    # Store connection details securely (encryption-at-rest is DB responsibility)
    # Consider using JSON for flexibility, or individual columns. JSON is simpler for now.
    connection_details: Mapped[Dict[str, Any]] = mapped_column(
        JSON, nullable=False,
        comment='DB connection parameters (host, port, user, password_secret, dbname)'
    )

    target_table: Mapped[str] = mapped_column(String(255), nullable=False, comment="Name of the table or view containing the crosswalk data.")
    sync_interval_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=3600, comment="How often to refresh the cache (in seconds).")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)

    # Sync Status Tracking
    last_sync_status: Mapped[CrosswalkSyncStatus] = mapped_column(DBEnum(CrosswalkSyncStatus, name="crosswalk_sync_status_enum", create_type=False), nullable=False, default=CrosswalkSyncStatus.PENDING)
    last_sync_time: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sync_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_sync_row_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Relationship to maps using this source
    crosswalk_maps: Mapped[List["CrosswalkMap"]] = relationship(back_populates="data_source")

    def __repr__(self):
        return f"<CrosswalkDataSource(id={self.id}, name='{self.name}', type='{self.db_type.value}')>"

class CrosswalkMap(Base):
    """Defines how to use a CrosswalkDataSource for tag mapping."""
    __tablename__ = "crosswalk_maps"

    name: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)

    data_source_id: Mapped[int] = mapped_column(ForeignKey("crosswalk_data_sources.id", ondelete="CASCADE"), nullable=False)
    data_source: Mapped["CrosswalkDataSource"] = relationship(back_populates="crosswalk_maps", lazy="joined")

    # Defines how to find the row in the remote table using incoming DICOM data
    match_columns: Mapped[List[Dict[str, str]]] = mapped_column(
        JSON, nullable=False,
        comment='How to match DICOM tags to source table columns. [{"column_name": "mrn", "dicom_tag": "0010,0020"}]'
    )

    # Defines which columns are used to build the unique cache key
    cache_key_columns: Mapped[List[str]] = mapped_column(
        JSON, nullable=False,
        comment='List of column names from the source table used to build the cache key.'
    )

    # Defines how to replace DICOM tags using data from the found row
    replacement_mapping: Mapped[List[Dict[str, str]]] = mapped_column(
        JSON, nullable=False,
        comment='How to map source table columns to target DICOM tags. [{"source_column": "new_mrn", "dicom_tag": "0010,0020", "dicom_vr": "LO"}]'
    )

    # Optional: Configuration for cache behavior
    cache_ttl_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, comment="Optional TTL for cache entries (overrides source sync interval for lookup).")
    on_cache_miss: Mapped[str] = mapped_column(String(50), nullable=False, default="fail", comment="Behavior on cache miss ('fail', 'query_db', 'log_only').")

    def __repr__(self):
        return f"<CrosswalkMap(id={self.id}, name='{self.name}', source_id={self.data_source_id})>"
