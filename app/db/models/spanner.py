# app/db/models/spanner.py
from typing import Optional, Dict, Any, List
from datetime import datetime
from sqlalchemy import String, Integer, Boolean, Text, JSON, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func, expression

from app.db.base import Base


class SpannerConfig(Base):
    """
    Database model for DICOM Query Spanning configurations.
    Defines how external queries should be distributed across multiple sources.
    """
    __tablename__ = "spanner_configs"

    # --- Basic Config ---
    name: Mapped[str] = mapped_column(
        String(100), unique=True, index=True, nullable=False,
        comment="Unique, user-friendly name for this spanner configuration."
    )
    description: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Optional description of what this spanner handles."
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=expression.true(), index=True,
        comment="Whether this spanner configuration is active."
    )

    # --- Protocol Support ---
    supports_cfind: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=expression.true(),
        comment="Whether this spanner handles C-FIND queries."
    )
    supports_cget: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=expression.false(),
        comment="Whether this spanner handles C-GET retrievals."
    )
    supports_cmove: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=expression.true(),
        comment="Whether this spanner handles C-MOVE retrievals."
    )
    supports_qido: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=expression.true(),
        comment="Whether this spanner handles QIDO-RS queries."
    )
    supports_wado: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=expression.true(),
        comment="Whether this spanner handles WADO-RS/WADO-URI retrievals."
    )

    # --- Strategy Configuration ---
    query_timeout_seconds: Mapped[int] = mapped_column(
        Integer, nullable=False, default=30,
        comment="Maximum time to wait for all sources to respond to queries."
    )
    retrieval_timeout_seconds: Mapped[int] = mapped_column(
        Integer, nullable=False, default=300,
        comment="Maximum time to wait for retrievals to complete."
    )
    
    # --- Failure Handling Strategy ---
    failure_strategy: Mapped[str] = mapped_column(
        String(20), nullable=False, default="BEST_EFFORT",
        comment="How to handle source failures: FAIL_FAST, BEST_EFFORT, MINIMUM_THRESHOLD"
    )
    minimum_success_threshold: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
        comment="Minimum number of sources that must succeed (for MINIMUM_THRESHOLD strategy)"
    )
    
    # --- Deduplication Strategy ---
    deduplication_strategy: Mapped[str] = mapped_column(
        String(20), nullable=False, default="FIRST_WINS",
        comment="How to handle duplicate results: FIRST_WINS, MOST_COMPLETE, MERGE_ALL"
    )
    
    # --- C-MOVE Proxy Strategy ---
    cmove_strategy: Mapped[str] = mapped_column(
        String(20), nullable=False, default="PROXY",
        comment="C-MOVE handling: DIRECT, PROXY, HYBRID"
    )
    
    # --- Parallel Processing ---
    max_concurrent_sources: Mapped[int] = mapped_column(
        Integer, nullable=False, default=5,
        comment="Maximum number of sources to query in parallel"
    )

    # --- State Tracking ---
    total_queries_processed: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total number of queries processed by this spanner"
    )
    total_retrievals_processed: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total number of retrievals processed by this spanner"
    )
    last_activity: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Timestamp of last query or retrieval activity"
    )

    # --- Relationships ---
    source_mappings: Mapped[List["SpannerSourceMapping"]] = relationship(
        "SpannerSourceMapping", 
        back_populates="spanner_config",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        protocols = []
        if self.supports_cfind: protocols.append("C-FIND")
        if self.supports_cget: protocols.append("C-GET")
        if self.supports_cmove: protocols.append("C-MOVE")
        if self.supports_qido: protocols.append("QIDO")
        if self.supports_wado: protocols.append("WADO")
        
        return (f"<SpannerConfig(id={self.id}, name='{self.name}', "
                f"enabled={self.is_enabled}, protocols=[{','.join(protocols)}], "
                f"sources={len(self.source_mappings) if self.source_mappings else 0})>")


class SpannerSourceMapping(Base):
    """
    Maps a spanner configuration to specific DIMSE Q/R sources with priority and settings.
    """
    __tablename__ = "spanner_source_mappings"

    # --- Foreign Keys ---
    spanner_config_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("spanner_configs.id", ondelete="CASCADE"), nullable=False,
        comment="Reference to the spanner configuration"
    )
    dimse_qr_source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("dimse_qr_sources.id", ondelete="CASCADE"), nullable=False,
        comment="Reference to the DIMSE Q/R source"
    )

    # --- Mapping Configuration ---
    priority: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1,
        comment="Priority for this source (1=highest, higher numbers=lower priority)"
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=expression.true(),
        comment="Whether this source mapping is enabled"
    )
    
    # --- Override Timeouts ---
    query_timeout_override: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
        comment="Override query timeout for this specific source (seconds)"
    )
    retrieval_timeout_override: Mapped[Optional[int]] = mapped_column(
        Integer, nullable=True,
        comment="Override retrieval timeout for this specific source (seconds)"
    )
    
    # --- Additional Query Filters ---
    additional_query_filters: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True,
        comment="Additional DICOM query filters to apply for this source mapping"
    )

    # --- Statistics ---
    queries_sent: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Number of queries sent to this source"
    )
    queries_successful: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Number of successful queries to this source"
    )
    retrievals_sent: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Number of retrievals sent to this source"
    )
    retrievals_successful: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Number of successful retrievals from this source"
    )
    last_used: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
        comment="Timestamp when this mapping was last used"
    )

    # --- Relationships ---
    spanner_config: Mapped["SpannerConfig"] = relationship(
        "SpannerConfig", 
        back_populates="source_mappings"
    )
    dimse_qr_source: Mapped["DimseQueryRetrieveSource"] = relationship(
        "DimseQueryRetrieveSource"
    )

    # --- Constraints ---
    __table_args__ = (
        UniqueConstraint('spanner_config_id', 'dimse_qr_source_id', 
                        name='uq_spanner_source_mapping'),
    )

    def __repr__(self):
        return (f"<SpannerSourceMapping(id={self.id}, "
                f"spanner={self.spanner_config_id}, source={self.dimse_qr_source_id}, "
                f"priority={self.priority}, enabled={self.is_enabled})>")


class SpannerQueryLog(Base):
    """
    Audit log for spanner queries and retrievals.
    """
    __tablename__ = "spanner_query_logs"

    # --- Query Info ---
    spanner_config_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("spanner_configs.id", ondelete="CASCADE"), nullable=False,
        comment="Reference to the spanner configuration used"
    )
    query_type: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="Type of query: C-FIND, C-GET, C-MOVE, QIDO, WADO"
    )
    query_level: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True,
        comment="Query level: PATIENT, STUDY, SERIES, INSTANCE"
    )
    query_filters: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSON, nullable=True,
        comment="Query filters that were applied"
    )
    
    # --- Request Info ---
    requesting_ae_title: Mapped[Optional[str]] = mapped_column(
        String(16), nullable=True,
        comment="AE Title of the requesting client (for DIMSE)"
    )
    requesting_ip: Mapped[Optional[str]] = mapped_column(
        String(45), nullable=True,  # IPv6 support
        comment="IP address of the requesting client"
    )
    
    # --- Results ---
    sources_queried: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0,
        comment="Number of sources that were queried"
    )
    sources_successful: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0,
        comment="Number of sources that responded successfully"
    )
    total_results_found: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0,
        comment="Total number of results found before deduplication"
    )
    deduplicated_results: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0,
        comment="Number of results after deduplication"
    )
    
    # --- Timing ---
    query_duration_seconds: Mapped[Optional[float]] = mapped_column(
        String(10), nullable=True,  # Store as string to avoid float precision issues
        comment="Total time taken for the query in seconds"
    )
    
    # --- Status ---
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="SUCCESS",
        comment="Overall status: SUCCESS, PARTIAL_SUCCESS, FAILURE, TIMEOUT"
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True,
        comment="Error message if the query failed"
    )

    # --- Relationships ---
    spanner_config: Mapped["SpannerConfig"] = relationship("SpannerConfig")

    def __repr__(self):
        return (f"<SpannerQueryLog(id={self.id}, type={self.query_type}, "
                f"spanner={self.spanner_config_id}, status={self.status}, "
                f"results={self.deduplicated_results})>")
