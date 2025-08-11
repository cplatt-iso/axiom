# app/schemas/spanner.py
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


# --- Enums for Strategy Types ---
class FailureStrategy(str, Enum):
    FAIL_FAST = "FAIL_FAST"
    BEST_EFFORT = "BEST_EFFORT" 
    MINIMUM_THRESHOLD = "MINIMUM_THRESHOLD"


class DeduplicationStrategy(str, Enum):
    FIRST_WINS = "FIRST_WINS"
    MOST_COMPLETE = "MOST_COMPLETE"
    MERGE_ALL = "MERGE_ALL"


class CMoveStrategy(str, Enum):
    DIRECT = "DIRECT"
    PROXY = "PROXY"
    HYBRID = "HYBRID"


class QueryStatus(str, Enum):
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILURE = "FAILURE"
    TIMEOUT = "TIMEOUT"


# --- Base Schema for Spanner Config ---
class SpannerConfigBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, 
                     description="Unique, user-friendly name for this spanner configuration.")
    description: Optional[str] = Field(None, 
                                      description="Optional description of what this spanner handles.")
    is_enabled: bool = Field(True, 
                            description="Whether this spanner configuration is active.")
    
    # Protocol Support
    supports_cfind: bool = Field(True, description="Whether this spanner handles C-FIND queries.")
    supports_cget: bool = Field(False, description="Whether this spanner handles C-GET retrievals.")
    supports_cmove: bool = Field(True, description="Whether this spanner handles C-MOVE retrievals.")
    supports_qido: bool = Field(True, description="Whether this spanner handles QIDO-RS queries.")
    supports_wado: bool = Field(True, description="Whether this spanner handles WADO-RS/WADO-URI retrievals.")
    
    # Timeout Configuration
    query_timeout_seconds: int = Field(30, gt=0, le=300, 
                                      description="Maximum time to wait for all sources to respond to queries (1-300 seconds).")
    retrieval_timeout_seconds: int = Field(300, gt=0, le=3600, 
                                          description="Maximum time to wait for retrievals to complete (1-3600 seconds).")
    
    # Strategy Configuration
    failure_strategy: FailureStrategy = Field(FailureStrategy.BEST_EFFORT, 
                                             description="How to handle source failures.")
    minimum_success_threshold: Optional[int] = Field(None, gt=0, 
                                                    description="Minimum number of sources that must succeed (required for MINIMUM_THRESHOLD strategy).")
    deduplication_strategy: DeduplicationStrategy = Field(DeduplicationStrategy.FIRST_WINS, 
                                                          description="How to handle duplicate results.")
    cmove_strategy: CMoveStrategy = Field(CMoveStrategy.PROXY, 
                                         description="C-MOVE handling strategy.")
    
    # Performance Configuration  
    max_concurrent_sources: int = Field(5, gt=0, le=20, 
                                       description="Maximum number of sources to query in parallel (1-20).")

    @model_validator(mode='after')
    def validate_minimum_threshold(self) -> 'SpannerConfigBase':
        """Validate that minimum_success_threshold is set when using MINIMUM_THRESHOLD strategy."""
        if self.failure_strategy == FailureStrategy.MINIMUM_THRESHOLD:
            if self.minimum_success_threshold is None:
                raise ValueError("minimum_success_threshold is required when using MINIMUM_THRESHOLD failure strategy")
        return self

    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate spanner name doesn't contain invalid characters."""
        v_stripped = v.strip()
        if not v_stripped:
            raise ValueError("Spanner name cannot be empty")
        # Allow alphanumeric, spaces, hyphens, underscores
        import re
        if not re.match(r'^[a-zA-Z0-9\s_-]+$', v_stripped):
            raise ValueError("Spanner name can only contain letters, numbers, spaces, hyphens, and underscores")
        return v_stripped


# --- Create Schema ---
class SpannerConfigCreate(SpannerConfigBase):
    pass


# --- Update Schema ---
class SpannerConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    is_enabled: Optional[bool] = None
    
    # Protocol Support (all optional)
    supports_cfind: Optional[bool] = None
    supports_cget: Optional[bool] = None
    supports_cmove: Optional[bool] = None
    supports_qido: Optional[bool] = None
    supports_wado: Optional[bool] = None
    
    # Timeout Configuration (optional)
    query_timeout_seconds: Optional[int] = Field(None, gt=0, le=300)
    retrieval_timeout_seconds: Optional[int] = Field(None, gt=0, le=3600)
    
    # Strategy Configuration (optional)
    failure_strategy: Optional[FailureStrategy] = None
    minimum_success_threshold: Optional[int] = Field(None, gt=0)
    deduplication_strategy: Optional[DeduplicationStrategy] = None
    cmove_strategy: Optional[CMoveStrategy] = None
    
    # Performance Configuration (optional)
    max_concurrent_sources: Optional[int] = Field(None, gt=0, le=20)

    # Re-apply name validator for updates
    _validate_name = field_validator('name', mode='before')(
        lambda v: SpannerConfigBase.validate_name(v) if v is not None else None
    )


# --- Read Schema ---
class SpannerConfigRead(SpannerConfigBase):
    id: int
    created_at: datetime
    updated_at: datetime
    
    # State tracking fields
    total_queries_processed: int = Field(0, description="Total number of queries processed by this spanner")
    total_retrievals_processed: int = Field(0, description="Total number of retrievals processed by this spanner")
    last_activity: Optional[datetime] = Field(None, description="Timestamp of last query or retrieval activity")
    
    # Include source mappings count (but not full objects to avoid circular imports)
    source_mappings_count: Optional[int] = Field(None, description="Number of sources mapped to this spanner")

    model_config = ConfigDict(from_attributes=True)


# --- Base Schema for Source Mapping ---
class SpannerSourceMappingBase(BaseModel):
    priority: int = Field(1, gt=0, description="Priority for this source (1=highest, higher numbers=lower priority)")
    is_enabled: bool = Field(True, description="Whether this source mapping is enabled")
    
    # Timeout overrides
    query_timeout_override: Optional[int] = Field(None, gt=0, le=300, 
                                                 description="Override query timeout for this specific source (seconds)")
    retrieval_timeout_override: Optional[int] = Field(None, gt=0, le=3600, 
                                                     description="Override retrieval timeout for this specific source (seconds)")
    
    # Additional filters
    additional_query_filters: Optional[Dict[str, Any]] = Field(None, 
                                                              description="Additional DICOM query filters to apply for this source mapping")

    @field_validator('additional_query_filters', mode='before')
    @classmethod
    def validate_additional_query_filters(cls, v):
        """Validate that additional query filters is valid JSON."""
        if v is None:
            return None
        if isinstance(v, str):
            import json
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                raise ValueError("additional_query_filters must be valid JSON")
        return v


# --- Create Schema for Source Mapping ---
class SpannerSourceMappingCreate(SpannerSourceMappingBase):
    dimse_qr_source_id: int = Field(..., gt=0, description="ID of the DIMSE Q/R source to map")


# --- Update Schema for Source Mapping ---
class SpannerSourceMappingUpdate(BaseModel):
    priority: Optional[int] = Field(None, gt=0)
    is_enabled: Optional[bool] = None
    query_timeout_override: Optional[int] = Field(None, gt=0, le=300)
    retrieval_timeout_override: Optional[int] = Field(None, gt=0, le=3600)
    additional_query_filters: Optional[Dict[str, Any]] = None

    # Re-apply validator for updates
    _validate_filters = field_validator('additional_query_filters', mode='before')(
        lambda v: SpannerSourceMappingBase.validate_additional_query_filters(v) if v is not None else None
    )


# --- Read Schema for Source Mapping ---
class SpannerSourceMappingRead(SpannerSourceMappingBase):
    id: int
    created_at: datetime
    updated_at: datetime
    spanner_config_id: int
    dimse_qr_source_id: int
    
    # Statistics
    queries_sent: int = Field(0, description="Number of queries sent to this source")
    queries_successful: int = Field(0, description="Number of successful queries to this source")
    retrievals_sent: int = Field(0, description="Number of retrievals sent to this source") 
    retrievals_successful: int = Field(0, description="Number of successful retrievals from this source")
    last_used: Optional[datetime] = Field(None, description="Timestamp when this mapping was last used")
    
    # Include basic source info (to avoid full circular reference)
    source_name: Optional[str] = Field(None, description="Name of the mapped DIMSE Q/R source")
    source_remote_ae_title: Optional[str] = Field(None, description="AE Title of the mapped source")

    model_config = ConfigDict(from_attributes=True)


# --- Query Log Schema ---
class SpannerQueryLogRead(BaseModel):
    id: int
    created_at: datetime
    spanner_config_id: int
    query_type: str
    query_level: Optional[str] = None
    query_filters: Optional[Dict[str, Any]] = None
    requesting_ae_title: Optional[str] = None
    requesting_ip: Optional[str] = None
    sources_queried: int
    sources_successful: int
    total_results_found: int
    deduplicated_results: int
    query_duration_seconds: Optional[str] = None
    status: QueryStatus
    error_message: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# --- Response Schemas for Lists ---
class SpannerConfigListResponse(BaseModel):
    configs: List[SpannerConfigRead] = []
    total: int = Field(0, description="Total number of spanner configurations")


class SpannerSourceMappingListResponse(BaseModel):
    mappings: List[SpannerSourceMappingRead] = []
    total: int = Field(0, description="Total number of source mappings")


class SpannerQueryLogListResponse(BaseModel):
    logs: List[SpannerQueryLogRead] = []
    total: int = Field(0, description="Total number of query logs")


# --- Test/Validation Schemas ---
class SpannerTestRequest(BaseModel):
    query_type: str = Field(..., pattern=r"^(C-FIND|QIDO)$", 
                           description="Type of test query to perform")
    query_level: Optional[str] = Field("STUDY", pattern=r"^(PATIENT|STUDY|SERIES|INSTANCE)$", 
                                      description="Query level for the test")
    test_filters: Optional[Dict[str, Any]] = Field(None, 
                                                  description="Test query filters (e.g., {'PatientID': 'TEST123'})")


class SpannerTestResult(BaseModel):
    spanner_config_id: int
    test_status: QueryStatus
    sources_tested: int
    sources_successful: int
    total_results: int
    test_duration_seconds: float
    error_message: Optional[str] = None
    source_results: List[Dict[str, Any]] = Field(default_factory=list, 
                                                description="Results from individual sources")


# Type aliases for clarity
SpannerConfigCreatePayload = SpannerConfigCreate
SpannerConfigUpdatePayload = SpannerConfigUpdate
SpannerSourceMappingCreatePayload = SpannerSourceMappingCreate
SpannerSourceMappingUpdatePayload = SpannerSourceMappingUpdate
