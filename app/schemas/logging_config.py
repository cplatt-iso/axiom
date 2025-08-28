# app/schemas/logging_config.py
from pydantic import BaseModel, Field
from typing import Optional


class ElasticsearchConfig(BaseModel):
    """Schema for Elasticsearch configuration."""
    host: str = Field(..., description="Elasticsearch hostname or IP address")
    port: int = Field(9200, ge=1, le=65535, description="Elasticsearch port number")
    scheme: str = Field("https", pattern="^(http|https)$", description="Connection scheme")
    username: Optional[str] = Field(None, description="Username for authentication")
    password: Optional[str] = Field(None, description="Password for authentication")
    verify_certs: bool = Field(True, description="Enable TLS certificate verification")
    ca_cert_path: Optional[str] = Field(None, description="Path to CA certificate file")
    index_pattern: str = Field("axiom-flow-*", description="Log index pattern")
    timeout_seconds: int = Field(10, ge=1, le=300, description="Connection timeout")
    max_retries: int = Field(3, ge=0, le=10, description="Maximum retry attempts")


class FluentdConfig(BaseModel):
    """Schema for Fluentd configuration."""
    host: str = Field("127.0.0.1", description="Fluentd hostname or IP address")
    port: int = Field(24224, ge=1, le=65535, description="Fluentd port number")
    tag_prefix: str = Field("axiom", description="Tag prefix for log entries")
    retry_wait: int = Field(1, ge=1, description="Retry wait time in seconds")
    max_retries: int = Field(60, ge=1, description="Maximum retry attempts")
    buffer_size: str = Field("10m", description="Maximum buffer size")


class LoggingConfig(BaseModel):
    """Complete logging configuration schema."""
    elasticsearch: ElasticsearchConfig
    fluentd: Optional[FluentdConfig] = None


class LoggingConfigResponse(BaseModel):
    """Response schema for logging configuration operations."""
    status: str = Field(..., description="Operation status")
    message: str = Field(..., description="Status message")
    config_applied: bool = Field(False, description="Whether config was applied")
    restart_required: bool = Field(False, description="Whether restart is required")
    errors: list = Field(default_factory=list, description="Any errors encountered")


class ConnectionTestResult(BaseModel):
    """Schema for connection test results."""
    status: str = Field(..., description="Test status (success/error)")
    message: str = Field(..., description="Test result message")
    response_time_ms: Optional[int] = Field(None, description="Response time in milliseconds")
    details: Optional[dict] = Field(default_factory=dict, description="Additional test details")
