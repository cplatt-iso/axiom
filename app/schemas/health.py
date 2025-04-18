# app/schemas/health.py
from typing import Dict, Optional

from pydantic import BaseModel, Field

class ComponentStatus(BaseModel):
    """Represents the status of a single system component."""
    status: str = Field(..., description="Status indicator (e.g., 'ok', 'error', 'degraded')")
    details: Optional[str] = Field(None, description="Optional details about the component's status")

class HealthCheckResponse(BaseModel):
    """Response model for the system health check endpoint."""
    status: str = Field(..., description="Overall system status")
    components: Dict[str, ComponentStatus] = Field(
        ...,
        description="Dictionary containing the status of individual components (e.g., 'database', 'message_queue')"
    )
