"""
Phase 1: Correlation ID Foundation
- Add correlation ID utilities and middleware
- Database schema updates  
- No breaking changes to existing code
"""
from contextvars import ContextVar
from typing import Optional
import uuid
import structlog

# Context variable for correlation ID (thread-safe)
_correlation_id: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)

def generate_correlation_id() -> str:
    """Generate a new correlation ID with axiom prefix"""
    return f"axm-{uuid.uuid4().hex[:12]}"

def set_correlation_id(correlation_id: str) -> str:
    """Set correlation ID in current context and structlog"""
    _correlation_id.set(correlation_id)
    # Bind to structlog context for automatic inclusion in logs
    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
    return correlation_id

def get_correlation_id() -> Optional[str]:
    """Get current correlation ID from context"""
    return _correlation_id.get()

def get_or_create_correlation_id() -> str:
    """Get existing correlation ID or create new one"""
    corr_id = get_correlation_id()
    if not corr_id:
        corr_id = generate_correlation_id()
        set_correlation_id(corr_id)
    return corr_id

def clear_correlation_id():
    """Clear correlation ID from current context"""
    _correlation_id.set(None)
    structlog.contextvars.clear_contextvars()
