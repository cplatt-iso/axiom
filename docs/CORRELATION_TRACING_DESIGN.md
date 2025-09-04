"""
Correlation ID and tracing system design for Axiom Flow

CONCEPT: Track objects through their entire lifecycle with correlation IDs
- Every DICOM study/series gets a correlation_id when it enters the system
- All logs, database records, and events use this correlation_id
- Frontend can query "show me everything that happened to study X"
"""

# 1. CORRELATION ID GENERATION
# Generate when object first enters system (API upload, DICOM listener, etc.)
import uuid
from contextvars import ContextVar
import structlog

# Thread-local correlation ID storage
correlation_id: ContextVar[str] = ContextVar('correlation_id', default=None)

def generate_correlation_id() -> str:
    """Generate a new correlation ID for tracking objects through the system"""
    return f"axm-{uuid.uuid4().hex[:12]}"

def set_correlation_id(corr_id: str):
    """Set correlation ID for current context"""
    correlation_id.set(corr_id)
    # Also set in structlog context
    structlog.contextvars.bind_contextvars(correlation_id=corr_id)

def get_correlation_id() -> str:
    """Get current correlation ID, generate if missing"""
    corr_id = correlation_id.get()
    if not corr_id:
        corr_id = generate_correlation_id()
        set_correlation_id(corr_id)
    return corr_id

# 2. DATABASE SCHEMA ADDITIONS
"""
Add correlation_id columns to all relevant tables:

ALTER TABLE processing_tasks ADD COLUMN correlation_id VARCHAR(20);
ALTER TABLE processing_results ADD COLUMN correlation_id VARCHAR(20);
ALTER TABLE dicom_studies ADD COLUMN correlation_id VARCHAR(20);
ALTER TABLE processing_rules_applied ADD COLUMN correlation_id VARCHAR(20);
ALTER TABLE dustbin_entries ADD COLUMN correlation_id VARCHAR(20);

CREATE INDEX idx_correlation_id_tasks ON processing_tasks(correlation_id);
CREATE INDEX idx_correlation_id_results ON processing_results(correlation_id);
"""

# 3. MIDDLEWARE FOR CORRELATION ID INJECTION
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

class CorrelationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Check for existing correlation ID in headers
        corr_id = request.headers.get("X-Correlation-ID") 
        
        if not corr_id:
            # Check if this is related to existing work (e.g., study_instance_uid in URL)
            study_uid = request.path_params.get("study_instance_uid")
            if study_uid:
                # Look up existing correlation_id for this study
                corr_id = await get_study_correlation_id(study_uid)
            
            if not corr_id:
                corr_id = generate_correlation_id()
        
        set_correlation_id(corr_id)
        
        # Add to response headers for client tracking
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = corr_id
        
        return response

# 4. LOGGING INTEGRATION
# structlog automatically includes correlation_id in all logs when set via contextvars

# 5. CELERY TASK INTEGRATION
from celery import current_task

def celery_task_with_correlation(correlation_id: str = None):
    """Decorator to ensure Celery tasks maintain correlation ID"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            if correlation_id:
                set_correlation_id(correlation_id)
            elif hasattr(current_task, 'correlation_id'):
                set_correlation_id(current_task.correlation_id)
            else:
                # Try to extract from task arguments (study data, etc.)
                corr_id = extract_correlation_from_args(args, kwargs)
                if corr_id:
                    set_correlation_id(corr_id)
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

# 6. NEW API ENDPOINTS FOR TRACING
"""
GET /api/v1/trace/{correlation_id} - Get all events for a correlation ID
GET /api/v1/trace/study/{study_instance_uid} - Get trace for a specific study
GET /api/v1/trace/association/{association_id} - Get all events for a DICOM association
"""

# 7. FRONTEND INTEGRATION POINTS
"""
Add "View Trace" buttons throughout the UI:
- Study list: "View processing history"
- Error reports: "Show full trace"
- Processing rules: "Show recent applications"
- DICOM associations: "Show association details"

LogViewer component gets new props:
- correlationId?: string
- studyInstanceUID?: string
- associationId?: string
"""
