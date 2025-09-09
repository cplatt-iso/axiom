"""
Phase 1 Implementation Plan: Foundation Setup

GOALS:
1. Add correlation ID infrastructure 
2. Set up middleware and logging
3. Database schema migration
4. Zero breaking changes to existing functionality

TASKS:
1. Create correlation utilities ✅ (already done)
2. Add middleware to FastAPI
3. Database migration 
4. Update existing models
5. Test with minimal integration
"""

# 1. Add middleware to main application
from app.core.middleware.correlation import CorrelationMiddleware

# Add to app/main.py:
app.add_middleware(CorrelationMiddleware)

# 2. Create database migration
"""
alembic revision --autogenerate -m "add_correlation_id_columns"

This will add correlation_id columns to all relevant tables:
- processing_tasks
- processing_results  
- dicom_studies
- dicom_series
- dustbin_entries
- processing_rules_applied
"""

# 3. Update SQLAlchemy models to include correlation_id
"""
Add to each model:
    correlation_id: str | None = Field(None, max_length=20, description="Correlation ID for request tracing")
"""

# 4. Test endpoints
"""
curl -H "X-Correlation-ID: test-123" http://localhost:8001/api/v1/health
# Should see correlation_id in logs and response headers
"""
