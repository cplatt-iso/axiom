"""
Correlation ID middleware for FastAPI
Handles X-Correlation-ID headers and context management
"""
import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from app.core.correlation import set_correlation_id, get_correlation_id, generate_correlation_id

logger = structlog.get_logger(__name__)

class CorrelationMiddleware(BaseHTTPMiddleware):
    """
    Middleware to handle correlation IDs for request tracing.
    
    - Extracts correlation ID from X-Correlation-ID header
    - Generates new ID if none provided
    - Sets in response headers
    - Maintains context throughout request lifecycle
    """
    
    async def dispatch(self, request: Request, call_next):
        # Extract correlation ID from request header
        correlation_id = request.headers.get("X-Correlation-ID")
        
        if not correlation_id:
            # Generate new correlation ID
            correlation_id = generate_correlation_id()
            
            # Log that we generated a new ID
            logger.debug("Generated new correlation ID", 
                        correlation_id=correlation_id,
                        path=request.url.path)
        else:
            logger.debug("Using provided correlation ID",
                        correlation_id=correlation_id,
                        path=request.url.path)
        
        # Set correlation ID in context
        set_correlation_id(correlation_id)
        
        try:
            # Process request
            response = await call_next(request)
            
            # Add correlation ID to response headers
            response.headers["X-Correlation-ID"] = correlation_id
            
            return response
            
        except Exception as e:
            # Ensure correlation ID is in error response
            logger.error("Request failed with correlation ID",
                        correlation_id=correlation_id,
                        error=str(e))
            raise
        
        finally:
            # Context cleanup happens automatically with contextvars
            pass
