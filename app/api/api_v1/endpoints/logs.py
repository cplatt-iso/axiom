# app/api/api_v1/endpoints/logs.py
"""
API endpoints for log retrieval and monitoring.
Because who has time to SSH into servers and tail logs like it's 2005?
"""
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.api import deps
from app.services.log_service import log_service, LogQueryParams
import structlog

logger = structlog.get_logger(__name__)

router = APIRouter()


class LogQueryRequest(BaseModel):
    """Request parameters for log queries."""
    start_time: Optional[datetime] = Field(None, description="Start time for log query (ISO format)")
    end_time: Optional[datetime] = Field(None, description="End time for log query (ISO format)")
    level: Optional[str] = Field(None, description="Log level filter (DEBUG, INFO, WARNING, ERROR)")
    service: Optional[str] = Field(None, description="Service name filter")
    container_name: Optional[str] = Field(None, description="Container name filter")
    search_text: Optional[str] = Field(None, description="Free text search in log messages")
    limit: int = Field(100, ge=1, le=1000, description="Maximum number of log entries to return")
    offset: int = Field(0, ge=0, description="Number of log entries to skip")


class LogResponse(BaseModel):
    """Response containing log entries and metadata."""
    entries: List[Dict[str, Any]]
    total: int
    took_ms: Optional[int] = None
    query_params: Dict[str, Any]
    error: Optional[str] = None


@router.get("/security-info", summary="Get log service security configuration")
async def logs_security_info(
    current_user=Depends(deps.get_current_active_user)
) -> Dict[str, Any]:
    """
    Get security configuration information for the log service.
    Shows TLS and authentication status.
    """
    try:
        from app.core.config import settings
        
        # Get current configuration
        config_info = {
            "elasticsearch": {
                "host": getattr(settings, 'ELASTICSEARCH_HOST', 'unknown'),
                "port": getattr(settings, 'ELASTICSEARCH_PORT', 9200),
                "scheme": getattr(settings, 'ELASTICSEARCH_SCHEME', 'http'),
                "tls_enabled": getattr(settings, 'ELASTICSEARCH_SCHEME', 'http') == 'https',
                "certificate_verification": getattr(settings, 'ELASTICSEARCH_VERIFY_CERTS', False),
                "ca_cert_configured": bool(getattr(settings, 'ELASTICSEARCH_CA_CERT_PATH', None)),
                "authentication_enabled": bool(getattr(settings, 'ELASTICSEARCH_USERNAME', None))
            },
            "security_recommendations": []
        }
        
        # Add security recommendations
        if not config_info["elasticsearch"]["tls_enabled"]:
            config_info["security_recommendations"].append(
                "Consider enabling HTTPS for Elasticsearch connection"
            )
        
        if config_info["elasticsearch"]["tls_enabled"] and not config_info["elasticsearch"]["certificate_verification"]:
            config_info["security_recommendations"].append(
                "Enable certificate verification for secure TLS connection"
            )
            
        if not config_info["elasticsearch"]["authentication_enabled"]:
            config_info["security_recommendations"].append(
                "Configure authentication credentials for Elasticsearch"
            )
        
        # Test actual connection security
        health = log_service.health_check()
        if health.get("status") == "ok":
            config_info["connection_status"] = "healthy"
            config_info["actual_tls_info"] = health.get("tls_info", {})
        else:
            config_info["connection_status"] = "unhealthy"
            config_info["connection_error"] = health.get("message")
        
        logger.info("Security configuration requested",
                   user_id=getattr(current_user, 'id', 'unknown'))
        
        return config_info
        
    except Exception as e:
        logger.error("Failed to get security info", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get security info: {str(e)}"
        )


@router.get("/health", summary="Check log service health")
async def logs_health_check(
    current_user=Depends(deps.get_current_active_user)
) -> Dict[str, Any]:
    """
    Check if the log service can connect to Elasticsearch.
    Useful for debugging when logs aren't showing up.
    """
    try:
        health = log_service.health_check()
        return {
            "status": "ok",
            "elasticsearch": health,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error("Log health check failed", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Log service health check failed: {str(e)}"
        )


@router.get("/", response_model=LogResponse, summary="Query application logs")
async def query_logs(
    start_time: Optional[datetime] = Query(None, description="Start time for log query"),
    end_time: Optional[datetime] = Query(None, description="End time for log query"),
    level: Optional[str] = Query(None, description="Log level (DEBUG, INFO, WARNING, ERROR)"),
    service: Optional[str] = Query(None, description="Service name filter"),
    container_name: Optional[str] = Query(None, description="Container name filter"),
    search_text: Optional[str] = Query(None, description="Search text in log messages"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum entries to return"),
    offset: int = Query(0, ge=0, description="Number of entries to skip"),
    current_user=Depends(deps.get_current_active_user)
) -> LogResponse:
    """
    Query application logs from Elasticsearch.
    
    This endpoint allows you to:
    - Filter logs by time range, level, service, or container
    - Search for specific text in log messages
    - Paginate through large result sets
    
    If no time range is specified, defaults to the last hour of logs.
    """
    try:
        # Validate log level if provided
        if level and level.upper() not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid log level '{level}'. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL"
            )
        
        # Validate time range
        if start_time and end_time and start_time >= end_time:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="start_time must be before end_time"
            )
        
        # Limit time range to prevent massive queries
        if start_time and end_time:
            time_diff = end_time - start_time
            if time_diff.days > 30:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Time range cannot exceed 30 days"
                )
        
        # Build query parameters
        params = LogQueryParams(
            start_time=start_time,
            end_time=end_time,
            level=level,
            service=service,
            container_name=container_name,
            search_text=search_text,
            limit=limit,
            offset=offset
        )
        
        # Execute query
        result = log_service.query_logs(params)
        
        # Log the query for audit purposes
        logger.info("Log query executed",
                   user_id=getattr(current_user, 'id', 'unknown'),
                   params=params.dict(),
                   result_count=len(result.get('entries', [])),
                   total_found=result.get('total', 0))
        
        return LogResponse(
            entries=result.get('entries', []),
            total=result.get('total', 0),
            took_ms=result.get('took_ms'),
            query_params=params.dict(),
            error=result.get('error')
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to query logs", 
                    error=str(e), 
                    user_id=getattr(current_user, 'id', 'unknown'))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query logs: {str(e)}"
        )


@router.post("/", response_model=LogResponse, summary="Query logs with POST body")
async def query_logs_post(
    request: LogQueryRequest,
    current_user=Depends(deps.get_current_active_user)
) -> LogResponse:
    """
    Query logs using POST body for complex queries.
    
    This is useful when you need to send complex search parameters
    that don't fit well in query parameters.
    """
    try:
        # Convert request to query parameters
        params = LogQueryParams(**request.dict())
        
        # Execute query
        result = log_service.query_logs(params)
        
        # Log the query for audit purposes
        logger.info("Log query executed (POST)",
                   user_id=getattr(current_user, 'id', 'unknown'),
                   params=params.dict(),
                   result_count=len(result.get('entries', [])))
        
        return LogResponse(
            entries=result.get('entries', []),
            total=result.get('total', 0),
            took_ms=result.get('took_ms'),
            query_params=params.dict(),
            error=result.get('error')
        )
        
    except Exception as e:
        logger.error("Failed to query logs (POST)", 
                    error=str(e), 
                    user_id=getattr(current_user, 'id', 'unknown'))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to query logs: {str(e)}"
        )


@router.get("/recent", summary="Get recent logs")
async def get_recent_logs(
    minutes: int = Query(15, ge=1, le=1440, description="Minutes of recent logs to fetch"),
    level: Optional[str] = Query(None, description="Log level filter"),
    service: Optional[str] = Query(None, description="Service filter"),
    limit: int = Query(50, ge=1, le=500, description="Maximum entries"),
    current_user=Depends(deps.get_current_active_user)
) -> LogResponse:
    """
    Quick endpoint to get recent logs.
    
    Useful for dashboard widgets or quick debugging.
    Defaults to the last 15 minutes of logs.
    """
    try:
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=minutes)
        
        params = LogQueryParams(
            start_time=start_time,
            end_time=end_time,
            level=level,
            service=service,
            limit=limit,
            offset=0
        )
        
        result = log_service.query_logs(params)
        
        logger.info("Recent logs query",
                   user_id=getattr(current_user, 'id', 'unknown'),
                   minutes=minutes,
                   result_count=len(result.get('entries', [])))
        
        return LogResponse(
            entries=result.get('entries', []),
            total=result.get('total', 0),
            took_ms=result.get('took_ms'),
            query_params=params.dict(),
            error=result.get('error')
        )
        
    except Exception as e:
        logger.error("Failed to get recent logs", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get recent logs: {str(e)}"
        )


@router.get("/services", summary="Get list of available services")
async def get_log_services(
    current_user=Depends(deps.get_current_active_user)
) -> Dict[str, List[str]]:
    """
    Get a list of services/containers that are generating logs.
    
    This helps the frontend populate dropdowns and filters.
    Uses Elasticsearch aggregation for better performance.
    """
    # Import log service at the top level to avoid "possibly unbound" errors
    from app.services.log_service import log_service
    
    try:
        # Check if Elasticsearch client is available
        if not log_service.client:
            raise Exception("Elasticsearch client not available")
        
        # Build aggregation query for last 24 hours
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)
        
        aggregation_query = {
            "size": 0,  # We only want aggregations, not actual log entries
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": start_time.isoformat(),
                        "lte": end_time.isoformat()
                    }
                }
            },
            "aggs": {
                "services": {
                    "terms": {"field": "service.keyword", "size": 100}
                },
                "containers": {
                    "terms": {"field": "container_name.keyword", "size": 100}
                },
                "levels": {
                    "terms": {"field": "level.keyword", "size": 10}
                }
            }
        }
        
        # Execute the aggregation query
        result = log_service.client.search(
            index=f"{log_service.index_pattern}",
            body=aggregation_query
        )
        
        # Extract unique values from aggregations
        services = [bucket["key"] for bucket in result["aggregations"]["services"]["buckets"]]
        containers = [bucket["key"] for bucket in result["aggregations"]["containers"]["buckets"]]
        levels = [bucket["key"] for bucket in result["aggregations"]["levels"]["buckets"]]
        
        # Ensure standard log levels are included
        standard_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        all_levels = list(set(levels + standard_levels))
        
        logger.info("Retrieved log services via aggregation", 
                   services_count=len(services),
                   containers_count=len(containers),
                   user_id=getattr(current_user, 'id', 'unknown'))
        
        return {
            "services": sorted(services),
            "containers": sorted(containers), 
            "levels": sorted(all_levels)
        }
        
    except Exception as e:
        logger.error("Failed to get log services", error=str(e))
        # Fallback to the existing method if aggregation fails
        try:
            params = LogQueryParams(
                start_time=datetime.now(timezone.utc) - timedelta(hours=24),
                limit=1000
            )
            
            result = log_service.query_logs(params)
            
            services = set()
            containers = set()
            
            for entry in result.get('entries', []):
                if entry.get('service'):
                    services.add(entry['service'])
                if entry.get('container_name'):
                    containers.add(entry['container_name'])
            
            return {
                "services": sorted(list(services)),
                "containers": sorted(list(containers)),
                "levels": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            }
        except Exception as fallback_error:
            logger.error("Fallback method also failed", error=str(fallback_error))
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get log services: {str(e)}"
            )
