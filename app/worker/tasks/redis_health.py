# app/worker/tasks/redis_health.py
"""
Redis health monitoring and connection testing for Celery workers.
"""

import structlog
from celery import shared_task
from redis.exceptions import ConnectionError as RedisConnectionError
from app.services.redis_batch_triggers import redis_batch_trigger
from app.worker.error_handlers import RedisConnectionHandler

logger = structlog.get_logger(__name__)

@shared_task(name="redis_health_check_task", acks_late=True)
def redis_health_check_task():
    """
    Periodic health check for Redis connection.
    This task helps identify Redis connectivity issues early.
    """
    log = logger.bind(task_name="redis_health_check_task")
    log.info("Starting Redis health check")
    
    try:
        # Test the Redis connection used by batch triggers
        is_healthy = redis_batch_trigger.health_check()
        
        if is_healthy:
            log.info("Redis health check passed - connection is healthy")
            return {
                "status": "healthy",
                "message": "Redis connection is working properly"
            }
        else:
            log.error("Redis health check failed - connection issues detected")
            return {
                "status": "unhealthy", 
                "message": "Redis connection test failed"
            }
            
    except RedisConnectionError as redis_exc:
        log.error(
            "Redis health check failed with connection error",
            error=str(redis_exc),
            error_type="RedisConnectionError"
        )
        return {
            "status": "connection_error",
            "message": f"Redis connection error: {redis_exc}"
        }
    except Exception as e:
        log.error(
            "Redis health check failed with unexpected error",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True
        )
        return {
            "status": "error",
            "message": f"Unexpected error during health check: {e}"
        }


@shared_task(name="worker_startup_check_task", acks_late=True)
def worker_startup_check_task():
    """
    Comprehensive startup check for worker dependencies.
    This runs when a worker starts to validate all connections.
    """
    log = logger.bind(task_name="worker_startup_check_task")
    log.info("Starting worker startup dependency check")
    
    issues = []
    
    # Test Redis connection
    try:
        if redis_batch_trigger.health_check():
            log.info("Worker startup check: Redis connection OK")
        else:
            issues.append("Redis connection failed")
            log.error("Worker startup check: Redis connection FAILED")
    except Exception as e:
        issues.append(f"Redis connection error: {e}")
        log.error("Worker startup check: Redis connection ERROR", error=str(e))
    
    # You can add more dependency checks here:
    # - Database connection test
    # - Storage backend test  
    # - External API connectivity tests
    
    if issues:
        log.error(
            "Worker startup check found issues",
            issues=issues,
            issue_count=len(issues)
        )
        return {
            "status": "issues_found",
            "issues": issues,
            "message": f"Found {len(issues)} dependency issues"
        }
    else:
        log.info("Worker startup check passed - all dependencies OK")
        return {
            "status": "healthy",
            "message": "All worker dependencies are healthy"
        }
