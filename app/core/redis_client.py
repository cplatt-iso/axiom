# app/core/redis_client.py
"""
Centralized Redis client for the Axiom backend application.
"""

import redis
import structlog
from typing import Optional

from app.core.config import settings

logger = structlog.get_logger(__name__)

# Global Redis client instance
redis_client: Optional[redis.Redis] = None

def initialize_redis_client() -> Optional[redis.Redis]:
    """
    Initialize the Redis client using application settings.
    Returns None if Redis is not configured or connection fails.
    """
    global redis_client
    
    if redis_client is not None:
        return redis_client
    
    try:
        if settings.REDIS_URL is None:
            logger.warning("REDIS_URL is not configured. Redis functionality will be unavailable.")
            return None
        
        # Create Redis client from URL
        redis_client = redis.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30
        )
        
        # Test connection
        redis_client.ping()
        logger.info("Redis client initialized successfully", url=settings.REDIS_URL)
        return redis_client
        
    except redis.ConnectionError as e:
        logger.error("Failed to connect to Redis", error=str(e), url=settings.REDIS_URL)
        redis_client = None
        return None
    except Exception as e:
        logger.error("Unexpected error initializing Redis client", error=str(e))
        redis_client = None
        return None

def get_redis_client() -> Optional[redis.Redis]:
    """
    Get the Redis client instance, initializing it if necessary.
    Returns None if Redis is not available.
    """
    if redis_client is None:
        return initialize_redis_client()
    return redis_client

# Initialize on module import
redis_client = initialize_redis_client()
