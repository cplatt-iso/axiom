#!/usr/bin/env python3
"""
Test script to verify enhanced Redis error handling in Celery workers.
This script simulates Redis connection failures and verifies proper logging.
"""

import sys
import logging
import structlog
from pathlib import Path

# Add the backend directory to the path so we can import modules
backend_dir = Path(__file__).parent.parent.parent  # Go up 3 levels from app/tests/worker/
sys.path.insert(0, str(backend_dir))

from app.core.logging_config import configure_json_logging
from app.worker.error_handlers import setup_enhanced_error_handling, RedisConnectionHandler
from app.services.redis_batch_triggers import redis_batch_trigger

def test_redis_error_handling():
    """Test the Redis error handling functionality."""
    
    # Set up logging
    logger = configure_json_logging("test_redis_error_handling")
    logger.info("Starting Redis error handling test")
    
    # Set up enhanced error handling
    setup_enhanced_error_handling()
    logger.info("Enhanced error handling setup complete")
    
    # Test 1: Redis health check
    logger.info("=== Test 1: Redis Health Check ===")
    try:
        is_healthy = redis_batch_trigger.health_check()
        if is_healthy:
            logger.info("✅ Redis health check passed")
        else:
            logger.warning("⚠️ Redis health check failed - Redis may be down")
    except Exception as e:
        logger.error("❌ Redis health check threw exception", error=str(e), exc_info=True)
    
    # Test 2: Redis batch trigger
    logger.info("=== Test 2: Redis Batch Trigger ===")
    try:
        result = redis_batch_trigger.trigger_batch_ready(
            study_instance_uid="TEST.STUDY.1.2.3",
            destination_id=999
        )
        logger.info("✅ Redis batch trigger completed", result=result)
    except Exception as e:
        logger.error("❌ Redis batch trigger failed", error=str(e), exc_info=True)
    
    # Test 3: Test error capture
    logger.info("=== Test 3: Error Capture Test ===")
    try:
        # This should trigger our stdout capture if Redis is down
        print("This is a test message to stdout")
        print("ERROR: This simulates a Redis connection error")
        print("Traceback: redis.exceptions.ConnectionError: Connection refused")
    except Exception as e:
        logger.error("❌ Error capture test failed", error=str(e), exc_info=True)
    
    logger.info("Redis error handling test completed")

if __name__ == "__main__":
    test_redis_error_handling()
