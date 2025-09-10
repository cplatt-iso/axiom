# Enhanced Redis Error Handling for Celery Workers

## Problem
When Redis goes down, Celery workers dump Python errors to stdout which get logged as 'info' level instead of 'error' level, making them hard to detect in log viewers.

## Solution
Implemented comprehensive Redis error handling with proper logging levels:

### 1. Enhanced Error Handlers (`app/worker/error_handlers.py`)

**Features:**
- **RedisConnectionHandler**: Decorator and utilities for proper Redis error handling
- **CeleryErrorHandler**: Enhanced Celery signal handlers for connection errors
- **Stdout/Stderr Capture**: Captures raw Python tracebacks and routes them through structured logging
- **Connection Error Detection**: Specifically identifies Redis connection issues

**Key Components:**
```python
@RedisConnectionHandler.handle_redis_error
def trigger_batch_ready(self, study_instance_uid: str, destination_id: int) -> bool:
    # Redis operations now have proper error handling
```

### 2. Redis Health Monitoring (`app/worker/tasks/redis_health.py`)

**Health Check Tasks:**
- `redis_health_check_task`: Periodic Redis connection testing (every 5 minutes)
- `worker_startup_check_task`: Comprehensive dependency check on worker startup

**Benefits:**
- Early detection of Redis connectivity issues
- Structured health status reporting
- Proactive monitoring of worker dependencies

### 3. Enhanced Service Integration

**Updated Services:**
- `app/services/redis_batch_triggers.py`: Uses new error handling decorator
- `app/worker/tasks/exception_management.py`: Handles Redis errors in task retry logic
- `app/worker/tasks/dimse/association_processing.py`: Enhanced Redis error reporting

### 4. Stdout/Stderr Error Capture

**How it works:**
- Replaces `sys.stdout` and `sys.stderr` with logging handlers
- Detects error patterns (traceback, redis.exceptions, connectionerror)
- Routes errors through structured logging at ERROR level
- Maintains compatibility with existing output

**Detection patterns:**
```python
error_indicators = [
    'traceback', 'error:', 'exception:', 'connectionerror',
    'redis.exceptions', 'kombu.exceptions', 'failed to connect'
]
```

## Implementation Details

### Error Logging Format
```json
{
  "event": "Redis connection error - Redis service may be down",
  "error_type": "RedisConnectionError", 
  "error_message": "Error connecting to redis:6379",
  "level": "error",
  "timestamp": "2025-09-09T19:25:38Z"
}
```

### Celery Beat Schedule Addition
```python
"redis-health-check": {
    "task": "redis_health_check_task",
    "schedule": timedelta(minutes=5),
},
```

### Task Retry Logic
```python
except RedisConnectionError as redis_exc:
    logger.error("Redis connection error - retrying task")
    raise self.retry(countdown=300, max_retries=3, exc=redis_exc)
```

## Usage

### Automatic Setup
Error handling is automatically configured when Celery workers start:
```python
from app.worker.error_handlers import setup_enhanced_error_handling
setup_enhanced_error_handling()  # Called in celery_app.py
```

### Manual Testing
```bash
# Test Redis error handling from the proper test directory
cd /home/icculus/axiom/backend
PYTHONPATH=/home/icculus/axiom/backend python app/tests/worker/test_redis_error_handling.py
```

## Benefits

1. **🔍 Proper Error Detection**: Redis errors now appear as ERROR level in logs
2. **📊 Enhanced Monitoring**: Health checks provide proactive monitoring
3. **🔄 Graceful Degradation**: Tasks retry appropriately when Redis is unavailable
4. **📝 Better Debugging**: Structured error information with context
5. **🚨 Alert-Ready**: Error-level logs can trigger monitoring alerts

## Error Patterns Now Detected

- `redis.exceptions.ConnectionError`
- `kombu.exceptions.ConnectionError` 
- Raw Python tracebacks from stdout
- Connection timeout errors
- Worker lost errors

## Log Viewer Benefits

Instead of seeing:
```
[INFO] Traceback (most recent call last): redis.exceptions.ConnectionError
```

You now see:
```json
{
  "level": "error",
  "event": "Redis connection error - Redis service may be down",
  "error_type": "RedisConnectionError"
}
```

This ensures Redis outages are immediately visible in log monitoring dashboards and can trigger appropriate alerts.
