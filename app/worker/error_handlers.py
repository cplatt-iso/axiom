# app/worker/error_handlers.py
"""
Enhanced error handling for Celery workers to ensure proper logging levels
and graceful handling of Redis connection failures.
"""

import sys
import traceback
import redis
from functools import wraps
from typing import Any, Callable
import structlog
from celery import signals
from celery.exceptions import Retry, WorkerLostError
from kombu.exceptions import ConnectionError as KombuConnectionError
from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

logger = structlog.get_logger(__name__)

class RedisConnectionHandler:
    """
    Centralized Redis connection error handling with proper logging.
    """
    
    @staticmethod
    def handle_redis_error(func: Callable) -> Callable:
        """
        Decorator to handle Redis connection errors gracefully with proper logging.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (RedisConnectionError, RedisTimeoutError, ConnectionError) as redis_err:
                logger.error(
                    "Redis connection error - Redis service may be down",
                    function=func.__name__,
                    error_type=type(redis_err).__name__,
                    error_message=str(redis_err),
                    exc_info=True
                )
                # Re-raise as a more specific error that can be handled upstream
                raise RedisConnectionError(f"Redis unavailable: {redis_err}") from redis_err
            except Exception as e:
                logger.error(
                    "Unexpected error in Redis operation",
                    function=func.__name__,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    exc_info=True
                )
                raise
        return wrapper
    
    @staticmethod
    def test_redis_connection(redis_client) -> bool:
        """
        Test Redis connection and log the result properly.
        Returns True if connected, False otherwise.
        """
        try:
            redis_client.ping()
            logger.debug("Redis connection test successful")
            return True
        except (RedisConnectionError, RedisTimeoutError, ConnectionError) as e:
            logger.error(
                "Redis connection test failed - Redis service appears to be down",
                error_type=type(e).__name__,
                error_message=str(e)
            )
            return False
        except Exception as e:
            logger.error(
                "Unexpected error during Redis connection test",
                error_type=type(e).__name__,
                error_message=str(e),
                exc_info=True
            )
            return False


class CeleryErrorHandler:
    """
    Enhanced error handling for Celery worker operations.
    """
    
    @staticmethod
    def setup_worker_error_handlers():
        """
        Set up enhanced error handlers for Celery worker events.
        """
        
        @signals.worker_ready.connect
        def worker_ready_handler(sender=None, **kwargs):
            """Log when worker is ready with proper structure."""
            logger.info(
                "Celery worker is ready and waiting for tasks",
                worker_pid=sender.pid if sender else None,
                worker_hostname=sender.hostname if sender else None
            )
        
        @signals.worker_shutdown.connect
        def worker_shutdown_handler(sender=None, **kwargs):
            """Log worker shutdown with proper structure."""
            logger.info(
                "Celery worker is shutting down",
                worker_pid=sender.pid if sender else None,
                worker_hostname=sender.hostname if sender else None
            )
        
        @signals.task_failure.connect
        def task_failure_handler(sender=None, task_id=None, exception=None, traceback=None, einfo=None, **kwargs):
            """
            Enhanced task failure handler that logs connection errors properly.
            """
            task_name = sender.name if sender else "unknown_task"
            
            # Check if this is a Redis/connection related error
            if isinstance(exception, (RedisConnectionError, RedisTimeoutError, KombuConnectionError)):
                logger.error(
                    "Task failed due to Redis/connection error - service may be down",
                    task_id=task_id,
                    task_name=task_name,
                    error_type=type(exception).__name__,
                    error_message=str(exception),
                    is_connection_error=True,
                    exc_info=True
                )
            elif isinstance(exception, WorkerLostError):
                logger.error(
                    "Worker lost - potential infrastructure issue",
                    task_id=task_id,
                    task_name=task_name,
                    error_type=type(exception).__name__,
                    error_message=str(exception),
                    is_worker_lost=True,
                    exc_info=True
                )
            else:
                logger.error(
                    "Task failed with application error",
                    task_id=task_id,
                    task_name=task_name,
                    error_type=type(exception).__name__,
                    error_message=str(exception),
                    exc_info=True
                )
        
        @signals.task_retry.connect
        def task_retry_handler(sender=None, task_id=None, reason=None, einfo=None, **kwargs):
            """Log task retries with proper structure."""
            task_name = sender.name if sender else "unknown_task"
            
            # Check if retry is due to connection issues
            if isinstance(reason, (RedisConnectionError, RedisTimeoutError, KombuConnectionError)):
                logger.warning(
                    "Task retry due to connection error",
                    task_id=task_id,
                    task_name=task_name,
                    retry_reason=str(reason),
                    is_connection_error=True
                )
            else:
                logger.warning(
                    "Task retry",
                    task_id=task_id,
                    task_name=task_name,
                    retry_reason=str(reason)
                )


def capture_stdout_errors():
    """
    Capture raw stdout/stderr errors and route them through structured logging.
    This helps catch Python tracebacks that would otherwise go to stdout as 'info'.
    """
    
    class LoggingStdoutHandler:
        def __init__(self, original_stdout):
            self.original_stdout = original_stdout
            
        def write(self, message):
            message = message.strip()
            if not message:
                return
                
            # Check if this looks like a Python traceback or error
            if any(indicator in message.lower() for indicator in [
                'traceback', 'error:', 'exception:', 'connectionerror',
                'redis.exceptions', 'kombu.exceptions', 'failed to connect'
            ]):
                logger.error(
                    "Captured error from stdout",
                    stdout_message=message,
                    source="stdout_capture"
                )
            else:
                # For non-error messages, still log but at debug level
                logger.debug(
                    "Captured stdout message",
                    stdout_message=message,
                    source="stdout_capture"
                )
            
            # Also write to original stdout for compatibility
            self.original_stdout.write(message + '\n')
            
        def flush(self):
            if hasattr(self.original_stdout, 'flush'):
                self.original_stdout.flush()
    
    class LoggingStderrHandler:
        def __init__(self, original_stderr):
            self.original_stderr = original_stderr
            
        def write(self, message):
            message = message.strip()
            if not message:
                return
                
            # Always log stderr as error level
            logger.error(
                "Captured error from stderr",
                stderr_message=message,
                source="stderr_capture"
            )
            
            # Also write to original stderr
            self.original_stderr.write(message + '\n')
            
        def flush(self):
            if hasattr(self.original_stderr, 'flush'):
                self.original_stderr.flush()
    
    # Replace stdout and stderr with our logging handlers
    sys.stdout = LoggingStdoutHandler(sys.stdout)
    sys.stderr = LoggingStderrHandler(sys.stderr)
    
    logger.info("Enhanced stdout/stderr error capture enabled")


def setup_enhanced_error_handling():
    """
    Set up all enhanced error handling for Celery workers.
    Call this early in worker startup.
    """
    try:
        # Set up Celery-specific error handlers
        CeleryErrorHandler.setup_worker_error_handlers()
        
        # Set up stdout/stderr capture
        capture_stdout_errors()
        
        logger.info("Enhanced error handling setup complete")
        
    except Exception as e:
        logger.error(
            "Failed to setup enhanced error handling",
            error=str(e),
            exc_info=True
        )
        raise
