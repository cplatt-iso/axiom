"""
Centralized logging configuration for all Axiom services.
This ensures consistent JSON logging across API, workers, listeners, etc.
Optimized for Fluent Bit + Elasticsearch integration.
"""
import logging
import logging.config
import sys
from typing import Optional

import structlog
from app.core.config import settings
from app.core.service_naming import get_axiom_service_name


def get_log_level() -> str:
    """Get the log level from settings, defaulting to INFO."""
    log_level_str = getattr(settings, 'LOG_LEVEL', "INFO").upper()
    return log_level_str


def configure_json_logging(
    service_name: Optional[str] = None,
    log_level: Optional[str] = None,
    disable_existing_loggers: bool = True
) -> structlog.BoundLogger:
    """
    Configure structured JSON logging using structlog for Fluent Bit integration.
    
    This creates single JSON objects per log line that Fluent Bit can parse properly.
    No double JSON encoding - Fluent Bit handles the Docker wrapper.
    
    Args:
        service_name: Optional service name to include in logger context
        log_level: Override log level (defaults to settings.LOG_LEVEL)
        disable_existing_loggers: Whether to disable existing loggers
    
    Returns:
        Configured structlog logger instance
    """
    if log_level is None:
        log_level = get_log_level()
    
    if not isinstance(log_level, str):
        log_level = str(log_level)
        
    # Use standardized service naming if not provided
    if service_name is None:
        service_name = get_axiom_service_name()
    
    # Configure standard Python logging to use simple formatter
    # Let structlog handle the JSON formatting
    logging_config = {
        "version": 1,
        "disable_existing_loggers": disable_existing_loggers,
        "formatters": {
            "plain": {
                "format": "%(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "plain",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "": {  # Root logger
                "handlers": ["console"],
                "level": log_level.upper(),
                "propagate": False,
            },
            # Common noisy libraries that we want to quiet down
            "urllib3": {"level": "WARNING", "handlers": ["console"], "propagate": False},
            "requests": {"level": "WARNING", "handlers": ["console"], "propagate": False},
            "aiohttp": {"level": "INFO", "handlers": ["console"], "propagate": False},
            "sqlalchemy.engine": {"level": "WARNING", "handlers": ["console"], "propagate": False},
            # RabbitMQ/AMQP libraries - very verbose at DEBUG level
            "aio_pika": {"level": "INFO", "handlers": ["console"], "propagate": False},
            "aiormq": {"level": "INFO", "handlers": ["console"], "propagate": False},
            "pika": {"level": "INFO", "handlers": ["console"], "propagate": False},
        },
    }
    
    # Clear any existing handlers first
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    
    # Apply the configuration
    logging.config.dictConfig(logging_config)
    
    # Configure structlog to output clean JSON that Fluent Bit can parse
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.CallsiteParameterAdder(
                parameters=[structlog.processors.CallsiteParameter.FILENAME,
                           structlog.processors.CallsiteParameter.LINENO]
            ),
            # Final processor: JSON output that goes directly to stdout
            # Fluent Bit will receive this as the 'log' field and parse it
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    # Create logger with service context if provided
    logger = structlog.get_logger()
    if service_name:
        logger = logger.bind(service=service_name)
    
    return logger


def configure_celery_logging() -> None:
    """Configure Celery-specific logging to use structured JSON format."""
    
    # Configure Celery's built-in loggers including beat scheduler
    celery_loggers = [
        'celery',
        'celery.worker',
        'celery.worker.strategy',  # Add worker strategy logs (task received messages)
        'celery.worker.consumer',  # Add worker consumer logs 
        'celery.task',
        'celery.redirected',
        'celery.beat',  # Add beat scheduler logs
        'celery.app.beat',  # Add beat app logs
    ]
    
    for logger_name in celery_loggers:
        logger = logging.getLogger(logger_name)
        logger.handlers = []  # Remove any existing handlers
        
        # Add plain formatter handler - let structlog handle JSON
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    
    # Configure structlog for Celery workers and beat
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.CallsiteParameterAdder(
                parameters=[structlog.processors.CallsiteParameter.FILENAME,
                           structlog.processors.CallsiteParameter.LINENO]
            ),
            # Use JSON renderer directly - no double encoding
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def configure_uvicorn_logging() -> None:
    """Configure Uvicorn-specific logging to use structured JSON format."""
    
    # Configure Uvicorn's built-in loggers
    uvicorn_loggers = [
        'uvicorn',
        'uvicorn.access',
        'uvicorn.error'
    ]
    
    for logger_name in uvicorn_loggers:
        logger = logging.getLogger(logger_name)
        logger.handlers = []  # Remove any existing handlers
        
        # Add plain formatter handler - let structlog handle JSON
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
