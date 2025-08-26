# app/core/logging_config.py
"""
Centralized logging configuration for all Axiom services.
This ensures consistent JSON logging across API, workers, listeners, etc.
"""
import logging
import logging.config
import sys
from typing import Optional

import structlog
from app.core.config import settings


def get_log_level() -> str:
    """Get the log level from settings, defaulting to INFO."""
    log_level_str = getattr(settings, 'LOG_LEVEL', "INFO").upper()
    return log_level_str


class ProcessorFormatter:
    """Custom processor for structlog integration with standard logging."""
    
    def __init__(self, processor):
        self.processor = processor
    
    def format(self, record):
        # This will be called by the standard logging handler
        return self.processor(record.getMessage())
    
    @staticmethod
    def wrap_for_formatter(logger, method_name, event_dict):
        """
        Wrap the final processor step for use with ProcessorFormatter.
        This formats the structured log data without JSON encoding here.
        """
        return event_dict


def configure_json_logging(
    service_name: Optional[str] = None,
    log_level: Optional[str] = None,
    disable_existing_loggers: bool = True
) -> structlog.BoundLogger:
    """
    Configure structured JSON logging using structlog.
    
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
    
    # Standard Python logging configuration
    logging_config = {
        "version": 1,
        "disable_existing_loggers": disable_existing_loggers,
        "formatters": {
            "json_formatter": {
                "()": "structlog.stdlib.ProcessorFormatter",
                "processor": structlog.processors.JSONRenderer(),
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "json_formatter",
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
        },
    }
    
    # Clear any existing handlers first
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    
    # Apply the configuration
    logging.config.dictConfig(logging_config)
    
    # Configure structlog - NO JSONRenderer here to avoid double encoding
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
            ProcessorFormatter.wrap_for_formatter,  # Only JSON formatting happens in the handler
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
    
    logger.info(
        "JSON logging configured",
        service=service_name,
        log_level=log_level,
        structlog_version=structlog.__version__
    )
    
    return logger


def configure_celery_logging() -> None:
    """Configure Celery-specific logging to use structured JSON format."""
    
    # Configure Celery's built-in loggers
    celery_loggers = [
        'celery',
        'celery.worker',
        'celery.task',
        'celery.redirected'
    ]
    
    for logger_name in celery_loggers:
        logger = logging.getLogger(logger_name)
        logger.handlers = []  # Remove any existing handlers
        
        # Add JSON formatter handler
        handler = logging.StreamHandler(sys.stdout)
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer()
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    
    # Configure structlog for Celery workers
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
            ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
