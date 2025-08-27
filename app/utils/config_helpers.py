"""
Helper utilities for accessing dynamic configuration values throughout the application.

This module provides convenient functions for accessing configuration values that may
have been overridden by administrators through the System Configuration API.
"""

from typing import Any, Optional
from sqlalchemy.orm import Session
from functools import lru_cache
from app import crud
from app.core.config import settings
from app.services.config_service import config_service

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


def get_config_value(db: Session, setting_key: str, default: Any = None) -> Any:
    """
    Get the effective configuration value for a setting.
    
    This function checks for database overrides first, then falls back to
    the settings object, then to the provided default.
    
    Args:
        db: Database session
        setting_key: The configuration setting key
        default: Default value to use if setting is not found
        
    Returns:
        The effective configuration value
    """
    return config_service.get_effective_setting(db, setting_key, default)


def get_processing_config(db: Session) -> dict:
    """Get all processing-related configuration values"""
    return {
        "delete_on_success": get_config_value(db, "DELETE_ON_SUCCESS", False),
        "delete_unmatched_files": get_config_value(db, "DELETE_UNMATCHED_FILES", False),
        "delete_on_no_destination": get_config_value(db, "DELETE_ON_NO_DESTINATION", False),
        "move_to_error_on_partial_failure": get_config_value(db, "MOVE_TO_ERROR_ON_PARTIAL_FAILURE", True),
        "log_original_attributes": get_config_value(db, "LOG_ORIGINAL_ATTRIBUTES", True),
    }


def get_dustbin_config(db: Session) -> dict:
    """Get all dustbin system configuration values"""
    return {
        "use_dustbin_system": get_config_value(db, "USE_DUSTBIN_SYSTEM", True),
        "retention_days": get_config_value(db, "DUSTBIN_RETENTION_DAYS", 30),
        "verification_timeout_hours": get_config_value(db, "DUSTBIN_VERIFICATION_TIMEOUT_HOURS", 24),
    }


def get_batch_processing_config(db: Session) -> dict:
    """Get all batch processing configuration values"""
    return {
        "completion_timeout": get_config_value(db, "EXAM_BATCH_COMPLETION_TIMEOUT", 3),
        "check_interval": get_config_value(db, "EXAM_BATCH_CHECK_INTERVAL", 2),
        "max_concurrent": get_config_value(db, "EXAM_BATCH_MAX_CONCURRENT", 10),
    }


def get_celery_config(db: Session) -> dict:
    """Get all Celery configuration values"""
    return {
        "worker_concurrency": get_config_value(db, "CELERY_WORKER_CONCURRENCY", 8),
        "prefetch_multiplier": get_config_value(db, "CELERY_PREFETCH_MULTIPLIER", 4),
        "task_max_retries": get_config_value(db, "CELERY_TASK_MAX_RETRIES", 3),
    }


def get_dicomweb_config(db: Session) -> dict:
    """Get all DICOMweb configuration values"""
    return {
        "poller_default_fallback_days": get_config_value(db, "DICOMWEB_POLLER_DEFAULT_FALLBACK_DAYS", 7),
        "poller_qido_limit": get_config_value(db, "DICOMWEB_POLLER_QIDO_LIMIT", 5000),
        "poller_max_sources": get_config_value(db, "DICOMWEB_POLLER_MAX_SOURCES", 100),
    }


def get_ai_config(db: Session) -> dict:
    """Get all AI configuration values"""
    return {
        "vocab_cache_enabled": get_config_value(db, "AI_VOCAB_CACHE_ENABLED", True),
        "vocab_cache_ttl_seconds": get_config_value(db, "AI_VOCAB_CACHE_TTL_SECONDS", 2592000),
        "vertex_ai_max_output_tokens_vocab": get_config_value(db, "VERTEX_AI_MAX_OUTPUT_TOKENS_VOCAB", 150),
    }


class ConfigCache:
    """
    Simple in-memory cache for configuration values to reduce database hits.
    
    Note: This is a basic implementation. In production, you might want to use
    Redis or implement cache invalidation when settings are updated.
    """
    
    def __init__(self, ttl_seconds: int = 300):  # 5 minute default TTL
        self.ttl_seconds = ttl_seconds
        self._cache = {}
    
    @lru_cache(maxsize=128)
    def get_cached_value(self, setting_key: str, db_session_id: int) -> Any:
        """
        Get a cached configuration value.
        
        Note: This is a simplified implementation. In a real system, you'd want
        proper cache invalidation when settings are updated.
        """
        # This is just a placeholder - in production you'd implement proper caching
        return None


# Example usage functions for common patterns
def should_delete_on_success(db: Session) -> bool:
    """Check if files should be deleted after successful processing"""
    return get_config_value(db, "DELETE_ON_SUCCESS", False)


def should_use_dustbin_system(db: Session) -> bool:
    """Check if the dustbin system should be used"""
    return get_config_value(db, "USE_DUSTBIN_SYSTEM", True)


def get_dustbin_retention_days(db: Session) -> int:
    """Get the dustbin retention period in days"""
    return get_config_value(db, "DUSTBIN_RETENTION_DAYS", 30)


def get_celery_worker_concurrency(db: Session) -> int:
    """Get the Celery worker concurrency setting"""
    return get_config_value(db, "CELERY_WORKER_CONCURRENCY", 8)
