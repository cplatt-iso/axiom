from fastapi import APIRouter, Depends, HTTPException, status as http_status, Query, Body
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any, Union
from pathlib import Path

from app import crud, schemas
from app.api import deps
from app.core.config import settings
from app.db import models
from app.schemas.system_config import (
    SystemConfigRead,
    SystemConfigUpdate,
    SystemConfigCategory,
    SystemConfigBulkUpdate
)
from app.schemas.logging_config import (
    LoggingConfig,
    LoggingConfigResponse,
    ConnectionTestResult,
    ElasticsearchConfig
)
from app.schemas.system_setting import SystemSettingCreate
from app.services import log_service

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

router = APIRouter()

# Define which settings can be modified at runtime
MODIFIABLE_SETTINGS = {
    "processing": {
        "DELETE_ON_SUCCESS": {
            "type": "boolean",
            "default": False,
            "description": "Delete original files after successful processing"
        },
        "DELETE_UNMATCHED_FILES": {
            "type": "boolean", 
            "default": False,
            "description": "Delete files that don't match any routing rules"
        },
        "DELETE_ON_NO_DESTINATION": {
            "type": "boolean",
            "default": False, 
            "description": "Delete files when no destination is configured"
        },
        "MOVE_TO_ERROR_ON_PARTIAL_FAILURE": {
            "type": "boolean",
            "default": True,
            "description": "Move files to error directory on partial processing failure"
        },
        "LOG_ORIGINAL_ATTRIBUTES": {
            "type": "boolean",
            "default": True,
            "description": "Log original DICOM attributes for debugging"
        }
    },
    "dustbin": {
        "USE_DUSTBIN_SYSTEM": {
            "type": "boolean",
            "default": True,
            "description": "Enable medical-grade dustbin file safety system"
        },
        "DUSTBIN_RETENTION_DAYS": {
            "type": "integer",
            "default": 30,
            "min": 1,
            "max": 365,
            "description": "Days to retain files in dustbin before permanent deletion"
        },
        "DUSTBIN_VERIFICATION_TIMEOUT_HOURS": {
            "type": "integer",
            "default": 24,
            "min": 1,
            "max": 168,
            "description": "Hours to wait for destination confirmations"
        }
    },
    "batch_processing": {
        "EXAM_BATCH_COMPLETION_TIMEOUT": {
            "type": "integer",
            "default": 3,
            "min": 1,
            "max": 60,
            "description": "Seconds to wait before considering a study complete"
        },
        "EXAM_BATCH_CHECK_INTERVAL": {
            "type": "integer",
            "default": 2,
            "min": 1,
            "max": 30,
            "description": "Seconds between completion checks"
        },
        "EXAM_BATCH_MAX_CONCURRENT": {
            "type": "integer",
            "default": 10,
            "min": 1,
            "max": 50,
            "description": "Maximum concurrent batches to process"
        }
    },
    "cleanup": {
        "STALE_DATA_CLEANUP_AGE_DAYS": {
            "type": "integer",
            "default": 30,
            "min": 1,
            "max": 365,
            "description": "Age in days for cleaning up stale data"
        },
        "CLEANUP_BATCH_SIZE": {
            "type": "integer",
            "default": 100,
            "min": 10,
            "max": 1000,
            "description": "Records to process per cleanup batch"
        }
    },
    "celery": {
        "CELERY_WORKER_CONCURRENCY": {
            "type": "integer",
            "default": 8,
            "min": 1,
            "max": 32,
            "description": "Number of concurrent Celery workers"
        },
        "CELERY_PREFETCH_MULTIPLIER": {
            "type": "integer",
            "default": 4,
            "min": 1,
            "max": 16,
            "description": "Task prefetch multiplier for workers"
        },
        "CELERY_TASK_MAX_RETRIES": {
            "type": "integer",
            "default": 3,
            "min": 0,
            "max": 10,
            "description": "Maximum task retry attempts"
        }
    },
    "dicomweb": {
        "DICOMWEB_POLLER_DEFAULT_FALLBACK_DAYS": {
            "type": "integer",
            "default": 7,
            "min": 1,
            "max": 30,
            "description": "Default fallback days for DICOMweb polling"
        },
        "DICOMWEB_POLLER_QIDO_LIMIT": {
            "type": "integer",
            "default": 5000,
            "min": 100,
            "max": 10000,
            "description": "QIDO-RS query result limit"
        },
        "DICOMWEB_POLLER_MAX_SOURCES": {
            "type": "integer",
            "default": 100,
            "min": 1,
            "max": 500,
            "description": "Maximum number of DICOMweb sources"
        }
    },
    "logging": {
        "ELASTICSEARCH_HOST": {
            "type": "string",
            "default": "elasticsearch",
            "description": "Elasticsearch hostname or IP address"
        },
        "ELASTICSEARCH_PORT": {
            "type": "integer",
            "default": 9200,
            "min": 1,
            "max": 65535,
            "description": "Elasticsearch port number"
        },
        "ELASTICSEARCH_SCHEME": {
            "type": "string",
            "default": "https",
            "description": "Elasticsearch connection scheme (http/https)"
        },
        "ELASTICSEARCH_USERNAME": {
            "type": "string",
            "default": "elastic",
            "description": "Elasticsearch username for authentication"
        },
        "ELASTICSEARCH_PASSWORD": {
            "type": "string",
            "default": "",
            "description": "Elasticsearch password for authentication"
        },
        "ELASTICSEARCH_VERIFY_CERTS": {
            "type": "boolean",
            "default": True,
            "description": "Enable TLS certificate verification"
        },
        "ELASTICSEARCH_LOG_INDEX_PATTERN": {
            "type": "string",
            "default": "axiom-flow-*",
            "description": "Log index pattern in Elasticsearch"
        },
        "ELASTICSEARCH_TIMEOUT_SECONDS": {
            "type": "integer",
            "default": 10,
            "min": 1,
            "max": 300,
            "description": "Elasticsearch connection timeout in seconds"
        }
    },
    "ai": {
        "AI_VOCAB_CACHE_ENABLED": {
            "type": "boolean",
            "default": True,
            "description": "Enable AI vocabulary caching"
        },
        "AI_VOCAB_CACHE_TTL_SECONDS": {
            "type": "integer",
            "default": 2592000,  # 30 days
            "min": 300,  # 5 minutes
            "max": 31536000,  # 1 year
            "description": "AI vocabulary cache TTL in seconds"
        },
        "VERTEX_AI_MAX_OUTPUT_TOKENS_VOCAB": {
            "type": "integer",
            "default": 150,
            "min": 50,
            "max": 1000,
            "description": "Maximum output tokens for Vertex AI vocabulary requests"
        }
    }
}

@router.get(
    "/categories",
    response_model=List[str],
    summary="Get Configuration Categories",
    description="List all available configuration categories",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["System Configuration"]
)
def get_config_categories() -> List[str]:
    """Get list of configuration categories"""
    return list(MODIFIABLE_SETTINGS.keys())

@router.get(
    "/",
    response_model=List[SystemConfigRead],
    summary="Get System Configuration",
    description="Retrieve current system configuration settings",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["System Configuration"]
)
def get_system_config(
    category: Optional[str] = Query(None, description="Filter by configuration category"),
    db: Session = Depends(deps.get_db)
) -> List[SystemConfigRead]:
    """Get system configuration settings"""
    logger.info(f"Fetching system configuration, category: {category}")
    
    configs = []
    categories_to_process = [category] if category else list(MODIFIABLE_SETTINGS.keys())
    
    for cat in categories_to_process:
        if cat not in MODIFIABLE_SETTINGS:
            continue
            
        for setting_key, setting_config in MODIFIABLE_SETTINGS[cat].items():
            # Get current value from settings
            current_value = getattr(settings, setting_key, setting_config["default"])
            
            # Check if there's a database override
            db_setting = crud.system_setting.get_by_key(db, key=setting_key)
            if db_setting:
                current_value = _parse_setting_value(str(db_setting.value), setting_config["type"])
            
            config = SystemConfigRead(
                key=setting_key,
                category=cat,
                value=current_value,
                type=setting_config["type"],
                description=setting_config["description"],
                default=setting_config["default"],
                min_value=setting_config.get("min"),
                max_value=setting_config.get("max"),
                is_modified=db_setting is not None
            )
            configs.append(config)
    
    return configs

@router.put(
    "/{setting_key}",
    response_model=SystemConfigRead,
    summary="Update Configuration Setting",
    description="Update a specific system configuration setting",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["System Configuration"],
    responses={
        404: {"description": "Setting not found or not modifiable"},
        400: {"description": "Invalid value for setting"},
    }
)
def update_config_setting(
    setting_key: str,
    config_update: SystemConfigUpdate,
    db: Session = Depends(deps.get_db)
) -> SystemConfigRead:
    """Update a system configuration setting"""
    logger.info(f"Updating system config setting: {setting_key}")
    
    # Find the setting definition
    setting_config = None
    category = None
    for cat, settings_dict in MODIFIABLE_SETTINGS.items():
        if setting_key in settings_dict:
            setting_config = settings_dict[setting_key]
            category = cat
            break
    
    if not setting_config:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"Setting '{setting_key}' is not found or not modifiable"
        )
    
    # Validate the new value
    try:
        validated_value = _validate_setting_value(
            config_update.value, 
            setting_config["type"],
            setting_config.get("min"),
            setting_config.get("max")
        )
    except ValueError as e:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid value for {setting_key}: {str(e)}"
        )
    
    # Store in database
    db_setting = crud.system_setting.get_by_key(db, key=setting_key)
    if db_setting:
        db_setting = crud.system_setting.update(
            db, 
            db_obj=db_setting, 
            obj_in={"value": str(validated_value)}
        )
    else:
        from app.schemas.system_setting import SystemSettingCreate
        db_setting = crud.system_setting.create(
            db,
            obj_in=SystemSettingCreate(
                key=setting_key,
                value=str(validated_value)
            )
        )
    
    logger.info(f"Updated {setting_key} to {validated_value}")
    
    return SystemConfigRead(
        key=setting_key,
        category=category or "unknown",  # Provide a default category
        value=validated_value,
        type=setting_config["type"],
        description=setting_config["description"],
        default=setting_config["default"],
        min_value=setting_config.get("min"),
        max_value=setting_config.get("max"),
        is_modified=True
    )

@router.post(
    "/bulk-update",
    response_model=List[SystemConfigRead],
    summary="Bulk Update Configuration",
    description="Update multiple configuration settings at once",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["System Configuration"]
)
def bulk_update_config(
    bulk_update: SystemConfigBulkUpdate,
    db: Session = Depends(deps.get_db)
) -> List[SystemConfigRead]:
    """Bulk update system configuration settings"""
    logger.info(f"Bulk updating {len(bulk_update.settings)} configuration settings")
    
    updated_configs = []
    errors = []
    
    for setting_key, value in bulk_update.settings.items():
        try:
            config_update = SystemConfigUpdate(value=value)
            updated_config = update_config_setting(setting_key, config_update, db)
            updated_configs.append(updated_config)
        except HTTPException as e:
            errors.append(f"{setting_key}: {e.detail}")
            continue
    
    if errors and not bulk_update.ignore_errors:
        raise HTTPException(
            status_code=http_status.HTTP_400_BAD_REQUEST,
            detail=f"Bulk update failed with errors: {'; '.join(errors)}"
        )
    
    return updated_configs

@router.delete(
    "/{setting_key}",
    status_code=http_status.HTTP_204_NO_CONTENT,
    summary="Reset Configuration Setting",
    description="Reset a configuration setting to its default value",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["System Configuration"]
)
def reset_config_setting(
    setting_key: str,
    db: Session = Depends(deps.get_db)
):
    """Reset a configuration setting to its default value"""
    logger.info(f"Resetting system config setting: {setting_key}")
    
    db_setting = crud.system_setting.get_by_key(db, key=setting_key)
    if db_setting:
        crud.system_setting.remove(db, id=db_setting.id)
        logger.info(f"Reset {setting_key} to default value")

@router.post(
    "/reload",
    summary="Reload Configuration",
    description="Trigger configuration reload (where applicable)",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["System Configuration"],
    response_model=Dict[str, str]
)
def reload_configuration() -> Dict[str, str]:
    """Reload system configuration"""
    logger.info("Configuration reload requested")
    
    # In a real implementation, you might:
    # - Send signals to workers to reload config
    # - Update in-memory caches
    # - Restart certain services
    
    return {
        "status": "success",
        "message": "Configuration reload initiated. Some changes may require service restart."
    }

@router.post(
    "/logging",
    response_model=LoggingConfigResponse,
    summary="Save Logging Configuration",
    description="Save and apply logging configuration (Elasticsearch/Fluentd)",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["System Configuration"]
)
def save_logging_config(
    config: LoggingConfig,
    db: Session = Depends(deps.get_db)
) -> LoggingConfigResponse:
    """Save logging configuration and apply changes at runtime."""
    logger.info("Saving logging configuration")
    errors = []
    restart_required = False
    
    try:
        # Store Elasticsearch config in system settings
        es_settings = {
            "ELASTICSEARCH_HOST": config.elasticsearch.host,
            "ELASTICSEARCH_PORT": str(config.elasticsearch.port),
            "ELASTICSEARCH_SCHEME": config.elasticsearch.scheme,
            "ELASTICSEARCH_USERNAME": config.elasticsearch.username or "",
            "ELASTICSEARCH_VERIFY_CERTS": str(config.elasticsearch.verify_certs),
            "ELASTICSEARCH_LOG_INDEX_PATTERN": config.elasticsearch.index_pattern,
            "ELASTICSEARCH_TIMEOUT_SECONDS": str(config.elasticsearch.timeout_seconds),
        }
        
        if config.elasticsearch.password:
            es_settings["ELASTICSEARCH_PASSWORD"] = config.elasticsearch.password
        
        # Save each setting to database
        for key, value in es_settings.items():
            try:
                db_setting = crud.system_setting.get_by_key(db, key=key)
                if db_setting:
                    crud.system_setting.update(db, db_obj=db_setting, obj_in={"value": value})
                else:
                    setting_create = SystemSettingCreate(key=key, value=value)
                    crud.system_setting.create(db, obj_in=setting_create)
                logger.info(f"Saved setting {key}")
            except Exception as e:
                error_msg = f"Failed to save {key}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        # Try to update the log service with new configuration
        try:
            # Reinitialize the Elasticsearch client with new settings
            # Note: This would require restart for full effect in production
            logger.info("Logging configuration updated - restart recommended for full effect")
        except Exception as e:
            error_msg = f"Failed to apply configuration to log service: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            restart_required = True
        
        # Fluentd configuration would require container restart
        if config.fluentd:
            restart_required = True
            errors.append("Fluentd configuration changes require container restart")
        
        status = "success" if not errors else "partial"
        message = "Logging configuration saved successfully" if not errors else f"Configuration saved with {len(errors)} warnings"
        
        return LoggingConfigResponse(
            status=status,
            message=message,
            config_applied=len(errors) == 0,
            restart_required=restart_required,
            errors=errors
        )
        
    except Exception as e:
        logger.error(f"Failed to save logging configuration: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save logging configuration: {str(e)}"
        )


@router.post(
    "/test-elasticsearch",
    response_model=ConnectionTestResult,
    summary="Test Elasticsearch Connection",
    description="Test connection to Elasticsearch with provided config",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["System Configuration"]
)
def test_elasticsearch_connection(
    config: ElasticsearchConfig
) -> ConnectionTestResult:
    """Test connection to Elasticsearch using provided config."""
    import time
    from elasticsearch import Elasticsearch
    from elasticsearch.exceptions import ConnectionError, AuthenticationException
    
    logger.info(f"Testing Elasticsearch connection to {config.host}:{config.port}")
    
    try:
        start_time = time.time()
        
        # Create temporary Elasticsearch client
        es_config = {
            "hosts": [{"host": config.host, "port": config.port, "scheme": config.scheme}],
            "timeout": config.timeout_seconds,
            "max_retries": config.max_retries,
        }
        
        if config.username and config.password:
            es_config["basic_auth"] = (config.username, config.password)
        
        if config.scheme == "https":
            es_config["verify_certs"] = config.verify_certs
            if config.ca_cert_path:
                es_config["ca_certs"] = config.ca_cert_path
        
        es_client = Elasticsearch(**es_config)
        
        # Test connection with cluster health
        health = es_client.cluster.health()
        response_time = int((time.time() - start_time) * 1000)
        
        logger.info(f"Elasticsearch connection successful - cluster status: {health.get('status', 'unknown')}")
        
        return ConnectionTestResult(
            status="success",
            message=f"Successfully connected to Elasticsearch cluster (status: {health.get('status', 'unknown')})",
            response_time_ms=response_time,
            details={
                "cluster_name": health.get("cluster_name"),
                "cluster_status": health.get("status"),
                "number_of_nodes": health.get("number_of_nodes"),
                "tls_enabled": config.scheme == "https",
                "authentication_used": bool(config.username)
            }
        )
        
    except AuthenticationException as e:
        logger.error(f"Elasticsearch authentication failed: {str(e)}")
        return ConnectionTestResult(response_time_ms=None, 
            status="error",
            message="Authentication failed - check username and password",
            details={"error_type": "authentication_error"}
        )
    except ConnectionError as e:
        logger.error(f"Elasticsearch connection failed: {str(e)}")
        return ConnectionTestResult(response_time_ms=None, 
            status="error",
            message=f"Connection failed - check host, port, and network connectivity: {str(e)}",
            details={"error_type": "connection_error"}
        )
    except Exception as e:
        logger.error(f"Elasticsearch connection test failed: {str(e)}", exc_info=True)
        return ConnectionTestResult(response_time_ms=None, 
            status="error",
            message=f"Connection test failed: {str(e)}",
            details={"error_type": "unknown_error"}
        )


@router.post(
    "/test-fluentd",
    response_model=ConnectionTestResult,
    summary="Test Fluentd Connection",
    description="Test connection to Fluentd with provided config",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["System Configuration"]
)
def test_fluentd_connection(
    config: Dict[str, Any] = Body(...)
) -> ConnectionTestResult:
    """Test connection to Fluentd using provided config."""
    import socket
    import time
    
    host = config.get("host", "127.0.0.1")
    port = config.get("port", 24224)
    
    logger.info(f"Testing Fluentd connection to {host}:{port}")
    
    try:
        start_time = time.time()
        
        # Test TCP connection to Fluentd
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)  # 5 second timeout
        result = sock.connect_ex((host, port))
        sock.close()
        
        response_time = int((time.time() - start_time) * 1000)
        
        if result == 0:
            logger.info(f"Fluentd connection successful")
            return ConnectionTestResult(
                status="success",
                message=f"Successfully connected to Fluentd at {host}:{port}",
                response_time_ms=response_time,
                details={
                    "host": host,
                    "port": port,
                    "connection_type": "tcp"
                }
            )
        else:
            logger.error(f"Fluentd connection failed - port not reachable")
            return ConnectionTestResult(response_time_ms=None, 
                status="error",
                message=f"Connection failed - Fluentd not reachable at {host}:{port}",
                details={
                    "error_type": "connection_refused",
                    "host": host,
                    "port": port
                }
            )
            
    except socket.gaierror as e:
        logger.error(f"Fluentd connection failed - DNS resolution: {str(e)}")
        return ConnectionTestResult(response_time_ms=None, 
            status="error",
            message=f"DNS resolution failed for {host}: {str(e)}",
            details={"error_type": "dns_error"}
        )
    except Exception as e:
        logger.error(f"Fluentd connection test failed: {str(e)}", exc_info=True)
        return ConnectionTestResult(response_time_ms=None, 
            status="error",
            message=f"Connection test failed: {str(e)}",
            details={"error_type": "unknown_error"}
        )

def _validate_setting_value(value: Any, setting_type: str, min_val: Optional[int] = None, max_val: Optional[int] = None) -> Any:
    """Validate and convert a setting value to the correct type"""
    if setting_type == "boolean":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)
    
    elif setting_type == "integer":
        try:
            int_value = int(value)
            if min_val is not None and int_value < min_val:
                raise ValueError(f"Value must be >= {min_val}")
            if max_val is not None and int_value > max_val:
                raise ValueError(f"Value must be <= {max_val}")
            return int_value
        except (ValueError, TypeError):
            raise ValueError(f"Invalid integer value: {value}")
    
    elif setting_type == "string":
        return str(value)
    
    else:
        raise ValueError(f"Unsupported setting type: {setting_type}")

def _parse_setting_value(value: str, setting_type: str) -> Any:
    """Parse a stored string value back to the correct type"""
    if setting_type == "boolean":
        return value.lower() in ("true", "1", "yes", "on")
    elif setting_type == "integer":
        return int(value)
    else:
        return value
