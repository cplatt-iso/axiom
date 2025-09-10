# app/worker/tasks/__init__.py
"""
Celery Tasks Module

This module imports all Celery tasks to ensure they are discoverable by Celery.
The imports are organized by protocol and functionality for better maintainability.
"""

# Import all tasks to ensure Celery can discover them
# Exception Management Tasks
from .exception_management import (
    retry_pending_exceptions_task,
    cleanup_stale_exception_data_task,
)

# DIMSE Protocol Tasks
from .dimse import (
    process_dicom_file_task,
    process_dicom_association_task,
)

# DICOMweb Protocol Tasks
from .dicomweb import (
    process_dicomweb_metadata_task,  # QIDO-RS
    process_stow_instance_task,      # STOW-RS
    # process_wado_retrieve_task,    # WADO-RS (placeholder for future)
)

# Google Healthcare API Tasks
from .google_healthcare import (
    process_google_healthcare_metadata_task,
)

# Health Monitoring Tasks
from .health_monitoring import (
    health_monitoring_task,
)

# Redis Health Tasks
from .redis_health import (
    redis_health_check_task,
    worker_startup_check_task,
)

# Utility functions (not tasks, but shared utilities)
from .utils import (
    _move_to_error_dir,
    _handle_final_file_disposition_with_dustbin,
    _handle_final_file_disposition,  # Deprecated
)

# Export all tasks for easy importing
__all__ = [
    # Exception Management
    "retry_pending_exceptions_task",
    "cleanup_stale_exception_data_task",
    
    # DIMSE Protocol
    "process_dicom_file_task",
    "process_dicom_association_task",
    
    # DICOMweb Protocol
    "process_dicomweb_metadata_task",
    "process_stow_instance_task",
    
    # Google Healthcare API
    "process_google_healthcare_metadata_task",
    
    # Health Monitoring
    "health_monitoring_task",
    
    # Redis Health
    "redis_health_check_task",
    "worker_startup_check_task",
    
    # Utilities (for internal use)
    "_move_to_error_dir",
    "_handle_final_file_disposition_with_dustbin",
    "_handle_final_file_disposition",
]
