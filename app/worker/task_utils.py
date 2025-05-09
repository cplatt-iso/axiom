# backend/app/worker/task_utils.py

import logging
from typing import Optional, Dict, Any, Union
import structlog

from app.db.models.storage_backend_config import (
    StorageBackendConfig as DBStorageBackendConfigModel,
    FileSystemBackendConfig,
    GcsBackendConfig,
    CStoreBackendConfig,
    GoogleHealthcareBackendConfig,
    StowRsBackendConfig
)
from app.services.storage_backends.base_backend import StorageBackendType # For enum comparison

logger = structlog.get_logger(__name__)

def build_storage_backend_config_dict(
    db_storage_backend_model: DBStorageBackendConfigModel,
    task_id: Optional[str] = "N/A", # Optional task_id for better logging context
    destination_name_override: Optional[str] = None # For cases like GHC source processing where dest name is different
) -> Optional[Dict[str, Any]]:
    """
    Builds the configuration dictionary required by get_storage_backend 
    from a database StorageBackendConfig model instance.

    This function centralizes the logic for converting polymorphic SQLAlchemy models
    into the flat dictionary format expected by the storage backend factory.

    Args:
        db_storage_backend_model: The SQLAlchemy model instance (e.g., GcsBackendConfig).
        task_id: Optional task ID for logging.
        destination_name_override: Optional name to use in the config dict, 
                                   useful if the db_storage_backend_model.name is not the desired runtime name.


    Returns:
        A dictionary suitable for get_storage_backend, or None if an error occurs.
    """
    log = logger.bind(
        task_id=task_id,
        db_model_id=getattr(db_storage_backend_model, 'id', 'UnknownID'),
        db_model_name=getattr(db_storage_backend_model, 'name', 'UnknownName'),
        db_model_type=type(db_storage_backend_model).__name__
    )

    if not db_storage_backend_model:
        log.error("build_storage_backend_config_dict: Received None for db_storage_backend_model.")
        return None

    actual_storage_config_dict: Dict[str, Any] = {}

    try:
        # 1. Resolve backend_type to a string
        backend_type_value_from_db_model = db_storage_backend_model.backend_type
        resolved_backend_type_for_config: Optional[str] = None

        if isinstance(backend_type_value_from_db_model, str):
            resolved_backend_type_for_config = backend_type_value_from_db_model
        elif isinstance(backend_type_value_from_db_model, StorageBackendType): # Check against enum
            resolved_backend_type_for_config = backend_type_value_from_db_model.value
        elif hasattr(backend_type_value_from_db_model, 'value') and isinstance(getattr(backend_type_value_from_db_model, 'value', None), str):
            # Fallback for other enum-like objects
            resolved_backend_type_for_config = backend_type_value_from_db_model.value
        else:
            log.error(
                "DB model 'backend_type' is invalid or not a string/enum.value.",
                raw_backend_type=backend_type_value_from_db_model,
                type_of_raw=type(backend_type_value_from_db_model).__name__
            )
            return None

        if resolved_backend_type_for_config is None: # Should have been caught by else above
            log.error("Failed to resolve backend_type for config dict construction.")
            return None

        actual_storage_config_dict['type'] = resolved_backend_type_for_config
        actual_storage_config_dict['name'] = destination_name_override or db_storage_backend_model.name

        # 2. Populate type-specific fields by DIRECTLY accessing attributes
        #    of db_storage_backend_model, which is an instance of the correct subclass thanks to SQLAlchemy's polymorphic loading.

        if resolved_backend_type_for_config == StorageBackendType.FILESYSTEM.value: # Compare with enum value
            if isinstance(db_storage_backend_model, FileSystemBackendConfig):
                actual_storage_config_dict['path'] = db_storage_backend_model.path
            else:
                log.error("Type mismatch: Expected FileSystemBackendConfig.")
                return None

        elif resolved_backend_type_for_config == StorageBackendType.GCS.value:
            if isinstance(db_storage_backend_model, GcsBackendConfig):
                actual_storage_config_dict['bucket'] = db_storage_backend_model.bucket
                actual_storage_config_dict['prefix'] = db_storage_backend_model.prefix
                # gcp_credentials_secret_name is handled by get_storage_backend using settings
            else:
                log.error("Type mismatch: Expected GcsBackendConfig.")
                return None

        elif resolved_backend_type_for_config == StorageBackendType.CSTORE.value:
            if isinstance(db_storage_backend_model, CStoreBackendConfig):
                actual_storage_config_dict['remote_ae_title'] = db_storage_backend_model.remote_ae_title
                actual_storage_config_dict['remote_host'] = db_storage_backend_model.remote_host
                actual_storage_config_dict['remote_port'] = db_storage_backend_model.remote_port
                actual_storage_config_dict['local_ae_title'] = db_storage_backend_model.local_ae_title
                actual_storage_config_dict['tls_enabled'] = db_storage_backend_model.tls_enabled
                actual_storage_config_dict['tls_ca_cert_secret_name'] = db_storage_backend_model.tls_ca_cert_secret_name
                actual_storage_config_dict['tls_client_cert_secret_name'] = db_storage_backend_model.tls_client_cert_secret_name
                actual_storage_config_dict['tls_client_key_secret_name'] = db_storage_backend_model.tls_client_key_secret_name
            else:
                log.error("Type mismatch: Expected CStoreBackendConfig.")
                return None

        elif resolved_backend_type_for_config == StorageBackendType.GOOGLE_HEALTHCARE.value:
            if isinstance(db_storage_backend_model, GoogleHealthcareBackendConfig):
                actual_storage_config_dict['gcp_project_id'] = db_storage_backend_model.gcp_project_id
                actual_storage_config_dict['gcp_location'] = db_storage_backend_model.gcp_location
                actual_storage_config_dict['gcp_dataset_id'] = db_storage_backend_model.gcp_dataset_id
                actual_storage_config_dict['gcp_dicom_store_id'] = db_storage_backend_model.gcp_dicom_store_id
                # gcp_credentials_secret_name is handled by get_storage_backend using settings
            else:
                log.error("Type mismatch: Expected GoogleHealthcareBackendConfig.")
                return None

        elif resolved_backend_type_for_config == StorageBackendType.STOW_RS.value:
             if isinstance(db_storage_backend_model, StowRsBackendConfig):
                actual_storage_config_dict['base_url'] = db_storage_backend_model.base_url
                actual_storage_config_dict['username_secret_name'] = db_storage_backend_model.username_secret_name
                actual_storage_config_dict['password_secret_name'] = db_storage_backend_model.password_secret_name
                actual_storage_config_dict['tls_ca_cert_secret_name'] = db_storage_backend_model.tls_ca_cert_secret_name
             else:
                log.error("Type mismatch: Expected StowRsBackendConfig.")
                return None
        else:
            log.error(
                "Unknown resolved_backend_type_for_config for building specific config.",
                type_val=resolved_backend_type_for_config
            )
            return None

        log.debug("Successfully built storage backend config dict.", config_dict=actual_storage_config_dict)
        return actual_storage_config_dict

    except AttributeError as attr_err:
        log.error(
            "Attribute error building specific config dict from DB model. "
            "A field expected for the backend type was missing on the model instance.",
            error=str(attr_err),
            exc_info=True
        )
        return None
    except Exception as config_build_err:
        log.error(
            "Generic error building specific config dict from DB model.",
            error=str(config_build_err),
            exc_info=True
        )
        return None
