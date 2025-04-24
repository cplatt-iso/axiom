# app/services/storage_backends/__init__.py

from typing import Dict, Any
from .base_backend import BaseStorageBackend, StorageBackendError
from .filesystem import FilesystemStorage
from .dicom_cstore import CStoreStorage
from .gcs import GcsStorage
from .google_healthcare import GoogleHealthcareDicomStoreStorage
from .stow_rs import StowRsStorage

def get_storage_backend(config: Dict[str, Any]) -> BaseStorageBackend:
    """
    Factory function to get a storage backend instance based on config.

    Args:
        config: A dictionary defining the backend type and its settings.
                Example: {'type': 'filesystem', 'path': '/path/to/store'}
                         {'type': 'cstore', 'ae_title': 'PACS', 'host': '...', 'port': ...}
                         {'type': 'gcs', 'bucket_name': 'my-bucket', 'path_prefix': 'optional/path'}
                         {'type': 'google_healthcare', 'project_id': '...', 'location': '...', ...}
                         {'type': 'stow_rs', 'stow_url': 'http://.../studies', 'auth_type': 'basic', ...}

    Returns:
        An instance of a BaseStorageBackend subclass.

    Raises:
        ValueError: If the backend type is unknown or config is invalid.
        StorageBackendError: If there's an issue initializing the backend.
    """
    backend_type = config.get("type")
    if not backend_type:
        raise ValueError("Storage backend configuration must include a 'type'.")

    # Normalize type for case-insensitive matching
    backend_type = str(backend_type).lower()

    try:
        if backend_type == "filesystem":
            return FilesystemStorage(config)
        elif backend_type == "cstore":
            return CStoreStorage(config)
        # --- ADDED Backend Types ---
        elif backend_type == "gcs":
            return GcsStorage(config)
        elif backend_type == "google_healthcare":
            return GoogleHealthcareDicomStoreStorage(config)
        elif backend_type == "stow_rs":
            return StowRsStorage(config)
        else:
            raise ValueError(f"Unknown storage backend type: '{backend_type}'")
    except Exception as e:
        # Catch potential initialization errors (missing keys, connection issues, etc.)
        # Wrap specific ValueErrors from validation in StorageBackendError for consistency
        if isinstance(e, ValueError):
             raise StorageBackendError(f"Invalid configuration for backend type '{backend_type}': {e}") from e
        # Re-raise StorageBackendErrors directly
        elif isinstance(e, StorageBackendError):
             raise
        # Wrap other unexpected errors
        else:
             raise StorageBackendError(f"Unexpected error initializing storage backend '{backend_type}': {e}") from e
