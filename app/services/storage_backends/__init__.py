# app/services/storage_backends/__init__.py

from typing import Dict, Any
from .base_backend import BaseStorageBackend, StorageBackendError
from .filesystem import FilesystemStorage
from .dicom_cstore import CStoreStorage
# from .gcs import GcsStorage # Import when implemented

def get_storage_backend(config: Dict[str, Any]) -> BaseStorageBackend:
    """
    Factory function to get a storage backend instance based on config.

    Args:
        config: A dictionary defining the backend type and its settings.
                Example: {'type': 'filesystem', 'path': '/path/to/store'}
                         {'type': 'cstore', 'ae_title': 'PACS', 'host': '...', 'port': ...}

    Returns:
        An instance of a BaseStorageBackend subclass.

    Raises:
        ValueError: If the backend type is unknown or config is invalid.
        StorageBackendError: If there's an issue initializing the backend.
    """
    backend_type = config.get("type")
    if not backend_type:
        raise ValueError("Storage backend configuration must include a 'type'.")

    backend_type = backend_type.lower()

    try:
        if backend_type == "filesystem":
            return FilesystemStorage(config)
        elif backend_type == "cstore":
            return CStoreStorage(config)
        # elif backend_type == "gcs":
        #     return GcsStorage(config)
        else:
            raise ValueError(f"Unknown storage backend type: {backend_type}")
    except Exception as e:
        # Catch potential initialization errors (missing keys, connection issues, etc.)
        raise StorageBackendError(f"Error initializing storage backend '{backend_type}': {e}") from e
