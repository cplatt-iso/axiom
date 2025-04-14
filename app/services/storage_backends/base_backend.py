# app/services/storage_backends/base_backend.py

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict
from pydicom.dataset import Dataset # Type hint for modified dataset

class StorageBackendError(Exception):
    """Custom exception for storage backend errors."""
    pass

class BaseStorageBackend(ABC):
    """Abstract base class for storage backends."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the backend with its specific configuration.

        Args:
            config: Dictionary containing backend-specific settings.
        """
        self.config = config
        self._validate_config()

    @abstractmethod
    def _validate_config(self):
        """Validate the configuration specific to the backend implementation."""
        pass

    @abstractmethod
    def store(self, modified_ds: Dataset, original_filepath: Path) -> Any:
        """
        Store the modified DICOM dataset.

        Args:
            modified_ds: The pydicom Dataset object after modifications.
            original_filepath: The path to the original file (useful for naming or context).

        Returns:
            Implementation-specific result, e.g., destination path, True/False, API response.

        Raises:
            StorageBackendError: If storing fails.
        """
        pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.config})>"
