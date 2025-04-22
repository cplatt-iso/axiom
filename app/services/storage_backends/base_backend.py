# app/services/storage_backends/base_backend.py

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional # Import Optional

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
    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None, # Allow None
        filename_context: Optional[str] = None, # Allow None
        source_identifier: Optional[str] = None, # Add source_identifier
        **kwargs: Any # Accept other potential future arguments
        ) -> Any:
        """
        Store the modified DICOM dataset.

        Args:
            modified_ds: The pydicom Dataset object after modifications.
            original_filepath: The path to the original file (if applicable).
            filename_context: A context string for deriving the filename (e.g., SOPInstanceUID).
            source_identifier: The identifier of the source system providing the data.
            **kwargs: For forward compatibility.

        Returns:
            Implementation-specific result, e.g., destination path, True/False, API response.

        Raises:
            StorageBackendError: If storing fails.
        """
        pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.config})>"
