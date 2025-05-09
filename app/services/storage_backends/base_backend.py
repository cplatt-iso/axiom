# app/services/storage_backends/base_backend.py

import enum
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

from pydicom.dataset import Dataset

class StorageBackendError(Exception):
    """Custom exception for storage backend errors."""
    # --- ADDED: Allow optional status_code ---
    def __init__(self, message: str, status_code: Optional[int] = None):
        self.status_code = status_code
        self.message = message
        # Include status code in the default message if present
        full_message = f"{message}{f' (Status Code: {status_code})' if status_code else ''}"
        super().__init__(full_message)
    # --- END ADDED ---

class StorageBackendType(str, enum.Enum):
    FILESYSTEM = "filesystem"
    GCS = "gcs"
    CSTORE = "cstore"
    GOOGLE_HEALTHCARE = "google_healthcare"
    STOW_RS = "stow_rs"

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
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None,
        **kwargs: Any
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
