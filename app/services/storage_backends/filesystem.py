# app/services/storage_backends/filesystem.py

import logging
import shutil
from pathlib import Path
from typing import Any, Dict

from pydicom.dataset import Dataset

from .base_backend import BaseStorageBackend, StorageBackendError
# from . import StorageBackendError # Import custom error

logger = logging.getLogger(__name__)

class FilesystemStorage(BaseStorageBackend):
    """Stores DICOM files to a local filesystem directory."""

    def _validate_config(self):
        """Validate filesystem configuration."""
        if "path" not in self.config:
            raise ValueError("Filesystem storage configuration requires a 'path'.")
        self.destination_dir = Path(self.config["path"])

    def store(self, modified_ds: Dataset, original_filepath: Path) -> str:
        """
        Saves the modified DICOM dataset to the configured directory.
        Uses the original filename.
        """
        # Ensure destination directory exists
        try:
            self.destination_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create destination directory {self.destination_dir}: {e}")
            raise StorageBackendError(f"Cannot create directory {self.destination_dir}") from e

        destination_path = self.destination_dir / original_filepath.name

        logger.debug(f"Storing modified DICOM to filesystem: {destination_path}")
        try:
            # Use save_as on the modified dataset
            modified_ds.save_as(destination_path, write_like_original=False)
            logger.info(f"Successfully stored file to: {destination_path}")
            return str(destination_path)
        except Exception as e:
            logger.error(f"Failed to save file to {destination_path}: {e}", exc_info=True)
            # Attempt to delete partially written file? Maybe not necessary.
            raise StorageBackendError(f"Failed to write file to {destination_path}") from e
