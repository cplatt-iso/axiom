# app/services/storage_backends/filesystem.py

import logging
import shutil
from pathlib import Path
from typing import Any, Dict, Optional

from pydicom.dataset import Dataset

from .base_backend import BaseStorageBackend, StorageBackendError

logger = logging.getLogger(__name__)

class FilesystemStorage(BaseStorageBackend):
    """Stores DICOM files to a local filesystem directory."""

    def __init__(self, config: Dict[str, Any]):
        self.backend_type = config.get("type", "filesystem")
        self.name = config.get("name", "Unnamed Filesystem Backend")
        self.path_str = config.get("path")

        if not self.path_str:
            raise ValueError("Filesystem storage configuration requires a 'path'.")

        self.destination_dir = Path(self.path_str)
        self._validate_config() # Call validation after setting attributes

    def _validate_config(self):
        """Validate filesystem configuration."""
        # Validation now checks instance attribute
        try:
            self.destination_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Filesystem storage directory ensured/checked: {self.destination_dir}")
        except OSError as e:
            logger.error(f"Initial check/creation failed for directory {self.destination_dir}: {e}. Will attempt again during store.")
            # Optionally raise StorageBackendError here if directory must exist at init
            # raise StorageBackendError(f"Cannot initialize filesystem backend: {e}") from e

    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None,
        **kwargs: Any
        ) -> str:
        if source_identifier:
            logger.debug(f"FilesystemStorage received source_identifier: {source_identifier}")

        try:
            self.destination_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create destination directory {self.destination_dir}: {e}")
            raise StorageBackendError(f"Cannot create directory {self.destination_dir}") from e

        filename: Optional[str] = None
        if filename_context:
            filename = filename_context
            if not filename.lower().endswith(('.dcm', '.dicom')):
                 filename += ".dcm"
            logger.debug(f"Using filename from context: {filename}")
        elif original_filepath:
            filename = original_filepath.name
            logger.debug(f"Using filename from original_filepath: {filename}")

        if not filename:
            err_msg = "Filesystem storage requires filename_context or original_filepath."
            logger.error(err_msg)
            raise StorageBackendError(err_msg)

        destination_path = self.destination_dir / filename

        logger.debug(f"Storing modified DICOM to filesystem: {destination_path}")
        try:
            modified_ds.save_as(str(destination_path), write_like_original=False)
            logger.info(f"Successfully stored file to: {destination_path}")
            return str(destination_path)
        except Exception as e:
            logger.error(f"Failed to save file to {destination_path}: {e}", exc_info=True)
            try:
                if destination_path.exists(): destination_path.unlink()
            except OSError:
                logger.warning(f"Failed cleanup of partial file {destination_path}.")
            raise StorageBackendError(f"Failed to write file to {destination_path}") from e

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(path={self.destination_dir})>"
