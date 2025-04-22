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

    def _validate_config(self):
        """Validate filesystem configuration."""
        if "path" not in self.config:
            raise ValueError("Filesystem storage configuration requires a 'path'.")
        self.destination_dir = Path(self.config["path"])
        try:
            self.destination_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Filesystem storage directory ensured: {self.destination_dir}")
        except OSError as e:
            logger.error(f"Initial check/creation failed for directory {self.destination_dir}: {e}. Will attempt again during store.")

    # Update signature to match BaseStorageBackend
    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None, # Add source_identifier
        **kwargs: Any # Add kwargs
        ) -> str:
        """
        Saves the modified DICOM dataset to the configured directory.
        Determines filename from filename_context or original_filepath.

        Args:
            modified_ds: The pydicom Dataset object after modifications.
            original_filepath: The path to the original file (if applicable).
            filename_context: A context string for deriving the filename.
            source_identifier: Identifier of the source system (currently unused by this backend).
            **kwargs: Additional keyword arguments (ignored).

        Returns:
            The full path to the saved file as a string.

        Raises:
            StorageBackendError: If the destination directory cannot be created or the file cannot be saved.
        """
        # Log if source_identifier was provided, even if unused
        if source_identifier:
            logger.debug(f"FilesystemStorage received source_identifier: {source_identifier} (unused)")

        # Ensure destination directory exists
        try:
            self.destination_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create destination directory {self.destination_dir}: {e}")
            raise StorageBackendError(f"Cannot create directory {self.destination_dir}") from e

        # Determine filename
        if filename_context:
            filename = filename_context
            if not filename.lower().endswith(('.dcm', '.dicom')):
                 filename += ".dcm"
            logger.debug(f"Using filename from context: {filename}")
        elif original_filepath:
            filename = original_filepath.name
            logger.debug(f"Using filename from original_filepath: {filename}")
        else:
            err_msg = "Filesystem storage requires either original_filepath or filename_context to determine output filename."
            logger.error(err_msg)
            raise StorageBackendError(err_msg)

        destination_path = self.destination_dir / filename

        logger.debug(f"Storing modified DICOM to filesystem: {destination_path}")
        try:
            # Use save_as on the modified dataset
            modified_ds.save_as(str(destination_path), write_like_original=False)
            logger.info(f"Successfully stored file to: {destination_path}")
            return str(destination_path)
        except Exception as e:
            logger.error(f"Failed to save file to {destination_path}: {e}", exc_info=True)
            # Attempt to delete partially written file
            try:
                if destination_path.exists(): destination_path.unlink()
            except OSError:
                logger.warning(f"Failed to remove potentially partial file {destination_path} after write error.")
            raise StorageBackendError(f"Failed to write file to {destination_path}") from e
