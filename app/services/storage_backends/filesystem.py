# app/services/storage_backends/filesystem.py

import logging
import shutil
from pathlib import Path
from typing import Any, Dict, Optional # <-- Import Optional

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
        # Also create the dir during validation/init if possible
        try:
            self.destination_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Filesystem storage directory ensured: {self.destination_dir}")
        except OSError as e:
            # Log error but don't raise here, raise during store if it fails then
            logger.error(f"Initial check/creation failed for directory {self.destination_dir}: {e}. Will attempt again during store.")


    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None, # Make original_filepath optional
        filename_context: Optional[str] = None # Add filename_context argument
        ) -> str:
        """
        Saves the modified DICOM dataset to the configured directory.
        Determines filename from filename_context or original_filepath.
        """
        # Ensure destination directory exists (attempt again if init failed)
        try:
            self.destination_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(f"Failed to create destination directory {self.destination_dir}: {e}")
            raise StorageBackendError(f"Cannot create directory {self.destination_dir}") from e

        # --- Determine Filename ---
        if filename_context:
            # Use context if provided (typically SOPInstanceUID.dcm for DICOMweb)
            filename = filename_context
            # Ensure it has a .dcm extension?
            if not filename.lower().endswith(('.dcm', '.dicom')):
                 filename += ".dcm"
            logger.debug(f"Using filename from context: {filename}")
        elif original_filepath:
            # Fallback to original filename if context not provided
            filename = original_filepath.name
            logger.debug(f"Using filename from original_filepath: {filename}")
        else:
            # Error if neither is provided
            err_msg = "Filesystem storage requires either original_filepath or filename_context to determine output filename."
            logger.error(err_msg)
            raise StorageBackendError(err_msg)
        # --- End Determine Filename ---

        destination_path = self.destination_dir / filename

        logger.debug(f"Storing modified DICOM to filesystem: {destination_path}")
        try:
            # Use save_as on the modified dataset
            # write_like_original=False is generally safer for processed datasets
            modified_ds.save_as(str(destination_path), write_like_original=False)
            logger.info(f"Successfully stored file to: {destination_path}")
            return str(destination_path)
        except Exception as e:
            logger.error(f"Failed to save file to {destination_path}: {e}", exc_info=True)
            # Attempt to delete partially written file? Maybe not necessary.
            try:
                if destination_path.exists(): destination_path.unlink()
            except OSError:
                logger.warning(f"Failed to remove potentially partial file {destination_path} after write error.")
            raise StorageBackendError(f"Failed to write file to {destination_path}") from e

