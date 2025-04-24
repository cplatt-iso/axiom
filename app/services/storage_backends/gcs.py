# app/services/storage_backends/gcs.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional

from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

# Import GCS client library
try:
    from google.cloud import storage
    from google.api_core import exceptions as google_exceptions
except ImportError:
    storage = None # type: ignore
    google_exceptions = None # type: ignore
    logging.getLogger(__name__).warning(
        "google-cloud-storage library not found. GCS backend will not be available."
    )

from .base_backend import BaseStorageBackend, StorageBackendError

logger = logging.getLogger(__name__)

class GcsStorage(BaseStorageBackend):
    """Stores DICOM files to Google Cloud Storage."""

    def _validate_config(self):
        """Validate GCS configuration."""
        if storage is None:
            raise StorageBackendError("google-cloud-storage library is not installed.")
        if not self.config.get("bucket_name"):
            raise ValueError("GCS storage configuration requires 'bucket_name'.")
        self.bucket_name = self.config["bucket_name"]
        # Path prefix is optional, default to empty string (root of bucket)
        self.path_prefix = self.config.get("path_prefix", "").strip('/')
        # Credentials are handled implicitly by the library (ADC)
        # You might add checks here if specific credential methods are required

        try:
            # Attempt to initialize client during validation for early failure detection
            self.client = storage.Client()
            # Optionally try to get the bucket to verify access
            # self.client.get_bucket(self.bucket_name)
            logger.info(f"GCS client initialized for bucket '{self.bucket_name}'.")
        except Exception as e:
            logger.error(f"Failed to initialize GCS client or access bucket '{self.bucket_name}': {e}", exc_info=True)
            raise StorageBackendError(f"GCS initialization failed: {e}") from e

    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None,
        **kwargs: Any
    ) -> str:
        """
        Uploads the modified DICOM dataset to the configured GCS bucket.

        Args:
            modified_ds: The pydicom Dataset object after modifications.
            original_filepath: The path to the original file (unused).
            filename_context: A context string for deriving the filename (e.g., SOPInstanceUID.dcm).
            source_identifier: Identifier of the source system (unused).
            **kwargs: Additional keyword arguments (ignored).

        Returns:
            The GCS URI of the stored object (e.g., gs://bucket_name/path/to/file.dcm).

        Raises:
            StorageBackendError: If storing fails due to GCS errors or missing filename context.
        """
        if not filename_context:
            err_msg = "GCS storage requires filename_context (e.g., SOPInstanceUID.dcm) to determine object name."
            logger.error(err_msg)
            raise StorageBackendError(err_msg)

        # Construct the destination blob name
        blob_name = filename_context
        if self.path_prefix:
            blob_name = f"{self.path_prefix}/{blob_name}"

        logger.debug(f"Attempting to store DICOM object to GCS: gs://{self.bucket_name}/{blob_name}")

        try:
            # Get DICOM data as bytes
            buffer = BytesIO()
            # Use dcmwrite for potentially better handling of file meta if needed
            dcmwrite(buffer, modified_ds, write_like_original=False)
            buffer.seek(0) # Reset buffer position to the beginning for reading

            # Get bucket and blob objects
            bucket = self.client.bucket(self.bucket_name)
            blob = bucket.blob(blob_name)

            # Upload from the buffer
            blob.upload_from_file(buffer, content_type="application/dicom")

            gcs_uri = f"gs://{self.bucket_name}/{blob_name}"
            logger.info(f"Successfully stored object to GCS: {gcs_uri}")
            return gcs_uri

        except google_exceptions.NotFound:
            err_msg = f"GCS bucket '{self.bucket_name}' not found or permission denied."
            logger.error(err_msg)
            raise StorageBackendError(err_msg)
        except google_exceptions.Forbidden as e:
            err_msg = f"Permission denied for GCS operation on gs://{self.bucket_name}/{blob_name}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            raise StorageBackendError(err_msg) from e
        except google_exceptions.GoogleAPICallError as e:
            # Catch broader API errors (network, server-side GCS issues)
            err_msg = f"GCS API call error during upload to gs://{self.bucket_name}/{blob_name}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            raise StorageBackendError(err_msg) from e
        except Exception as e:
            # Catch other potential errors (pydicom writing, etc.)
            err_msg = f"Unexpected error storing to GCS gs://{self.bucket_name}/{blob_name}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            raise StorageBackendError(f"{err_msg}: {e}") from e
        finally:
            if 'buffer' in locals() and buffer:
                buffer.close()
