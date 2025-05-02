# app/services/storage_backends/gcs.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional

from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

try:
    from google.cloud import storage
    from google.api_core import exceptions as google_exceptions
except ImportError:
    storage = None
    google_exceptions = None
    logging.getLogger(__name__).warning("google-cloud-storage library not found. GCS backend unusable.")

from .base_backend import BaseStorageBackend, StorageBackendError

logger = logging.getLogger(__name__)

class GcsStorage(BaseStorageBackend):

    def __init__(self, config: Dict[str, Any]):
        # Store specific attributes, not the whole dict as self.config
        self.backend_type = config.get("type", "gcs")
        self.name = config.get("name", "Unnamed GCS Backend") # Keep name if needed
        self.bucket = config.get("bucket") # Changed from bucket_name
        self.prefix = config.get("prefix", "").strip('/')

        if storage is None:
            raise StorageBackendError("GCS backend unavailable: google-cloud-storage library not installed.")
        if not self.bucket:
            raise ValueError("GCS storage configuration requires 'bucket'.")

        try:
            self.client = storage.Client()
            logger.info(f"GCS client initialized for bucket '{self.bucket}'. Prefix: '{self.prefix or '[None]'}'")
        except Exception as e:
            logger.error(f"Failed to initialize GCS client for bucket '{self.bucket}': {e}", exc_info=True)
            raise StorageBackendError(f"GCS initialization failed: {e}") from e

        # Validation logic now happens within __init__
        self._validate_config() # Call validation at the end

    def _validate_config(self):
        # Validation now checks instance attributes set in __init__
        # Example: Check if bucket seems accessible (optional)
        try:
             # Quick check - does not guarantee write access
             self.client.get_bucket(self.bucket)
             logger.debug(f"Bucket '{self.bucket}' exists and is accessible (basic check).")
        except google_exceptions.NotFound:
             logger.error(f"GCS Bucket '{self.bucket}' specified in config not found.")
             raise ValueError(f"GCS bucket '{self.bucket}' not found.")
        except google_exceptions.Forbidden:
             logger.warning(f"Permission denied accessing GCS bucket '{self.bucket}'. Check service account permissions.")
             # Don't raise here, maybe permissions allow writes but not gets
        except Exception as e:
            logger.error(f"Error accessing GCS bucket '{self.bucket}' during validation: {e}")
            # Don't raise here, could be transient issue, let store() fail later
            pass


    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None,
        **kwargs: Any
    ) -> str:
        if not filename_context:
            raise StorageBackendError("GCS storage requires filename_context.")

        blob_name = filename_context
        if self.prefix:
            blob_name = f"{self.prefix}/{blob_name}"

        logger.debug(f"Attempting GCS store: gs://{self.bucket}/{blob_name}")

        buffer = None
        try:
            buffer = BytesIO()
            dcmwrite(buffer, modified_ds, write_like_original=False)
            buffer.seek(0)
            bucket_obj = self.client.bucket(self.bucket)
            blob = bucket_obj.blob(blob_name)
            blob.upload_from_file(buffer, content_type="application/dicom")
            gcs_uri = f"gs://{self.bucket}/{blob_name}"
            logger.info(f"Successfully stored to GCS: {gcs_uri}")
            return gcs_uri
        except google_exceptions.NotFound: raise StorageBackendError(f"GCS bucket '{self.bucket}' not found.")
        except google_exceptions.Forbidden as e: raise StorageBackendError(f"Permission denied for GCS operation on gs://{self.bucket}/{blob_name}") from e
        except google_exceptions.GoogleAPICallError as e: raise StorageBackendError(f"GCS API call error uploading to gs://{self.bucket}/{blob_name}") from e
        except Exception as e: raise StorageBackendError(f"Unexpected error storing to GCS gs://{self.bucket}/{blob_name}: {e}") from e
        finally:
            if buffer: buffer.close()

    def __repr__(self) -> str:
        # Represent based on instance attributes
        return f"<{self.__class__.__name__}(bucket={self.bucket}, prefix={self.prefix})>"
