# app/services/storage_backends/google_healthcare.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional

import requests # Use requests for HTTP
from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

# Import Google Auth library for credentials
try:
    import google.auth
    import google.auth.transport.requests
except ImportError:
    google = None # type: ignore
    logging.getLogger(__name__).warning(
        "google-auth library not found. Google Healthcare DICOM Store backend will not be available."
    )

from .base_backend import BaseStorageBackend, StorageBackendError

logger = logging.getLogger(__name__)

class GoogleHealthcareDicomStoreStorage(BaseStorageBackend):
    """Sends DICOM files to a Google Cloud Healthcare DICOM Store via STOW-RS."""

    def _validate_config(self):
        """Validate Google Healthcare DICOM Store configuration."""
        if google is None:
            raise StorageBackendError("google-auth library is not installed.")

        required_keys = ["project_id", "location", "dataset_id", "dicom_store_id"]
        if not all(key in self.config for key in required_keys):
            raise ValueError(f"Google Healthcare storage config requires: {', '.join(required_keys)}")

        self.project_id = self.config["project_id"]
        self.location = self.config["location"]
        self.dataset_id = self.config["dataset_id"]
        self.dicom_store_id = self.config["dicom_store_id"]

        # Construct the base DICOMweb URL
        # Reference: https://cloud.google.com/healthcare-api/docs/dicom#dicomweb_path
        self.base_url = (
            f"https://healthcare.googleapis.com/v1/projects/{self.project_id}"
            f"/locations/{self.location}/datasets/{self.dataset_id}"
            f"/dicomStores/{self.dicom_store_id}/dicomWeb"
        )
        self.stow_url = f"{self.base_url}/studies" # STOW-RS endpoint

        # Authentication is handled by google-auth library using ADC or service account key file
        # We just need to fetch the credentials at runtime.
        try:
            # Attempt to get credentials during validation for early failure detection
            credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-healthcare'])
            # Check if credentials are valid (optional, but good practice)
            if not credentials or not credentials.valid:
                 # Attempt to refresh credentials if possible
                 auth_req = google.auth.transport.requests.Request()
                 credentials.refresh(auth_req)

            if not credentials or not credentials.valid:
                 raise StorageBackendError("Could not obtain valid Google Cloud credentials. Check ADC or GOOGLE_APPLICATION_CREDENTIALS.")

            logger.info(f"Google Cloud credentials obtained successfully for project: {project or 'Default'}")
            self.credentials = credentials # Store for later use

        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.error(f"Failed to find Google Cloud default credentials: {e}. Ensure ADC is configured or GOOGLE_APPLICATION_CREDENTIALS is set.")
            raise StorageBackendError("Could not find Google Cloud credentials.") from e
        except Exception as e:
            logger.error(f"Failed to initialize Google Cloud credentials: {e}", exc_info=True)
            raise StorageBackendError(f"Google Cloud credential initialization failed: {e}") from e

    def _get_auth_token(self) -> str:
        """Refreshes and returns the OAuth2 access token."""
        try:
            auth_req = google.auth.transport.requests.Request()
            # Refresh credentials if they are expired or don't exist
            if not self.credentials or not self.credentials.valid:
                self.credentials.refresh(auth_req)

            if not self.credentials or not self.credentials.valid:
                 raise StorageBackendError("Could not obtain valid Google Cloud access token after refresh.")

            return self.credentials.token # type: ignore # Access token attribute
        except Exception as e:
            logger.error(f"Failed to get/refresh Google Cloud access token: {e}", exc_info=True)
            raise StorageBackendError("Failed to obtain Google Cloud access token.") from e


    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None, # Unused by STOW-RS itself
        source_identifier: Optional[str] = None,
        **kwargs: Any
    ) -> Dict[str, Any]: # Return the JSON response from Google API
        """
        Uploads the modified DICOM dataset to the configured Google Healthcare DICOM Store via STOW-RS.

        Args:
            modified_ds: The pydicom Dataset object after modifications.
            original_filepath: The path to the original file (unused).
            filename_context: A filename or identifier for logging (unused).
            source_identifier: Identifier of the source system (unused).
            **kwargs: Additional keyword arguments (ignored).

        Returns:
            The JSON response dictionary from the Google Healthcare API STOW-RS endpoint.

        Raises:
            StorageBackendError: If storing fails due to network, auth, or API errors.
        """
        sop_instance_uid = getattr(modified_ds, 'SOPInstanceUID', 'Unknown_SOPInstanceUID')
        log_identifier = f"SOPInstanceUID {sop_instance_uid}"

        logger.debug(f"Attempting STOW-RS to Google Healthcare DICOM Store: {self.stow_url} for {log_identifier}")

        try:
            # Get a fresh auth token
            access_token = self._get_auth_token()
            headers = {
                "Authorization": f"Bearer {access_token}",
                # Content-Type for multipart/related is set by requests library via 'files' param
                "Accept": "application/dicom+json" # We expect a JSON response
            }

            # Prepare DICOM data as bytes in memory
            buffer = BytesIO()
            dcmwrite(buffer, modified_ds, write_like_original=False)
            buffer.seek(0)
            dicom_bytes = buffer.getvalue()
            buffer.close()

            # Construct the multipart/related request using requests 'files' parameter
            # The key ('dicomfile' here) doesn't usually matter for STOW-RS
            # requests expects a tuple: (filename, file_content, content_type)
            files = {
                'dicomfile': ('dicom.dcm', dicom_bytes, 'application/dicom')
            }

            # Make the POST request
            response = requests.post(
                self.stow_url,
                headers=headers,
                files=files, # requests handles multipart/related encoding
                timeout=120 # Allow longer timeout for uploads
            )

            # Check response status
            if 200 <= response.status_code < 300:
                logger.info(f"Successfully stored {log_identifier} via STOW-RS to Google Healthcare. Status: {response.status_code}")
                try:
                    # Return the JSON response body which contains details per PS3.18 Table 10.6.1-2
                    response_json = response.json()
                    return response_json
                except requests.exceptions.JSONDecodeError:
                    logger.warning(f"STOW-RS to Google Healthcare succeeded (Status: {response.status_code}) but response was not valid JSON.")
                    # Return a success indicator even without body
                    return {"status": "success", "message": f"Upload successful (Status: {response.status_code}), but response body was not JSON."}
            else:
                # Handle API errors
                error_details = response.text # Default to text
                try:
                    error_json = response.json()
                    error_details = error_json.get("error", {}).get("message", response.text)
                except requests.exceptions.JSONDecodeError:
                    pass # Keep text if JSON parsing fails

                err_msg = (f"STOW-RS to Google Healthcare failed for {log_identifier}. "
                           f"Status: {response.status_code}, Details: {error_details[:500]}") # Limit error detail length
                logger.error(err_msg)
                raise StorageBackendError(err_msg, status_code=response.status_code)

        except requests.exceptions.Timeout as e:
             err_msg = f"Network timeout during STOW-RS to Google Healthcare ({self.stow_url}) for {log_identifier}"
             logger.error(f"{err_msg}: {e}", exc_info=False)
             raise StorageBackendError(err_msg) from e
        except requests.exceptions.RequestException as e:
            # Catch connection errors, etc.
            err_msg = f"Network error during STOW-RS to Google Healthcare ({self.stow_url}) for {log_identifier}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            raise StorageBackendError(f"{err_msg}: {e}") from e
        except StorageBackendError: # Re-raise auth errors etc.
             raise
        except Exception as e:
            # Catch other potential errors (pydicom writing, etc.)
            err_msg = f"Unexpected error storing via STOW-RS to Google Healthcare for {log_identifier}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            raise StorageBackendError(f"{err_msg}: {e}") from e
