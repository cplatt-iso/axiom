# app/services/storage_backends/google_healthcare.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Union
import uuid

import requests
import pydicom # Ensure pydicom is imported
from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

# Import Google Auth library for credentials
try:
    import google.auth
    import google.auth.transport.requests
except ImportError:
    google = None # type: ignore
    # Use standard logging before structlog might be configured
    logging.getLogger(__name__).warning(
        "google-auth library not found. Google Healthcare DICOM Store backend will not be available."
    )

# Use structlog if available, otherwise fallback to standard logging
# This helps if this module is imported before logging is fully configured elsewhere
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)


from .base_backend import BaseStorageBackend, StorageBackendError


class GoogleHealthcareDicomStoreStorage(BaseStorageBackend):
    """Sends DICOM files to a Google Cloud Healthcare DICOM Store via STOW-RS."""

    def _validate_config(self):
        """Validate Google Healthcare DICOM Store configuration."""
        if google is None:
            raise StorageBackendError("google-auth library is not installed.")

        required_keys = ["project_id", "location", "dataset_id", "dicom_store_id"]
        if not all(key in self.config for key in required_keys):
            missing = [key for key in required_keys if key not in self.config]
            raise ValueError(f"Google Healthcare storage config missing required keys: {', '.join(missing)}")

        self.project_id = self.config["project_id"]
        self.location = self.config["location"]
        self.dataset_id = self.config["dataset_id"]
        self.dicom_store_id = self.config["dicom_store_id"]

        # Construct the base DICOMweb URL
        self.base_url = (
            f"https://healthcare.googleapis.com/v1/projects/{self.project_id}"
            f"/locations/{self.location}/datasets/{self.dataset_id}"
            f"/dicomStores/{self.dicom_store_id}/dicomWeb"
        )
        self.stow_url = f"{self.base_url}/studies"

        # Authentication: Attempt to get credentials and validate/refresh them
        try:
            credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-healthcare'])
            # Create auth request object *before* checking validity
            auth_req = google.auth.transport.requests.Request()

            # Check if credentials are valid, if not, try refreshing them
            if not credentials or not credentials.valid:
                 logger.info("Google Cloud credentials require refresh, attempting...")
                 try:
                     # Credentials might be None if auth.default fails initially
                     if credentials:
                          credentials.refresh(auth_req)
                          logger.info("Google Cloud credentials refreshed successfully.")
                     else:
                          # If credentials object itself is None, refresh won't work
                           raise StorageBackendError("Cannot refresh None credentials object.")
                 except Exception as refresh_error:
                     # Handle potential errors during refresh
                     logger.error("Failed to refresh Google Cloud credentials", error=str(refresh_error), exc_info=False)
                     # Raise specific error if refresh fails
                     raise StorageBackendError("Failed to refresh Google Cloud credentials.") from refresh_error

            # After attempting refresh (if needed), check validity again
            if not credentials or not credentials.valid:
                 logger.error("Could not obtain valid Google Cloud credentials even after refresh attempt.")
                 raise StorageBackendError("Could not obtain valid Google Cloud credentials.")

            # If we get here, credentials should be valid
            logger.info("Google Cloud credentials obtained and valid", project_id=(project or 'Default'))
            self.credentials = credentials # Store for later use

        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.error("Failed to find Google Cloud default credentials. Ensure ADC is configured or GOOGLE_APPLICATION_CREDENTIALS is set.", error=str(e), exc_info=False)
            raise StorageBackendError("Could not find Google Cloud credentials.") from e
        except StorageBackendError as e: # Re-raise specific errors from refresh block
             raise
        except Exception as e:
            # Catch other potential errors during auth.default or initial checks
            logger.error("Failed to initialize Google Cloud credentials", error=str(e), exc_info=True)
            raise StorageBackendError(f"Google Cloud credential initialization failed: {e}") from e


    def _get_auth_token(self) -> str:
        """Refreshes (if needed) and returns the OAuth2 access token."""
        if not hasattr(self, 'credentials') or not self.credentials:
             # This should ideally not happen if _validate_config succeeded
             logger.error("Credentials attribute missing or None when trying to get token.")
             raise StorageBackendError("Google Cloud credentials not initialized.")
        try:
            auth_req = google.auth.transport.requests.Request()
            # Check validity before returning token, refresh if necessary
            if not self.credentials.valid:
                logger.info("Refreshing Google Cloud access token before use...")
                self.credentials.refresh(auth_req)
                logger.info("Google Cloud access token refreshed.")

            if not self.credentials.valid or not self.credentials.token:
                 # Check validity and token presence *after* potential refresh
                 raise StorageBackendError("Could not obtain valid Google Cloud access token after refresh attempt.")

            return self.credentials.token
        except Exception as e:
            logger.error("Failed to get/refresh Google Cloud access token", error=str(e), exc_info=True)
            raise StorageBackendError("Failed to obtain Google Cloud access token.") from e


    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None, # Primarily for logging here
        source_identifier: Optional[str] = None, # Primarily for logging here
        **kwargs: Any
    ) -> Union[Dict[str, Any], str]: # Return dict or specific string "duplicate"
        """
        Uploads DICOM dataset via STOW-RS, handling 409 Conflict gracefully.
        Returns the JSON response on success, or the string "duplicate" on 409.
        """
        sop_instance_uid = getattr(modified_ds, 'SOPInstanceUID', 'Unknown_SOPInstanceUID')
        study_instance_uid = getattr(modified_ds, 'StudyInstanceUID', 'Unknown_StudyInstanceUID')
        # Bind context for structured logging
        log = logger.bind(
            sop_instance_uid=sop_instance_uid,
            study_instance_uid=study_instance_uid,
            google_dicom_store=f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}",
            storage_backend_type="google_healthcare"
        )

        log.debug("Attempting STOW-RS upload", stow_url=self.stow_url, filename_context=filename_context, source_identifier=source_identifier)

        buffer = None
        try:
            # Get potentially refreshed auth token
            access_token = self._get_auth_token()

            # Ensure dataset has file_meta for dcmwrite
            if not hasattr(modified_ds, 'file_meta') or not modified_ds.file_meta:
                 log.warning("Dataset missing file_meta, creating default for STOW-RS upload.")
                 file_meta = pydicom.dataset.FileMetaDataset()
                 file_meta.FileMetaInformationVersion = b'\x00\x01'
                 sop_class_uid_data = modified_ds.get("SOPClassUID")
                 # Safely extract SOP Class UID value, default if missing/malformed
                 sop_class_uid_val_list = getattr(sop_class_uid_data, 'value', ['1.2.840.10008.5.1.4.1.1.2']) # Default CT
                 sop_class_uid_val = sop_class_uid_val_list[0] if isinstance(sop_class_uid_val_list, list) and sop_class_uid_val_list else '1.2.840.10008.5.1.4.1.1.2'
                 file_meta.MediaStorageSOPClassUID = sop_class_uid_val
                 file_meta.MediaStorageSOPInstanceUID = sop_instance_uid
                 # Use Explicit VR Little Endian if possible, fallback to Implicit
                 transfer_syntax = getattr(modified_ds.get("TransferSyntaxUID"), 'value', [pydicom.uid.ImplicitVRLittleEndian])[0]
                 # Ensure we use a known TS UID, default to Implicit LE if needed
                 if transfer_syntax not in pydicom.uid.TRANSFER_SYNTAXES:
                      transfer_syntax = pydicom.uid.ImplicitVRLittleEndian
                      log.debug("Using default Implicit VR LE Transfer Syntax for file meta.")

                 file_meta.TransferSyntaxUID = transfer_syntax
                 file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
                 file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
                 modified_ds.file_meta = file_meta


            # Write dataset to in-memory buffer
            buffer = BytesIO()
            dcmwrite(buffer, modified_ds, write_like_original=False)
            buffer.seek(0)
            dicom_bytes = buffer.getvalue()

            # Prepare multipart request
            boundary = uuid.uuid4().hex
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/dicom+json", # Request JSON response
                "Content-Type": f'multipart/related; type="application/dicom"; boundary="{boundary}"'
            }

            body = b""
            body += f"--{boundary}\r\n".encode('utf-8')
            body += b"Content-Type: application/dicom\r\n\r\n"
            body += dicom_bytes
            body += b"\r\n"
            body += f"--{boundary}--\r\n".encode('utf-8')

            # Make the STOW-RS POST request
            response = requests.post(self.stow_url, headers=headers, data=body, timeout=120) # Increased timeout

            # --- Process Response ---
            if 200 <= response.status_code < 300:
                log.info("STOW-RS successful", status_code=response.status_code)
                try:
                    return response.json() # Return full JSON response on success
                except requests.exceptions.JSONDecodeError:
                    log.warning("STOW-RS success response was not valid JSON", status_code=response.status_code)
                    return {"status": "success", "message": f"Upload successful (Status: {response.status_code}), but response body was not JSON."}

            elif response.status_code == 409: # Conflict - Likely Duplicate
                failure_details = ""
                failure_reason = "Conflict"
                try:
                    response_json = response.json()
                    # Attempt to parse DICOM JSON response for details (PS3.18 Table 10.6.1-2)
                    failed_seq_val = response_json.get("00081198", {}).get("Value", [{}])
                    failed_seq = failed_seq_val[0] if isinstance(failed_seq_val, list) and failed_seq_val else {}

                    reason_code_data = failed_seq.get("00081197", {})
                    reason_code_val = getattr(reason_code_data, 'Value', [None])
                    reason_code = reason_code_val[0] if isinstance(reason_code_val, list) and reason_code_val else None

                    if reason_code == 272: failure_reason = "Duplicate SOPInstanceUID"
                    elif reason_code == 43264: failure_reason = "Instance Coercion" # 0xA900
                    # Add more reason codes if needed

                    retrieve_url_data = response_json.get("00081190", {})
                    retrieve_url_val = getattr(retrieve_url_data, 'Value', [None])
                    retrieve_url = retrieve_url_val[0] if isinstance(retrieve_url_val, list) and retrieve_url_val else None
                    failure_details = f"ReasonCode: {reason_code}, ConflictingStudyURL: {retrieve_url}"

                except Exception as parse_exc:
                    log.warning("Failed to parse details from 409 Conflict response", parse_error=str(parse_exc))
                    failure_details = response.text[:200] # Fallback

                log.warning("STOW-RS Conflict detected (Duplicate Instance?)",
                            status_code=response.status_code,
                            failure_reason=failure_reason,
                            failure_details=failure_details)
                # Return specific string code for handled duplicate
                return "duplicate" # <--- RETURN "duplicate"

            else: # Other HTTP errors (4xx, 5xx)
                error_details_str = response.text # Default
                try:
                    error_json = response.json()
                    # Try to get message from standard Google error format
                    error_details_str = str(error_json.get("error", {}).get("message", error_json))
                except requests.exceptions.JSONDecodeError:
                    pass # Keep raw text if not JSON

                err_msg = (f"STOW-RS to Google Healthcare failed. Status: {response.status_code}")
                log.error(err_msg, status_code=response.status_code, error_details=error_details_str[:500])
                # Raise generic error for Celery retry / task failure
                raise StorageBackendError(f"{err_msg}, Details: {error_details_str[:500]}")

        # --- Exception Handling ---
        except requests.exceptions.Timeout as e:
             err_msg = f"Network timeout during STOW-RS to Google Healthcare ({self.stow_url})"
             log.error(err_msg, error=str(e), exc_info=False)
             raise StorageBackendError(err_msg) from e
        except requests.exceptions.RequestException as e:
            err_msg = f"Network error during STOW-RS to Google Healthcare ({self.stow_url})"
            log.error(err_msg, error=str(e), exc_info=True)
            raise StorageBackendError(f"{err_msg}: {e}") from e
        except StorageBackendError as e: # Re-raise auth/config errors
             log.error("Pre-existing StorageBackendError encountered", error=str(e), exc_info=True)
             raise
        except Exception as e:
            # Catch other potential errors (pydicom writing, etc.)
            err_msg = f"Unexpected error storing via STOW-RS to Google Healthcare"
            log.error(err_msg, error=str(e), exc_info=True)
            raise StorageBackendError(f"{err_msg}: {e}") from e
        finally:
            if buffer:
                buffer.close()
