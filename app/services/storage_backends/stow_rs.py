# app/services/storage_backends/stow_rs.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Literal
from urllib.parse import urljoin
import uuid # Import uuid

import requests
from requests.auth import HTTPBasicAuth
from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

from .base_backend import BaseStorageBackend, StorageBackendError

logger = logging.getLogger(__name__)

StowAuthType = Literal["none", "basic", "bearer", "apikey"]

class StowRsStorage(BaseStorageBackend):
    """Sends DICOM files to a generic DICOMweb STOW-RS endpoint."""

    def _validate_config(self):
        # Validation remains the same
        if not self.config.get("stow_url"):
            if not self.config.get("base_url"):
                 raise ValueError("STOW-RS storage config requires either 'stow_url' or 'base_url'.")
            else:
                 base = self.config["base_url"]
                 base_with_slash = base if base.endswith('/') else base + '/'
                 self.stow_url = urljoin(base_with_slash, "studies")
                 logger.debug(f"Constructed STOW URL from base_url: {self.stow_url}")
        else:
             self.stow_url = self.config["stow_url"]
        if not self.stow_url.startswith(('http://', 'https://')):
            raise ValueError("STOW URL must start with http:// or https://")
        self.auth_type: StowAuthType = self.config.get("auth_type", "none")
        self.auth_config: Optional[Dict[str, Any]] = self.config.get("auth_config")
        if self.auth_type == "basic":
            if not isinstance(self.auth_config, dict) or 'username' not in self.auth_config or 'password' not in self.auth_config:
                raise ValueError("For 'basic' auth_type, 'auth_config' must be a dict with 'username' and 'password'.")
        elif self.auth_type == "bearer":
            if not isinstance(self.auth_config, dict) or 'token' not in self.auth_config:
                raise ValueError("For 'bearer' auth_type, 'auth_config' must be a dict with 'token'.")
        elif self.auth_type == "apikey":
            if not isinstance(self.auth_config, dict) or 'header_name' not in self.auth_config or 'key' not in self.auth_config:
                raise ValueError("For 'apikey' auth_type, 'auth_config' must be a dict with 'header_name' and 'key'.")
        elif self.auth_type == "none":
             if self.auth_config is not None and len(self.auth_config) > 0:
                 logger.warning("Auth type is 'none' but auth_config is provided. Config will be ignored.")
                 self.auth_config = None
        else:
             raise ValueError(f"Unsupported auth_type for STOW-RS: '{self.auth_type}'")
        self.timeout = self.config.get("timeout", 120)
        logger.info(f"STOW-RS backend configured for URL: {self.stow_url}, Auth: {self.auth_type}")


    def _get_request_auth(self) -> Optional[requests.auth.AuthBase]:
        # Remains the same
        if self.auth_type == 'basic' and self.auth_config:
            username = self.auth_config.get('username')
            password = self.auth_config.get('password')
            if isinstance(username, str) and isinstance(password, str):
                 return HTTPBasicAuth(username, password)
        return None

    def _get_request_headers(self) -> Dict[str, str]:
        # Remains the same
        headers = {"Accept": "application/dicom+json"}
        if self.auth_type == 'bearer' and self.auth_config:
            token = self.auth_config.get('token')
            if isinstance(token, str) and token: headers['Authorization'] = f"Bearer {token}"
            else: logger.warning(f"Bearer auth configured for STOW-RS but token in auth_config is invalid.")
        elif self.auth_type == 'apikey' and self.auth_config:
            header_name = self.auth_config.get('header_name')
            key_value = self.auth_config.get('key')
            if isinstance(header_name, str) and header_name.strip() and isinstance(key_value, str) and key_value: headers[header_name.strip()] = key_value
            else: logger.warning(f"API Key auth configured for STOW-RS but header_name or key in auth_config are invalid.")
        return headers

    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        sop_instance_uid = getattr(modified_ds, 'SOPInstanceUID', 'Unknown_SOPInstanceUID')
        log_identifier = f"SOPInstanceUID {sop_instance_uid}"

        logger.debug(f"Attempting STOW-RS to generic endpoint: {self.stow_url} for {log_identifier}")

        buffer = None # Define buffer outside try for finally block
        try:
            # Get auth and headers
            req_auth = self._get_request_auth()
            req_headers = self._get_request_headers()

            # Prepare DICOM data as bytes in memory
            buffer = BytesIO()
            dcmwrite(buffer, modified_ds, write_like_original=False)
            buffer.seek(0)
            dicom_bytes = buffer.getvalue()
            # buffer closed in finally

            # --- Manually Construct Multipart Body and Headers ---
            boundary = uuid.uuid4().hex
            # Add the correct Content-Type header
            req_headers["Content-Type"] = f'multipart/related; type="application/dicom"; boundary="{boundary}"'

            # Construct the body parts
            body = b""
            body += f"--{boundary}\r\n".encode('utf-8')
            body += b"Content-Type: application/dicom\r\n"
            body += b"\r\n"
            body += dicom_bytes
            body += b"\r\n"
            body += f"--{boundary}--\r\n".encode('utf-8')
            # --- End Manual Construction ---

            # Make the POST request with raw data and correct headers
            response = requests.post(
                self.stow_url,
                headers=req_headers,
                auth=req_auth,
                data=body, # Send the manually constructed body
                # files=files, # DO NOT use files parameter anymore
                timeout=self.timeout
            )

            # Check response status
            if 200 <= response.status_code < 300:
                logger.info(f"Successfully stored {log_identifier} via STOW-RS to {self.stow_url}. Status: {response.status_code}")
                try:
                    response_json = response.json()
                    return response_json
                except requests.exceptions.JSONDecodeError:
                    logger.warning(f"STOW-RS to {self.stow_url} succeeded (Status: {response.status_code}) but response was not valid JSON.")
                    return {"status": "success", "message": f"Upload successful (Status: {response.status_code}), but response body was not JSON."}
            else:
                # Handle API errors
                error_details = response.text
                try:
                    error_json = response.json()
                    failed_seq = error_json.get("FailedSOPSequence", {}).get("Value", [])
                    if failed_seq and isinstance(failed_seq, list) and len(failed_seq) > 0:
                         first_failure = failed_seq[0]
                         reason_code = first_failure.get("FailureReason", {}).get("Value", ["Unknown"])[0]
                         error_details = f"Reason Code: {reason_code}"
                    elif isinstance(error_json.get("error"), dict):
                         error_details = error_json.get("error", {}).get("message", response.text)
                    elif error_json.get("Message"):
                         error_details = error_json.get("Message")
                except requests.exceptions.JSONDecodeError:
                    pass
                err_msg = (f"STOW-RS to {self.stow_url} failed for {log_identifier}. "
                           f"Status: {response.status_code}, Details: {error_details[:500]}")
                logger.error(err_msg)
                # Pass only message string to exception
                raise StorageBackendError(err_msg)

        except requests.exceptions.Timeout as e:
             err_msg = f"Network timeout during STOW-RS to {self.stow_url} for {log_identifier}"
             logger.error(f"{err_msg}: {e}", exc_info=False)
             raise StorageBackendError(err_msg) from e
        except requests.exceptions.RequestException as e:
            err_msg = f"Network error during STOW-RS to {self.stow_url} for {log_identifier}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            raise StorageBackendError(f"{err_msg}: {e}") from e
        except StorageBackendError:
             raise
        except Exception as e:
            err_msg = f"Unexpected error storing via STOW-RS to {self.stow_url} for {log_identifier}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            # Wrap the original exception message
            raise StorageBackendError(f"{err_msg}: {e}") from e
        finally:
            if buffer: # Check if buffer was assigned
                buffer.close()
