# app/services/storage_backends/stow_rs.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Literal, Union
from urllib.parse import urljoin
import uuid

import requests
from requests.auth import HTTPBasicAuth
from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

from .base_backend import BaseStorageBackend, StorageBackendError

logger = logging.getLogger(__name__)

StowAuthType = Literal["none", "basic", "bearer", "apikey"]

class StowRsStorage(BaseStorageBackend):
    """Sends DICOM files to a generic DICOMweb STOW-RS endpoint."""

    def __init__(self, config: Dict[str, Any]):
        self.backend_type = config.get("type", "stow_rs")
        self.name = config.get("name", "Unnamed STOW-RS Backend")
        self.base_url = config.get("base_url") # Required
        # Stow URL is now derived, not primary config
        self.stow_url: Optional[str] = None
        self.auth_type: StowAuthType = config.get("auth_type", "none") # Keep auth fields
        self.auth_config: Optional[Dict[str, Any]] = config.get("auth_config")
        self.timeout = config.get("timeout", 120)

        if not self.base_url:
            raise ValueError("STOW-RS storage config requires 'base_url'.")
        if not self.base_url.startswith(('http://', 'https://')):
            raise ValueError("Base URL must start with http:// or https://")

        # Derive stow_url from base_url
        base = self.base_url
        base_with_slash = base if base.endswith('/') else base + '/'
        self.stow_url = urljoin(base_with_slash, "studies")
        logger.debug(f"STOW-RS using URL: {self.stow_url} derived from base: {self.base_url}")

        self._validate_config() # Call validation

    def _validate_config(self):
        # Validation focuses on auth consistency now
        if self.auth_type == "basic":
            if not isinstance(self.auth_config, dict) or 'username' not in self.auth_config or 'password' not in self.auth_config:
                raise ValueError("Basic auth needs 'auth_config' dict with 'username'/'password'.")
        elif self.auth_type == "bearer":
            if not isinstance(self.auth_config, dict) or 'token' not in self.auth_config:
                raise ValueError("Bearer auth needs 'auth_config' dict with 'token'.")
        elif self.auth_type == "apikey":
            if not isinstance(self.auth_config, dict) or 'header_name' not in self.auth_config or 'key' not in self.auth_config:
                raise ValueError("API Key auth needs 'auth_config' dict with 'header_name'/'key'.")
        elif self.auth_type == "none":
             if self.auth_config is not None and len(self.auth_config) > 0:
                 logger.warning("Auth type 'none' but auth_config provided; ignoring config.")
                 self.auth_config = None
        else:
             raise ValueError(f"Unsupported auth_type for STOW-RS: '{self.auth_type}'")
        logger.info(f"STOW-RS backend validated for URL: {self.stow_url}, Auth: {self.auth_type}")

    def _get_request_auth(self) -> Optional[requests.auth.AuthBase]:
        if self.auth_type == 'basic' and self.auth_config:
            username = self.auth_config.get('username'); password = self.auth_config.get('password')
            if isinstance(username, str) and isinstance(password, str): return HTTPBasicAuth(username, password)
        return None

    def _get_request_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/dicom+json"}
        if self.auth_type == 'bearer' and self.auth_config:
            token = self.auth_config.get('token')
            if isinstance(token, str) and token: headers['Authorization'] = f"Bearer {token}"
            else: logger.warning(f"Invalid Bearer token in auth_config.")
        elif self.auth_type == 'apikey' and self.auth_config:
            header_name = self.auth_config.get('header_name'); key_value = self.auth_config.get('key')
            if isinstance(header_name, str) and header_name.strip() and isinstance(key_value, str) and key_value: headers[header_name.strip()] = key_value
            else: logger.warning(f"Invalid API Key header/key in auth_config.")
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
        if not self.stow_url: raise StorageBackendError("STOW URL not configured/derived.")

        logger.debug(f"Attempting STOW-RS to endpoint: {self.stow_url} for {log_identifier}")

        buffer = None
        try:
            req_auth = self._get_request_auth(); req_headers = self._get_request_headers()
            buffer = BytesIO(); dcmwrite(buffer, modified_ds, write_like_original=False); buffer.seek(0)
            dicom_bytes = buffer.getvalue()
            boundary = uuid.uuid4().hex
            req_headers["Content-Type"] = f'multipart/related; type="application/dicom"; boundary="{boundary}"'
            body = b""; body += f"--{boundary}\r\n".encode('utf-8'); body += b"Content-Type: application/dicom\r\n\r\n"; body += dicom_bytes; body += b"\r\n"; body += f"--{boundary}--\r\n".encode('utf-8')

            response = requests.post( self.stow_url, headers=req_headers, auth=req_auth, data=body, timeout=self.timeout )

            if 200 <= response.status_code < 300:
                logger.info(f"STOW-RS success to {self.stow_url}. Status: {response.status_code}")
                try: return response.json()
                except requests.exceptions.JSONDecodeError: logger.warning(f"STOW-RS success but non-JSON response."); return {"status": "success", "message": "Upload successful, non-JSON response."}
            else:
                error_details = response.text
                try:
                    error_json = response.json(); failed_seq = error_json.get("FailedSOPSequence", {}).get("Value", [])
                    if failed_seq and isinstance(failed_seq, list) and len(failed_seq) > 0:
                         first_failure = failed_seq[0]; reason_code = first_failure.get("FailureReason", {}).get("Value", ["Unknown"])[0]
                         error_details = f"Reason Code: {reason_code}"
                    elif isinstance(error_json.get("error"), dict): error_details = error_json.get("error", {}).get("message", response.text)
                    elif error_json.get("Message"): error_details = error_json.get("Message")
                except requests.exceptions.JSONDecodeError: pass
                err_msg = (f"STOW-RS to {self.stow_url} failed for {log_identifier}. Status: {response.status_code}")
                logger.error(err_msg, details=error_details[:500])
                raise StorageBackendError(f"{err_msg}, Details: {error_details[:500]}")

        except requests.exceptions.Timeout as e: raise StorageBackendError(f"Timeout during STOW-RS to {self.stow_url}") from e
        except requests.exceptions.RequestException as e: raise StorageBackendError(f"Network error during STOW-RS to {self.stow_url}: {e}") from e
        except StorageBackendError: raise
        except Exception as e: raise StorageBackendError(f"Unexpected error storing via STOW-RS to {self.stow_url}: {e}") from e
        finally:
            if buffer: buffer.close()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(stow_url={self.stow_url}, auth={self.auth_type})>"
