# app/services/storage_backends/stow_rs.py

import logging
import tempfile # For temporary CA cert file
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Literal, Union
from urllib.parse import urljoin
import uuid
import os # For temp file cleanup if needed

import requests
from requests.auth import HTTPBasicAuth, AuthBase
from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

# Assuming gcp_utils is accessible
from app.core import gcp_utils # Ensure this import path is correct
from app.core.gcp_utils import SecretManagerError, SecretNotFoundError # For specific error handling

from .base_backend import BaseStorageBackend, StorageBackendError

# Use structlog. If it's a hard dependency, the try-except can be simplified
# for Pylance to better understand the logger type.
import structlog
logger: structlog.stdlib.BoundLogger = structlog.get_logger(__name__)


StowAuthType = Literal["none", "basic", "bearer", "apikey"]

class StowRsStorage(BaseStorageBackend):
    """Sends DICOM files to a generic DICOMweb STOW-RS endpoint with authentication."""

    def __init__(self, config: Dict[str, Any]):
        # Initialize attributes that _validate_config might use IF they are not solely derived from 'config'
        # In this case, most are derived from 'config' which is set by super()
        self.stow_url: Optional[str] = None 
        self.auth_config: Dict[str, Any] = {} 
        self.tls_ca_cert_path: Optional[str] = None
        self._temp_ca_file: Optional[tempfile._TemporaryFileWrapper] = None
        
        # These will be properly set by _validate_config using self.config
        # or can be set here if _validate_config doesn't already do it.
        # Let's ensure _validate_config sets them up.
        self.name: str = "Unnamed STOW-RS Backend" # Default, will be overwritten
        self.backend_type: str = "stow_rs" # Default, will be overwritten
        self.base_url: Optional[str] = None # Will be set
        self.auth_type: StowAuthType = "none" # Will be set
        self.timeout: int = 120 # Will be set

        super().__init__(config) # This will set self.config and then call self._validate_config()

    def _validate_config(self):
        """
        Validates the configuration provided during initialization,
        fetches secrets, and sets up auth_config and TLS cert path.
        This method is called by the parent BaseStorageBackend's __init__.
        Assumes self.config is already set by BaseStorageBackend.__init__
        """
        # --- Get initial values from self.config (which was set by BaseStorageBackend) ---
        self.name = self.config.get("name", "Unnamed STOW-RS Backend")
        self.backend_type = self.config.get("type", "stow_rs") # Should match StowRsAuthType Literals
        self.base_url = self.config.get("base_url")
        self.timeout = self.config.get("timeout", 120) # seconds
        # auth_type is also from config, will be fetched below

        log = logger.bind(backend_name=self.name, backend_type=self.backend_type) # NOW self.name and self.backend_type are set

        if not self.base_url:
            log.error("STOW-RS config missing 'base_url'.")
            raise ValueError("STOW-RS storage config requires 'base_url'.")
        if not self.base_url.startswith(('http://', 'https://')):
            log.error("STOW-RS 'base_url' must start with http:// or https://.", base_url=self.base_url)
            raise ValueError("Base URL must start with http:// or https://")

        base = self.base_url
        base_with_slash = base if base.endswith('/') else base + '/'
        self.stow_url = urljoin(base_with_slash, "studies") # self.stow_url is now set
        log = log.bind(stow_url=self.stow_url) 
        log.debug("STOW-RS URL derived.", base_url=self.base_url)

        # --- Authentication Setup ---
        self.auth_type = self.config.get("auth_type", "none") # Set self.auth_type
        log = log.bind(auth_type=self.auth_type)

        # ... rest of _validate_config remains the same, as it uses self.config.get() ...
        # and sets self.auth_config, self.tls_ca_cert_path, self._temp_ca_file
        try:
            if self.auth_type == "basic":
                username_secret = self.config.get("basic_auth_username_secret_name")
                password_secret = self.config.get("basic_auth_password_secret_name")
                if not username_secret or not password_secret:
                    log.error("Basic auth configured, but secret names for username/password missing.")
                    raise ValueError("Basic auth requires 'basic_auth_username_secret_name' and 'basic_auth_password_secret_name'.")
                log.info("Fetching Basic Auth credentials from Secret Manager.", username_secret=username_secret, password_secret=password_secret)
                self.auth_config["username"] = gcp_utils.get_secret(username_secret)
                self.auth_config["password"] = gcp_utils.get_secret(password_secret)
                log.info("Basic Auth credentials fetched successfully.")

            elif self.auth_type == "bearer":
                token_secret = self.config.get("bearer_token_secret_name")
                if not token_secret:
                    log.error("Bearer auth configured, but 'bearer_token_secret_name' missing.")
                    raise ValueError("Bearer auth requires 'bearer_token_secret_name'.")
                log.info("Fetching Bearer token from Secret Manager.", token_secret=token_secret)
                self.auth_config["token"] = gcp_utils.get_secret(token_secret)
                log.info("Bearer token fetched successfully.")

            elif self.auth_type == "apikey":
                key_secret = self.config.get("api_key_secret_name")
                header_name = self.config.get("api_key_header_name_override")
                if not key_secret:
                    log.error("API Key auth configured, but 'api_key_secret_name' missing.")
                    raise ValueError("API Key auth requires 'api_key_secret_name'.")
                if not header_name:
                     log.error("API Key auth configured, but 'api_key_header_name_override' missing. Schema should require this.")
                     raise ValueError("API Key auth requires 'api_key_header_name_override'.")

                log.info("Fetching API Key from Secret Manager.", key_secret=key_secret, header_name=header_name)
                self.auth_config["key"] = gcp_utils.get_secret(key_secret)
                self.auth_config["header_name"] = header_name
                log.info("API Key fetched successfully.")

            elif self.auth_type == "none":
                self.auth_config = {} 
                log.info("No authentication configured for STOW-RS.")
            
            else:
                log.error("Unsupported 'auth_type' provided for STOW-RS.", auth_type_received=self.auth_type)
                raise ValueError(f"Unsupported auth_type for STOW-RS: '{self.auth_type}'")

        except SecretNotFoundError as e:
            log.error("A configured GCP secret was not found.", secret_error=str(e), exc_info=True)
            raise StorageBackendError(f"Failed to initialize STOW-RS backend: Secret not found - {e}") from e
        except SecretManagerError as e:
            log.error("Failed to fetch a secret from GCP Secret Manager.", secret_error=str(e), exc_info=True)
            raise StorageBackendError(f"Failed to initialize STOW-RS backend: GCP Secret Manager error - {e}") from e
        except ValueError: 
            raise
        except Exception as e: 
            log.error("Unexpected error during STOW-RS auth setup.", error=str(e), exc_info=True)
            raise StorageBackendError(f"Unexpected error setting up STOW-RS auth: {e}") from e

        # --- TLS Custom CA Certificate Setup ---
        tls_ca_cert_secret = self.config.get("tls_ca_cert_secret_name")
        if tls_ca_cert_secret:
            log.info("Custom CA certificate configured. Fetching from Secret Manager.", ca_cert_secret=tls_ca_cert_secret)
            try:
                ca_cert_pem = gcp_utils.get_secret(tls_ca_cert_secret)
                self._temp_ca_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=".pem")
                self._temp_ca_file.write(ca_cert_pem)
                self._temp_ca_file.flush() 
                self.tls_ca_cert_path = self._temp_ca_file.name
                log.info("Custom CA certificate fetched and saved to temporary file.", temp_ca_path=self.tls_ca_cert_path)
            except SecretNotFoundError as e:
                log.error("Custom CA cert secret not found.", secret_name=tls_ca_cert_secret, error=str(e))
                self._cleanup_temp_ca_file() 
                raise StorageBackendError(f"Failed to initialize STOW-RS: Custom CA cert secret '{tls_ca_cert_secret}' not found.") from e
            except (SecretManagerError, OSError, IOError) as e: 
                log.error("Error fetching or writing custom CA cert.", secret_name=tls_ca_cert_secret, error=str(e), exc_info=True)
                self._cleanup_temp_ca_file()
                raise StorageBackendError(f"Failed to process custom CA cert for STOW-RS: {e}") from e
        
        log.info("STOW-RS backend validated and configured.", auth_type=self.auth_type, stow_url=self.stow_url, custom_ca_configured=bool(self.tls_ca_cert_path))

    def _get_request_auth(self) -> Optional[AuthBase]:
        if self.auth_type == 'basic' and self.auth_config:
            username = self.auth_config.get('username')
            password = self.auth_config.get('password')
            if isinstance(username, str) and isinstance(password, str):
                return HTTPBasicAuth(username, password)
            else:
                # Log with the specific context for this message
                logger.warning("Basic auth configured but username/password missing in auth_config after init. This shouldn't happen.", backend_name=self.name)
        return None

    def _get_request_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/dicom+json"} 
        if self.auth_type == 'bearer' and self.auth_config:
            token = self.auth_config.get('token')
            if isinstance(token, str) and token:
                headers['Authorization'] = f"Bearer {token}"
            else:
                logger.warning("Bearer auth configured but token missing in auth_config after init.", backend_name=self.name)
        elif self.auth_type == 'apikey' and self.auth_config:
            header_name = self.auth_config.get('header_name')
            key_value = self.auth_config.get('key')
            if isinstance(header_name, str) and header_name.strip() and isinstance(key_value, str) and key_value:
                headers[header_name.strip()] = key_value
            else:
                logger.warning("API Key auth configured but header/key missing in auth_config after init.", backend_name=self.name)
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
        # Create a logger instance with bound context for this method
        log = logger.bind(
            backend_name=self.name,
            stow_url=self.stow_url,
            sop_instance_uid=sop_instance_uid,
            auth_type=self.auth_type,
            original_filepath_log=str(original_filepath) if original_filepath else "N/A",
            source_identifier_log=source_identifier
        )

        if not self.stow_url: # Should be caught by _validate_config
            log.error("STOW URL not configured/derived. Cannot store.")
            raise StorageBackendError("STOW URL not configured. Backend initialization likely failed.")

        log.debug("Attempting STOW-RS store operation.")

        buffer = None
        try:
            req_auth = self._get_request_auth()
            req_headers = self._get_request_headers()
            
            buffer = BytesIO()
            dcmwrite(buffer, modified_ds, write_like_original=False) # DICOM Part10 format
            buffer.seek(0)
            dicom_bytes = buffer.getvalue()

            # Construct multipart/related payload
            boundary = uuid.uuid4().hex
            # The Content-Type for requests should be multipart/related.
            # The DICOM content itself inside the multipart is application/dicom.
            req_headers["Content-Type"] = f'multipart/related; type="application/dicom"; boundary="{boundary}"'
            
            body = b""
            body += f"--{boundary}\r\n".encode('utf-8')
            body += b"Content-Type: application/dicom\r\n" # This part's content type
            body += b"Content-Transfer-Encoding: binary\r\n\r\n" # Explicitly state binary
            body += dicom_bytes
            body += b"\r\n" # Ensure newline before next boundary
            body += f"--{boundary}--\r\n".encode('utf-8')
            
            # Determine verify parameter for requests
            verify_param = self.tls_ca_cert_path if self.tls_ca_cert_path else True # True for default system CAs

            log.debug("Sending STOW-RS request.", headers_sent=req_headers.keys(), verify_param=verify_param)

            response = requests.post(
                self.stow_url,
                headers=req_headers,
                auth=req_auth,
                data=body,
                timeout=self.timeout,
                verify=verify_param 
            )

            log = log.bind(response_status_code=response.status_code)

            if 200 <= response.status_code < 300:
                log.info("STOW-RS store successful.")
                try:
                    return response.json()
                except requests.exceptions.JSONDecodeError:
                    log.warning("STOW-RS success but non-JSON response.", response_text_preview=response.text[:200])
                    return {"status": "success", "message": "Upload successful, non-JSON response."}
            else:
                # Try to get more detailed error from response
                error_details = response.text
                try:
                    error_json = response.json()
                    # Check for DICOM specific error sequences (e.g., FailedSOPSequence)
                    # This structure can vary between STOW-RS servers.
                    if "FailedSOPSequence" in error_json and isinstance(error_json["FailedSOPSequence"], dict) and "Value" in error_json["FailedSOPSequence"]:
                        failed_sops = error_json["FailedSOPSequence"]["Value"]
                        if isinstance(failed_sops, list) and len(failed_sops) > 0:
                            first_failure = failed_sops[0]
                            reason_code = first_failure.get("FailureReason", {}).get("Value", ["Unknown"])[0]
                            reason_str = f" (DICOM FailureReason: {reason_code})"
                            error_details = f"{response.status_code}{reason_str} - {response.text}"
                    elif "Message" in error_json: # Common in some error responses
                         error_details = error_json["Message"]
                    elif "error" in error_json and isinstance(error_json["error"], dict) and "message" in error_json["error"]:
                         error_details = error_json["error"]["message"]
                except requests.exceptions.JSONDecodeError:
                    pass # Stick with response.text

                err_msg = f"STOW-RS to {self.stow_url} failed."
                log.error(err_msg, stow_error_details=error_details[:1000]) # Log more details
                # Pass status_code to StorageBackendError
                raise StorageBackendError(f"{err_msg} Status: {response.status_code}. Details: {error_details[:500]}", status_code=response.status_code)

        except requests.exceptions.Timeout as e:
            log.error("Timeout during STOW-RS request.", error=str(e))
            raise StorageBackendError(f"Timeout during STOW-RS to {self.stow_url}", status_code=None) from e
        except requests.exceptions.RequestException as e: # Covers ConnectionError, etc.
            log.error("Network error during STOW-RS request.", error=str(e), error_type=type(e).__name__)
            status_code_from_exc = getattr(e.response, 'status_code', None) if e.response is not None else None
            raise StorageBackendError(f"Network error during STOW-RS to {self.stow_url}: {e}", status_code=status_code_from_exc) from e
        except StorageBackendError: # Re-raise our own errors
            raise
        except Exception as e:
            log.exception("Unexpected error during STOW-RS store operation.") # Use .exception for full traceback
            raise StorageBackendError(f"Unexpected error storing via STOW-RS to {self.stow_url}: {e}", status_code=None) from e
        finally:
            if buffer:
                buffer.close()
            # Temp CA file is cleaned up by __del__ or explicit cleanup call

    def _cleanup_temp_ca_file(self):
        # Create a logger instance with bound context for this method
        log = logger.bind(backend_name=self.name)
        if self._temp_ca_file:
            try:
                ca_path = self._temp_ca_file.name
                self._temp_ca_file.close() # Close it first
                if Path(ca_path).exists(): # Check if it still exists before unlinking
                    os.unlink(ca_path) # Delete the file from disk
                log.info("Temporary CA certificate file cleaned up.")
            except Exception as e:
                log.error("Error cleaning up temporary CA certificate file.", temp_ca_path=self._temp_ca_file.name, error=str(e), exc_info=True)
            finally:
                self._temp_ca_file = None
                self.tls_ca_cert_path = None

    def __del__(self):
        """Destructor to ensure temporary CA file is cleaned up."""
        self._cleanup_temp_ca_file()

    def __repr__(self) -> str:
        return (f"<{self.__class__.__name__}(stow_url={self.stow_url}, auth={self.auth_type}, "
                f"name='{self.name}', custom_ca_path='{self.tls_ca_cert_path}')>")