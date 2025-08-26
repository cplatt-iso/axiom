# backend/app/services/storage_backends/google_healthcare.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Union, List
import uuid
import json
from urllib.parse import urlencode, quote
import threading # Import threading for locks

import httpx # Use synchronous httpx
import pydicom
from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

# --- Define variables BEFORE try/except ---
google_auth = None
google_auth_exceptions = None
google_auth_transport_requests = None
# Removed _async_transport_request as it's not needed for sync

try:
    # Attempt imports
    import google.auth
    import google.auth.exceptions
    import google.auth.transport.requests

    # --- Assign imported modules to variables ---
    google_auth = google.auth
    google_auth_exceptions = google.auth.exceptions
    google_auth_transport_requests = google.auth.transport.requests
    logging.getLogger(__name__).info("Successfully imported google-auth modules.")

except ImportError:
    logging.getLogger(__name__).warning("google-auth library or dependencies not found. Google Healthcare backend unusable.")


try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = structlog.get_logger(__name__)

from .base_backend import BaseStorageBackend, StorageBackendError
# Keep schema import for type hints if used elsewhere, doesn't affect runtime here
# from app.schemas.data_browser import StudyResultItem


class GoogleHealthcareQueryError(Exception):
    pass


# --- Global state for credentials and client (use threading lock) ---
_credentials = None
_http_client = None
_credential_lock = threading.Lock()
_client_lock = threading.Lock()

# --- Synchronous Credential Handling ---
def get_ghc_credentials():
    """Gets or refreshes GHC credentials synchronously."""
    global _credentials
    with _credential_lock:
        if _credentials and _credentials.valid:
            return _credentials

        logger.info("Attempting to get/refresh GHC credentials (sync)...")
        if google_auth is None:
            logger.error("google.auth module was not loaded successfully (import failed).")
            raise GoogleHealthcareQueryError("google.auth module not loaded.")
        try:
            creds, project_id = google_auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            if not creds:
                raise GoogleHealthcareQueryError("Could not obtain default Google Cloud credentials.")

            if not creds.valid:
                 logger.info("GHC credentials require refresh (sync)...")
                 try:
                     # Use the synchronous transport if available
                     if google_auth_transport_requests:
                         auth_req_sync = google_auth_transport_requests.Request()
                         creds.refresh(auth_req_sync) # Direct synchronous call
                         logger.info("GHC credentials refreshed (sync).")
                     else:
                          logger.error("No google-auth synchronous transport available for refresh.")
                          raise GoogleHealthcareQueryError("Cannot refresh credentials - no sync auth transport available.")

                 except Exception as refresh_error:
                     logger.error("Failed to refresh GHC credentials (sync)", error=str(refresh_error), exc_info=False) # type: ignore[call-arg]
                     raise GoogleHealthcareQueryError("Failed to refresh GHC credentials (sync).") from refresh_error

            if not creds.valid:
                 logger.error("Could not obtain valid GHC credentials after sync refresh attempt.")
                 raise GoogleHealthcareQueryError("Could not obtain valid GHC credentials.")

            logger.info("GHC credentials obtained and valid (sync)", project_id=(project_id or 'Default')) # type: ignore[call-arg]
            _credentials = creds
            return _credentials

        except Exception as e:
             is_default_creds_error = False
             if google_auth_exceptions and isinstance(e, google_auth_exceptions.DefaultCredentialsError):
                 is_default_creds_error = True
                 logger.error("GHC: Failed to find Google Cloud default credentials.", error=str(e), exc_info=False) # type: ignore[call-arg]
                 raise GoogleHealthcareQueryError("Could not find Google Cloud default credentials. Ensure Application Default Credentials (ADC) are configured correctly in the environment (e.g., via GOOGLE_APPLICATION_CREDENTIALS environment variable or gcloud auth application-default login).") from e
             if not is_default_creds_error:
                  logger.error("GHC: Failed to initialize Google Cloud credentials", error=str(e), exc_info=True) # type: ignore[call-arg]
                  raise GoogleHealthcareQueryError(f"GHC credential initialization failed: {e}") from e

# --- Synchronous Token Getter ---
def _get_auth_token() -> str:
    """Gets a valid auth token synchronously."""
    creds = get_ghc_credentials() # Calls the sync getter
    if not creds:
         raise GoogleHealthcareQueryError("Credentials object is None after get_ghc_credentials.")
    if not creds.token:
        # Attempt refresh again if token is missing, although get_ghc_credentials should handle validity
        logger.warning("Token is None after get_ghc_credentials, attempting explicit refresh.")
        try:
            if google_auth_transport_requests:
                auth_req_sync = google_auth_transport_requests.Request()
                creds.refresh(auth_req_sync)
                logger.info("Token refreshed explicitly.")
            else:
                 raise GoogleHealthcareQueryError("Cannot refresh credentials - no sync auth transport available.")
        except Exception as refresh_error:
             logger.error("Explicit token refresh failed", error=str(refresh_error)) # type: ignore[call-arg]
             raise GoogleHealthcareQueryError("Failed to obtain valid token during explicit refresh.") from refresh_error

        if not creds.token: # Check again after explicit refresh
            logger.error("Token still None after explicit refresh. Cannot proceed.")
            raise GoogleHealthcareQueryError("Failed to obtain valid token from credentials object (token is None).")

    return creds.token

# --- Synchronous HTTP Client Getter ---
def get_http_client() -> httpx.Client:
    """Gets or initializes a shared synchronous httpx Client."""
    global _http_client
    if _http_client is None:
        with _client_lock:
            # Double-check locking
            if _http_client is None:
                logger.info("Initializing shared httpx.Client for GHC service.")
                timeout = httpx.Timeout(30.0, connect=10.0)
                # Create a synchronous client
                _http_client = httpx.Client(timeout=timeout, http2=True)
    return _http_client


class GoogleHealthcareDicomStoreStorage(BaseStorageBackend):

    def __init__(self, config: Dict[str, Any]):
        self.backend_type: str = config.get("type", "google_healthcare")
        self.name: str = config.get("name", "Unnamed GHC Backend")
        self.project_id: Optional[str] = config.get("gcp_project_id")
        self.location: Optional[str] = config.get("gcp_location")
        self.dataset_id: Optional[str] = config.get("gcp_dataset_id")
        self.dicom_store_id: Optional[str] = config.get("gcp_dicom_store_id")
        # Client and credentials will be initialized on first use via helpers
        self._client: Optional[httpx.Client] = None
        self.credentials = None

        self.required_keys: List[str] = ["gcp_project_id", "gcp_location", "gcp_dataset_id", "gcp_dicom_store_id"]

        self._validate_config() # Initial basic validation

        # URLs remain the same
        self.base_url: str = (
            f"https://healthcare.googleapis.com/v1/projects/{self.project_id}"
            f"/locations/{self.location}/datasets/{self.dataset_id}"
            f"/dicomStores/{self.dicom_store_id}/dicomWeb"
        )
        self.stow_url: str = f"{self.base_url}/studies"
        self.qido_studies_url: str = f"{self.base_url}/studies"
        self.qido_series_url: str = f"{self.base_url}/series"
        self.qido_instances_url: str = f"{self.base_url}/instances"


    def _validate_config(self) -> None:
        """Basic synchronous validation."""
        missing = [key for key in self.required_keys if not getattr(self, key.replace("gcp_", ""), None)]
        if missing:
            raise ValueError(f"Google Healthcare storage config (name: {self.name}) missing required keys: {', '.join(missing)}")
        logger.debug("GHC Backend: Synchronous config key validation passed.", backend_name=self.name) # type: ignore[call-arg]

    # Removed initialize_client as it's handled implicitly by getters now
    # Removed client property as it's handled by get_http_client()

    def _get_auth_headers(self) -> Dict[str, str]:
        """Gets auth headers synchronously."""
        try:
            token = _get_auth_token() # Call sync token getter
            return {"Authorization": f"Bearer {token}"}
        except GoogleHealthcareQueryError as e:
             logger.error("GHC Backend: Failed to get auth token.", backend_name=self.name, error=str(e)) # type: ignore[call-arg]
             raise StorageBackendError(f"Failed to obtain GHC access token for {self.name}.") from e


    # --- Synchronous store method ---
    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None,
        **kwargs: Any
    ) -> Union[Dict[str, Any], str]:

        sop_instance_uid: str = getattr(modified_ds, 'SOPInstanceUID', 'Unknown_SOPInstanceUID')
        study_instance_uid: str = getattr(modified_ds, 'StudyInstanceUID', 'Unknown_StudyInstanceUID')
        log = logger.bind( # type: ignore[attr-defined]
            sop_instance_uid=sop_instance_uid, study_instance_uid=study_instance_uid,
            google_dicom_store=f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}",
            storage_backend_type=self.backend_type,
            storage_backend_name=self.name
        )
        log.debug("Attempting GHC STOW-RS upload (sync)", stow_url=self.stow_url)
        buffer: Optional[BytesIO] = None
        try:
            # Get client and headers synchronously
            client = get_http_client()
            auth_headers = self._get_auth_headers()

            # File meta creation logic remains the same
            if not hasattr(modified_ds, 'file_meta') or not modified_ds.file_meta:
                 log.warning("Dataset missing file_meta, creating default for GHC STOW.")
                 file_meta = pydicom.dataset.FileMetaDataset() # type: ignore[attr-defined]
                 file_meta.FileMetaInformationVersion = b'\x00\x01'
                 sop_class_uid_data = modified_ds.get("SOPClassUID")
                 sop_class_uid_val_list = getattr(sop_class_uid_data, 'value', ['1.2.840.10008.5.1.4.1.1.2'])
                 sop_class_uid_val = sop_class_uid_val_list[0] if isinstance(sop_class_uid_val_list, list) and sop_class_uid_val_list else '1.2.840.10008.5.1.4.1.1.2'
                 file_meta.MediaStorageSOPClassUID = sop_class_uid_val
                 file_meta.MediaStorageSOPInstanceUID = sop_instance_uid
                 try:
                     transfer_syntax = modified_ds.file_meta.TransferSyntaxUID
                 except AttributeError:
                     log.warning("No file_meta or TransferSyntaxUID found on file_meta. Defaulting to ImplicitVRLittleEndian.")
                     transfer_syntax = pydicom.uid.ImplicitVRLittleEndian # type: ignore[attr-defined]
                 if transfer_syntax not in pydicom.uid.TRANSFER_SYNTAXES: # type: ignore[attr-defined]
                     log.warning(f"Unrecognized TransferSyntaxUID '{transfer_syntax}'. Defaulting to ImplicitVRLittleEndian.")
                     transfer_syntax = pydicom.uid.ImplicitVRLittleEndian # type: ignore[attr-defined]
                 file_meta.TransferSyntaxUID = transfer_syntax
                 file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID # type: ignore[attr-defined]
                 file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
                 modified_ds.file_meta = file_meta

            buffer = BytesIO()
            dcmwrite(buffer, modified_ds, write_like_original=False)
            buffer.seek(0)
            dicom_bytes: bytes = buffer.getvalue()

            boundary: str = uuid.uuid4().hex
            headers: Dict[str, str] = {
                **auth_headers,
                "Accept": "application/dicom+json",
                "Content-Type": f'multipart/related; type="application/dicom"; boundary="{boundary}"'
            }

            body: bytes = b""
            body += f"--{boundary}\r\n".encode('utf-8')
            body += b"Content-Type: application/dicom\r\n\r\n"
            body += dicom_bytes
            body += b"\r\n"
            body += f"--{boundary}--\r\n".encode('utf-8')

            # --- Synchronous HTTP POST ---
            response = client.post(self.stow_url, headers=headers, content=body)

            # Response handling logic remains the same
            if 200 <= response.status_code < 300:
                log.info("GHC STOW-RS successful (sync)", status_code=response.status_code)
                try:
                    return response.json()
                except json.JSONDecodeError:
                    log.warning("GHC STOW success but non-JSON response.")
                    return {"status": "success", "message": "Upload successful, non-JSON response."}
            elif response.status_code == 409:
                # Conflict handling
                failure_details = ""
                failure_reason = "Conflict"
                try:
                    response_json = response.json()
                    failed_seq_val = response_json.get("00081198", {}).get("Value", [{}])
                    failed_seq = failed_seq_val[0] if isinstance(failed_seq_val, list) and failed_seq_val else {}
                    reason_code_data = failed_seq.get("00081197", {})
                    reason_code_val = getattr(reason_code_data, 'Value', [None])
                    reason_code = reason_code_val[0] if isinstance(reason_code_val, list) and reason_code_val else None
                    if reason_code == 272: failure_reason = "Duplicate SOPInstanceUID"
                    elif reason_code == 43264: failure_reason = "Instance Coercion"
                    retrieve_url_data = response_json.get("00081190", {})
                    retrieve_url_val = getattr(retrieve_url_data, 'Value', [None])
                    retrieve_url = retrieve_url_val[0] if isinstance(retrieve_url_val, list) and retrieve_url_val else None
                    failure_details = f"ReasonCode: {reason_code}, ConflictingStudyURL: {retrieve_url}"
                except Exception as parse_exc:
                    log.warning("Failed parse 409 Conflict details", parse_error=str(parse_exc))
                    failure_details = response.text[:200]
                log.warning("GHC STOW-RS Conflict (Duplicate Instance?)", status_code=response.status_code, reason=failure_reason, details=failure_details)
                return "duplicate"
            else:
                # Other error handling
                error_details_str = response.text
                try:
                    error_json = response.json()
                    error_details_str = str(error_json.get("error", {}).get("message", error_json))
                except json.JSONDecodeError:
                    pass
                err_msg = f"STOW-RS to GHC failed. Status: {response.status_code}"
                log.error(err_msg, status_code=response.status_code, details=error_details_str[:500])
                raise StorageBackendError(f"{err_msg}, Details: {error_details_str[:500]}")
        # Exception handling remains mostly the same, catches httpx errors now
        except httpx.TimeoutException as e:
             raise StorageBackendError(f"Network timeout during GHC STOW ({self.stow_url}) for {self.name}") from e
        except httpx.RequestError as e:
             raise StorageBackendError(f"Network error during GHC STOW ({self.stow_url}) for {self.name}: {e}") from e
        except StorageBackendError as e:
             raise e
        except Exception as e:
             log.error(f"Unexpected error storing via GHC STOW for {self.name}", error=str(e), exc_info=True)
             raise StorageBackendError(f"Unexpected error storing via GHC STOW for {self.name}: {e}") from e
        finally:
            if buffer:
                 buffer.close()

    # --- Synchronous search methods ---
    def search_studies(
        self,
        query_params: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        fuzzymatching: bool = False,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        log = logger.bind(storage_backend_name=self.name) # type: ignore[attr-defined]
        log.debug("Calling sync search_for_studies")
        # Calls the synchronous helper
        return search_for_studies(
             base_url=self.base_url,
             query_params=query_params,
             limit=limit,
             offset=offset,
             fuzzymatching=fuzzymatching,
             fields=fields
         )

    def search_series(
        self,
        study_instance_uid: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
        limit: int = 1000,
        offset: int = 0,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        log = logger.bind(storage_backend_name=self.name) # type: ignore[attr-defined]
        log.debug("Calling sync search_for_series")
        return search_for_series(
             base_url=self.base_url,
             study_instance_uid=study_instance_uid,
             query_params=query_params,
             limit=limit,
             offset=offset,
             fields=fields
         )

    def search_instances(
        self,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
        limit: int = 1000,
        offset: int = 0,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        log = logger.bind(storage_backend_name=self.name) # type: ignore[attr-defined]
        log.debug("Calling sync search_for_instances")
        return search_for_instances(
             base_url=self.base_url,
             study_instance_uid=study_instance_uid,
             series_instance_uid=series_instance_uid,
             query_params=query_params,
             limit=limit,
             offset=offset,
             fields=fields
         )

    def __repr__(self) -> str:
        store_path: str = f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}"
        return f"<{self.__class__.__name__}(name={self.name}, dicom_store={store_path})>"


# --- Shared SYNC Helper Functions for QIDO ---

def _execute_qido_query(
    url: str,
    query_params: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    offset: int = 0,
    fuzzymatching: bool = False,
    fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Executes a QIDO query synchronously."""
    log = logger.bind(qido_url=url) # type: ignore[attr-defined]
    params = {}
    if query_params:
        params.update(query_params)
    params["limit"] = limit
    params["offset"] = offset
    if fuzzymatching:
        params["fuzzymatching"] = "true"
    if fields:
        params["includefield"] = fields

    try:
        # Get sync client and auth headers
        client = get_http_client()
        auth_headers = _get_auth_token() # Direct call to sync token getter
        headers = {"Authorization": f"Bearer {auth_headers}", "Accept": "application/dicom+json"} # Construct headers

        log.debug("Executing GHC QIDO query (sync)", request_params=params)
        # Use synchronous get
        response = client.get(url, params=params, headers=headers)

        if response.status_code == 200:
            results = response.json()
            log.info("GHC QIDO query successful (sync)", result_count=len(results))
            return results
        elif response.status_code == 204:
             log.info("GHC QIDO query successful (sync - 204 No Content)", result_count=0)
             return []
        else:
            # Error handling remains the same
            error_details = response.text
            try: error_details = str(response.json().get("error", {}).get("message", error_details))
            except: pass
            log.error("GHC QIDO query failed (sync)", status_code=response.status_code, details=error_details[:500])
            raise GoogleHealthcareQueryError(f"QIDO query failed ({response.status_code}): {error_details[:500]}")

    except httpx.TimeoutException as e:
        raise GoogleHealthcareQueryError(f"Network timeout during GHC QIDO ({url})") from e
    except httpx.RequestError as e:
        raise GoogleHealthcareQueryError(f"Network error during GHC QIDO ({url}): {e}") from e
    except GoogleHealthcareQueryError as e:
        raise e
    except Exception as e:
        log.error("Unexpected error during GHC QIDO query (sync)", error=str(e), exc_info=True)
        raise GoogleHealthcareQueryError(f"Unexpected error during GHC QIDO: {e}") from e


def search_for_studies(
    base_url: str,
    query_params: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    offset: int = 0,
    fuzzymatching: bool = False,
    fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Synchronous search for studies."""
    url = f"{base_url}/studies"
    return _execute_qido_query(url, query_params, limit, offset, fuzzymatching, fields)


def search_for_series(
    base_url: str,
    study_instance_uid: Optional[str] = None,
    query_params: Optional[Dict[str, Any]] = None,
    limit: int = 1000,
    offset: int = 0,
    fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Synchronous search for series."""
    url = f"{base_url}/studies/{study_instance_uid}/series" if study_instance_uid else f"{base_url}/series"
    return _execute_qido_query(url, query_params, limit, offset, False, fields)


def search_for_instances(
    base_url: str,
    study_instance_uid: Optional[str] = None,
    series_instance_uid: Optional[str] = None,
    query_params: Optional[Dict[str, Any]] = None,
    limit: int = 1000,
    offset: int = 0,
    fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Synchronous search for instances."""
    if study_instance_uid and series_instance_uid:
        url = f"{base_url}/studies/{study_instance_uid}/series/{series_instance_uid}/instances"
    elif study_instance_uid:
        url = f"{base_url}/studies/{study_instance_uid}/instances"
    else:
        url = f"{base_url}/instances"
    return _execute_qido_query(url, query_params, limit, offset, False, fields)
