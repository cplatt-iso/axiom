# backend/app/services/google_healthcare_service.py

import asyncio
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode, quote
import json
import httpx

# --- Simplified Google Auth Imports ---
# Assume google.auth itself imports correctly based on previous logs
import google.auth
import google.auth.exceptions # Keep trying to import this for specific error handling

# Try importing the specific async transport request object
_async_transport_request = None
try:
    from google.auth.transport.aiohttp import Request as AsyncAuthRequest
    _async_transport_request = AsyncAuthRequest
    logging.getLogger(__name__).info("Successfully imported google.auth.transport.aiohttp.Request for async refresh.")
except ImportError:
    logging.getLogger(__name__).warning("Could not import google.auth.transport.aiohttp.Request. Async credential refresh may fallback to sync/blocking.")
    # Fallback needed if async refresh isn't possible
    try:
        import google.auth.transport.requests
    except ImportError:
         # This would be very bad - means sync refresh also impossible
         logging.getLogger(__name__).critical("Failed to import google.auth.transport.requests - basic auth will likely fail.")
         google.auth.transport.requests = None # Ensure it's None

# --- End Simplified Imports ---


# Use structlog if available
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    # import logging # Already imported via standard logging above
    logger = logging.getLogger(__name__)


class GoogleHealthcareQueryError(Exception):
    """Custom exception for Google Healthcare query errors."""
    pass

# --- Credential Management ---
_credentials = None
_credential_lock = asyncio.Lock()

async def get_ghc_credentials():
    """Gets and refreshes Google Cloud credentials asynchronously."""
    global _credentials
    async with _credential_lock:
        if _credentials and _credentials.valid:
            return _credentials

        logger.info("Attempting to get/refresh GHC credentials...")
        # Check if google.auth module loaded - should be fine now based on logs
        if not google.auth:
             logger.error("Fatal: google.auth module not loaded.")
             raise GoogleHealthcareQueryError("google.auth module not loaded.")

        try:
            creds, project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            if not creds:
                raise GoogleHealthcareQueryError("Could not obtain default Google Cloud credentials.")

            if not creds.valid:
                 logger.info("GHC credentials require refresh...")
                 try:
                     # --- Use the imported async request object if available ---
                     if _async_transport_request:
                         request = _async_transport_request()
                         await creds.refresh(request)
                         logger.info("GHC credentials refreshed (async).")
                     # --- Fallback to sync if async import failed ---
                     elif google.auth.transport.requests:
                         logger.warning("GHC: Using sync credential refresh (async transport unavailable).")
                         auth_req_sync = google.auth.transport.requests.Request()
                         await asyncio.to_thread(creds.refresh, auth_req_sync)
                         logger.info("GHC credentials refreshed (sync fallback).")
                     else:
                          logger.error("No google-auth transport available for refresh.")
                          raise GoogleHealthcareQueryError("Cannot refresh credentials - no auth transport available.")
                     # --- End transport usage ---

                 except Exception as refresh_error:
                     logger.error("Failed to refresh GHC credentials", error=str(refresh_error), exc_info=False)
                     raise GoogleHealthcareQueryError("Failed to refresh GHC credentials.") from refresh_error

            if not creds.valid:
                 logger.error("Could not obtain valid GHC credentials after refresh attempt.")
                 raise GoogleHealthcareQueryError("Could not obtain valid GHC credentials.")

            logger.info("GHC credentials obtained and valid", project_id=(project_id or 'Default'))
            _credentials = creds
            return _credentials

        # --- Catch specific exception IF exceptions module was loaded ---
        except Exception as e:
             is_default_creds_error = False
             # Check google.auth.exceptions exists before using it
             if google.auth.exceptions and isinstance(e, google.auth.exceptions.DefaultCredentialsError):
                 is_default_creds_error = True
                 logger.error("GHC: Failed to find Google Cloud default credentials.", error=str(e), exc_info=False)
                 raise GoogleHealthcareQueryError("Could not find Google Cloud credentials.") from e
             if not is_default_creds_error:
                  logger.error("GHC: Failed to initialize Google Cloud credentials", error=str(e), exc_info=True)
                  raise GoogleHealthcareQueryError(f"GHC credential initialization failed: {e}") from e
        # --- End specific exception catch ---


async def _get_auth_token() -> str:
    """Gets a valid OAuth2 token."""
    creds = await get_ghc_credentials()
    if not creds:
         raise GoogleHealthcareQueryError("Credentials object is None after get_ghc_credentials.")
    if not creds.token:
        logger.warning("Credentials valid but token is None, attempting final check/access.")
        # If token is still None after refresh, something is wrong.
        raise GoogleHealthcareQueryError("Failed to obtain valid token from credentials object (token is None).")
    return creds.token

# --- Async HTTP Client (keep as is) ---
_async_http_client = None
_client_lock = asyncio.Lock()

async def get_http_client() -> httpx.AsyncClient:
    """Provides a shared httpx.AsyncClient instance."""
    global _async_http_client
    if _async_http_client is None:
        async with _client_lock:
            if _async_http_client is None:
                logger.info("Initializing shared httpx.AsyncClient for GHC service.")
                timeout = httpx.Timeout(30.0, connect=10.0)
                _async_http_client = httpx.AsyncClient(timeout=timeout, http2=True)
    return _async_http_client

# --- QIDO Search Functions (keep as is, using httpx) ---

async def search_for_studies(
    gcp_project_id: str,
    gcp_location: str,
    gcp_dataset_id: str,
    gcp_dicom_store_id: str,
    query_params: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    offset: int = 0,
    fuzzymatching: bool = False,
    fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Queries Google Healthcare studies using QIDO-RS via httpx."""
    base_api_url = "https://healthcare.googleapis.com/v1"
    dicomweb_path = (
        f"{base_api_url}/projects/{gcp_project_id}/locations/{gcp_location}"
        f"/datasets/{gcp_dataset_id}/dicomStores/{gcp_dicom_store_id}/dicomWeb/studies"
    )

    params: Dict[str, Any] = { "limit": str(limit), "offset": str(offset) }
    if fuzzymatching: params["fuzzymatching"] = "true"
    if fields: params["includefield"] = fields
    if query_params: params.update({k: str(v) for k, v in query_params.items()})

    log = logger.bind(ghc_dicomweb_path=dicomweb_path, qido_params=params)
    log.info("Executing GHC searchStudies via httpx")
    client = await get_http_client()
    token = await _get_auth_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/dicom+json, application/json",
    }

    try:
        response = await client.get(dicomweb_path, headers=headers, params=params)
        response.raise_for_status()

        if response.status_code == 204:
            log.info("GHC searchStudies returned 204 No Content.")
            return []

        results = response.json()
        if not isinstance(results, list):
             log.warning("GHC searchStudies response was not a JSON list", response_type=type(results).__name__)
             return []
        log.info("GHC searchStudies completed", result_count=len(results))
        return results

    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        log.error(
            f"HTTP Error during GHC searchStudies: {e.response.status_code}",
            status_code=e.response.status_code,
            error_details=error_body[:500],
            request_url=str(e.request.url)
        )
        raise GoogleHealthcareQueryError(f"HTTP {e.response.status_code} for {e.request.url}: {error_body[:200]}") from e
    except httpx.RequestError as e:
        log.error(f"Network Error during GHC searchStudies: {e}", request_url=str(e.request.url))
        raise GoogleHealthcareQueryError(f"Network error contacting {e.request.url}: {e}") from e
    except Exception as e:
        log.error(f"Unexpected error during GHC searchStudies: {e}", exc_info=True)
        raise GoogleHealthcareQueryError(f"Unexpected error during searchStudies: {e}") from e

async def search_for_series(
    gcp_project_id: str,
    gcp_location: str,
    gcp_dataset_id: str,
    gcp_dicom_store_id: str,
    study_instance_uid: Optional[str] = None,
    query_params: Optional[Dict[str, Any]] = None,
    limit: int = 1000,
    offset: int = 0,
    fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Queries Google Healthcare series using QIDO-RS via httpx."""
    base_api_url = "https://healthcare.googleapis.com/v1"
    base_dicomweb_path = (
        f"{base_api_url}/projects/{gcp_project_id}/locations/{gcp_location}"
        f"/datasets/{gcp_dataset_id}/dicomStores/{gcp_dicom_store_id}/dicomWeb"
    )

    if study_instance_uid:
        safe_study_uid = quote(study_instance_uid)
        dicomweb_path = f"{base_dicomweb_path}/studies/{safe_study_uid}/series"
    else:
        dicomweb_path = f"{base_dicomweb_path}/series"

    params: Dict[str, Any] = { "limit": str(limit), "offset": str(offset) }
    if fields: params["includefield"] = fields
    if query_params: params.update({k: str(v) for k, v in query_params.items()})

    log = logger.bind(ghc_dicomweb_path=dicomweb_path, qido_params=params)
    log.info("Executing GHC searchForSeries via httpx")
    client = await get_http_client()
    token = await _get_auth_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/dicom+json, application/json"}

    try:
        response = await client.get(dicomweb_path, headers=headers, params=params)
        response.raise_for_status()
        if response.status_code == 204: return []
        results = response.json()
        if not isinstance(results, list):
             log.warning("GHC searchForSeries response was not a JSON list", response_type=type(results).__name__)
             return []
        log.info("GHC searchForSeries completed", result_count=len(results))
        return results
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        log.error(f"HTTP Error during GHC searchForSeries: {e.response.status_code}", status_code=e.response.status_code, error_details=error_body[:500], request_url=str(e.request.url))
        raise GoogleHealthcareQueryError(f"HTTP {e.response.status_code} for {e.request.url}: {error_body[:200]}") from e
    except httpx.RequestError as e:
        log.error(f"Network Error during GHC searchForSeries: {e}", request_url=str(e.request.url))
        raise GoogleHealthcareQueryError(f"Network error contacting {e.request.url}: {e}") from e
    except Exception as e:
        log.error(f"Unexpected error during GHC searchForSeries: {e}", exc_info=True)
        raise GoogleHealthcareQueryError(f"Unexpected error during searchForSeries: {e}") from e

async def search_for_instances(
    gcp_project_id: str,
    gcp_location: str,
    gcp_dataset_id: str,
    gcp_dicom_store_id: str,
    study_instance_uid: Optional[str] = None,
    series_instance_uid: Optional[str] = None,
    query_params: Optional[Dict[str, Any]] = None,
    limit: int = 1000,
    offset: int = 0,
    fields: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """Queries Google Healthcare instances using QIDO-RS via httpx."""
    base_api_url = "https://healthcare.googleapis.com/v1"
    base_dicomweb_path = (
        f"{base_api_url}/projects/{gcp_project_id}/locations/{gcp_location}"
        f"/datasets/{gcp_dataset_id}/dicomStores/{gcp_dicom_store_id}/dicomWeb"
    )

    path_part = ""
    if study_instance_uid and series_instance_uid:
        safe_study_uid = quote(study_instance_uid)
        safe_series_uid = quote(series_instance_uid)
        path_part = f"/studies/{safe_study_uid}/series/{safe_series_uid}/instances"
    elif study_instance_uid:
        safe_study_uid = quote(study_instance_uid)
        path_part = f"/studies/{safe_study_uid}/instances"
    else:
        path_part = "/instances"
    dicomweb_path = f"{base_dicomweb_path}{path_part}"

    params: Dict[str, Any] = { "limit": str(limit), "offset": str(offset) }
    if fields: params["includefield"] = fields
    if query_params: params.update({k: str(v) for k, v in query_params.items()})

    log = logger.bind(ghc_dicomweb_path=dicomweb_path, qido_params=params)
    log.info("Executing GHC searchForInstances via httpx")
    client = await get_http_client()
    token = await _get_auth_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/dicom+json, application/json"}

    try:
        response = await client.get(dicomweb_path, headers=headers, params=params)
        response.raise_for_status()
        if response.status_code == 204: return []
        results = response.json()
        if not isinstance(results, list):
             log.warning("GHC searchForInstances response was not a JSON list", response_type=type(results).__name__)
             return []
        log.info("GHC searchForInstances completed", result_count=len(results))
        return results
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        log.error(f"HTTP Error during GHC searchForInstances: {e.response.status_code}", status_code=e.response.status_code, error_details=error_body[:500], request_url=str(e.request.url))
        raise GoogleHealthcareQueryError(f"HTTP {e.response.status_code} for {e.request.url}: {error_body[:200]}") from e
    except httpx.RequestError as e:
        log.error(f"Network Error during GHC searchForInstances: {e}", request_url=str(e.request.url))
        raise GoogleHealthcareQueryError(f"Network error contacting {e.request.url}: {e}") from e
    except Exception as e:
        log.error(f"Unexpected error during GHC searchForInstances: {e}", exc_info=True)
        raise GoogleHealthcareQueryError(f"Unexpected error during searchForInstances: {e}") from e
