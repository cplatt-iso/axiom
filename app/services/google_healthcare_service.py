# backend/app/services/google_healthcare_service.py

import asyncio
import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode, quote
import json
import google.oauth2
import google.oauth2.service_account
import httpx

from app.core.config import settings # ADDED IMPORT

# --- Simplified Google Auth Imports ---
# Assume google.auth itself imports correctly based on previous logs
import google.auth
import google.auth.exceptions # Keep trying to import this for specific error handling

# Try importing the specific async transport request object
_async_transport_request = None
try:
    # --- MODIFIED IMPORT PATH ---
    from google.auth.aio.transport.aiohttp import Request as AsyncAuthRequest
    _async_transport_request = AsyncAuthRequest
    logging.getLogger(__name__).info("Successfully imported google.auth.aio.transport.aiohttp.Request for async refresh.")
except ImportError:
    logging.getLogger(__name__).warning("Could not import google.auth.aio.transport.aiohttp.Request. Async credential refresh may fallback to sync/blocking. Consider `pip install google-auth[aiohttp]`.")
    # Fallback needed if async refresh isn't possible
    # The sync transport path might also have changed if you rely on it,
    # but google.auth.transport.requests is a common one for sync.
    try:
        import google.auth.transport.requests # Keep this for sync fallback for now
    except ImportError:
         # This would be very bad - means sync refresh also impossible
         logging.getLogger(__name__).critical("Failed to import google.auth.transport.requests - basic auth will likely fail.")
         # Ensure it's None if you check for it later
         if 'google' in globals() and hasattr(google.auth, 'transport') and not hasattr(google.auth.transport, 'requests'):
            google.auth.transport.requests = None


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
        if settings.GOOGLE_APPLICATION_CREDENTIALS:
            # Existing code for handling service account key file...
            # If this block is truly empty for now, add pass
            logger.info(f"Attempting to use Service Account Key from path: {settings.GOOGLE_APPLICATION_CREDENTIALS}")
            try:
                # creds = google.oauth2.service_account.Credentials.from_service_account_file(
                creds = google.oauth2.service_account.Credentials.from_service_account_file(
                    settings.GOOGLE_APPLICATION_CREDENTIALS,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                _credentials = creds
                logger.info("Successfully loaded credentials from Service Account Key JSON file.")
            except FileNotFoundError:
                logger.error(f"Service Account Key JSON file not found at path: {settings.GOOGLE_APPLICATION_CREDENTIALS}")
                raise
            except Exception as e:
                logger.error(f"Failed to load credentials from Service Account Key JSON file: {e}", exc_info=True)
                raise
            # pass # If intentionally empty for now
        else:
            # Try Application Default Credentials
            if not _credentials or (_credentials.expired and hasattr(_credentials, 'refresh')):
                logger.info("Attempting to use Application Default Credentials (ADC).")
                try:
                    # Ensure google.auth.default is called correctly
                    creds, project_id = await asyncio.to_thread(
                        google.auth.default, scopes=["https://www.googleapis.com/auth/cloud-platform"]
                    )
                    _credentials = creds
                    # Example: if L89, L96, L106, L109 were in this block, they'd be adjusted:
                    # logger.error(f"Failed to obtain Application Default Credentials. Error: {adc_err}") # Example for L89
                    # logger.info(f"VERTEX_AI_INIT: Using inferred project ID from ADC: {project_id}") # Example for L96
                    # logger.error(f"VERTEX_AI_INIT: Unexpected error obtaining Application Default Credentials. Error: {e}", exc_info=True) # Example for L106
                    if project_id:
                        logger.info(f"ADC obtained. Inferred project ID: {project_id}")
                    else:
                        logger.info("ADC obtained. Project ID not inferred by google-auth library.")

                except google.auth.exceptions.DefaultCredentialsError as adc_err:
                    logger.error(f"Failed to obtain Application Default Credentials: {adc_err}", exc_info=True) # MODIFIED
                    raise
                except Exception as e:
                    logger.error(f"An unexpected error occurred getting ADC: {e}", exc_info=True) # MODIFIED
                    raise
        # ... (rest of the function)


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

    log_context = {"ghc_dicomweb_path": dicomweb_path, "qido_params": params}
    log = logger
    if hasattr(logger, 'bind'):
        try:
            log = logger.bind(**log_context) # Could add # type: ignore[attr-defined] here
        except Exception: # Fallback if bind fails
            pass # log remains the original logger
    
    log.info(f"Executing GHC searchStudies via httpx. Path: {log_context['ghc_dicomweb_path']}, Params: {log_context['qido_params']}")
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
             log.warning(f"GHC searchStudies response was not a JSON list. Response type: {type(results).__name__}") # MODIFIED
             return []
        log.info(f"GHC searchStudies completed. Result count: {len(results)}") # MODIFIED
        return results

    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        log.error(
            f"HTTP Error during GHC searchStudies: {e.response.status_code}. URL: {e.request.url}. Details: {error_body[:500]}",
            # For structlog, these would be structured. For standard, they are in the message.
            # status_code=e.response.status_code, error_details=error_body[:500], request_url=str(e.request.url)
        )
        raise GoogleHealthcareQueryError(f"HTTP {e.response.status_code} for {e.request.url}: {error_body[:200]}") from e
    except httpx.RequestError as e:
        log.error(
            f"Network Error during GHC searchStudies: {e}. URL: {e.request.url}",
            # request_url=str(e.request.url)
        )
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

    log_context = {"ghc_dicomweb_path": dicomweb_path, "qido_params": params}
    log = logger
    if hasattr(logger, 'bind'):
        try:
            log = logger.bind(**log_context) # Could add # type: ignore[attr-defined] here
        except Exception:
            pass

    log.info(f"Executing GHC searchForSeries via httpx. Path: {log_context['ghc_dicomweb_path']}, Params: {log_context['qido_params']}")
    client = await get_http_client()
    token = await _get_auth_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/dicom+json, application/json"}

    try:
        response = await client.get(dicomweb_path, headers=headers, params=params)
        response.raise_for_status()
        if response.status_code == 204: return []
        results = response.json()
        if not isinstance(results, list):
             log.warning(f"GHC searchForSeries response was not a JSON list. Type: {type(results).__name__}")
             return []
        log.info(f"GHC searchForSeries completed. Result count: {len(results)}")
        return results
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        log.error(
            f"HTTP Error during GHC searchForSeries: {e.response.status_code}. URL: {e.request.url}. Details: {error_body[:500]}",
            # status_code=e.response.status_code, error_details=error_body[:500], request_url=str(e.request.url)
        )
        raise GoogleHealthcareQueryError(f"HTTP {e.response.status_code} for {e.request.url}: {error_body[:200]}") from e
    except httpx.RequestError as e:
        log.error(
            f"Network Error during GHC searchForSeries: {e}. URL: {e.request.url}",
            # request_url=str(e.request.url)
        )
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
    if query_params: params.update({k: str(v) for k, v in query_params.items()}) # MODIFIED: Completed dict comprehension

    log_context = {"ghc_dicomweb_path": dicomweb_path, "qido_params": params}
    log = logger
    if hasattr(logger, 'bind'):
        try:
            log = logger.bind(**log_context) # Could add # type: ignore[attr-defined] here
        except Exception:
            pass

    log.info(f"Executing GHC searchForInstances via httpx. Path: {log_context['ghc_dicomweb_path']}, Params: {log_context['qido_params']}")
    client = await get_http_client()
    token = await _get_auth_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/dicom+json, application/json"}

    try:
        response = await client.get(dicomweb_path, headers=headers, params=params)
        response.raise_for_status()
        if response.status_code == 204: return []
        results = response.json()
        if not isinstance(results, list):
             log.warning(f"GHC searchForInstances response was not a JSON list. Type: {type(results).__name__}")
             return []
        log.info(f"GHC searchForInstances completed. Result count: {len(results)}")
        return results
    except httpx.HTTPStatusError as e:
        error_body = e.response.text
        log.error(
            f"HTTP Error during GHC searchForInstances: {e.response.status_code}. URL: {e.request.url}. Details: {error_body[:500]}",
            # status_code=e.response.status_code, error_details=error_body[:500], request_url=str(e.request.url)
        )
        raise GoogleHealthcareQueryError(f"HTTP {e.response.status_code} for {e.request.url}: {error_body[:200]}") from e
    except httpx.RequestError as e:
        log.error(
            f"Network Error during GHC searchForInstances: {e}. URL: {e.request.url}",
            # request_url=str(e.request.url)
        )
        raise GoogleHealthcareQueryError(f"Network error contacting {e.request.url}: {e}") from e
    except Exception as e:
        log.error(f"Unexpected error during GHC searchForInstances: {e}", exc_info=True)
        raise GoogleHealthcareQueryError(f"Unexpected error during searchForInstances: {e}") from e
