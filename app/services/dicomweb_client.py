# app/services/dicomweb_client.py

import logging
import json # <--- Added missing import
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urljoin, urlencode
from datetime import datetime, timedelta #<-- Removed unused timezone import

import requests
from requests.auth import HTTPBasicAuth

# Import the config model for type hinting
from app.core.config import DicomWebSourceConfig, DicomWebSourceAuth

logger = logging.getLogger(__name__)

class DicomWebClientError(Exception):
    """Custom exception for DICOMweb client errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, url: Optional[str] = None):
        self.status_code = status_code
        self.url = url
        # Ensure URL and Status Code are included even if None for clarity
        super().__init__(f"{message} (URL: {url or 'N/A'}, Status: {status_code or 'N/A'})")

def _get_auth(auth_config: DicomWebSourceAuth) -> Optional[requests.auth.AuthBase]:
    """Helper to get requests Auth object based on config."""
    if auth_config.type == 'basic':
        if auth_config.username and auth_config.password is not None:
            return HTTPBasicAuth(auth_config.username, auth_config.password)
        else:
            logger.warning("Basic auth configured but username or password missing.")
            return None
    elif auth_config.type == 'bearer':
        if auth_config.token:
            return None # Auth handled by headers
        else:
            logger.warning("Bearer auth configured but token missing.")
            return None
    elif auth_config.type == 'apikey':
        if auth_config.header_name and auth_config.header_value:
             return None # Auth handled by headers
        else:
            logger.warning("API Key auth configured but header name or value missing.")
            return None
    elif auth_config.type == 'none':
        return None
    else:
        logger.warning(f"Unsupported auth type: {auth_config.type}")
        return None

def _get_headers(config: DicomWebSourceConfig) -> Dict[str, str]:
    """Builds request headers including auth and custom headers."""
    headers = {"Accept": "application/dicom+json"} # Default Accept header
    if config.request_headers:
        headers.update(config.request_headers)

    auth_config = config.auth
    if auth_config.type == 'bearer' and auth_config.token:
        headers['Authorization'] = f"Bearer {auth_config.token}"
    elif auth_config.type == 'apikey' and auth_config.header_name and auth_config.header_value:
        headers[auth_config.header_name] = auth_config.header_value

    return headers

def _resolve_dynamic_query_params(params: Dict[str, str]) -> Dict[str, str]:
    """Resolves special values like '-1' for dates."""
    resolved_params = {}
    # Use UTC now for date calculations to be consistent
    today = datetime.utcnow().date()
    for key, value in params.items():
        # Case-insensitive check for StudyDate parameter
        if key.lower() == "studydate" and isinstance(value, str):
            try:
                if value.startswith('-'):
                    days_ago = int(value)
                    target_date = today + timedelta(days=days_ago)
                    resolved_params[key] = target_date.strftime('%Y%m%d')
                elif '-' in value and len(value.split('-')) == 2:
                     resolved_params[key] = value # Keep date range
                elif len(value) == 8 and value.isdigit():
                     resolved_params[key] = value # Keep single date
                else:
                     logger.warning(f"Unsupported dynamic date format for '{key}': '{value}'. Skipping.")
            except ValueError:
                 logger.warning(f"Invalid dynamic date value for '{key}': '{value}'. Skipping.")
        else:
            resolved_params[key] = value # Keep other params as is
    return resolved_params

def query_qido(config: DicomWebSourceConfig, level: str = "STUDY", custom_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Performs a QIDO-RS query.
    """
    # --- Ensure base_url ends with a slash ---
    base_url = str(config.qido_url_prefix)
    if not base_url.endswith('/'):
        base_url += '/'
    # --- End Ensure slash ---

    # --- Path logic (No leading slash) ---
    level_lower = level.lower()
    if level_lower == 'study':
        path_segment = "studies"
    elif level_lower == 'series':
        path_segment = "series"
    elif level_lower == 'instance':
        path_segment = "instances"
    else:
        raise ValueError(f"Unsupported QIDO query level: {level}")
    # --- End Path logic ---

    # --- Construct URL using urljoin ---
    url = urljoin(base_url, path_segment)
    # --- End URL construction ---

    # --- CORRECTED Parameter Logic ---
    # Decide which parameters to use as the base
    if custom_params and any(k in custom_params for k in ["StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID", "PatientID"]):
        # If custom params specify a UID or PatientID, IGNORE config defaults and use only custom params
        logger.debug(f"QIDO {level}: Using specific custom_params, ignoring config defaults: {custom_params}")
        base_params = custom_params.copy() # Use only the specific filters provided
    else:
        # Otherwise (e.g., broad study query with date), start with config defaults
        logger.debug(f"QIDO {level}: Using config defaults as base: {config.qido_query_params}")
        base_params = config.qido_query_params.copy()
        # And add any non-UID custom params (like StudyDate range from poller)
        if custom_params:
            logger.debug(f"QIDO {level}: Merging non-UID custom_params: {custom_params}")
            base_params.update(custom_params)

    # Resolve dynamic values (like dates) in the final set of parameters
    resolved_params = _resolve_dynamic_query_params(base_params)
    # --- END CORRECTED Parameter Logic ---

    # Add limit/since AFTER resolving base params
    resolved_params.setdefault('limit', '1000')
    # Add since=0 specifically for broad study queries for Orthanc compatibility
    if level.upper() == "STUDY" and not any(k in resolved_params for k in ["PatientID", "StudyInstanceUID"]):
        resolved_params.setdefault('since', '0')

    auth = _get_auth(config.auth)
    headers = _get_headers(config)

    # Log the final URL that will be requested
    final_url_for_request = requests.Request('GET', url, params=resolved_params).prepare().url
    logger.info(f"Constructed QIDO URL: {final_url_for_request}")
    logger.debug(f"QIDO Query - URL Base: {url}, Final Params: {resolved_params}, Auth: {auth is not None}, Headers: {list(headers.keys())}")

    try:
        response = requests.get(
            url,
            params=resolved_params,
            headers=headers,
            auth=auth,
            timeout=60
        )
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '')
        # Allow application/json as well as application/dicom+json
        if not ('application/dicom+json' in content_type or 'application/json' in content_type):
            logger.warning(f"Unexpected QIDO response Content-Type: {content_type} from {url}. Attempting to parse as JSON.")

        results = response.json()
        if not isinstance(results, list):
             logger.error(f"QIDO response was not a JSON list. Response text: {response.text[:500]}")
             raise DicomWebClientError("QIDO response was not a JSON list", url=final_url_for_request, status_code=response.status_code)

        logger.info(f"QIDO Query successful for {config.name} ({level}): Found {len(results)} results.")
        return results

    except requests.exceptions.Timeout as e:
        logger.error(f"QIDO Timeout for {config.name}: {e}")
        raise DicomWebClientError(f"Timeout connecting to QIDO endpoint: {e}", url=final_url_for_request) from e
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code if e.response is not None else None
        logger.error(f"QIDO Request failed for {config.name}: {e}, Status: {status_code}")
        raise DicomWebClientError(f"QIDO request failed: {e}", status_code=status_code, url=final_url_for_request) from e
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode QIDO JSON response from {config.name}: {e}. Response text: {response.text[:500]}")
        raise DicomWebClientError(f"Invalid JSON response from QIDO endpoint: {e}", url=final_url_for_request, status_code=response.status_code) from e
    except Exception as e:
        logger.error(f"Unexpected error during QIDO query for {config.name}: {e}", exc_info=True)
        raise DicomWebClientError(f"Unexpected QIDO error: {e}", url=final_url_for_request) from e

def retrieve_instance_metadata(config: DicomWebSourceConfig, study_uid: str, series_uid: str, instance_uid: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves DICOM instance metadata using WADO-RS.
    """
    # --- Ensure base_url ends with a slash ---
    base_url = str(config.wado_url_prefix)
    if not base_url.endswith('/'):
        base_url += '/'
    # --- End Ensure slash ---

    # Path construction WITHOUT leading slash
    path_segment = f"studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/metadata"
    url = urljoin(base_url, path_segment)

    auth = _get_auth(config.auth)
    headers = _get_headers(config)

    # Log the final URL
    final_url_for_request = requests.Request('GET', url).prepare().url
    logger.info(f"Constructed WADO Metadata URL: {final_url_for_request}")
    logger.debug(f"WADO Metadata Request - URL Base: {url}, Auth: {auth is not None}, Headers: {list(headers.keys())}")

    try:
        response = requests.get(url, headers=headers, auth=auth, timeout=30)

        if response.status_code == 404:
            logger.warning(f"Instance metadata not found (404) for {instance_uid} at {config.name}")
            return None

        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '')
        if not ('application/dicom+json' in content_type or 'application/json' in content_type):
            logger.warning(f"Unexpected WADO metadata response Content-Type: {content_type} from {url}. Attempting to parse as JSON.")

        # WADO metadata response is typically a list containing ONE dataset
        metadata_list = response.json()
        if isinstance(metadata_list, list) and len(metadata_list) > 0:
            if len(metadata_list) > 1:
                logger.warning(f"WADO metadata response for {instance_uid} contained {len(metadata_list)} items, expected 1. Using the first.")
            instance_data = metadata_list[0]
            if not isinstance(instance_data, dict):
                 logger.error(f"WADO metadata response list item was not a JSON object: {instance_data}")
                 raise DicomWebClientError("WADO metadata response list item was not a JSON object", url=final_url_for_request, status_code=response.status_code)
            logger.debug(f"WADO metadata successfully retrieved for {instance_uid} from {config.name}")
            return instance_data
        else:
            # Handle case where response might be single object instead of list
            if isinstance(metadata_list, dict):
                 logger.warning(f"WADO metadata response for {instance_uid} was a single object, not a list. Using it directly.")
                 return metadata_list
            else:
                 logger.error(f"WADO metadata response was not a list or object. Response text: {response.text[:500]}")
                 raise DicomWebClientError(f"WADO metadata response was not a list or object ({type(metadata_list)})", url=final_url_for_request, status_code=response.status_code)

    except requests.exceptions.Timeout as e:
        logger.error(f"WADO Metadata Timeout for {instance_uid} at {config.name}: {e}")
        raise DicomWebClientError(f"Timeout connecting to WADO endpoint: {e}", url=final_url_for_request) from e
    except requests.exceptions.RequestException as e:
        if e.response is not None and e.response.status_code == 404:
            return None # Already handled above, just return None cleanly
        status_code = e.response.status_code if e.response is not None else None
        logger.error(f"WADO Metadata Request failed for {instance_uid} at {config.name}: {e}, Status: {status_code}")
        raise DicomWebClientError(f"WADO metadata request failed: {e}", status_code=status_code, url=final_url_for_request) from e
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode WADO metadata JSON response for {instance_uid} from {config.name}: {e}. Response text: {response.text[:500]}")
        raise DicomWebClientError(f"Invalid JSON response from WADO endpoint: {e}", url=final_url_for_request, status_code=response.status_code) from e
    except Exception as e:
        logger.error(f"Unexpected error during WADO metadata retrieval for {instance_uid} at {config.name}: {e}", exc_info=True)
        raise DicomWebClientError(f"Unexpected WADO metadata error: {e}", url=final_url_for_request) from e
