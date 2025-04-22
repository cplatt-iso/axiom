# app/services/dicomweb_client.py

import logging
import json
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urljoin, urlencode
from datetime import datetime, timedelta

import requests
from requests.auth import HTTPBasicAuth

# Use the actual DB model for type hinting
from app.db.models import DicomWebSourceState

logger = logging.getLogger(__name__)

class DicomWebClientError(Exception):
    """Custom exception for DICOMweb client errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, url: Optional[str] = None):
        self.status_code = status_code
        self.url = url
        super().__init__(f"{message} (URL: {url or 'N/A'}, Status: {status_code or 'N/A'})")

# --- Auth Helper (_get_auth) - remains the same ---
def _get_auth(config: DicomWebSourceState) -> Optional[requests.auth.AuthBase]:
    """Helper to get requests Auth object based on DicomWebSourceState."""
    if config.auth_type == 'basic':
        if isinstance(config.auth_config, dict) and 'username' in config.auth_config and 'password' in config.auth_config:
            username = config.auth_config.get('username')
            password = config.auth_config.get('password')
            if isinstance(username, str) and isinstance(password, str):
                 return HTTPBasicAuth(username, password)
            else:
                 logger.warning(f"Basic auth configured for '{config.source_name}' but username/password in auth_config are not strings.")
                 return None
        else:
            logger.warning(f"Basic auth configured for '{config.source_name}' but auth_config is missing, not a dict, or lacks keys.")
            return None
    elif config.auth_type in ['bearer', 'apikey', 'none']:
        return None
    else:
        logger.warning(f"Unsupported auth type configured for '{config.source_name}': {config.auth_type}")
        return None

# --- Headers Helper (_get_headers) - remains the same (with apikey) ---
def _get_headers(config: DicomWebSourceState) -> Dict[str, str]:
    """Builds request headers including auth and custom headers based on DicomWebSourceState."""
    headers = {"Accept": "application/dicom+json"}

    if config.auth_type == 'bearer':
        if isinstance(config.auth_config, dict) and 'token' in config.auth_config:
            token = config.auth_config.get('token')
            if isinstance(token, str) and token:
                headers['Authorization'] = f"Bearer {token}"
            else:
                 logger.warning(f"Bearer auth configured for '{config.source_name}' but token in auth_config is missing or not a string.")
        else:
             logger.warning(f"Bearer auth configured for '{config.source_name}' but auth_config is missing or not a dict.")
    elif config.auth_type == 'apikey':
        if isinstance(config.auth_config, dict) and 'header_name' in config.auth_config and 'key' in config.auth_config:
            header_name = config.auth_config.get('header_name')
            key_value = config.auth_config.get('key')
            if isinstance(header_name, str) and header_name.strip() and isinstance(key_value, str) and key_value:
                 headers[header_name.strip()] = key_value
                 logger.debug(f"Using API Key Header '{header_name.strip()}' for source '{config.source_name}'")
            else:
                 logger.warning(f"API Key auth configured for '{config.source_name}' but header_name or key in auth_config are invalid/empty.")
        else:
             logger.warning(f"API Key auth configured for '{config.source_name}' but auth_config is missing, not a dict, or lacks 'header_name'/'key'.")

    return headers

# --- Dynamic Query Param Resolver (_resolve_dynamic_query_params) - remains the same ---
def _resolve_dynamic_query_params(params: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Resolves special values like '-N' for dates. Returns string dictionary."""
    if not params:
        return {}

    resolved_params: Dict[str, str] = {}
    today = datetime.utcnow().date()
    for key, value in params.items():
         str_key = str(key)
         str_value = str(value) if value is not None else ""

         if str_key.lower() == "studydate" and str_value:
             try:
                 if str_value.startswith('-') and str_value[1:].isdigit():
                     days_ago = int(str_value)
                     target_date = today + timedelta(days=days_ago)
                     # Changed date range format slightly for clarity: YYYYMMDD- (from N days ago up to infinity)
                     resolved_params[str_key] = target_date.strftime('%Y%m%d') + "-"
                     logger.debug(f"Resolved dynamic date '{str_value}' to '{resolved_params[str_key]}'")
                 elif str_value.endswith('-') and (len(str_value) == 9 and str_value[:-1].isdigit()): # YYYYMMDD-
                     resolved_params[str_key] = str_value # Keep date range start
                 elif '-' in str_value and len(str_value.split('-')) == 2: # YYYYMMDD-YYYYMMDD
                     start_date, end_date = str_value.split('-')
                     if (len(start_date) == 8 and start_date.isdigit() and
                         len(end_date) == 8 and end_date.isdigit()):
                         resolved_params[str_key] = str_value
                     else:
                          logger.warning(f"Unsupported date range format for '{str_key}': '{str_value}'. Skipping.")
                 elif len(str_value) == 8 and str_value.isdigit(): # YYYYMMDD
                      resolved_params[str_key] = str_value
                 else:
                      logger.warning(f"Unsupported dynamic date format for '{str_key}': '{str_value}'. Skipping.")
             except (ValueError, TypeError) as e:
                  logger.warning(f"Invalid dynamic date value for '{str_key}': '{str_value}'. Error: {e}. Skipping.")
         elif str_value: # Add only if value is not empty
             resolved_params[str_key] = str_value
    return resolved_params


def query_qido(config: DicomWebSourceState, level: str = "STUDY", custom_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Performs a QIDO-RS query using DicomWebSourceState config.
    Prioritizes config.search_filters over custom_params for StudyDate.
    """
    if not config.base_url:
        raise ValueError(f"Base URL is not configured for source '{config.source_name}'")

    # URL Construction (remains the same)
    base_url_with_slash = config.base_url if config.base_url.endswith('/') else config.base_url + '/'
    service_base_url = urljoin(base_url_with_slash, config.qido_prefix or '')
    service_base_url_with_slash = service_base_url if service_base_url.endswith('/') else service_base_url + '/'
    level_map = {"study": "studies", "series": "series", "instance": "instances"}
    path_segment = level_map.get(level.lower())
    if not path_segment:
        raise ValueError(f"Unsupported QIDO query level: {level}")
    url = urljoin(service_base_url_with_slash, path_segment)

    # --- CORRECTED Parameter Merging Logic ---
    query_params: Dict[str, Any] = {}
    if isinstance(config.search_filters, dict):
        query_params = config.search_filters.copy()
        logger.debug(f"QIDO {level}: Started with config search_filters: {query_params}")
    else:
        logger.debug(f"QIDO {level}: No base search_filters found in config.")

    # Merge custom_params (from poller), giving precedence to config filters for StudyDate
    if custom_params:
        logger.debug(f"QIDO {level}: Merging with custom_params: {custom_params}")
        study_date_from_custom = custom_params.get("StudyDate")

        # Only use StudyDate from custom_params IF it's not already set by config.search_filters
        if study_date_from_custom and "StudyDate" not in query_params:
            query_params["StudyDate"] = study_date_from_custom
            logger.info(f"QIDO {level}: Using StudyDate '{study_date_from_custom}' from custom_params (not found in config filters).")
        elif study_date_from_custom and "StudyDate" in query_params:
             logger.info(f"QIDO {level}: Ignoring StudyDate '{study_date_from_custom}' from custom_params; using value from config filters: '{query_params['StudyDate']}'")

        # Merge other custom parameters (these will override config filters if keys match)
        for key, value in custom_params.items():
            if key != "StudyDate":
                query_params[key] = value

    # Resolve dynamic values AFTER merging
    resolved_params = _resolve_dynamic_query_params(query_params)
    # --- END CORRECTED Parameter Merging Logic ---

    # Set default limit if not provided in resolved params
    resolved_params.setdefault('limit', '1000')

    # --- REMOVED since=0 ---
    # if level.upper() == "STUDY" and not any(k in resolved_params for k in ["PatientID", "StudyInstanceUID"]):
    #     resolved_params.setdefault('since', '0')
    # --- END REMOVED ---

    auth = _get_auth(config)
    headers = _get_headers(config)

    # Prepare request for logging URL (remains the same)
    try:
        prepared_request = requests.Request('GET', url, params=resolved_params).prepare()
        final_url_for_request = prepared_request.url
        logger.info(f"Constructed QIDO URL: {final_url_for_request}")
        logger.debug(f"QIDO Query - Final Params: {resolved_params}, Auth: {auth is not None}, Headers: {list(headers.keys())}")
    except Exception as prep_e:
         logger.error(f"Error preparing QIDO request URL for {config.source_name}: {prep_e}")
         final_url_for_request = url # Fallback

    # Request execution and error handling (remains the same)
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
        if not ('application/dicom+json' in content_type or 'application/json' in content_type):
            logger.warning(f"Unexpected QIDO response Content-Type: {content_type} from {url}. Attempting to parse as JSON.")

        results = response.json()
        if not isinstance(results, list):
             logger.error(f"QIDO response from {config.source_name} was not a JSON list. Response text: {response.text[:500]}")
             raise DicomWebClientError("QIDO response was not a JSON list", url=final_url_for_request, status_code=response.status_code)

        logger.info(f"QIDO Query successful for {config.source_name} ({level}): Found {len(results)} results.")
        return results
    except requests.exceptions.Timeout as e:
        logger.error(f"QIDO Timeout for {config.source_name}: {e}")
        raise DicomWebClientError(f"Timeout connecting to QIDO endpoint: {e}", url=final_url_for_request) from e
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code if e.response is not None else None
        response_text = e.response.text[:500] if e.response is not None else "N/A"
        logger.error(f"QIDO Request failed for {config.source_name}: {e}, Status: {status_code}, Response: {response_text}")
        raise DicomWebClientError(f"QIDO request failed: {e}. Response: {response_text}", status_code=status_code, url=final_url_for_request) from e
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode QIDO JSON response from {config.source_name}: {e}. Response text: {response.text[:500]}")
        raise DicomWebClientError(f"Invalid JSON response from QIDO endpoint: {e}", url=final_url_for_request, status_code=response.status_code) from e
    except Exception as e:
        logger.error(f"Unexpected error during QIDO query for {config.source_name}: {e}", exc_info=True)
        raise DicomWebClientError(f"Unexpected QIDO error: {e}", url=final_url_for_request) from e

# retrieve_instance_metadata function remains the same
def retrieve_instance_metadata(config: DicomWebSourceState, study_uid: str, series_uid: str, instance_uid: str) -> Optional[Dict[str, Any]]:
    # ... (no changes needed here based on the problem description) ...
    if not config.base_url:
        raise ValueError(f"Base URL is not configured for source '{config.source_name}'")

    base_url_with_slash = config.base_url if config.base_url.endswith('/') else config.base_url + '/'
    service_base_url = urljoin(base_url_with_slash, config.wado_prefix or '')
    service_base_url_with_slash = service_base_url if service_base_url.endswith('/') else service_base_url + '/'

    instance_path = f"studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/metadata"
    url = urljoin(service_base_url_with_slash, instance_path)

    auth = _get_auth(config)
    headers = _get_headers(config)

    try:
        prepared_request = requests.Request('GET', url).prepare()
        final_url_for_request = prepared_request.url
        logger.info(f"Constructed WADO Metadata URL: {final_url_for_request}")
        logger.debug(f"WADO Metadata Request - URL Base: {url}, Auth: {auth is not None}, Headers: {list(headers.keys())}")
    except Exception as prep_e:
        logger.error(f"Error preparing WADO request URL for {instance_uid} at {config.source_name}: {prep_e}")
        final_url_for_request = url

    try:
        response = requests.get(url, headers=headers, auth=auth, timeout=30)
        if response.status_code == 404:
            logger.warning(f"Instance metadata not found (404) for {instance_uid} at {config.source_name}")
            return None
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '')
        if not ('application/dicom+json' in content_type or 'application/json' in content_type):
            logger.warning(f"Unexpected WADO metadata response Content-Type: {content_type} from {url}. Attempting to parse as JSON.")
        metadata_list = response.json()
        if isinstance(metadata_list, list) and len(metadata_list) > 0:
            if len(metadata_list) > 1:
                logger.warning(f"WADO metadata response for {instance_uid} at {config.source_name} contained {len(metadata_list)} items, expected 1. Using the first.")
            instance_data = metadata_list[0]
            if not isinstance(instance_data, dict):
                 logger.error(f"WADO metadata response item from {config.source_name} was not a JSON object: {instance_data}")
                 raise DicomWebClientError("WADO metadata response list item was not a JSON object", url=final_url_for_request, status_code=response.status_code)
            logger.debug(f"WADO metadata successfully retrieved for {instance_uid} from {config.source_name}")
            return instance_data
        elif isinstance(metadata_list, dict):
             logger.warning(f"WADO metadata response for {instance_uid} from {config.source_name} was a single object, not a list. Using it directly.")
             return metadata_list
        else:
             logger.error(f"WADO metadata response from {config.source_name} was not a list or object. Response text: {response.text[:500]}")
             raise DicomWebClientError(f"WADO metadata response was not a list or object ({type(metadata_list)})", url=final_url_for_request, status_code=response.status_code)
    except requests.exceptions.Timeout as e:
        logger.error(f"WADO Metadata Timeout for {instance_uid} at {config.source_name}: {e}")
        raise DicomWebClientError(f"Timeout connecting to WADO endpoint: {e}", url=final_url_for_request) from e
    except requests.exceptions.RequestException as e:
        if e.response is not None and e.response.status_code == 404:
            return None
        status_code = e.response.status_code if e.response is not None else None
        response_text = e.response.text[:500] if e.response is not None else "N/A"
        logger.error(f"WADO Metadata Request failed for {instance_uid} at {config.source_name}: {e}, Status: {status_code}, Response: {response_text}")
        raise DicomWebClientError(f"WADO metadata request failed: {e}. Response: {response_text}", status_code=status_code, url=final_url_for_request) from e
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode WADO metadata JSON response for {instance_uid} from {config.source_name}: {e}. Response text: {response.text[:500]}")
        raise DicomWebClientError(f"Invalid JSON response from WADO endpoint: {e}", url=final_url_for_request, status_code=response.status_code) from e
    except Exception as e:
        logger.error(f"Unexpected error during WADO metadata retrieval for {instance_uid} at {config.source_name}: {e}", exc_info=True)
        raise DicomWebClientError(f"Unexpected WADO metadata error: {e}", url=final_url_for_request) from e
