# app/services/dicomweb_client.py

import logging
import structlog
import json
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urljoin, urlencode
from datetime import datetime, timedelta, date, timezone
import re

import requests
from requests.auth import HTTPBasicAuth, AuthBase # MODIFIED: Added AuthBase here

# Need access to the DB model for type hinting
from app.db.models import DicomWebSourceState
from app.core.config import settings

logger = structlog.get_logger(__name__)

class DicomWebClientError(Exception):
    """Custom exception for DICOMweb client errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, url: Optional[str] = None):
        self.status_code = status_code
        self.url = url
        super().__init__(f"{message} (URL: {url or 'N/A'}, Status: {status_code or 'N/A'})")

# --- Auth Helper (_get_auth) ---
def _get_auth(config: DicomWebSourceState) -> Optional[AuthBase]: # MODIFIED: Changed to AuthBase
    """Helper to get requests Auth object based on DicomWebSourceState."""
    # Assuming config.auth_type is a string attribute from the model instance
    auth_type_val = getattr(config, 'auth_type', None) # Use getattr
    auth_config_val = getattr(config, 'auth_config', None) # Use getattr
    source_name_val = getattr(config, 'source_name', 'UnknownSource') # Use getattr

    if auth_type_val == 'basic': 
        if isinstance(auth_config_val, dict) and 'username' in auth_config_val and 'password' in auth_config_val:
            username = auth_config_val.get('username')
            password = auth_config_val.get('password')
            if isinstance(username, str) and isinstance(password, str):
                 return HTTPBasicAuth(username, password)
            else:
                 # Keep warnings
                 logger.warning(f"Basic auth configured for '{source_name_val}' but username/password in auth_config are not strings.")
                 return None
        else:
            logger.warning(f"Basic auth configured for '{source_name_val}' but auth_config is missing, not a dict, or lacks keys.")
            return None
    elif auth_type_val in ['bearer', 'apikey', 'none']:
        # Bearer/APIKey handled in headers
        return None
    else:
        logger.warning(f"Unsupported auth type configured for '{source_name_val}': {auth_type_val}")
        return None

# --- Headers Helper (_get_headers) ---
def _get_headers(config: DicomWebSourceState) -> Dict[str, str]:
    """Builds request headers including auth and custom headers based on DicomWebSourceState."""
    headers = {"Accept": "application/dicom+json"} # Default accept header

    auth_type_val = getattr(config, 'auth_type', None) # Use getattr
    auth_config_val = getattr(config, 'auth_config', None) # Use getattr
    source_name_val = getattr(config, 'source_name', 'UnknownSource') # Use getattr

    # Assuming config.auth_type is a string attribute
    if auth_type_val == 'bearer':
        if isinstance(auth_config_val, dict) and 'token' in auth_config_val:
            token = auth_config_val.get('token')
            if isinstance(token, str) and token:
                headers['Authorization'] = f"Bearer {token}"
            else:
                 logger.warning(f"Bearer auth configured for '{source_name_val}' but token in auth_config is missing or not a string.")
        else:
             logger.warning(f"Bearer auth configured for '{source_name_val}' but auth_config is missing or not a dict.")
    # Assuming config.auth_type is a string attribute
    elif auth_type_val == 'apikey': 
        if isinstance(auth_config_val, dict) and 'header_name' in auth_config_val and 'key' in auth_config_val:
            header_name = auth_config_val.get('header_name')
            key_value = auth_config_val.get('key')
            if isinstance(header_name, str) and header_name.strip() and isinstance(key_value, str) and key_value:
                 headers[header_name.strip()] = key_value
                 # Optional: Keep debug log if useful
                 # logger.debug(f"Using API Key Header '{header_name.strip()}' for source '{source_name_val}'")
            else:
                 logger.warning(f"API Key auth configured for '{source_name_val}' but header_name or key in auth_config are invalid/empty.")
        else:
             logger.warning(f"API Key auth configured for '{source_name_val}' but auth_config is missing, not a dict, or lacks 'header_name'/'key'.")
    
    # Add static headers from DicomWebSourceState if they exist
    static_hdrs_value = getattr(config, 'static_headers', None) # Use getattr
    if isinstance(static_hdrs_value, dict):
        headers.update(static_hdrs_value)
    elif static_hdrs_value is not None: # Check if it's not None before warning
        logger.warning(f"static_headers for source {source_name_val} is not a dict, ignoring.")

    return headers

# --- Dynamic Query Param Resolver (_resolve_dynamic_query_params) ---
def _resolve_dynamic_query_params(params: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """
    Resolves special values like '-N'd, 'TODAY', 'YESTERDAY' for dates.
    Returns string dictionary suitable for requests params.
    """
    if not params:
        return {}

    resolved_params: Dict[str, str] = {}
    today = date.today()

    for key, value in params.items():
         str_key = str(key)
         str_value = str(value).strip() if value is not None else ""

         if str_key.lower() == "studydate" and str_value:
             val_upper = str_value.upper()
             resolved_date_value: Optional[str] = None

             if val_upper == "TODAY":
                 resolved_date_value = today.strftime('%Y%m%d')
             elif val_upper == "YESTERDAY":
                 yesterday = today - timedelta(days=1)
                 resolved_date_value = yesterday.strftime('%Y%m%d')
             else:
                 match_days_ago = re.match(r"^-(\d+)D$", val_upper)
                 if match_days_ago:
                     try:
                         days = int(match_days_ago.group(1))
                         if days >= 0:
                             start_date = today - timedelta(days=days)
                             resolved_date_value = f"{start_date.strftime('%Y%m%d')}-"
                         else:
                             logger.warning(f"Invalid negative number of days in date filter: {str_value}")
                     except (ValueError, OverflowError):
                         logger.warning(f"Invalid number of days in date filter: {str_value}")
                 elif re.match(r"^\d{8}$", val_upper): resolved_date_value = val_upper
                 elif re.match(r"^\d{8}-$", val_upper): resolved_date_value = val_upper
                 elif re.match(r"^\d{8}-\d{8}$", val_upper): resolved_date_value = val_upper
                 else:
                      logger.warning(f"Unsupported dynamic date format for '{str_key}': '{str_value}'. Skipping.")

             if resolved_date_value:
                 resolved_params[str_key] = resolved_date_value
                 # Optional: Keep debug log if useful
                 # logger.debug(f"Resolved dynamic date '{str_value}' to '{resolved_date_value}' for key '{str_key}'")
             # If unresolved, skip adding it

         elif str_value: # Add other non-empty params
             resolved_params[str_key] = str_value

    return resolved_params


# --- MODIFIED query_qido ---
def query_qido(
    config: DicomWebSourceState,
    level: str = "STUDY",
    custom_params: Optional[Dict[str, Any]] = None,
    prioritize_custom_params: bool = False # <-- ADDED FLAG
) -> List[Dict[str, Any]]:
    """
    Performs a QIDO-RS query using DicomWebSourceState config.

    Args:
        config: The source configuration.
        level: The query level ('STUDY', 'SERIES', 'INSTANCE').
        custom_params: Additional parameters from the caller (e.g., Data Browser).
        prioritize_custom_params: If True, parameters in custom_params will
                                 overwrite those from config.search_filters.
                                 Set to True for Data Browser calls.
    """
    base_url_val = getattr(config, 'base_url', None) # Use getattr
    source_name_val = getattr(config, 'source_name', 'UnknownSource') # Use getattr

    if not (isinstance(base_url_val, str) and base_url_val.strip()):
        raise ValueError(f"Base URL is not configured or is not a string for source '{source_name_val}'")

    # URL Construction
    base_url_with_slash = base_url_val if base_url_val.endswith('/') else base_url_val + '/'
    
    qido_prefix_val = getattr(config, 'qido_prefix', '') # Use getattr, default to empty string
    qido_prefix_str = qido_prefix_val if isinstance(qido_prefix_val, str) else ''

    service_base_url = urljoin(base_url_with_slash, qido_prefix_str) # Ensure qido_prefix_str is string
    service_base_url_with_slash = service_base_url if service_base_url.endswith('/') else service_base_url + '/'
    level_map = {"study": "studies", "series": "series", "instance": "instances"}
    path_segment = level_map.get(level.lower())
    if not path_segment:
        raise ValueError(f"Unsupported QIDO query level: {level}")
    url = urljoin(service_base_url_with_slash, path_segment)

    # --- CORRECTED Parameter Merging Logic ---
    query_params: Dict[str, Any] = {}
    
    # Ensure config.search_filters is a dict before copying
    config_filters_val = getattr(config, 'search_filters', None) # Use getattr
    if isinstance(config_filters_val, dict):
        query_params = config_filters_val.copy()
        logger.debug(f"QIDO {level}: Started with config search_filters: {query_params}")
    else:
        logger.debug(f"QIDO {level}: No base search_filters found in config or not a dict.")

    if custom_params:
        logger.debug(f"QIDO {level}: Merging with custom_params: {custom_params}, Priority: {'Custom' if prioritize_custom_params else 'Config'}")
        for key, value in custom_params.items():
            if key in query_params and prioritize_custom_params:
                # Overwrite config value if prioritizing custom params
                logger.info(f"QIDO {level}: Overwriting config filter '{key}={query_params[key]}' with custom param '{key}={value}'")
                query_params[key] = value
            elif key not in query_params:
                # Add custom param if not present in config
                query_params[key] = value
            # else: implicit - key exists in config, but not prioritizing custom -> keep config value
            # Optional: Add log for ignored custom param if needed
            # elif key in query_params and not prioritize_custom_params:
            #      logger.debug(f"QIDO {level}: Ignoring custom param '{key}={value}', using config value.")

    # Resolve dynamic values AFTER merging
    resolved_params = _resolve_dynamic_query_params(query_params)

    # Set default limit if not provided
    resolved_params.setdefault('limit', str(settings.DICOMWEB_POLLER_QIDO_LIMIT))

    # Auth and Headers (No changes needed here)
    auth = _get_auth(config)
    headers = _get_headers(config)

    # Prepare request for logging URL (No changes needed here)
    try:
        prepared_request = requests.Request('GET', url, params=resolved_params).prepare()
        final_url_for_request = prepared_request.url
        logger.info(f"Constructed QIDO URL: {final_url_for_request}")
        logger.debug(f"QIDO Query - Final Params: {resolved_params}, Auth: {auth is not None}, Headers: {list(headers.keys())}")
    except Exception as prep_e:
         logger.error(f"Error preparing QIDO request URL for {source_name_val}: {prep_e}")
         final_url_for_request = url

    # Request execution and error handling
    response: Optional[requests.Response] = None # Initialize response
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
        logger.error(f"QIDO Request failed for {source_name_val}: {e}, Status: {status_code}, Response: {response_text}")
        raise DicomWebClientError(f"QIDO request failed: {e}. Response: {response_text}", status_code=status_code, url=final_url_for_request) from e
    except json.JSONDecodeError as e:
        # Safe access to response attributes
        resp_status_code = response.status_code if response is not None else None # MODIFIED
        resp_text = response.text[:500] if response is not None else "No response text"
        logger.error(f"Failed to decode QIDO JSON response from {source_name_val}: {e}. Response text: {resp_text}")
        raise DicomWebClientError(f"Invalid JSON response from QIDO endpoint: {e}", url=final_url_for_request, status_code=resp_status_code) from e
    except Exception as e:
        # Safe access to response attributes
        resp_status_code = response.status_code if response is not None else None # MODIFIED
        resp_text = response.text[:500] if response is not None else "No response text"
        logger.error(f"Unexpected error during QIDO query for {source_name_val}: {e}. Response: {resp_text}", exc_info=True)
        raise DicomWebClientError(f"Unexpected QIDO error: {e}. Response: {resp_text}", url=final_url_for_request, status_code=resp_status_code) from e


# --- retrieve_instance_metadata (Keep as is) ---
def retrieve_instance_metadata(config: DicomWebSourceState, study_uid: str, series_uid: str, instance_uid: str) -> Optional[Dict[str, Any]]:
    """Retrieves instance metadata via WADO-RS."""
    base_url_val = getattr(config, 'base_url', None) # Use getattr
    source_name_val = getattr(config, 'source_name', 'UnknownSource') # Use getattr

    if not (isinstance(base_url_val, str) and base_url_val.strip()):
        raise ValueError(f"Base URL is not configured or is not a string for source '{source_name_val}'")

    base_url_with_slash = base_url_val if base_url_val.endswith('/') else base_url_val + '/'
    
    wado_prefix_val = getattr(config, 'wado_prefix', '') # Use getattr, default to empty string
    wado_prefix_str = wado_prefix_val if isinstance(wado_prefix_val, str) else ''
    service_base_url = urljoin(base_url_with_slash, wado_prefix_str) # Ensure wado_prefix_str is string
    service_base_url_with_slash = service_base_url if service_base_url.endswith('/') else service_base_url + '/'

    instance_path = f"studies/{study_uid}/series/{series_uid}/instances/{instance_uid}/metadata"
    url = urljoin(service_base_url_with_slash, instance_path)

    auth = _get_auth(config)
    headers = _get_headers(config)

    # Optional: Keep prep request logging if useful
    # try:
    #     prepared_request = requests.Request('GET', url).prepare()
    #     final_url_for_request = prepared_request.url
    #     logger.info(f"Constructed WADO Metadata URL: {final_url_for_request}")
    #     logger.debug(f"WADO Metadata Request - URL Base: {url}, Auth: {auth is not None}, Headers: {list(headers.keys())}")
    # except Exception as prep_e:
    #     logger.error(f"Error preparing WADO request URL for {instance_uid} at {config.source_name}: {prep_e}")
    final_url_for_request = url # Simplified for now

    response: Optional[requests.Response] = None # Initialize response
    try:
        response = requests.get(url, headers=headers, auth=auth, timeout=30)
        if response.status_code == 404:
            logger.warning(f"Instance metadata not found (404) for {instance_uid} at {source_name_val}")
            return None
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '')
        if not ('application/dicom+json' in content_type or 'application/json' in content_type):
            logger.warning(f"Unexpected WADO metadata response Content-Type: {content_type} from {url}. Attempting to parse as JSON.")
        metadata_list = response.json()
        if isinstance(metadata_list, list) and len(metadata_list) > 0:
            if len(metadata_list) > 1:
                logger.warning(f"WADO metadata response for {instance_uid} at {config.source_name} contained {len(metadata_list)} items, using first.")
            instance_data = metadata_list[0]
            if not isinstance(instance_data, dict):
                 logger.error(f"WADO metadata response item from {config.source_name} was not a JSON object: {instance_data}")
                 raise DicomWebClientError("WADO metadata response list item was not a JSON object", url=final_url_for_request, status_code=response.status_code)
            logger.debug(f"WADO metadata successfully retrieved for {instance_uid} from {config.source_name}")
            return instance_data
        elif isinstance(metadata_list, dict):
             # Handle cases where server might return single object directly
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
            return None # Treat 404 as not found, not an error here
        status_code = e.response.status_code if e.response is not None else None
        response_text = e.response.text[:500] if e.response is not None else "N/A"
        logger.error(f"WADO Metadata Request failed for {instance_uid} at {source_name_val}: {e}, Status: {status_code}, Response: {response_text}")
        raise DicomWebClientError(f"WADO metadata request failed: {e}. Response: {response_text}", status_code=status_code, url=final_url_for_request) from e
    except json.JSONDecodeError as e:
        # Safe access to response attributes
        resp_status_code = response.status_code if response is not None else None # MODIFIED
        resp_text = response.text[:500] if response is not None else "No response text"
        logger.error(f"Failed to decode WADO metadata JSON response for {instance_uid} from {source_name_val}: {e}. Response text: {resp_text}")
        raise DicomWebClientError(f"Invalid JSON response from WADO endpoint: {e}", url=final_url_for_request, status_code=resp_status_code) from e
    except Exception as e:
        # Safe access to response attributes
        resp_status_code = response.status_code if response is not None else None # MODIFIED
        resp_text = response.text[:500] if response is not None else "No response text"
        logger.error(f"Unexpected error during WADO metadata retrieval for {instance_uid} at {source_name_val}: {e}. Response: {resp_text}", exc_info=True)
        raise DicomWebClientError(f"Unexpected WADO metadata error: {e}. Response: {resp_text}", url=final_url_for_request, status_code=resp_status_code) from e