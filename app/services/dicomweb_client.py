# app/services/dicomweb_client.py

import logging
import json
from typing import List, Dict, Optional, Any, Tuple
from urllib.parse import urljoin, urlencode
from datetime import datetime, timedelta, date, timezone
import re 

import requests
from requests.auth import HTTPBasicAuth

from app.db.models import DicomWebSourceState
from app.core.config import settings

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

# --- Dynamic Query Param Resolver (_resolve_dynamic_query_params) - UPDATED ---
def _resolve_dynamic_query_params(params: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """
    Resolves special values like '-N'd, 'TODAY', 'YESTERDAY' for dates.
    Returns string dictionary suitable for requests params.
    """
    if not params:
        return {}

    resolved_params: Dict[str, str] = {}
    today = date.today() # Use current date (no timezone needed for YYYYMMDD)

    for key, value in params.items():
         str_key = str(key)
         str_value = str(value).strip() if value is not None else ""

         # --- ADDED: Handle dynamic date strings for StudyDate ---
         if str_key.lower() == "studydate" and str_value:
             val_upper = str_value.upper()
             resolved_date_value: Optional[str] = None

             if val_upper == "TODAY":
                 resolved_date_value = today.strftime('%Y%m%d')
             elif val_upper == "YESTERDAY":
                 yesterday = today - timedelta(days=1)
                 resolved_date_value = yesterday.strftime('%Y%m%d')
             else:
                 # Check for -<N>d format (N days ago until now) -> YYYYMMDD-
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
                 # Check for existing DICOM formats (YYYYMMDD, YYYYMMDD-, YYYYMMDD-YYYYMMDD)
                 elif re.match(r"^\d{8}$", val_upper): resolved_date_value = val_upper
                 elif re.match(r"^\d{8}-$", val_upper): resolved_date_value = val_upper
                 elif re.match(r"^\d{8}-\d{8}$", val_upper): resolved_date_value = val_upper
                 else:
                      logger.warning(f"Unsupported dynamic date format for '{str_key}': '{str_value}'. Skipping.")

             if resolved_date_value:
                 resolved_params[str_key] = resolved_date_value
                 logger.debug(f"Resolved dynamic date '{str_value}' to '{resolved_date_value}' for key '{str_key}'")
             # If resolved_date_value is still None after checks, it means it was invalid/unsupported, so we skip adding it.

         # --- END ADDED ---
         elif str_value: # Add other non-empty, non-StudyDate params
             resolved_params[str_key] = str_value

    return resolved_params


def query_qido(config: DicomWebSourceState, level: str = "STUDY", custom_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Performs a QIDO-RS query using DicomWebSourceState config.
    Prioritizes config.search_filters over custom_params for StudyDate.
    Resolves dynamic date values in parameters.
    """
    if not config.base_url:
        raise ValueError(f"Base URL is not configured for source '{config.source_name}'")

    # URL Construction
    base_url_with_slash = config.base_url if config.base_url.endswith('/') else config.base_url + '/'
    service_base_url = urljoin(base_url_with_slash, config.qido_prefix or '')
    service_base_url_with_slash = service_base_url if service_base_url.endswith('/') else service_base_url + '/'
    level_map = {"study": "studies", "series": "series", "instance": "instances"}
    path_segment = level_map.get(level.lower())
    if not path_segment:
        raise ValueError(f"Unsupported QIDO query level: {level}")
    url = urljoin(service_base_url_with_slash, path_segment)

    # Parameter Merging Logic (remains the same)
    query_params: Dict[str, Any] = {}
    if isinstance(config.search_filters, dict):
        query_params = config.search_filters.copy()
        logger.debug(f"QIDO {level}: Started with config search_filters: {query_params}")
    else:
        logger.debug(f"QIDO {level}: No base search_filters found in config.")

    if custom_params:
        logger.debug(f"QIDO {level}: Merging with custom_params: {custom_params}")
        study_date_from_custom = custom_params.get("StudyDate")

        if study_date_from_custom and "StudyDate" not in query_params:
            query_params["StudyDate"] = study_date_from_custom
            logger.info(f"QIDO {level}: Using StudyDate '{study_date_from_custom}' from custom_params (not found in config filters).")
        elif study_date_from_custom and "StudyDate" in query_params:
             logger.info(f"QIDO {level}: Ignoring StudyDate '{study_date_from_custom}' from custom_params; using value from config filters: '{query_params['StudyDate']}'")

        for key, value in custom_params.items():
            if key != "StudyDate":
                query_params[key] = value

    # Resolve dynamic values AFTER merging
    resolved_params = _resolve_dynamic_query_params(query_params)

    # Set default limit if not provided in resolved params
    resolved_params.setdefault('limit', str(settings.DICOMWEB_POLLER_QIDO_LIMIT)) # Use setting

    auth = _get_auth(config)
    headers = _get_headers(config)

    # Prepare request for logging URL
    try:
        prepared_request = requests.Request('GET', url, params=resolved_params).prepare()
        final_url_for_request = prepared_request.url
        logger.info(f"Constructed QIDO URL: {final_url_for_request}")
        logger.debug(f"QIDO Query - Final Params: {resolved_params}, Auth: {auth is not None}, Headers: {list(headers.keys())}")
    except Exception as prep_e:
         logger.error(f"Error preparing QIDO request URL for {config.source_name}: {prep_e}")
         final_url_for_request = url # Fallback

    # Request execution and error handling
    try:
        response = requests.get(
            url,
            params=resolved_params,
            headers=headers,
            auth=auth,
            timeout=60 # Consider making timeout configurable
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
