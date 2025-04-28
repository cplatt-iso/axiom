# app/services/data_browser_service.py
import logging
from typing import List, Dict, Any, Optional, Generator, Tuple, Literal
from datetime import date, timedelta, datetime, timezone
import re

# Pynetdicom specific imports for C-FIND
from pynetdicom import AE, debug_logger as pynetdicom_debug_logger
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelFind,
    Verification # Added for basic context
)
# --- REMOVED build_context import ---
# from pynetdicom.presentation import build_context
# --- END REMOVAL ---
from pydicom.dataset import Dataset as PydicomDataset
from pydicom.tag import Tag

# SQLAlchemy specific imports
from sqlalchemy.orm import Session

# Application specific imports
from app import crud
from app.db import models
from app.schemas.data_browser import (
    DataBrowserQueryParam,
    DataBrowserQueryResponse,
    StudyResultItem
)
from app.core.config import settings
from app.services import dicomweb_client
from app.worker.dimse_qr_poller import _resolve_dynamic_date_filter

logger = logging.getLogger(__name__)
# pynetdicom_debug_logger() # Uncomment for pynetdicom detailed logs

# --- Custom Exceptions (Keep as is) ---
class QueryServiceError(Exception):
    """Base exception for errors during query execution."""
    def __init__(self, message: str, source_type: Optional[str] = None, source_id: Optional[int] = None):
        self.source_type = source_type
        self.source_id = source_id
        super().__init__(f"{source_type or 'Unknown Source'} (ID: {source_id or 'N/A'}): {message}")

class SourceNotFoundError(QueryServiceError):
    """Raised when the specified source ID is not found."""
    pass

class InvalidParameterError(QueryServiceError):
    """Raised for invalid query parameters."""
    pass

class RemoteConnectionError(QueryServiceError):
    """Raised for connection issues with the remote source."""
    pass

class RemoteQueryError(QueryServiceError):
    """Raised when the remote source returns a failure status."""
    pass

# --- Helper Functions ---

def _build_find_identifier(query_params: List[DataBrowserQueryParam]) -> PydicomDataset:
    """Builds a pydicom Dataset for C-FIND based on request parameters."""
    identifier = PydicomDataset()
    identifier.QueryRetrieveLevel = "STUDY" # Assume STUDY level for now

    default_return_keys = [
        "PatientID", "PatientName", "StudyInstanceUID", "StudyDate", "StudyTime",
        "AccessionNumber", "ModalitiesInStudy", "ReferringPhysicianName",
        "PatientBirthDate", "StudyDescription", "NumberOfStudyRelatedSeries",
        "NumberOfStudyRelatedInstances",
    ]

    filter_keys_used = set()

    for param in query_params:
        value_to_set = param.value
        if param.field.lower() == "studydate":
            resolved_value = _resolve_dynamic_date_filter(param.value)
            if resolved_value is None:
                 logger.warning(f"C-FIND: Skipping invalid dynamic date filter: {param.field}={param.value}")
                 continue
            value_to_set = resolved_value

        try:
            keyword = param.field
            try:
                tag = Tag(keyword)
                keyword = tag.keyword or keyword
            except Exception:
                 pass
            setattr(identifier, keyword, value_to_set)
            filter_keys_used.add(keyword)
            logger.debug(f"C-FIND Filter Added: {keyword}={value_to_set}")
        except Exception as e:
            logger.warning(f"C-FIND: Error setting filter/identifier attribute {param.field}={value_to_set}: {e}")

    for key in default_return_keys:
        if key not in filter_keys_used:
            try:
                setattr(identifier, key, "")
            except Exception as e:
                logger.warning(f"C-FIND: Could not set return key {key}: {e}")

    logger.debug(f"Constructed C-FIND Identifier:\n{identifier}")
    return identifier

def _build_qido_params(query_params: List[DataBrowserQueryParam]) -> Dict[str, str]:
    """Builds a dictionary for QIDO-RS query parameters."""
    qido_dict: Dict[str, str] = {}
    for param in query_params:
        value_to_set = param.value
        if param.field.lower() == "studydate":
            resolved_value = _resolve_dynamic_date_filter(param.value)
            if resolved_value is None:
                logger.warning(f"QIDO: Skipping invalid dynamic date filter: {param.field}={param.value}")
                continue
            value_to_set = resolved_value
        qido_dict[param.field] = value_to_set
    logger.debug(f"Constructed QIDO parameters: {qido_dict}")
    return qido_dict

# --- C-FIND Implementation ---
async def _execute_cfind_query(
    source_config: models.DimseQueryRetrieveSource,
    query_identifier: PydicomDataset
) -> List[Dict[str, Any]]:
    """Executes a C-FIND query against the configured DIMSE source."""
    ae = AE(ae_title=source_config.local_ae_title)
    find_sop_class = StudyRootQueryRetrieveInformationModelFind

    # --- CORRECTED: Define transfer syntaxes and add context directly ---
    # Using common implicit/explicit VR little endian should be safe
    preferred_ts = ['1.2.840.10008.1.2.1', '1.2.840.10008.1.2']
    ae.add_requested_context(find_sop_class, transfer_syntax=preferred_ts)
    # --- END CORRECTION ---

    ae.add_requested_context(Verification) # Add Verification for basic echo test

    # --- REMOVED build_context and the second add_requested_context call ---
    # context = build_context(find_sop_class, ts)
    # ae.add_requested_context(context) # Incorrect usage
    # --- END REMOVAL ---

    results: List[Dict[str, Any]] = []
    assoc = None
    host = source_config.remote_host
    port = source_config.remote_port
    remote_ae = source_config.remote_ae_title
    source_id = source_config.id
    source_name = source_config.name

    logger.info(f"Attempting C-FIND to {remote_ae}@{host}:{port}")

    # Set timeouts (consider making these configurable per source later)
    ae.acse_timeout = 30
    ae.dimse_timeout = 60
    ae.network_timeout = 30

    try:
        assoc = ae.associate(host, port, ae_title=remote_ae)
        if assoc.is_established:
            logger.info(f"Association established with {remote_ae} for C-FIND.")

            # Send C-FIND request
            responses = assoc.send_c_find(query_identifier, find_sop_class)
            response_count = 0
            for (status_dataset, result_identifier) in responses:
                if status_dataset is None:
                     logger.warning(f"Received response for {source_name} with no status dataset.")
                     continue

                if not hasattr(status_dataset, 'Status'):
                     logger.error(f"Received response for {source_name} missing Status tag.")
                     raise RemoteQueryError("Invalid C-FIND response (Missing Status)", source_type="dimse-qr", source_id=source_id)

                status_int = int(status_dataset.Status)
                logger.debug(f"C-FIND Response Status: 0x{status_int:04X}")

                if status_int in (0xFF00, 0xFF01): # Pending
                    if result_identifier:
                        response_count += 1
                        try:
                            result_dict = result_identifier.to_json_dict()
                            results.append(result_dict)
                            logger.debug(f" -> C-FIND Pending Result Received [{response_count}]")
                        except Exception as json_err:
                             logger.warning(f"Failed to convert C-FIND result identifier to JSON: {json_err}. Identifier:\n{result_identifier}")
                    else:
                         logger.warning("C-FIND Pending status received without identifier dataset.")

                elif status_int == 0x0000: # Success
                    logger.info(f"C-FIND successful for source '{source_name}'. Found {len(results)} total results.")
                    break # Stop processing responses

                elif status_int == 0xFE00: # Cancel
                     logger.warning(f"C-FIND Cancelled by remote AE {remote_ae}.")
                     break

                elif status_int == 0xA700: # Out of Resources
                     logger.warning(f"C-FIND failed for {source_name} - Out of Resources (0xA700). Returning partial results.")
                     break
                elif status_int == 0xA900: # Identifier does not match SOP Class
                     logger.error(f"C-FIND failed for {source_name} - Identifier does not match SOP Class (0xA900). Check query parameters.")
                     raise InvalidParameterError(f"Query parameters do not match SOP Class (0xA900)", source_type="dimse-qr", source_id=source_id)
                elif status_int == 0xC000: # Unable to process
                      error_comment = status_dataset.get('ErrorComment', 'Unable to process')
                      logger.error(f"C-FIND failed for {source_name} - Unable to Process (0xC000). Comment: {error_comment}")
                      raise RemoteQueryError(f"Remote AE Unable to Process query (0xC000): {error_comment}", source_type="dimse-qr", source_id=source_id)
                else: # Other failure status
                    error_comment = status_dataset.get('ErrorComment', 'No comment')
                    raise RemoteQueryError(f"C-FIND Failed (Status: 0x{status_int:04X} - {error_comment})", source_type="dimse-qr", source_id=source_id)

            assoc.release()
            logger.info("Association released.")

        else:
            logger.error(f"Association rejected/aborted by {remote_ae} at {host}:{port}")
            raise RemoteConnectionError(f"Association failed with {remote_ae}", source_type="dimse-qr", source_id=source_id)

    except RemoteConnectionError as rce:
        raise rce
    except RemoteQueryError as rqe:
        raise rqe
    except InvalidParameterError as ipe:
        raise ipe
    except Exception as e:
        logger.error(f"Error during C-FIND operation for source '{source_name}': {e}", exc_info=True)
        if assoc and assoc.is_established:
            try: assoc.abort()
            except Exception: pass
        raise QueryServiceError(f"C-FIND operation failed: {e}", source_type="dimse-qr", source_id=source_id) from e

    return results

# --- QIDO Implementation (Keep as is) ---
async def _execute_qido_query(
    source_config: models.DicomWebSourceState,
    query_params: Dict[str, str]
) -> List[Dict[str, Any]]:
    """Executes a QIDO-RS query against the configured DICOMweb source."""
    logger.info(f"Attempting QIDO query to {source_config.source_name} ({source_config.base_url})")
    try:
        qido_results = dicomweb_client.query_qido(
            config=source_config,
            level="STUDY",
            custom_params=query_params
        )
        logger.info(f"QIDO query successful for source '{source_config.source_name}'. Found {len(qido_results)} results.")
        return qido_results
    except dicomweb_client.DicomWebClientError as e:
         logger.error(f"Error during QIDO for source '{source_config.source_name}': {e}", exc_info=True)
         if e.status_code and 400 <= e.status_code < 500:
             raise InvalidParameterError(f"QIDO Error: {e}", source_type="dicomweb", source_id=source_config.id) from e
         elif e.status_code and e.status_code >= 500:
             raise RemoteQueryError(f"QIDO Remote Error: {e}", source_type="dicomweb", source_id=source_config.id) from e
         else:
              raise RemoteConnectionError(f"QIDO Connection Error: {e}", source_type="dicomweb", source_id=source_config.id) from e
    except Exception as e:
         logger.error(f"Unexpected error during QIDO query for '{source_config.source_name}': {e}", exc_info=True)
         raise QueryServiceError(f"QIDO Error: {e}", source_type="dicomweb", source_id=source_config.id) from e

# --- get_source_info_for_response (Keep as is) ---
def get_source_info_for_response(db: Session, source_id: int) -> Dict[str, str]:
    """Attempts to get basic source info (name, type) for error responses."""
    info = {"name": "Unknown", "type": "Unknown"}
    try:
        dimse_config = crud.crud_dimse_qr_source.get(db, id=source_id)
        if dimse_config:
            info["name"] = dimse_config.name
            info["type"] = "dimse-qr"
            return info

        dicomweb_config = crud.dicomweb_source.get(db, id=source_id)
        if dicomweb_config:
            info["name"] = dicomweb_config.source_name
            info["type"] = "dicomweb"
            return info
    except Exception as e:
        logger.error(f"Failed to retrieve source info for error response (ID: {source_id}): {e}")
    return info


# --- Main Service Function execute_query (Keep as is) ---
async def execute_query(
    db: Session,
    source_id: int,
    query_params: List[DataBrowserQueryParam]
) -> DataBrowserQueryResponse:
    """
    Executes a query against a specified configured source (DIMSE Q/R or DICOMweb).
    """
    source_config = None
    source_type = "Unknown"
    source_name = "Unknown"

    # 1. Try finding as DIMSE Q/R source
    dimse_config = crud.crud_dimse_qr_source.get(db, id=source_id)
    if dimse_config:
        source_config = dimse_config
        source_type = "dimse-qr"
        source_name = dimse_config.name
        logger.info(f"Found DIMSE Q/R source config: {source_name} (ID: {source_id})")
    else:
        # 2. Try finding as DICOMweb source
        dicomweb_config = crud.dicomweb_source.get(db, id=source_id)
        if dicomweb_config:
            source_config = dicomweb_config
            source_type = "dicomweb"
            source_name = dicomweb_config.source_name
            logger.info(f"Found DICOMweb source config: {source_name} (ID: {source_id})")
        else:
             # 3. Source not found
             raise SourceNotFoundError(f"Source configuration with ID {source_id} not found.", source_id=source_id)

    # 4. Check if source is enabled
    if not source_config.is_enabled:
         logger.warning(f"Query attempt on disabled source: {source_name} (ID: {source_id}, Type: {source_type})")
         return DataBrowserQueryResponse(
             query_status="success",
             message="Source is disabled, no query performed.",
             source_id=source_id,
             source_name=source_name,
             source_type=source_type,
             results=[]
         )

    # 5. Prepare and Execute Query based on type
    results_list: List[Dict[str, Any]] = []
    message = "Query executed successfully."
    query_status: Literal["success", "error", "partial"] = "success" # type: ignore

    try:
        if source_type == "dimse-qr":
             find_identifier = _build_find_identifier(query_params)
             if isinstance(source_config, models.DimseQueryRetrieveSource):
                 results_list = await _execute_cfind_query(source_config, find_identifier)
             else:
                  raise TypeError("Internal error: source_config type mismatch for C-FIND.")
        elif source_type == "dicomweb":
            qido_params = _build_qido_params(query_params)
            if isinstance(source_config, models.DicomWebSourceState):
                results_list = await _execute_qido_query(source_config, qido_params)
            else:
                 raise TypeError("Internal error: source_config type mismatch for QIDO.")
        else:
             raise QueryServiceError("Unknown source type encountered.", source_type=source_type, source_id=source_id)

    except QueryServiceError as e:
        query_status = "error"
        message = str(e)
        results_list = []
        raise e
    except Exception as e:
        query_status = "error"
        message = f"Unexpected error during query: {e}"
        results_list = []
        logger.exception(f"Unexpected error in execute_query for source {source_id}: {e}")
        raise QueryServiceError(message, source_type=source_type, source_id=source_id) from e

    # 6. Format results into the response schema
    formatted_results: List[StudyResultItem] = []
    for result_dict in results_list:
        try:
            # Add source info with correct keys
            result_dict["source_id"] = source_id
            result_dict["source_name"] = source_name
            result_dict["source_type"] = source_type
            # Validate and transform
            formatted_item = StudyResultItem.model_validate(result_dict)
            formatted_results.append(formatted_item)
        except Exception as validation_err:
            logger.warning(f"Failed to validate/format study result item for source {source_id}: {validation_err}. Item: {result_dict}", exc_info=True)

    return DataBrowserQueryResponse(
        query_status=query_status,
        message=message if query_status != "success" else None,
        source_id=source_id,
        source_name=source_name,
        source_type=source_type,
        results=formatted_results
    )
