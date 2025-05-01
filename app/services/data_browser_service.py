# app/services/data_browser_service.py
# Use structlog if available
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
    pynetdicom_debug_logger_config = structlog.stdlib.ProcessorFormatter.wrap_for_formatter # For pynetdicom config later
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    pynetdicom_debug_logger_config = None # Indicate structlog not available

from typing import List, Dict, Any, Optional, Generator, Tuple, Literal
from datetime import date, timedelta, datetime, timezone
import re

# Pynetdicom specific imports
from pynetdicom import AE, evt # Import evt for potential future use
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelFind,
    Verification
)
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
    StudyResultItem,
    QueryLevel # Import QueryLevel
)
from app.core.config import settings
from app.services import dicomweb_client
from app.worker.dimse_qr_poller import _resolve_dynamic_date_filter


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

# --- Helper Functions (Modified _build_find_identifier) ---

def _build_find_identifier(query_params: List[DataBrowserQueryParam], query_level: QueryLevel) -> PydicomDataset:
    """Builds a pydicom Dataset for C-FIND based on request parameters and level."""
    identifier = PydicomDataset()
    identifier.QueryRetrieveLevel = query_level.value # Use query_level

    # Define return keys based on level
    if query_level == QueryLevel.STUDY:
        default_return_keys = [
            "PatientID", "PatientName", "StudyInstanceUID", "StudyDate", "StudyTime",
            "AccessionNumber", "ModalitiesInStudy", "ReferringPhysicianName",
            "PatientBirthDate", "StudyDescription", "NumberOfStudyRelatedSeries",
            "NumberOfStudyRelatedInstances",
        ]
    elif query_level == QueryLevel.SERIES:
         default_return_keys = ["SeriesInstanceUID", "SeriesNumber", "Modality", "SeriesDescription", "NumberOfSeriesRelatedInstances"]
    elif query_level == QueryLevel.INSTANCE:
         default_return_keys = ["SOPInstanceUID", "InstanceNumber", "SOPClassUID"]
    else:
        default_return_keys = [] # Should not happen with Literal validation

    filter_keys_used = set()
    log = logger.bind(query_level=query_level.value) # Add level to logs

    for param in query_params:
        # --- Ensure value is treated as string for DICOM identifier ---
        value_to_set = str(param.value) if param.value is not None else ""
        # --- END Ensure ---
        if param.field.lower() == "studydate": # Dynamic date handling specifically for StudyDate
            resolved_value = _resolve_dynamic_date_filter(param.value)
            if resolved_value is None:
                 log.warning("C-FIND: Skipping invalid dynamic date filter", field=param.field, value=param.value)
                 continue
            value_to_set = resolved_value # Use resolved date string

        try:
            # Attempt to resolve keyword or use tag directly
            keyword = param.field
            try:
                 # Check if field looks like a tag (e.g., "00100010")
                 if re.match(r"^[0-9a-fA-F]{8}$", keyword.replace(",", "")):
                      tag = Tag(f"({keyword[:4]},{keyword[4:]})") # Construct tag assuming format GGGGeeee
                      keyword = tag.keyword or keyword # Use keyword if found, else keep original tag string? Be careful here.
                 # If not tag format, assume keyword
                 elif not Tag(keyword).is_valid: # Quick check if it's a valid keyword string
                       raise ValueError("Not a valid keyword") # Treat as invalid if not tag or known keyword

            except Exception:
                 # Fallback: If not a valid known keyword or tag format, treat as is but warn
                 log.warning("C-FIND: Field is not a standard DICOM keyword or recognized tag format, using as provided.", field=param.field)
                 # keyword = param.field # Keep as is

            setattr(identifier, keyword, value_to_set)
            filter_keys_used.add(keyword)
            log.debug("C-FIND Filter Added", keyword=keyword, value=value_to_set)
        except Exception as e:
            log.warning("C-FIND: Error setting filter/identifier attribute", field=param.field, value=value_to_set, error=str(e))

    # Add empty return keys required by the level
    for key in default_return_keys:
        if key not in filter_keys_used:
            try:
                setattr(identifier, key, "") # Request return of this key
            except Exception as e:
                log.warning("C-FIND: Could not set return key", key=key, error=str(e))

    log.debug("Constructed C-FIND Identifier", identifier=str(identifier)) # Convert dataset to string for logging
    return identifier

def _build_qido_params(query_params: List[DataBrowserQueryParam]) -> Dict[str, str]:
    """Builds a dictionary for QIDO-RS query parameters."""
    qido_dict: Dict[str, str] = {}
    for param in query_params:
        value_to_set = str(param.value) if param.value is not None else "" # Ensure string
        if param.field.lower() == "studydate": # Dynamic date handling
            resolved_value = _resolve_dynamic_date_filter(param.value)
            if resolved_value is None:
                logger.warning("QIDO: Skipping invalid dynamic date filter", field=param.field, value=param.value)
                continue
            value_to_set = resolved_value
        qido_dict[param.field] = value_to_set
    logger.debug("Constructed QIDO parameters", qido_params=qido_dict)
    return qido_dict

# --- C-FIND Implementation (Keep previous version) ---
async def _execute_cfind_query(
    source_config: models.DimseQueryRetrieveSource,
    query_identifier: PydicomDataset,
    query_level: QueryLevel
) -> List[Dict[str, Any]]:
    """Executes a C-FIND query against the configured DIMSE source."""
    ae = AE(ae_title=source_config.local_ae_title)

    # Determine SOP Class based on level
    if query_level == QueryLevel.STUDY:
        find_sop_class = StudyRootQueryRetrieveInformationModelFind
    elif query_level == QueryLevel.SERIES:
        # Assuming Study Root for SERIES level as well, need Study UID in identifier
        find_sop_class = StudyRootQueryRetrieveInformationModelFind
    elif query_level == QueryLevel.INSTANCE:
        # Assuming Study Root for INSTANCE level, need Study/Series UID in identifier
        find_sop_class = StudyRootQueryRetrieveInformationModelFind
    else: # Should not happen
         raise InvalidParameterError(f"Unsupported query level for C-FIND: {query_level}", source_type="dimse-qr", source_id=source_config.id)


    preferred_ts = ['1.2.840.10008.1.2.1', '1.2.840.10008.1.2'] # Explicit VR LE, Implicit VR LE
    ae.add_requested_context(find_sop_class, transfer_syntax=preferred_ts)
    ae.add_requested_context(Verification)

    results: List[Dict[str, Any]] = []
    assoc = None
    host = source_config.remote_host
    port = source_config.remote_port
    remote_ae = source_config.remote_ae_title
    source_id = source_config.id
    source_name = source_config.name

    log = logger.bind(remote_ae=remote_ae, remote_host=host, remote_port=port, local_ae=source_config.local_ae_title, source_name=source_name, source_id=source_id, query_level=query_level.value)
    log.info("Attempting C-FIND association")

    ae.acse_timeout = 30
    ae.dimse_timeout = 60
    ae.network_timeout = 30

    try:
        assoc = ae.associate(host, port, ae_title=remote_ae)
        if assoc.is_established:
            log.info("Association established for C-FIND.")
            responses = assoc.send_c_find(query_identifier, find_sop_class)
            response_count = 0
            for (status_dataset, result_identifier) in responses:
                if status_dataset is None:
                     log.warning("Received C-FIND response with no status dataset.")
                     continue
                if not hasattr(status_dataset, 'Status'):
                     log.error("Received C-FIND response missing Status tag.")
                     # Release association before raising
                     if assoc.is_established: assoc.release()
                     raise RemoteQueryError("Invalid C-FIND response (Missing Status)", source_type="dimse-qr", source_id=source_id)

                status_int = int(status_dataset.Status)
                log.debug("C-FIND Response Status", status=f"0x{status_int:04X}")

                if status_int in (0xFF00, 0xFF01): # Pending
                    if result_identifier:
                        response_count += 1
                        try:
                            # Convert result to dict, add metadata manually before validation
                            result_dict = result_identifier.to_json_dict()
                            result_dict["source_id"] = source_id
                            result_dict["source_name"] = source_name
                            result_dict["source_type"] = "dimse-qr"
                            results.append(result_dict)
                            log.debug("C-FIND Pending Result Received", result_number=response_count)
                        except Exception as json_err:
                             log.warning("Failed to convert C-FIND result identifier to JSON", error=str(json_err), result_identifier=str(result_identifier))
                    else:
                         log.warning("C-FIND Pending status received without identifier dataset.")
                elif status_int == 0x0000: # Success
                    log.info("C-FIND successful.", total_results=len(results))
                    break
                elif status_int == 0xFE00: # Cancel
                     log.warning("C-FIND Cancelled by remote AE.")
                     break
                else: # Failure or other status
                    error_comment_tag = Tag(0x0000, 0x0902) # Error Comment Tag
                    error_comment = status_dataset.get(error_comment_tag, 'Unknown failure')
                    status_desc = f"Status: 0x{status_int:04X}"
                    if status_int == 0xA700: status_desc = "Out of Resources"
                    elif status_int == 0xA900: status_desc = "Identifier does not match SOP Class"
                    elif status_int == 0xC000: status_desc = "Unable to Process"
                    log.error("C-FIND Failed", status_code=f"0x{status_int:04X}", status_description=status_desc, error_comment=error_comment)
                    # Release association before raising
                    if assoc.is_established: assoc.release()
                    # Raise specific errors for known failure codes
                    if status_int == 0xA900:
                         raise InvalidParameterError(f"Query parameters do not match SOP Class ({status_desc})", source_type="dimse-qr", source_id=source_id)
                    else:
                         raise RemoteQueryError(f"C-FIND Failed ({status_desc}): {error_comment}", source_type="dimse-qr", source_id=source_id)

            # Release association after loop finishes (if not already released on error)
            if assoc.is_established:
                assoc.release()
                log.info("Association released.")
        else:
            log.error("Association rejected/aborted")
            raise RemoteConnectionError(f"Association failed with {remote_ae}", source_type="dimse-qr", source_id=source_id)

    except RemoteConnectionError as rce: raise rce
    except RemoteQueryError as rqe: raise rqe
    except InvalidParameterError as ipe: raise ipe
    except Exception as e:
        log.error("Error during C-FIND operation", error=str(e), exc_info=True)
        if assoc and assoc.is_established:
            try: assoc.abort()
            except Exception: pass
        raise QueryServiceError(f"C-FIND operation failed: {e}", source_type="dimse-qr", source_id=source_id) from e

    return results


# --- QIDO Implementation (Modified) ---
async def _execute_qido_query(
    source_config: models.DicomWebSourceState,
    query_params: Dict[str, str],
    query_level: QueryLevel,
    prioritize_custom_params: bool = False
) -> List[Dict[str, Any]]:
    """Executes a QIDO-RS query against the configured DICOMweb source."""
    log = logger.bind(source_name=source_config.source_name, source_id=source_config.id, base_url=source_config.base_url, query_level=query_level.value)
    log.info("Attempting QIDO query")
    try:
        # Call the updated dicomweb_client function
        qido_results = dicomweb_client.query_qido(
            config=source_config,
            level=query_level.value, # Pass query level string
            custom_params=query_params,
            prioritize_custom_params=prioritize_custom_params
        )
        log.info("QIDO query successful.", result_count=len(qido_results))
        # Add source info before returning
        for result in qido_results:
             result["source_id"] = source_config.id
             result["source_name"] = source_config.source_name
             result["source_type"] = "dicomweb"
        return qido_results
    except dicomweb_client.DicomWebClientError as e:
         log.error("Error during QIDO query", status_code=e.status_code, error=str(e), exc_info=False) # Less verbose trace
         if e.status_code and 400 <= e.status_code < 500:
             raise InvalidParameterError(f"QIDO Error ({e.status_code}): {e}", source_type="dicomweb", source_id=source_config.id) from e
         elif e.status_code and e.status_code >= 500:
             raise RemoteQueryError(f"QIDO Remote Error ({e.status_code}): {e}", source_type="dicomweb", source_id=source_config.id) from e
         else: # Includes timeouts, connection errors
              raise RemoteConnectionError(f"QIDO Connection Error: {e}", source_type="dicomweb", source_id=source_config.id) from e
    except Exception as e:
         log.error("Unexpected error during QIDO query", error=str(e), exc_info=True)
         raise QueryServiceError(f"QIDO Error: {e}", source_type="dicomweb", source_id=source_config.id) from e


# --- get_source_info_for_response (MODIFIED) ---
def get_source_info_for_response(db: Session, source_id: int, source_type: Literal["dicomweb", "dimse-qr"]) -> Dict[str, str]:
    """Gets basic source info (name, type) based on ID AND type."""
    info = {"name": "Unknown", "type": source_type} # Default to provided type
    try:
        if source_type == "dimse-qr":
            config = crud.crud_dimse_qr_source.get(db, id=source_id)
            if config: info["name"] = config.name
        elif source_type == "dicomweb":
            config = crud.dicomweb_source.get(db, id=source_id)
            if config: info["name"] = config.source_name
        else:
            # Should not happen if request validation uses Literal
             logger.warning("get_source_info called with invalid source_type", provided_type=source_type, source_id=source_id)
             info["type"] = "Unknown"

    except Exception as e:
        logger.error("Failed to retrieve source info for error response", source_id=source_id, source_type=source_type, error=str(e))
    return info


# --- Main Service Function execute_query (MODIFIED) ---
async def execute_query(
    db: Session,
    source_id: int,
    source_type: Literal["dicomweb", "dimse-qr"], # <-- ADDED source_type argument
    query_params: List[DataBrowserQueryParam],
    query_level: QueryLevel # <-- ADDED query_level argument
) -> DataBrowserQueryResponse:
    """
    Executes a query against a specified configured source based on ID and TYPE.
    """
    source_config: Any = None # Use Any temporarily, will be specific type later
    source_name = "Unknown" # Default

    log = logger.bind(source_id=source_id, source_type=source_type, query_level=query_level.value)
    log.info("Executing data browser query")

    # 1. Fetch config based on EXPLICIT type
    try:
        if source_type == "dimse-qr":
            source_config = crud.crud_dimse_qr_source.get(db, id=source_id)
            if source_config: source_name = source_config.name
        elif source_type == "dicomweb":
            source_config = crud.dicomweb_source.get(db, id=source_id)
            if source_config: source_name = source_config.source_name
        else:
            # This case should be prevented by FastAPI validation using Literal
            raise InvalidParameterError(f"Unsupported source_type: {source_type}", source_type=source_type, source_id=source_id)

        if source_config is None:
            raise SourceNotFoundError(f"Source configuration not found.", source_type=source_type, source_id=source_id)

        log = log.bind(source_name=source_name) # Add name to context
        log.debug("Source configuration found")

    except SourceNotFoundError as e:
        raise e # Re-raise to be caught by endpoint handler
    except Exception as db_exc:
        log.error("Database error fetching source configuration", error=str(db_exc), exc_info=True)
        raise QueryServiceError("Failed to fetch source configuration from database", source_type=source_type, source_id=source_id) from db_exc


    # 2. Check if source is enabled
    if not source_config.is_enabled:
         log.warning("Query attempt on disabled source")
         # Return success but indicate no query was run
         return DataBrowserQueryResponse(
             query_status="success",
             message="Source is disabled, no query performed.",
             source_id=source_id,
             source_name=source_name,
             source_type=source_type,
             results=[]
         )

    # 3. Prepare and Execute Query based on type
    results_list: List[Dict[str, Any]] = []
    message = "Query executed successfully." # Default success message
    query_status: Literal["success", "error", "partial"] = "success" # Type hint for status

    try:
        if source_type == "dimse-qr":
             log.debug("Preparing C-FIND identifier")
             find_identifier = _build_find_identifier(query_params, query_level)
             if not isinstance(source_config, models.DimseQueryRetrieveSource): raise TypeError("Config type mismatch") # Type safety check
             results_list = await _execute_cfind_query(source_config, find_identifier, query_level)
        elif source_type == "dicomweb":
            log.debug("Preparing QIDO parameters")
            qido_params = _build_qido_params(query_params)
            if not isinstance(source_config, models.DicomWebSourceState): raise TypeError("Config type mismatch") # Type safety check
            results_list = await _execute_qido_query(
                source_config,
                qido_params,
                query_level,
                prioritize_custom_params=True
            )
        # No else needed due to explicit type check/raise earlier

    except QueryServiceError as e:
        # Service layer errors (Connection, Remote, InvalidParam, SourceNotFound handled earlier)
        query_status = "error"
        message = str(e)
        results_list = []
        # Re-raise so endpoint can generate appropriate HTTP response/log
        raise e
    except Exception as e:
        # Catch truly unexpected errors during query execution
        query_status = "error"
        message = f"Unexpected error during query execution: {e}"
        results_list = []
        log.exception("Unexpected error during query execution") # Log full trace
        # Wrap in QueryServiceError before raising
        raise QueryServiceError(message, source_type=source_type, source_id=source_id) from e

    # 4. Format results into the response schema
    # Note: The _execute functions now add source info directly to result dicts
    formatted_results: List[StudyResultItem] = []
    log.debug("Formatting results for response", raw_result_count=len(results_list))
    for result_dict in results_list:
        try:
            # Validate and transform using the Pydantic model
            formatted_item = StudyResultItem.model_validate(result_dict)
            formatted_results.append(formatted_item)
        except Exception as validation_err:
            log.warning("Failed to validate/format result item", validation_error=str(validation_err), raw_item=str(result_dict)[:500], exc_info=True)

    log.info("Query execution complete", final_status=query_status, formatted_result_count=len(formatted_results))
    return DataBrowserQueryResponse(
        query_status=query_status,
        # Only include message if status is not success
        message=message if query_status != "success" else None,
        source_id=source_id,
        source_name=source_name,
        source_type=source_type,
        results=formatted_results
    )
