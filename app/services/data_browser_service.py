# app/services/data_browser_service.py

# --- Logging Setup ---
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# --- Standard Libraries ---
import tempfile
import os
import re
import ssl # For TLS Context
from typing import List, Dict, Any, Optional, Generator, Tuple, Literal
from datetime import date, timedelta, datetime, timezone

# --- Third-Party Libraries ---
from pynetdicom import AE, evt, Association # For type hinting
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelFind,
    Verification
)
from pydicom.dataset import Dataset as PydicomDataset
from pydicom.tag import Tag
from sqlalchemy.orm import Session

# --- Application Specific Imports ---
from app import crud
from app.db import models
from app.schemas.data_browser import (
    DataBrowserQueryParam,
    DataBrowserQueryResponse,
    StudyResultItem,
    QueryLevel
)
from app.core.config import settings
from app.services import dicomweb_client # Keep for QIDO
# Import helper for dynamic dates (ensure it's correct)
from app.worker.dimse_qr_poller import _resolve_dynamic_date_filter

# --- Secret Manager Imports ---
try:
    from app.core.gcp_utils import (
        fetch_secret_content,
        SecretManagerError,
        SecretNotFoundError,
        PermissionDeniedError
    )
    SECRET_MANAGER_ENABLED = True
    logger.debug("Secret Manager utilities loaded.")
except ImportError:
    logger.warning("GCP Utils / Secret Manager not available. DIMSE/C-STORE TLS via secrets will fail.")
    SECRET_MANAGER_ENABLED = False
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass
    def fetch_secret_content(*args, **kwargs): raise NotImplementedError("Secret Manager fetch not available")

# --- Custom Exceptions ---
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


# --- Helper: Build C-FIND Identifier ---
def _build_find_identifier(query_params: List[DataBrowserQueryParam], query_level: QueryLevel) -> PydicomDataset:
    """Builds a pydicom Dataset for C-FIND based on request parameters and level."""
    identifier = PydicomDataset()
    identifier.QueryRetrieveLevel = query_level.value
    log = logger.bind(query_level=query_level.value)

    # Define return keys based on level
    if query_level == QueryLevel.STUDY:
        default_return_keys = ["PatientID", "PatientName", "StudyInstanceUID", "StudyDate", "StudyTime", "AccessionNumber", "ModalitiesInStudy", "ReferringPhysicianName", "PatientBirthDate", "StudyDescription", "NumberOfStudyRelatedSeries", "NumberOfStudyRelatedInstances"]
    elif query_level == QueryLevel.SERIES:
         default_return_keys = ["SeriesInstanceUID", "SeriesNumber", "Modality", "SeriesDescription", "NumberOfSeriesRelatedInstances"]
    elif query_level == QueryLevel.INSTANCE:
         default_return_keys = ["SOPInstanceUID", "InstanceNumber", "SOPClassUID"]
    else: default_return_keys = []

    filter_keys_used = set()
    for param in query_params:
        value_to_set = str(param.value) if param.value is not None else ""
        if param.field.lower() == "studydate":
            resolved_value = _resolve_dynamic_date_filter(param.value)
            if resolved_value is None: log.warning("C-FIND: Skipping invalid dynamic date filter", field=param.field, value=param.value); continue
            value_to_set = resolved_value

        try: # Resolve keyword/tag
            keyword = param.field
            try:
                 if re.match(r"^[0-9a-fA-F]{8}$", keyword.replace(",", "")): tag = Tag(f"({keyword[:4]},{keyword[4:]})"); keyword = tag.keyword or keyword
                 elif not Tag(keyword).is_valid: raise ValueError("Not a valid keyword")
            except Exception: log.warning("C-FIND: Field not standard DICOM keyword/tag, using as provided.", field=param.field)
            setattr(identifier, keyword, value_to_set)
            filter_keys_used.add(keyword)
            log.debug("C-FIND Filter Added", keyword=keyword, value=value_to_set)
        except Exception as e: log.warning("C-FIND: Error setting filter attribute", field=param.field, value=value_to_set, error=str(e))

    # Add empty return keys required by the level
    for key in default_return_keys:
        if key not in filter_keys_used:
            try: setattr(identifier, key, "")
            except Exception as e: log.warning("C-FIND: Could not set return key", key=key, error=str(e))

    log.debug("Constructed C-FIND Identifier", identifier_str=str(identifier)) # Log dataset as string
    return identifier

# --- Helper: Build QIDO Parameters ---
def _build_qido_params(query_params: List[DataBrowserQueryParam]) -> Dict[str, str]:
    """Builds a dictionary for QIDO-RS query parameters."""
    qido_dict: Dict[str, str] = {}
    for param in query_params:
        value_to_set = str(param.value) if param.value is not None else ""
        if param.field.lower() == "studydate":
            resolved_value = _resolve_dynamic_date_filter(param.value)
            if resolved_value is None: logger.warning("QIDO: Skipping invalid dynamic date filter", field=param.field, value=param.value); continue
            value_to_set = resolved_value
        qido_dict[param.field] = value_to_set
    logger.debug("Constructed QIDO parameters", qido_params=qido_dict)
    return qido_dict


# --- C-FIND Implementation (Using ssl.SSLContext) ---
async def _execute_cfind_query(
    source_config: models.DimseQueryRetrieveSource,
    query_identifier: PydicomDataset,
    query_level: QueryLevel
) -> List[Dict[str, Any]]:
    """Executes a C-FIND query against the configured DIMSE source. Supports TLS via ssl.SSLContext."""
    # Extract config
    host = source_config.remote_host; port = source_config.remote_port
    remote_ae = source_config.remote_ae_title; local_ae_title = source_config.local_ae_title
    source_id = source_config.id; source_name = source_config.name
    tls_enabled = getattr(source_config, 'tls_enabled', False)
    tls_ca_cert_secret = getattr(source_config, 'tls_ca_cert_secret_name', None)
    tls_client_cert_secret = getattr(source_config, 'tls_client_cert_secret_name', None)
    tls_client_key_secret = getattr(source_config, 'tls_client_key_secret_name', None)

    log = logger.bind(remote_ae=remote_ae, remote_host=host, remote_port=port, local_ae=local_ae_title, source_name=source_name, source_id=source_id, query_level=query_level.value, tls_enabled=tls_enabled)
    log.info("Attempting C-FIND operation")

    # Initialize AE
    ae = AE(ae_title=local_ae_title)
    if query_level == QueryLevel.STUDY: find_sop_class = StudyRootQueryRetrieveInformationModelFind
    elif query_level == QueryLevel.SERIES: find_sop_class = StudyRootQueryRetrieveInformationModelFind
    elif query_level == QueryLevel.INSTANCE: find_sop_class = StudyRootQueryRetrieveInformationModelFind
    else: raise InvalidParameterError(f"Unsupported query level for C-FIND: {query_level}", source_type="dimse-qr", source_id=source_id)
    preferred_ts = ['1.2.840.10008.1.2.1', '1.2.840.10008.1.2']
    ae.add_requested_context(find_sop_class, transfer_syntax=preferred_ts)
    ae.add_requested_context(Verification)
    ae.acse_timeout = settings.DIMSE_ACSE_TIMEOUT
    ae.dimse_timeout = settings.DIMSE_DIMSE_TIMEOUT
    ae.network_timeout = settings.DIMSE_NETWORK_TIMEOUT

    # Association / Query Variables
    results: List[Dict[str, Any]] = []
    assoc: Optional[Association] = None
    ca_cert_file, client_cert_file, client_key_file = None, None, None # Temp file paths
    temp_files_created: List[str] = [] # Track files for cleanup

    try:
        ssl_context: Optional[ssl.SSLContext] = None # Initialize ssl_context

        # --- TLS Setup (if enabled) ---
        if tls_enabled:
            log.info("TLS enabled for C-FIND. Preparing SSLContext...")
            if not SECRET_MANAGER_ENABLED: raise QueryServiceError("Secret Manager unavailable.", source_type="dimse-qr", source_id=source_id)
            if not tls_ca_cert_secret: raise QueryServiceError("TLS CA cert secret name missing.", source_type="dimse-qr", source_id=source_id)

            try:
                # Fetch CA Cert (Required)
                log.debug("Fetching CA cert for TLS", secret_name=tls_ca_cert_secret)
                ca_bytes = fetch_secret_content(tls_ca_cert_secret)
                tf_ca = tempfile.NamedTemporaryFile(delete=False, suffix="-ca.pem", prefix="cfind_scu_")
                tf_ca.write(ca_bytes); tf_ca.close(); ca_cert_file = tf_ca.name; temp_files_created.append(ca_cert_file)
                log.debug("CA cert written to temporary file", path=ca_cert_file)

                # Fetch Client Cert/Key for mTLS if configured
                if tls_client_cert_secret and tls_client_key_secret:
                    log.debug("Fetching client cert/key for mTLS...")
                    cert_bytes = fetch_secret_content(tls_client_cert_secret)
                    key_bytes = fetch_secret_content(tls_client_key_secret)
                    tf_cert = tempfile.NamedTemporaryFile(delete=False, suffix="-cert.pem", prefix="cfind_scu_")
                    tf_cert.write(cert_bytes); tf_cert.close(); client_cert_file = tf_cert.name; temp_files_created.append(client_cert_file)
                    tf_key = tempfile.NamedTemporaryFile(delete=False, suffix="-key.pem", prefix="cfind_scu_")
                    tf_key.write(key_bytes); tf_key.close(); client_key_file = tf_key.name; temp_files_created.append(client_key_file)
                    try: os.chmod(client_key_file, 0o600)
                    except OSError as chmod_err: log.warning(f"Could not set permissions on temp key file {client_key_file}: {chmod_err}")
                    log.debug("Client cert/key written to temporary files", cert_path=client_cert_file, key_path=client_key_file)
                elif tls_client_cert_secret or tls_client_key_secret:
                    raise QueryServiceError("Both client cert/key secrets required for mTLS", source_type="dimse-qr", source_id=source_id)

                # Create and Configure ssl.SSLContext
                log.debug("Creating ssl.SSLContext for SCU...")
                ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_file)
                ssl_context.check_hostname = False # Verify server hostname matches certificate CN/SAN
                ssl_context.verify_mode = ssl.CERT_REQUIRED # Ensure server cert is verified against CA

                if client_cert_file and client_key_file:
                    log.debug("Loading client cert chain into SSLContext for mTLS...")
                    ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
                log.info("SSLContext configured successfully.")

            except (SecretManagerError, SecretNotFoundError, PermissionDeniedError, IOError, OSError, ssl.SSLError) as e:
                 log.error(f"Failed to prepare TLS configuration for C-FIND: {e}", exc_info=True)
                 raise QueryServiceError(f"TLS setup failed: {e}", source_type="dimse-qr", source_id=source_id) from e

        # --- Associate ---
        if tls_enabled:
            if ssl_context is None: raise QueryServiceError("TLS SSLContext not created.", source_type="dimse-qr", source_id=source_id)
            log.info("Requesting TLS association...")
            assoc = ae.associate(host, port, ae_title=remote_ae, tls_args=(ssl_context, None))
        else:
            log.info("Requesting non-TLS association...")
            assoc = ae.associate(host, port, ae_title=remote_ae)

        # --- Process Association and Send C-FIND ---
        if assoc.is_established:
            log.info(f"Association established for C-FIND {'(TLS)' if tls_enabled else ''}.")
            responses = assoc.send_c_find(query_identifier, find_sop_class)
            response_count = 0
            for (status_dataset, result_identifier) in responses:
                if status_dataset is None: log.warning("Received C-FIND response with no status."); continue
                if not hasattr(status_dataset, 'Status'): log.error("Response missing Status."); raise RemoteQueryError("Invalid C-FIND response (Missing Status)", ...)
                status_int = int(status_dataset.Status)
                log.debug("C-FIND Response Status", status=f"0x{status_int:04X}")
                if status_int in (0xFF00, 0xFF01): # Pending
                    if result_identifier:
                        response_count += 1
                        try:
                            result_dict = result_identifier.to_json_dict()
                            result_dict["source_id"] = source_id; result_dict["source_name"] = source_name; result_dict["source_type"] = "dimse-qr"
                            results.append(result_dict); log.debug("C-FIND Pending Result Received", num=response_count)
                        except Exception as json_err: log.warning("Failed to convert C-FIND result", error=str(json_err), result_data=str(result_identifier)[:200])
                    else: log.warning("C-FIND Pending status without identifier.")
                elif status_int == 0x0000: log.info("C-FIND successful.", results=len(results)); break # Success
                elif status_int == 0xFE00: log.warning("C-FIND Cancelled by remote."); break # Cancel
                else: # Failure
                    error_comment = status_dataset.get(Tag(0x0000, 0x0902), 'Unknown failure')
                    status_desc = f"Status 0x{status_int:04X}"
                    if status_int == 0xA700: status_desc="Out of Resources"
                    elif status_int == 0xA900: status_desc="Identifier Mismatch"
                    elif status_int == 0xC000: status_desc="Unable to Process"
                    log.error("C-FIND Failed", status=status_desc, comment=error_comment)
                    if status_int == 0xA900: raise InvalidParameterError(f"Query Mismatch ({status_desc})", ...)
                    else: raise RemoteQueryError(f"C-FIND Failed ({status_desc}): {error_comment}", ...)

            if assoc.is_established: assoc.release(); log.info("Association released.")
        else:
            log.error("Association rejected/aborted")
            raise RemoteConnectionError(f"Association failed with {remote_ae}", ...)

    # --- Exception Handling ---
    except RemoteConnectionError as rce: raise rce
    except RemoteQueryError as rqe: raise rqe
    except InvalidParameterError as ipe: raise ipe
    except QueryServiceError as qse: raise qse # Includes TLS setup errors
    except ssl.SSLError as e: # Catch SSL errors specifically during association
        log.error(f"TLS Handshake/SSL Error during C-FIND: {e}", exc_info=True)
        if assoc and not assoc.is_released and not assoc.is_aborted: assoc.abort()
        raise RemoteConnectionError(f"TLS Handshake Error: {e}", source_type="dimse-qr", source_id=source_id) from e
    except Exception as e: # Catch other unexpected errors
        log.error("Unexpected error during C-FIND operation", error=str(e), exc_info=True)
        if assoc and not assoc.is_released and not assoc.is_aborted: assoc.abort()
        raise QueryServiceError(f"C-FIND operation failed: {e}", source_type="dimse-qr", source_id=source_id) from e
    finally:
        # --- Cleanup Temporary TLS Files ---
        for file_path in temp_files_created:
            try:
                if os.path.exists(file_path): os.remove(file_path); log.debug(f"Cleaned up temporary TLS file: {file_path}")
            except OSError as rm_err: log.warning(f"Failed to clean up temporary TLS file {file_path}: {rm_err}")
        # Ensure association is closed
        if assoc and not assoc.is_released and not assoc.is_aborted:
             log.warning("Association found open in finally block, aborting.")
             try: assoc.abort()
             except Exception: pass # Ignore errors during abort
    return results


# --- QIDO Implementation (_execute_qido_query - UNCHANGED logic) ---
async def _execute_qido_query(
    source_config: models.DicomWebSourceState,
    query_params: Dict[str, str],
    query_level: QueryLevel,
    prioritize_custom_params: bool = False
) -> List[Dict[str, Any]]:
    """Executes a QIDO-RS query against the configured DICOMweb source."""
    log = logger.bind(
        source_name=source_config.source_name,
        source_id=source_config.id,
        base_url=source_config.base_url,
        query_level=query_level.value
    )
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


# --- get_source_info_for_response (UNCHANGED logic) ---
def get_source_info_for_response(
    db: Session,
    source_id: int,
    source_type: Literal["dicomweb", "dimse-qr"]
) -> Dict[str, str]:
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


# --- Main Service Function execute_query (UNCHANGED logic) ---
async def execute_query(
    db: Session,
    source_id: int,
    source_type: Literal["dicomweb", "dimse-qr"],
    query_params: List[DataBrowserQueryParam],
    query_level: QueryLevel
) -> DataBrowserQueryResponse:
    """
    Executes a query against a specified configured source based on ID and TYPE.
    """
    source_config: Any = None # Use Any temporarily, will be specific type later
    source_name = "Unknown" # Default

    log = logger.bind(
        source_id=source_id,
        source_type=source_type,
        query_level=query_level.value
    )
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
             # --- Calls the C-FIND function modified for TLS ---
             results_list = await _execute_cfind_query(source_config, find_identifier, query_level)
             # ----------------------------------------------------
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
