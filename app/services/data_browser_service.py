# app/services/data_browser_service.py
# Use structlog if available

from typing import List, Dict, Any, Optional, Generator, Tuple, Literal
from datetime import date, timedelta, datetime, timezone
import re
import tempfile
import os
import ssl
from pathlib import Path # Ensure Path is imported if not already

# Pynetdicom specific imports
from pynetdicom.ae import ApplicationEntity as AE
from pynetdicom.association import Association # Add this import
from pynetdicom import evt # Keep evt import as is
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind, # type: ignore[attr-defined]
    StudyRootQueryRetrieveInformationModelFind, # type: ignore[attr-defined]
    Verification # type: ignore[attr-defined]
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
# Ensure GoogleHealthcareSource model is imported if you plan to use it
from app.db.models.google_healthcare_source import GoogleHealthcareSource
from app.core.config import settings
from app.services import dicomweb_client
from app.worker.dimse_qr_poller import _resolve_dynamic_date_filter

import structlog # type: ignore
logger = structlog.get_logger(__name__)
pynetdicom_debug_logger_config = structlog.stdlib.ProcessorFormatter.wrap_for_formatter # For pynetdicom config later

# --- GCP Secret Manager Imports (similar to dicom_cstore.py) ---
GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY_DB_SVC = False # Default, specific to this module

# Define default dummy versions first
class _SecretManagerErrorDBService(Exception): pass
class _SecretNotFoundErrorDBService(_SecretManagerErrorDBService): pass
class _PermissionDeniedErrorDBService(_SecretManagerErrorDBService): pass

class _DummyGcpUtilsDataBrowser:
    GCP_SECRET_MANAGER_AVAILABLE = False
    SecretManagerError = _SecretManagerErrorDBService
    SecretNotFoundError = _SecretNotFoundErrorDBService
    PermissionDeniedError = _PermissionDeniedErrorDBService
    def get_secret(self, *args, **kwargs):
        raise self.SecretManagerError("gcp_utils module not imported in data_browser_service, cannot get_secret.")
    def _get_sm_client(self):
        raise self.SecretManagerError("gcp_utils module not imported in data_browser_service, cannot _get_sm_client.")

gcp_utils_db_svc = _DummyGcpUtilsDataBrowser() # type: ignore

try:
    from app.core import gcp_utils as _real_gcp_utils_db_svc
    from app.core.gcp_utils import (
        SecretManagerError as RealSecretManagerError,
        SecretNotFoundError as RealSecretNotFoundError,
        PermissionDeniedError as RealPermissionDeniedError
    )
    gcp_utils_db_svc = _real_gcp_utils_db_svc # type: ignore
    # Re-assign local exceptions to the real ones
    _SecretManagerErrorDBService = RealSecretManagerError # type: ignore[no-redef,misc]
    _SecretNotFoundErrorDBService = RealSecretNotFoundError # type: ignore[no-redef,misc]
    _PermissionDeniedErrorDBService = RealPermissionDeniedError # type: ignore[no-redef,misc]
    GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY_DB_SVC = True
    logger.info("data_browser_service.py: Successfully imported app.core.gcp_utils.")
except ImportError:
    logger.error(
        "data_browser_service.py: CRITICAL - Failed to import app.core.gcp_utils. Secret Manager functionality for DIMSE TLS will be UNAVAILABLE."
    )
# --- END GCP Secret Manager Imports ---


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
    log = logger.bind(query_level=query_level.value) # type: ignore[attr-defined] # Add level to logs

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
                      keyword = tag.keyword or keyword # type: ignore[attr-defined] # Use keyword if found, else keep original tag string? Be careful here.
                 # If not tag format, assume keyword
                 elif not Tag(keyword).is_valid: # type: ignore[attr-defined] # Quick check if it's a valid keyword string
                       raise ValueError("Not a valid keyword") # Treat as invalid if not tag or known keyword

            except Exception:
                 # Fallback: If not a valid known keyword or tag format, treat as is but warn
                 logger.warning("C-FIND: Field is not a standard DICOM keyword or recognized tag format, using as provided.", field=param.field) # type: ignore
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
                logger.warning("QIDO: Skipping invalid dynamic date filter", field=param.field, value=param.value) # type: ignore
                continue
            value_to_set = resolved_value
        qido_dict[param.field] = value_to_set
    logger.debug("Constructed QIDO parameters", qido_params=qido_dict) # type: ignore
    return qido_dict

# --- Helper functions for DIMSE TLS with Secret Manager ---
def _fetch_and_write_secret_for_cfind(secret_id: str, suffix: str, context_log) -> str:
    """Fetches a secret and writes it to a temporary file for C-FIND."""
    if not GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY_DB_SVC:
        context_log.error("Cannot fetch secret for C-FIND: GCP Utils module was not imported.")
        raise RemoteConnectionError("Cannot fetch secret: GCP Utils module not available for C-FIND.")
    
    context_log.debug("Fetching secret for C-FIND via gcp_utils.", secret_id_to_fetch=secret_id)
    try:
        # Assuming settings.VERTEX_AI_PROJECT holds your general GCP project ID for secrets
        project_id_for_secret = settings.VERTEX_AI_PROJECT
        if not project_id_for_secret:
            context_log.error("VERTEX_AI_PROJECT not configured in settings. Cannot fetch secret.")
            raise RemoteConnectionError("GCP Project ID not configured for fetching secrets.")

        secret_string = gcp_utils_db_svc.get_secret(secret_id=secret_id, project_id=project_id_for_secret)
        secret_bytes = secret_string.encode('utf-8')
        
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="cfind_scu_tls_")
        tf.write(secret_bytes)
        tf.close()
        temp_path = tf.name
        context_log.debug("Secret for C-FIND written to temp file.", temp_file_path=temp_path)
        if suffix == "-key.pem": # Or other specific key suffixes
            try:
                os.chmod(temp_path, 0o600)
            except OSError as chmod_err:
                context_log.warning("Could not set permissions on temp key file for C-FIND.", error=str(chmod_err))
        return temp_path
    except (_SecretManagerErrorDBService, _SecretNotFoundErrorDBService, _PermissionDeniedErrorDBService, ValueError) as sm_err:
        context_log.error("Failed to fetch secret for C-FIND.", error=str(sm_err), secret_id_attempted=secret_id)
        raise RemoteConnectionError(f"Failed to fetch required TLS secret '{secret_id}' for C-FIND: {sm_err}") from sm_err
    except (IOError, OSError) as file_err:
        context_log.error("Failed to write secret to temp file for C-FIND.", error=str(file_err))
        raise RemoteConnectionError(f"Failed to write TLS secret '{secret_id}' to file for C-FIND: {file_err}") from file_err

def _get_tls_temp_files_for_cfind(source_config: models.DimseQueryRetrieveSource, context_log) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Gets temporary file paths for TLS certs fetched from Secret Manager for C-FIND."""
    ca_cert_path: Optional[str] = None
    client_cert_path: Optional[str] = None
    client_key_path: Optional[str] = None

    # These field names are assumed to be on the DimseQueryRetrieveSource model
    # e.g., tls_ca_cert_secret_name, tls_client_cert_secret_name, tls_client_key_secret_name
    
    if not hasattr(source_config, 'tls_ca_cert_secret_name'):
        context_log.error("DimseQueryRetrieveSource model is missing 'tls_ca_cert_secret_name' attribute.")
        raise RemoteConnectionError("Source configuration error: missing CA cert secret name attribute.")

    if source_config.tls_ca_cert_secret_name:
        ca_cert_path = _fetch_and_write_secret_for_cfind(source_config.tls_ca_cert_secret_name, "-ca.pem", context_log)
    else: # CA is typically required for server auth
        context_log.warning("TLS CA certificate secret name is not configured for C-FIND. Server auth may fail or be insecure.")
        # Depending on policy, you might want to raise an error here if CA is strictly required.
        # For now, allow proceeding, pynetdicom might handle it or fail.

    if hasattr(source_config, 'tls_client_cert_secret_name') and source_config.tls_client_cert_secret_name and \
       hasattr(source_config, 'tls_client_key_secret_name') and source_config.tls_client_key_secret_name:
        client_cert_path = _fetch_and_write_secret_for_cfind(source_config.tls_client_cert_secret_name, "-cert.pem", context_log)
        client_key_path = _fetch_and_write_secret_for_cfind(source_config.tls_client_key_secret_name, "-key.pem", context_log)
    elif (hasattr(source_config, 'tls_client_cert_secret_name') and source_config.tls_client_cert_secret_name) or \
         (hasattr(source_config, 'tls_client_key_secret_name') and source_config.tls_client_key_secret_name):
        context_log.warning("For mTLS in C-FIND, both client cert and key secret names must be provided. Proceeding without client cert/key.")
        
    return ca_cert_path, client_cert_path, client_key_path
# --- END Helper functions for DIMSE TLS ---


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
    assoc: Optional[Association] = None # Corrected type hint for assoc
    host = source_config.remote_host
    port = source_config.remote_port
    remote_ae = source_config.remote_ae_title
    source_id = source_config.id
    source_name = source_config.name

    # Bind source_id and source_name earlier for consistent logging context
    log_context = logger.bind(
        remote_ae=remote_ae, remote_host=host, remote_port=port,
        local_ae=source_config.local_ae_title, source_name=source_name,
        source_id=source_id, query_level=query_level.value
    ) # type: ignore[attr-defined]  # This ignore should handle the Pylance warning for logger.bind
    log_context.info("Attempting C-FIND association")

    ae.acse_timeout = settings.DIMSE_ACSE_TIMEOUT
    ae.dimse_timeout = settings.DIMSE_DIMSE_TIMEOUT
    ae.network_timeout = settings.DIMSE_NETWORK_TIMEOUT
    # max_pdu_size is not defined on source_config or in global settings.
    # Pynetdicom's default (0 - no specific limit proposed) will be used.
    # If you need to set a specific max_pdu_size, consider adding it to global settings
    # and then uncommenting and modifying the line below:
    # ae.maximum_pdu_size = settings.DIMSE_MAX_PDU_SIZE


    temp_tls_files_cfind: List[str] = []
    ssl_context_cfind: Optional[ssl.SSLContext] = None

    try:
        if source_config.tls_enabled:
            log_context.info("TLS enabled for C-FIND. Preparing TLS context.")
            try:
                ca_file, client_cert_file, client_key_file = _get_tls_temp_files_for_cfind(source_config, log_context)
                if ca_file: temp_tls_files_cfind.append(ca_file)
                if client_cert_file: temp_tls_files_cfind.append(client_cert_file)
                if client_key_file: temp_tls_files_cfind.append(client_key_file)

                if not ca_file and not (client_cert_file and client_key_file): # Check if any TLS files were actually fetched
                    log_context.warning("TLS enabled but no CA cert or client cert/key pair could be fetched. Attempting association without specific TLS params, may fail or be insecure.")
                else:
                    ssl_context_cfind = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ssl_context_cfind.check_hostname = False # Typically false for DICOM unless hostname matches AE title/config
                    ssl_context_cfind.verify_mode = ssl.CERT_REQUIRED if ca_file else ssl.CERT_NONE
                    
                    if ca_file:
                        ssl_context_cfind.load_verify_locations(cafile=ca_file)
                    
                    if client_cert_file and client_key_file:
                        ssl_context_cfind.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
                    log_context.debug("SSLContext for C-FIND prepared.")

            except RemoteConnectionError as tls_prep_err: # Catch errors from fetching/writing secrets
                log_context.error("Failed to prepare TLS files for C-FIND.", error=str(tls_prep_err))
                raise # Re-raise to be caught by the main try-except
            except ssl.SSLError as ssl_err:
                log_context.error("SSLContext configuration error for C-FIND.", error=str(ssl_err))
                raise RemoteConnectionError(f"SSLContext config error for C-FIND: {ssl_err}") from ssl_err
            except Exception as e:
                log_context.error("Unexpected error during TLS setup for C-FIND.", error=str(e), exc_info=True)
                raise RemoteConnectionError(f"Unexpected TLS setup error for C-FIND: {e}") from e
        
        log_context.debug("Attempting to associate.", tls_enabled=source_config.tls_enabled, has_ssl_context=bool(ssl_context_cfind))
        
        tls_arguments: Optional[Tuple[ssl.SSLContext, Optional[str]]] = None
        if ssl_context_cfind:
            # If check_hostname were True in the SSLContext, you might pass `host` here.
            # Since it's False, None is appropriate for the hostname verification part.
            tls_arguments = (ssl_context_cfind, None) 

        if tls_arguments:
            assoc = ae.associate(host, port, ae_title=remote_ae, tls_args=tls_arguments) # type: ignore[arg-type]
        else:
            assoc = ae.associate(host, port, ae_title=remote_ae)

        if assoc: # Check if association object was created
            if assoc.is_established:
                log_context.info("Association established for C-FIND.")
                responses = assoc.send_c_find(query_identifier, find_sop_class)
                response_count = 0
                for (status_dataset, result_identifier) in responses:
                    if status_dataset is None:
                         log_context.warning("Received C-FIND response with no status dataset.")
                         continue
                    if not hasattr(status_dataset, 'Status'):
                         log_context.error("Received C-FIND response missing Status tag.")
                         # Release association before raising
                         if assoc.is_established: assoc.release()
                         raise RemoteQueryError("Invalid C-FIND response (Missing Status)", source_type="dimse-qr", source_id=source_id)

                    status_int = int(status_dataset.Status)
                    log_context.debug("C-FIND Response Status", status=f"0x{status_int:04X}")

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
                                log_context.debug("C-FIND Pending Result Received", result_number=response_count)
                            except Exception as json_err:
                                 log_context.warning("Failed to convert C-FIND result identifier to JSON", error=str(json_err), result_identifier=str(result_identifier))
                        else:
                             log_context.warning("C-FIND Pending status received without identifier dataset.")
                    elif status_int == 0x0000: # Success
                        log_context.info("C-FIND successful.", total_results=len(results))
                        break
                    elif status_int == 0xFE00: # Cancel
                         log_context.warning("C-FIND Cancelled by remote AE.")
                         break
                    else: # Failure or other status
                        error_comment_tag = Tag(0x0000, 0x0902) # Error Comment Tag
                        error_comment = status_dataset.get(error_comment_tag, 'Unknown failure')
                        status_desc = f"Status: 0x{status_int:04X}"
                        if status_int == 0xA700: status_desc = "Out of Resources"
                        elif status_int == 0xA900: status_desc = "Identifier does not match SOP Class"
                        elif status_int == 0xC000: status_desc = "Unable to Process"
                        log_context.error("C-FIND Failed", status_code=f"0x{status_int:04X}", status_description=status_desc, error_comment=error_comment)
                        # Release association before raising
                        # No need to check assoc here again, as we are inside 'if assoc:'
                        if assoc.is_established: assoc.release()
                        # Raise specific errors for known failure codes
                        if status_int == 0xA900:
                             raise InvalidParameterError(f"Query parameters do not match SOP Class ({status_desc})", source_type="dimse-qr", source_id=source_id)
                        else:
                             raise RemoteQueryError(f"C-FIND Failed ({status_desc}): {error_comment}", source_type="dimse-qr", source_id=source_id)

                # Release association after loop finishes (if not already released on error)
                # No need to check assoc here again
                if assoc.is_established:
                    assoc.release()
                    log_context.info("Association released.")
            else: # assoc is not None, but not established
                log_context.error("Association rejected/aborted")
                raise RemoteConnectionError(f"Association failed with {remote_ae}", source_type="dimse-qr", source_id=source_id)
        else:
            log_context.error("Association object is None. Association failed to initialize.")
            raise RemoteConnectionError("Failed to initialize association object.", source_type="dimse-qr", source_id=source_id)

    except RemoteConnectionError as rce: raise rce
    except RemoteQueryError as rqe: raise rqe
    except InvalidParameterError as ipe: raise ipe
    except ssl.SSLError as e: # Catch SSL errors specifically during association
        log_context.error("TLS/SSL Error during C-FIND association", error=str(e), exc_info=True)
        raise RemoteConnectionError(f"TLS/SSL Error with {remote_ae}: {e}", source_type="dimse-qr", source_id=source_id) from e
    except Exception as e:
        log_context.error("Error during C-FIND operation", error=str(e), exc_info=True)
        if assoc and assoc.is_established and not assoc.is_released: # Check if established before trying to abort
            try: assoc.abort()
            except Exception as abort_exc: log_context.error("Exception during C-FIND abort", abort_error=str(abort_exc))
        elif assoc and not assoc.is_established: # If assoc object exists but not established (e.g. rejected)
            pass # Nothing to abort or release typically
        raise QueryServiceError(f"C-FIND operation failed: {e}", source_type="dimse-qr", source_id=source_id) from e
    finally:
        for file_path in temp_tls_files_cfind:
            try:
                if file_path and os.path.exists(file_path):
                    os.remove(file_path)
                    log_context.debug("Cleaned up temp TLS file for C-FIND.", temp_file_path=file_path)
            except OSError as e:
                log_context.warning("Failed to clean up temp TLS file for C-FIND.", temp_file_path=file_path, error=str(e))
        # Ensure association is released or aborted if it exists and wasn't handled
        if assoc and not assoc.is_released and not assoc.is_aborted:
            log_context.debug("Ensuring association is aborted in finally block.")
            try:
                assoc.abort() # Use abort for ungraceful termination if not already released
            except Exception as final_abort_exc:
                log_context.error("Exception during final C-FIND abort", error=str(final_abort_exc))


    return results


# --- QIDO Implementation (Modified) ---
async def _execute_qido_query(
    source_config: models.DicomWebSourceState,
    query_params: Dict[str, str],
    query_level: QueryLevel,
    prioritize_custom_params: bool = False
) -> List[Dict[str, Any]]:
    """Executes a QIDO-RS query against the configured DICOMweb source."""
    log = logger.bind(source_name=source_config.source_name, source_id=source_config.id, base_url=source_config.base_url, query_level=query_level.value) # type: ignore[attr-defined]
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
def get_source_info_for_response(db: Session, source_id: int, source_type: Literal["dicomweb", "dimse-qr", "google_healthcare"]) -> Dict[str, str]: # MODIFIED Literal
    """Gets basic source info (name, type) based on ID AND type."""
    info = {"name": "Unknown", "type": source_type} # Default to provided type
    try:
        if source_type == "dimse-qr":
            config = crud.crud_dimse_qr_source.get(db, id=source_id)
            if config: info["name"] = str(config.name) # Ensure string conversion
        elif source_type == "dicomweb":
            # Ensure you are using the correct CRUD object for DicomWebSourceState
            config = crud.dicomweb_source.get(db, id=source_id) # MODIFIED: dicomweb_source_state -> dicomweb_source
            if config: info["name"] = str(config.source_name) # Ensure string conversion
        elif source_type == "google_healthcare":
            config = crud.google_healthcare_source.get(db, id=source_id) # Assuming crud.google_healthcare_source exists
            if config: info["name"] = str(config.name) # Or appropriate name attribute
        else:
            # Should not happen if request validation uses Literal
             logger.warning("get_source_info called with invalid source_type", provided_type=source_type, source_id=source_id) # type: ignore
             info["type"] = "Unknown"

    except Exception as e:
        logger.error("Failed to retrieve source info for error response", source_id=source_id, source_type=source_type, error=str(e)) # type: ignore
    return info


# --- Main Service Function execute_query (MODIFIED) ---
async def execute_query(
    db: Session,
    source_id: int,
    source_type: Literal["dicomweb", "dimse-qr", "google_healthcare"], # MODIFIED Literal
    query_params: List[DataBrowserQueryParam],
    query_level: QueryLevel
) -> DataBrowserQueryResponse:
    """
    Executes a query against a specified configured source based on ID and TYPE.
    """
    source_config: Any = None
    source_name = "Unknown"

    log = logger.bind(source_id=source_id, source_type=source_type, query_level=query_level.value) # type: ignore[attr-defined]
    log.info("Executing data browser query")

    # 1. Fetch config based on EXPLICIT type
    try:
        if source_type == "dimse-qr":
            source_config = crud.crud_dimse_qr_source.get(db, id=source_id)
            if source_config: source_name = source_config.name
        elif source_type == "dicomweb":
            source_config = crud.dicomweb_source.get(db, id=source_id) # MODIFIED: dicomweb_source_state -> dicomweb_source
            if source_config: source_name = source_config.source_name
        elif source_type == "google_healthcare":
            source_config = crud.google_healthcare_source.get(db, id=source_id) # Assuming crud.google_healthcare_source exists
            if source_config: source_name = source_config.name # Or appropriate name attribute
        else:
            # This case should be prevented by FastAPI validation using Literal
            # However, to satisfy Pylance if Literal isn't exhaustive, add a check or assert never
            log.error(f"Unsupported source_type encountered in execute_query: {source_type}")
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
             if not isinstance(source_config, models.DimseQueryRetrieveSource): raise TypeError("Config type mismatch for DIMSE Q/R")
             results_list = await _execute_cfind_query(source_config, find_identifier, query_level)
        elif source_type == "dicomweb":
            log.debug("Preparing QIDO parameters")
            qido_params_dict = _build_qido_params(query_params) # This returns Dict[str, str]
            if not isinstance(source_config, models.DicomWebSourceState): raise TypeError("Config type mismatch for DICOMweb")
            results_list = await _execute_qido_query(
                source_config,
                qido_params_dict, # Pass the dictionary here
                query_level,
                prioritize_custom_params=True
            )
        elif source_type == "google_healthcare":
            log.info(f"Google Healthcare query for source ID {source_id} - Not yet fully implemented.")
            # Placeholder: Fetch config and potentially build GHC specific query params
            # ghc_query_params = _build_ghc_params(query_params) # You'd need to create this helper
            if not isinstance(source_config, models.GoogleHealthcareSource): raise TypeError("Config type mismatch for Google Healthcare")
            
            # Actual GHC query logic would go here. For now, return empty.
            # Example: results_list = await _execute_ghc_query(source_config, ghc_query_params, query_level)
            results_list = []
            message = "Google Healthcare query executed (placeholder)."
            query_status = "success" # Or "partial" if some parts are implemented
            log.debug("Google Healthcare query (placeholder) finished.")
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