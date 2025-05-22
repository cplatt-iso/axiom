# app/services/network/dimse/scu_service.py
import ssl
import os
import tempfile
# import logging # logging is imported in the except block for structlog
from contextlib import contextmanager
from typing import Optional, List, Tuple, Dict, Any, Generator, Union

from pynetdicom.ae import ApplicationEntity as AE  # MODIFIED
from pynetdicom.association import Association  # MODIFIED
from pynetdicom.presentation import PresentationContext, build_context
from pynetdicom.sop_class import (  # MODIFIED
    SOPClass,

    StudyRootQueryRetrieveInformationModelFind, # type: ignore[attr-defined]
    StudyRootQueryRetrieveInformationModelMove, # type: ignore[attr-defined]
)
from pynetdicom.service_class import VerificationServiceClass

from pydicom.dataset import Dataset
from app.core.config import settings

# Attempt to import gcp_utils and its exceptions
try:
    from app.core import gcp_utils
    # We will use gcp_utils.SecretManagerError directly if this import succeeds
    GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY = True
except ImportError:
    # Define gcp_utils and its exceptions only if the import fails
    # This logger will be a standard Python logger at this point if structlog also fails
    import logging as _logging_fallback_gcp
    _logging_fallback_gcp.getLogger(__name__).error(
        "scu_service: CRITICAL - Failed to import app.core.gcp_utils. Secret Manager functionality will be UNAVAILABLE."
    )
    GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY = False
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass

    class gcp_utils_dummy_scu: # type: ignore
        GCP_SECRET_MANAGER_AVAILABLE = False
        # Make dummy exceptions available on the dummy module
        SecretManagerError = SecretManagerError
        SecretNotFoundError = SecretNotFoundError
        PermissionDeniedError = PermissionDeniedError
        def _get_sm_client(self):
            raise self.SecretManagerError("gcp_utils module not imported for SCU.")
        def get_secret(self, *args, **kwargs):
            raise self.SecretManagerError("gcp_utils module not imported for SCU, cannot get_secret.")
    gcp_utils = gcp_utils_dummy_scu() # type: ignore

# Setup logger (structlog preferred, fallback to standard logging)
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging # Fallback if structlog isn't there
    logger = logging.getLogger(__name__)


class DimseScuError(Exception):
    def __init__(self, message: str, remote_ae: Optional[str] = None, details: Optional[str] = None):
        self.remote_ae = remote_ae
        self.details = details
        full_message = f"DIMSE SCU Error"
        if remote_ae: full_message += f" (Target: {remote_ae})"
        full_message += f": {message}"
        if details: full_message += f" | Details: {details}"
        super().__init__(full_message)

class TlsConfigError(DimseScuError): pass
class AssociationError(DimseScuError): pass
class DimseCommandError(DimseScuError): pass


def _fetch_and_write_secret_scu(secret_id: str, suffix: str, log_context) -> str:
    log = log_context.bind(secret_id=secret_id) if hasattr(log_context, 'bind') else log_context
    
    if not GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY:
        log.error("Cannot fetch secret: GCP Utils module was not imported into scu_service.")
        # Use the locally defined SecretManagerError if gcp_utils didn't import
        raise TlsConfigError("Cannot fetch secret: GCP Utils module not available for SCU.")
    
    log.debug("Fetching secret via gcp_utils.get_secret for SCU...")
    try:
        project_id_for_secret = settings.VERTEX_AI_PROJECT 
        if not project_id_for_secret:
            raise TlsConfigError("Project ID for secrets (e.g. VERTEX_AI_PROJECT) not configured.")
        
        # This will call either the real gcp_utils.get_secret or the dummy one
        secret_string = gcp_utils.get_secret(secret_id=secret_id, project_id=project_id_for_secret)
        secret_bytes = secret_string.encode('utf-8')
        
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="dimse_scu_tls_")
        tf.write(secret_bytes)
        tf.close()
        temp_path = tf.name
        log.debug("Secret written to temp file for SCU.", path=temp_path)
        if suffix == "-key.pem":
            try:
                os.chmod(temp_path, 0o600)
            except OSError as chmod_err:
                log.warning("Could not set permissions on SCU temp key file.", error=str(chmod_err))
        return temp_path
    # Catch specific exceptions from gcp_utils (real or dummy)
    except (gcp_utils.SecretManagerError, gcp_utils.SecretNotFoundError, gcp_utils.PermissionDeniedError, ValueError) as sm_err:
        log.error("Failed to fetch secret for SCU using gcp_utils.get_secret.", error=str(sm_err))
        raise TlsConfigError(f"Failed to fetch SCU TLS secret '{secret_id}'", details=str(sm_err)) from sm_err
    except (IOError, OSError) as file_err:
        log.error("Failed to write SCU secret to temp file.", error=str(file_err))
        raise TlsConfigError(f"Failed to write SCU TLS secret '{secret_id}' to file", details=str(file_err)) from file_err


def _prepare_scu_tls_context(
    tls_ca_cert_secret: Optional[str],
    tls_client_cert_secret: Optional[str],
    tls_client_key_secret: Optional[str],
    log_context
) -> Tuple[Optional[ssl.SSLContext], List[str]]:
    log = log_context
    temp_files_created: List[str] = []
    ca_cert_file: Optional[str] = None
    client_cert_file: Optional[str] = None
    client_key_file: Optional[str] = None
    ssl_context: Optional[ssl.SSLContext] = None

    log.info("Preparing SCU TLS context...")

    if not GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY:
        # This path means gcp_utils is the dummy one.
        # Its _get_sm_client will raise its own dummy SecretManagerError.
        pass # No need to raise TlsConfigError here, _get_sm_client will handle it if called.
    
    # This block will use the real gcp_utils or the dummy one
    try:
        gcp_utils._get_sm_client() # Attempt on-demand client initialization
        if not gcp_utils.GCP_SECRET_MANAGER_AVAILABLE: # Check the flag on the (real or dummy) gcp_utils
            raise TlsConfigError("Secret Manager unavailable for TLS: GCP SM Client not available or failed init.")
    except gcp_utils.SecretManagerError as e: # Catch the (real or dummy) SecretManagerError
        raise TlsConfigError(f"Secret Manager client initialization failed for SCU TLS: {e}")


    if not tls_ca_cert_secret: 
        raise TlsConfigError("TLS CA certificate secret name is required for SCU verification.")

    try:
        ca_cert_file = _fetch_and_write_secret_scu(tls_ca_cert_secret, "-ca.pem", log)
        temp_files_created.append(ca_cert_file)

        has_client_cert = bool(tls_client_cert_secret)
        has_client_key = bool(tls_client_key_secret)
        if has_client_cert and has_client_key:
            client_cert_file = _fetch_and_write_secret_scu(tls_client_cert_secret, "-cert.pem", log) # type: ignore
            temp_files_created.append(client_cert_file)
            client_key_file = _fetch_and_write_secret_scu(tls_client_key_secret, "-key.pem", log) # type: ignore
            temp_files_created.append(client_key_file)
        elif has_client_cert != has_client_key:
             raise TlsConfigError("Both client cert and key secrets required for mTLS, or neither.")

        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_file)
        ssl_context.check_hostname = False 
        ssl_context.verify_mode = ssl.CERT_REQUIRED 

        if client_cert_file and client_key_file:
            ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)

        log.info("SCU SSLContext configured successfully.")
        return ssl_context, temp_files_created
    except (TlsConfigError, ssl.SSLError) as e:
        for path in temp_files_created:
            try:
                if path and os.path.exists(path): os.remove(path)
            except OSError: pass
        if isinstance(e, TlsConfigError): raise
        else: raise TlsConfigError("SSL configuration error for SCU", details=str(e)) from e
    except Exception as e:
        for path in temp_files_created:
            try:
                if path and os.path.exists(path): os.remove(path)
            except OSError: pass
        raise TlsConfigError("Unexpected SCU TLS setup error", details=str(e)) from e


@contextmanager
def manage_association(
    remote_host: str,
    remote_port: int,
    remote_ae_title: str,
    local_ae_title: str = "AXIOM_SCU",
    contexts: Optional[List[PresentationContext]] = None,
    tls_enabled: bool = False,
    tls_ca_cert_secret: Optional[str] = None,
    tls_client_cert_secret: Optional[str] = None,
    tls_client_key_secret: Optional[str] = None
) -> Generator[Association, None, None]:
    current_log_context = {
        "remote_ae": remote_ae_title, "remote_host": remote_host, "remote_port": remote_port,
        "local_ae": local_ae_title, "tls_enabled": tls_enabled
    }
    if hasattr(logger, 'bind'):
        log = logger.bind(**current_log_context) # type: ignore[attr-defined]
    else:
        log = logger
    
    if not remote_host or not remote_port or not remote_ae_title:
        raise ValueError("Remote host, port, and AE title are required.")

    ae = AE(ae_title=local_ae_title)
    if contexts:
         ae.requested_contexts = contexts
    
    ae.acse_timeout = settings.DIMSE_ACSE_TIMEOUT
    ae.dimse_timeout = settings.DIMSE_DIMSE_TIMEOUT
    ae.network_timeout = settings.DIMSE_NETWORK_TIMEOUT

    assoc: Optional[Association] = None
    ssl_context_scu: Optional[ssl.SSLContext] = None 
    temp_files_created_scu: List[str] = [] 

    try:
        if tls_enabled:
            ssl_context_scu, temp_files_created_scu = _prepare_scu_tls_context(
                tls_ca_cert_secret=tls_ca_cert_secret,
                tls_client_cert_secret=tls_client_cert_secret,
                tls_client_key_secret=tls_client_key_secret,
                log_context=log
            )

        # For tls_args, pynetdicom expects (SSLContext, hostname_str) or None.
        # If check_hostname is False on the context, the hostname_str might not be strictly used by pynetdicom's OpenSSL backend for verification,
        # but the type hint for pynetdicom might still expect a string or None.
        # Providing remote_host is safer if check_hostname were true.
        # Given check_hostname = False, None should be fine for the second element.
        tls_args_scu = (ssl_context_scu, remote_host if ssl_context_scu and ssl_context_scu.check_hostname else None) if tls_enabled and ssl_context_scu else None
        assoc = ae.associate(remote_host, remote_port, ae_title=remote_ae_title, tls_args=tls_args_scu) # type: ignore[arg-type] # If None is truly fine

        if assoc.is_established:
            yield assoc
        else:
            reason = "Unknown"
            if assoc.is_rejected: reason = f"Rejected by {getattr(assoc,'result_source','N/A')}, code {getattr(assoc,'result_reason','N/A')}"
            elif assoc.is_aborted: reason = "Aborted"
            raise AssociationError(f"Association failed: {reason}", remote_ae=remote_ae_title)

    except (TlsConfigError, AssociationError, ValueError): raise
    except ssl.SSLError as e: raise AssociationError("TLS Handshake Error", remote_ae=remote_ae_title, details=str(e)) from e
    except Exception as e: raise AssociationError("Unexpected association error", remote_ae=remote_ae_title, details=str(e)) from e
    finally:
        if assoc and assoc.is_established:
            assoc.release()
        elif assoc and not assoc.is_released and not assoc.is_aborted:
             try: assoc.abort()
             except Exception: pass

        for file_path in temp_files_created_scu:
            try:
                if file_path and os.path.exists(file_path): os.remove(file_path)
            except OSError: pass


def find_studies(
    config: Dict[str, Any],
    identifier: Dataset,
    find_sop_class: Union[str, SOPClass] = StudyRootQueryRetrieveInformationModelFind
    ) -> List[Dataset]:
    current_log_context = {"remote_ae": config.get("remote_ae_title"), "operation": "C-FIND"}
    if hasattr(logger, 'bind'):
        log = logger.bind(**current_log_context) # type: ignore[attr-defined]
    else:
        log = logger
    
    results: List[Dataset] = []
    contexts = [build_context(find_sop_class)]

    try:
        with manage_association(
            remote_host=config["remote_host"], remote_port=config["remote_port"],
            remote_ae_title=config["remote_ae_title"], local_ae_title=config.get("local_ae_title","AXIOM_SCU"),
            contexts=contexts, tls_enabled=config.get("tls_enabled",False),
            tls_ca_cert_secret=config.get("tls_ca_cert_secret_name"),
            tls_client_cert_secret=config.get("tls_client_cert_secret_name"),
            tls_client_key_secret=config.get("tls_client_key_secret_name")
        ) as assoc:
            responses = assoc.send_c_find(identifier, find_sop_class)
            success_or_pending_received = False
            for status_dataset, result_identifier in responses:
                if status_dataset is None: continue
                if not hasattr(status_dataset,'Status'): raise DimseCommandError("Invalid C-FIND response (No Status)",remote_ae=config["remote_ae_title"])
                status_int = int(status_dataset.Status)
                if status_int in (0xFF00, 0xFF01): # Pending
                    success_or_pending_received = True
                    if result_identifier: results.append(result_identifier)
                elif status_int == 0x0000: success_or_pending_received = True; break # Success
                elif status_int == 0xFE00: success_or_pending_received = True; raise DimseCommandError("C-FIND Cancelled (0xFE00)",remote_ae=config["remote_ae_title"])
                else: success_or_pending_received = True; error_comment=status_dataset.get((0x0000,0x0902),'Unknown'); raise DimseCommandError(f"C-FIND Failed (0x{status_int:04X})",remote_ae=config["remote_ae_title"],details=str(error_comment))
            if not success_or_pending_received: raise DimseCommandError("No valid C-FIND status received",remote_ae=config["remote_ae_title"])
        return results
    except (TlsConfigError,AssociationError,DimseCommandError,ValueError) as e: raise
    except Exception as e: raise DimseScuError("Unexpected C-FIND service error",remote_ae=config.get("remote_ae_title"),details=str(e)) from e


def move_study(
    config: Dict[str, Any],
    study_instance_uid: str,
    move_destination_ae: str,
    move_sop_class: Union[str, SOPClass] = StudyRootQueryRetrieveInformationModelMove
    ) -> Dict[str, Any]:
    current_log_context = {
        "remote_ae": config.get("remote_ae_title"), "operation": "C-MOVE",
        "study_uid": study_instance_uid, "move_destination": move_destination_ae
    }
    if hasattr(logger, 'bind'):
        log = logger.bind(**current_log_context) # type: ignore[attr-defined]
    else:
        log = logger

    contexts = [build_context(move_sop_class)]
    identifier = Dataset(); identifier.QueryRetrieveLevel="STUDY"; identifier.StudyInstanceUID=study_instance_uid
    final_status = -1; sub_ops_summary={"completed":0,"failed":0,"warning":0}

    try:
        with manage_association(
            remote_host=config["remote_host"],remote_port=config["remote_port"],
            remote_ae_title=config["remote_ae_title"],local_ae_title=config.get("local_ae_title","AXIOM_SCU"),
            contexts=contexts,tls_enabled=config.get("tls_enabled",False),
            tls_ca_cert_secret=config.get("tls_ca_cert_secret_name"),
            tls_client_cert_secret=config.get("tls_client_cert_secret_name"),
            tls_client_key_secret=config.get("tls_client_key_secret_name")
        ) as assoc:
            responses = assoc.send_c_move(identifier, move_destination_ae, move_sop_class)
            for status_dataset, response_identifier in responses: # response_identifier is usually None for C-MOVE
                if status_dataset is None: continue
                if not hasattr(status_dataset,'Status'): raise DimseCommandError("Invalid C-MOVE response (No Status)",remote_ae=config["remote_ae_title"])
                status_int=int(status_dataset.Status); final_status=status_int
                comp=status_dataset.get("NumberOfCompletedSuboperations",0); fail=status_dataset.get("NumberOfFailedSuboperations",0); warn=status_dataset.get("NumberOfWarningSuboperations",0)
                sub_ops_summary["completed"]=max(sub_ops_summary["completed"],comp); sub_ops_summary["failed"]=max(sub_ops_summary["failed"],fail); sub_ops_summary["warning"]=max(sub_ops_summary["warning"],warn)
                if status_int == 0x0000: break # Success
                elif status_int in (0xFF00, 0xFF01): continue # Pending
                elif status_int == 0xFE00: raise DimseCommandError("C-MOVE Cancelled (0xFE00)",remote_ae=config["remote_ae_title"])
                elif status_int == 0xB000: continue # Sub-ops complete, no failures or warnings
                else: error_comment=status_dataset.get((0x0000,0x0902),'No comment'); details=f"Status:0x{status_int:04X}. Comp:{comp},Fail:{fail},Warn:{warn}. Comment:{error_comment}"; raise DimseCommandError("C-MOVE Failed",remote_ae=config["remote_ae_title"],details=details)
            if final_status != 0x0000: details=f"LastStatus:0x{final_status:04X}. Comp:{sub_ops_summary['completed']},Fail:{sub_ops_summary['failed']},Warn:{sub_ops_summary['warning']}."; raise DimseCommandError("C-MOVE no success status",remote_ae=config["remote_ae_title"],details=details)
        return {"status":"success","message":"C-MOVE successful","sub_operations":sub_ops_summary}
    except (TlsConfigError,AssociationError,DimseCommandError,ValueError) as e: raise
    except Exception as e: raise DimseScuError("Unexpected C-MOVE error",remote_ae=config.get("remote_ae_title"),details=str(e)) from e
