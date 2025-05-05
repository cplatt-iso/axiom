# app/services/network/dimse/scu_service.py

import ssl
import os
import tempfile
import logging
from contextlib import contextmanager
from typing import Optional, List, Tuple, Dict, Any, Generator, Union

# Pynetdicom imports
from pynetdicom import AE, Association
from pynetdicom.presentation import PresentationContext, build_context
from pynetdicom.sop_class import (
    _SERVICE_CLASSES, # Access underlying dict for convenience if needed
    SOPClass, # Import base SOPClass type for type hinting
    StudyRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelMove,
    Verification
)
from pydicom.dataset import Dataset
from pydicom.uid import UID # Import UID type if needed for validation

# Application imports
from app.core.config import settings

# Secret Manager Imports
try:
    from app.core.gcp_utils import (
        fetch_secret_content,
        SecretManagerError,
        SecretNotFoundError,
        PermissionDeniedError
    )
    SECRET_MANAGER_ENABLED = True
except ImportError:
    logging.getLogger(__name__).warning("scu_service: GCP Utils / Secret Manager not available. TLS secret fetching will fail.")
    SECRET_MANAGER_ENABLED = False
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass
    def fetch_secret_content(*args, **kwargs): raise NotImplementedError("Secret Manager fetch not available")

# Configure logging
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning("scu_service: structlog not found, using standard logging.")


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


def _fetch_and_write_secret(secret_name: str, suffix: str, log_context) -> str:
    log = log_context.bind(secret_name=secret_name) if hasattr(log_context, 'bind') else log_context
    log.debug("Fetching secret...")
    try:
        secret_bytes = fetch_secret_content(secret_name)
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="dimse_scu_tls_")
        tf.write(secret_bytes)
        tf.close()
        temp_path = tf.name
        log.debug("Secret written to temp file", path=temp_path)
        if suffix == "-key.pem":
            try:
                os.chmod(temp_path, 0o600)
                log.debug("Set permissions for key file")
            except OSError as chmod_err:
                log.warning("Could not set permissions on temp key file", error=str(chmod_err))
        return temp_path
    except (SecretManagerError, SecretNotFoundError, PermissionDeniedError) as sm_err:
        log.error("Failed to fetch secret", error=str(sm_err), exc_info=True)
        raise TlsConfigError(f"Failed to fetch required TLS secret '{secret_name}'", details=str(sm_err)) from sm_err
    except (IOError, OSError) as file_err:
        log.error("Failed to write secret to temp file", error=str(file_err), exc_info=True)
        raise TlsConfigError(f"Failed to write TLS secret '{secret_name}' to file", details=str(file_err)) from file_err


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
    if not SECRET_MANAGER_ENABLED: raise TlsConfigError("Secret Manager unavailable for TLS.")
    if not tls_ca_cert_secret: raise TlsConfigError("TLS CA certificate secret name is required for SCU verification.")

    try:
        ca_cert_file = _fetch_and_write_secret(tls_ca_cert_secret, "-ca.pem", log)
        temp_files_created.append(ca_cert_file)

        has_client_cert = bool(tls_client_cert_secret)
        has_client_key = bool(tls_client_key_secret)
        if has_client_cert and has_client_key:
            log.info("Client cert/key provided, attempting mTLS setup...")
            client_cert_file = _fetch_and_write_secret(tls_client_cert_secret, "-cert.pem", log)
            temp_files_created.append(client_cert_file)
            client_key_file = _fetch_and_write_secret(tls_client_key_secret, "-key.pem", log)
            temp_files_created.append(client_key_file)
        elif has_client_cert != has_client_key:
             raise TlsConfigError("Both client cert and key secrets required for mTLS, or neither.")

        log.debug("Creating ssl.SSLContext for SCU...")
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_file)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_REQUIRED

        if client_cert_file and client_key_file:
            log.debug("Loading client cert chain into SSLContext for mTLS...")
            ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)

        log.info("SCU SSLContext configured successfully.")
        return ssl_context, temp_files_created

    except (TlsConfigError, ssl.SSLError) as e:
        log.error("Failed to prepare SCU TLS context", error=str(e), exc_info=True)
        for path in temp_files_created:
            try:
                if path and os.path.exists(path): os.remove(path)
            except OSError as rm_err: log.warning(f"Failed cleanup during TLS error handling", path=path, error=str(rm_err))
        if isinstance(e, TlsConfigError): raise
        else: raise TlsConfigError("SSL configuration error", details=str(e)) from e
    except Exception as e:
        log.error("Unexpected error preparing SCU TLS context", error=str(e), exc_info=True)
        for path in temp_files_created:
            try:
                if path and os.path.exists(path): os.remove(path)
            except OSError as rm_err: log.warning(f"Failed cleanup during TLS error handling", path=path, error=str(rm_err))
        raise TlsConfigError("Unexpected TLS setup error", details=str(e)) from e


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
    log = logger.bind(
        remote_ae=remote_ae_title, remote_host=remote_host, remote_port=remote_port,
        local_ae=local_ae_title, tls_enabled=tls_enabled
    ) if hasattr(logger, 'bind') else logger
    log.debug("Managing DIMSE association...")

    if not remote_host or not remote_port or not remote_ae_title:
        raise ValueError("Remote host, port, and AE title are required.")

    ae = AE(ae_title=local_ae_title)
    if contexts:
         ae.requested_contexts = contexts
    else:
        log.warning("No presentation contexts specified for association.")

    ae.acse_timeout = settings.DIMSE_ACSE_TIMEOUT
    ae.dimse_timeout = settings.DIMSE_DIMSE_TIMEOUT
    ae.network_timeout = settings.DIMSE_NETWORK_TIMEOUT

    assoc: Optional[Association] = None
    ssl_context: Optional[ssl.SSLContext] = None
    temp_files_created: List[str] = []

    try:
        if tls_enabled:
            ssl_context, temp_files_created = _prepare_scu_tls_context(
                tls_ca_cert_secret=tls_ca_cert_secret,
                tls_client_cert_secret=tls_client_cert_secret,
                tls_client_key_secret=tls_client_key_secret,
                log_context=log
            )

        tls_args = (ssl_context, None) if tls_enabled and ssl_context else None
        log.info("Requesting association...")
        assoc = ae.associate(remote_host, remote_port, ae_title=remote_ae_title, tls_args=tls_args)

        if assoc.is_established:
            log.info("Association established successfully.")
            yield assoc
        else:
            reason = "Unknown"
            if assoc.is_rejected:
                reason_code = getattr(assoc, 'result_reason', 'N/A')
                reason_source = getattr(assoc, 'result_source', 'N/A')
                reason = f"Rejected by {reason_source}, code {reason_code}"
            elif assoc.is_aborted:
                reason = "Aborted"
            log.error("Association failed", reason=reason)
            raise AssociationError(f"Association failed: {reason}", remote_ae=remote_ae_title)

    except (TlsConfigError, AssociationError, ValueError):
         raise
    except ssl.SSLError as e:
        log.error(f"TLS Handshake/SSL Error during association: {e}", exc_info=True)
        raise AssociationError("TLS Handshake Error", remote_ae=remote_ae_title, details=str(e)) from e
    except Exception as e:
        log.error(f"Unexpected error during association management: {e}", exc_info=True)
        raise AssociationError(f"Unexpected association error", remote_ae=remote_ae_title, details=str(e)) from e
    finally:
        log.debug("Association context manager cleanup...")
        if assoc and assoc.is_established:
            log.debug("Releasing association...")
            assoc.release()
            log.info("Association released.")
        elif assoc and not assoc.is_released and not assoc.is_aborted:
             log.warning("Association neither established nor released/aborted cleanly. Aborting.")
             try: assoc.abort()
             except Exception: pass

        if temp_files_created:
            log.debug("Cleaning up temporary TLS files...", count=len(temp_files_created))
            for file_path in temp_files_created:
                try:
                    if file_path and os.path.exists(file_path):
                        os.remove(file_path)
                        log.debug(f"Removed temp TLS file: {file_path}")
                except OSError as rm_err:
                    log.warning("Failed to remove temporary TLS file", path=file_path, error=str(rm_err))


def find_studies(
    config: Dict[str, Any],
    identifier: Dataset,
    find_sop_class: Union[str, SOPClass] = StudyRootQueryRetrieveInformationModelFind # Default to object
    ) -> List[Dataset]:
    log = logger.bind(
        remote_ae=config.get("remote_ae_title"),
        operation="C-FIND"
    ) if hasattr(logger, 'bind') else logger
    log.info("Executing C-FIND operation via service")

    results: List[Dataset] = []
    # build_context accepts UID string or SOPClass object
    contexts = [build_context(find_sop_class)]

    try:
        with manage_association(
            remote_host=config["remote_host"],
            remote_port=config["remote_port"],
            remote_ae_title=config["remote_ae_title"],
            local_ae_title=config.get("local_ae_title", "AXIOM_SCU"),
            contexts=contexts,
            tls_enabled=config.get("tls_enabled", False),
            tls_ca_cert_secret=config.get("tls_ca_cert_secret_name"),
            tls_client_cert_secret=config.get("tls_client_cert_secret_name"),
            tls_client_key_secret=config.get("tls_client_key_secret_name")
        ) as assoc:
            log.info("Association acquired, sending C-FIND request...")
            # send_c_find accepts UID string or SOPClass object
            responses = assoc.send_c_find(identifier, find_sop_class)
            response_count = 0
            success_or_pending_received = False

            for status_dataset, result_identifier in responses:
                if status_dataset is None: log.warning("Received C-FIND response with no status."); continue
                if not hasattr(status_dataset, 'Status'):
                    raise DimseCommandError("Invalid C-FIND response (Missing Status)", remote_ae=config["remote_ae_title"])

                status_int = int(status_dataset.Status)
                log.debug("C-FIND Response Status", status=f"0x{status_int:04X}")

                if status_int in (0xFF00, 0xFF01): # Pending
                    success_or_pending_received = True
                    if result_identifier:
                        response_count += 1
                        results.append(result_identifier)
                        log.debug("C-FIND Pending Result Received", num=response_count)
                    else:
                        log.warning("C-FIND Pending status without identifier.")
                elif status_int == 0x0000: # Success
                    success_or_pending_received = True
                    log.info("C-FIND successful.", results=len(results));
                    break
                elif status_int == 0xFE00: # Cancel
                    success_or_pending_received = True
                    log.warning("C-FIND Cancelled by remote.");
                    raise DimseCommandError("C-FIND Cancelled by remote (0xFE00)", remote_ae=config["remote_ae_title"])
                else: # Failure
                    success_or_pending_received = True
                    error_comment = status_dataset.get((0x0000, 0x0902), 'Unknown failure')
                    log.error("C-FIND Failed", status=f"0x{status_int:04X}", comment=error_comment)
                    raise DimseCommandError(f"C-FIND Failed (Status: 0x{status_int:04X})", remote_ae=config["remote_ae_title"], details=str(error_comment))

            if not success_or_pending_received:
                raise DimseCommandError("No valid C-FIND status (Success/Pending/Failure) received", remote_ae=config["remote_ae_title"])

        log.info("C-FIND operation completed successfully via service.", result_count=len(results))
        return results

    except (TlsConfigError, AssociationError, DimseCommandError, ValueError) as e:
        log.error(f"C-FIND service failed: {e}", exc_info=True)
        raise
    except Exception as e:
        log.error(f"Unexpected error during C-FIND service execution: {e}", exc_info=True)
        raise DimseScuError("Unexpected C-FIND service error", remote_ae=config.get("remote_ae_title"), details=str(e)) from e


def move_study(
    config: Dict[str, Any],
    study_instance_uid: str,
    move_destination_ae: str,
    move_sop_class: Union[str, SOPClass] = StudyRootQueryRetrieveInformationModelMove # Default to object
    ) -> Dict[str, Any]:
    log = logger.bind(
        remote_ae=config.get("remote_ae_title"),
        operation="C-MOVE",
        study_uid=study_instance_uid,
        move_destination=move_destination_ae
    ) if hasattr(logger, 'bind') else logger
    log.info("Executing C-MOVE operation via service")

    contexts = [build_context(move_sop_class)] # Use object or UID string

    identifier = Dataset()
    identifier.QueryRetrieveLevel = "STUDY"
    identifier.StudyInstanceUID = study_instance_uid

    final_status = -1
    sub_ops_summary = {"completed": 0, "failed": 0, "warning": 0}

    try:
        with manage_association(
            remote_host=config["remote_host"],
            remote_port=config["remote_port"],
            remote_ae_title=config["remote_ae_title"],
            local_ae_title=config.get("local_ae_title", "AXIOM_SCU"),
            contexts=contexts,
            tls_enabled=config.get("tls_enabled", False),
            tls_ca_cert_secret=config.get("tls_ca_cert_secret_name"),
            tls_client_cert_secret=config.get("tls_client_cert_secret_name"),
            tls_client_key_secret=config.get("tls_client_key_secret_name")
        ) as assoc:
            log.info("Association acquired, sending C-MOVE request...")
            # send_c_move accepts UID string or SOPClass object
            responses = assoc.send_c_move(identifier, move_destination_ae, move_sop_class)

            for status_dataset, response_identifier in responses:
                if status_dataset is None: log.warning("Received C-MOVE response with no status."); continue
                if not hasattr(status_dataset, 'Status'):
                    raise DimseCommandError("Invalid C-MOVE response (Missing Status)", remote_ae=config["remote_ae_title"])

                status_int = int(status_dataset.Status)
                final_status = status_int
                log.debug("C-MOVE Response Status", status=f"0x{status_int:04X}")

                rem = status_dataset.get("NumberOfRemainingSuboperations", "N/A")
                comp = status_dataset.get("NumberOfCompletedSuboperations", 0)
                fail = status_dataset.get("NumberOfFailedSuboperations", 0)
                warn = status_dataset.get("NumberOfWarningSuboperations", 0)
                log.info(f"  -> Status: 0x{status_int:04X}, Rem: {rem}, Comp: {comp}, Fail: {fail}, Warn: {warn}")

                sub_ops_summary["completed"] = max(sub_ops_summary["completed"], comp)
                sub_ops_summary["failed"] = max(sub_ops_summary["failed"], fail)
                sub_ops_summary["warning"] = max(sub_ops_summary["warning"], warn)

                if status_int == 0x0000:
                    log.info("C-MOVE completed successfully.")
                    break
                elif status_int in (0xFF00, 0xFF01):
                    continue
                elif status_int == 0xFE00:
                    log.warning("C-MOVE Cancelled by remote.")
                    raise DimseCommandError("C-MOVE Cancelled by remote (0xFE00)", remote_ae=config["remote_ae_title"])
                elif status_int == 0xB000:
                    log.info("C-MOVE sub-operations completed without failures or warnings.")
                    continue
                else:
                    error_comment = status_dataset.get((0x0000, 0x0902), 'No comment')
                    log.error("C-MOVE failed", status=f"0x{status_int:04X}", comment=error_comment)
                    details = f"Status: 0x{status_int:04X}. Completed: {comp}, Failed: {fail}, Warning: {warn}. Comment: {error_comment}"
                    raise DimseCommandError(f"C-MOVE Failed", remote_ae=config["remote_ae_title"], details=details)

            if final_status != 0x0000:
                log.error("C-MOVE finished without final success status", last_status=f"0x{final_status:04X}")
                details = f"Last Status: 0x{final_status:04X}. Completed: {sub_ops_summary['completed']}, Failed: {sub_ops_summary['failed']}, Warning: {sub_ops_summary['warning']}."
                raise DimseCommandError(f"C-MOVE finished without explicit success", remote_ae=config["remote_ae_title"], details=details)

        log.info("C-MOVE operation completed successfully via service.", sub_ops=sub_ops_summary)
        return {"status": "success", "message": "C-MOVE successful", "sub_operations": sub_ops_summary}

    except (TlsConfigError, AssociationError, DimseCommandError, ValueError) as e:
        log.error(f"C-MOVE service failed: {e}", exc_info=True)
        raise
    except Exception as e:
        log.error(f"Unexpected error during C-MOVE service execution: {e}", exc_info=True)
        raise DimseScuError("Unexpected C-MOVE service error", remote_ae=config.get("remote_ae_title"), details=str(e)) from e
