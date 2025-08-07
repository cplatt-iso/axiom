# backend/app/services/storage_backends/dicom_cstore.py
import structlog
from app.core.config import settings
import logging
import socket
import tempfile
import os
import ssl
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List, cast

from pydicom.dataset import Dataset
# MODIFIED pynetdicom imports
from pynetdicom.ae import ApplicationEntity as AE
from pynetdicom.association import Association
from pynetdicom._globals import ALL_TRANSFER_SYNTAXES # Consider replacing with explicit list or DEFAULT_TRANSFER_SYNTAXES
from pynetdicom.presentation import PresentationContext

from .base_backend import BaseStorageBackend, StorageBackendError
from ..network.dimse.transfer_syntax_negotiation import (
    create_presentation_contexts_with_fallback,
    analyze_accepted_contexts,
    find_compatible_transfer_syntax,
    get_transfer_syntax_recommendation,
    TransferSyntaxStrategy
)

# --- MODIFIED: GCP Secret Manager Imports ---
GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY = False # Default

# Define default dummy versions first
class SecretManagerError(Exception): pass
class SecretNotFoundError(SecretManagerError): pass
class PermissionDeniedError(SecretManagerError): pass

class _DummyGcpUtilsCStore:
    GCP_SECRET_MANAGER_AVAILABLE = False
    # Expose dummy exceptions on the dummy module itself
    SecretManagerError = SecretManagerError
    SecretNotFoundError = SecretNotFoundError
    PermissionDeniedError = PermissionDeniedError
    def get_secret(self, *args, **kwargs):
        raise self.SecretManagerError("gcp_utils module not imported, cannot get_secret.")
    def _get_sm_client(self):
        raise self.SecretManagerError("gcp_utils module not imported, cannot _get_sm_client.")

gcp_utils = _DummyGcpUtilsCStore() # type: ignore # Initialize with dummy

try:
    from app.core import gcp_utils as _real_gcp_utils_cstore
    from app.core.gcp_utils import (
        SecretManagerError as RealSecretManagerErrorCStore,
        SecretNotFoundError as RealSecretNotFoundErrorCStore,
        PermissionDeniedError as RealPermissionDeniedErrorCStore
    )
    gcp_utils = _real_gcp_utils_cstore # type: ignore
    SecretManagerError = RealSecretManagerErrorCStore # type: ignore[no-redef]
    SecretNotFoundError = RealSecretNotFoundErrorCStore # type: ignore[no-redef]
    PermissionDeniedError = RealPermissionDeniedErrorCStore # type: ignore[no-redef]
    GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY = True
    structlog.get_logger(__name__).info("dicom_cstore.py: Successfully imported app.core.gcp_utils.")
except ImportError:
    structlog.get_logger(__name__).error(
        "dicom_cstore.py: CRITICAL - Failed to import app.core.gcp_utils. Secret Manager functionality will be UNAVAILABLE."
    )
    # gcp_utils remains _DummyGcpUtilsCStore, and dummy exceptions remain defined
# --- END MODIFICATION ---


logger = structlog.get_logger(__name__)

class CStoreStorage(BaseStorageBackend):
    # Add type hints for attributes that are validated to be non-None
    name: str
    dest_ae_title: str
    dest_host: str
    dest_port: int
    calling_ae_title: str
    # ... other attributes ...

    def __init__(self, config: Dict[str, Any]):
        log = logger.bind(config_received_name=config.get("name", "UnnamedCStore"))
        log.debug("Initializing CStoreStorage backend...")

        self.backend_type = "cstore"
        # Store initial values from config onto self, to be used by _validate_config
        self._name_init = config.get("name", "Unnamed CStore Backend")
        self._remote_ae_title_init = config.get("remote_ae_title")
        self._remote_host_init = config.get("remote_host")
        self._remote_port_str_init = config.get("remote_port")
        self._calling_ae_title_init = config.get("local_ae_title", "AXIOM_SCU")

        self.network_timeout = config.get("network_timeout", 30)
        self.acse_timeout = config.get("acse_timeout", 30)
        self.dimse_timeout = config.get("dimse_timeout", 60)
        self.max_pdu_size = config.get("max_pdu_size", 0)

        # Transfer syntax negotiation strategy
        self.transfer_syntax_strategy = config.get("transfer_syntax_strategy", "conservative")
        self.max_association_retries = config.get("max_association_retries", 3)
        
        self.tls_enabled = config.get("tls_enabled", False)
        self.tls_ca_cert_secret_name = config.get("tls_ca_cert_secret_name")
        self.tls_client_cert_secret_name = config.get("tls_client_cert_secret_name")
        self.tls_client_key_secret_name = config.get("tls_client_key_secret_name")

        self._validate_config() # Call the implemented abstract method

        # Finalize attributes after validation
        self.name = self._name_init
        self.dest_ae_title = cast(str, self._remote_ae_title_init)
        self.dest_host = cast(str, self._remote_host_init)
        self.dest_port = self._parse_port(self._remote_port_str_init)
        self.calling_ae_title = self._calling_ae_title_init
        
        log.debug(f"CStoreStorage backend '{self.name}' initialized.", redacted_config=self.get_redacted_config())

    def _parse_port(self, port_val: Any) -> int:
        log = logger.bind(backend_name=self.name)
        if port_val is None:
             raise ValueError(f"[{self.name}] 'remote_port' is required.")
        try:
            port_int = int(port_val)
            if not (0 < port_int < 65536):
                raise ValueError(f"Port number {port_int} is out of range.")
            return port_int
        except (TypeError, ValueError) as e:
             raise ValueError(f"[{self.name}] Invalid port '{port_val}': {e}")

    def _validate_config(self): # MODIFIED: Renamed and signature changed
        log = logger.bind(backend_name=self._name_init) # Use attribute set in __init__
        errors: List[str] = []
        if not self._remote_ae_title_init: errors.append("'remote_ae_title' is required.")
        if not self._remote_host_init: errors.append("'remote_host' is required.")
        if self._remote_port_str_init is None: errors.append("'remote_port' is required.") # Check before parsing

        if self.tls_enabled: # Use attribute set in __init__
            if not GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY:
                errors.append("TLS is enabled, but the GCP Utils module (for Secret Manager) could not be imported. Cannot fetch secrets.")
            else:
                try:
                    gcp_utils._get_sm_client() 
                    if not gcp_utils.GCP_SECRET_MANAGER_AVAILABLE:
                         errors.append("TLS is enabled, but GCP Secret Manager client is not available or failed to initialize.")
                except gcp_utils.SecretManagerError as e: 
                     errors.append(f"TLS is enabled, but Secret Manager client initialization failed: {e}")
            
            if not self.tls_ca_cert_secret_name: # Use attribute set in __init__
                 errors.append("TLS is enabled, but 'tls_ca_cert_secret_name' is missing (CA required to verify remote peer).")
            
            has_client_cert = bool(self.tls_client_cert_secret_name) # Use attribute
            has_client_key = bool(self.tls_client_key_secret_name)   # Use attribute
            if has_client_cert != has_client_key:
                 errors.append("For mTLS, both client cert and key secrets must be provided, or neither.")

        if errors:
             log.error("CStoreStorage configuration validation failed.", validation_errors=errors)
             raise ValueError(f"[{self._name_init}] Configuration errors: {'; '.join(errors)}")
        log.debug("CStoreStorage configuration validated successfully.")

    def get_redacted_config(self) -> Dict[str, Any]:
        return {
            "name": self.name, "backend_type": self.backend_type,
            "remote_ae_title": self.dest_ae_title, "remote_host": self.dest_host, "remote_port": self.dest_port,
            "local_ae_title": self.calling_ae_title, "tls_enabled": self.tls_enabled,
            "transfer_syntax_strategy": self.transfer_syntax_strategy,
            "max_association_retries": self.max_association_retries,
            "tls_ca_cert_secret_name": self.tls_ca_cert_secret_name,
            "tls_client_cert_secret_name": "[REDACTED]" if self.tls_client_cert_secret_name else None,
            "tls_client_key_secret_name": "[REDACTED]" if self.tls_client_key_secret_name else None,
            "network_timeout": self.network_timeout, "acse_timeout": self.acse_timeout,
            "dimse_timeout": self.dimse_timeout, "max_pdu_size": self.max_pdu_size,
        }

    def _fetch_and_write_secret(self, secret_id: str, suffix: str) -> str:
        log = logger.bind(secret_id=secret_id, backend_name=self.name)
        if not GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY:
            log.error("Cannot fetch secret: GCP Utils module was not imported.")
            raise StorageBackendError("Cannot fetch secret: GCP Utils module not available.")
        
        log.debug("Fetching secret via gcp_utils.get_secret...")
        try:
            # Use the project ID from global settings if not overridden locally
            project_id_for_secret = settings.VERTEX_AI_PROJECT # Or your specific settings.GCP_PROJECT_ID
            secret_string = gcp_utils.get_secret(secret_id=secret_id, project_id=project_id_for_secret)
            secret_bytes = secret_string.encode('utf-8')
            
            tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="cstore_scu_tls_")
            tf.write(secret_bytes)
            tf.close()
            temp_path = tf.name
            log.debug("Secret written to temp file.", temp_file_path=temp_path)
            if suffix == "-key.pem":
                try:
                    os.chmod(temp_path, 0o600)
                except OSError as chmod_err:
                    log.warning("Could not set permissions on temp key file.", error=str(chmod_err))
            return temp_path
        except (SecretManagerError, SecretNotFoundError, PermissionDeniedError, ValueError) as sm_err:
            log.error("Failed to fetch secret using gcp_utils.get_secret.", error=str(sm_err))
            raise StorageBackendError(f"Failed to fetch required TLS secret '{secret_id}': {sm_err}") from sm_err
        except (IOError, OSError) as file_err:
            log.error("Failed to write secret to temp file.", error=str(file_err))
            raise StorageBackendError(f"Failed to write TLS secret '{secret_id}' to file: {file_err}") from file_err

    def _get_tls_files(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        log = logger.bind(backend_name=self.name)
        ca_cert_path: Optional[str] = None
        client_cert_path: Optional[str] = None
        client_key_path: Optional[str] = None

        if not self.tls_ca_cert_secret_name:
             raise StorageBackendError("TLS CA certificate secret name is required but missing (should be caught by validation).")
        
        ca_cert_path = self._fetch_and_write_secret(self.tls_ca_cert_secret_name, "-ca.pem")

        if self.tls_client_cert_secret_name and self.tls_client_key_secret_name:
            client_cert_path = self._fetch_and_write_secret(self.tls_client_cert_secret_name, "-cert.pem")
            client_key_path = self._fetch_and_write_secret(self.tls_client_key_secret_name, "-key.pem")
        
        return ca_cert_path, client_cert_path, client_key_path

    def store( # MODIFIED parameter name
        self, modified_ds: Dataset, original_filepath: Optional[Path]=None,
        filename_context: Optional[str]=None, source_identifier: Optional[str]=None, **kwargs: Any
    ) -> Dict[str, Any]:
        log = logger.bind(backend_name=self.name, source_identifier=source_identifier)
        log_identifier = filename_context; sop_instance_uid="Unknown"; sop_class_uid="Unknown"
        try:
            sop_instance_uid=modified_ds.SOPInstanceUID; sop_class_uid=modified_ds.SOPClassUID # MODIFIED
            if not log_identifier: log_identifier=f"SOP {sop_instance_uid}"
            log=log.bind(sop_instance_uid=sop_instance_uid,sop_class_uid=sop_class_uid)
        except AttributeError:
            if not log_identifier: log_identifier="Dataset with missing UIDs"
        log=log.bind(log_identifier=log_identifier)

        # Try multiple strategies with progressive fallback
        strategies_to_try = self._get_negotiation_strategies()
        
        for attempt, strategy in enumerate(strategies_to_try, 1):
            log_attempt = log.bind(attempt=attempt, strategy=strategy, max_attempts=len(strategies_to_try))
            log_attempt.info("Attempting C-STORE with transfer syntax strategy")
            
            try:
                result = self._attempt_store_with_strategy(
                    modified_ds, strategy, log_attempt
                )
                if result["status"] == "success":
                    log_attempt.info("C-STORE successful", strategy_used=strategy)
                    result["strategy_used"] = strategy
                    result["attempts_made"] = attempt
                    return result
                    
            except StorageBackendError as e:
                log_attempt.warning(
                    "C-STORE attempt failed with strategy", 
                    error=str(e),
                    will_retry=attempt < len(strategies_to_try)
                )
                
                # If this was the last attempt, re-raise the error
                if attempt == len(strategies_to_try):
                    # Add recommendation for future attempts
                    recommendation = get_transfer_syntax_recommendation(
                        failed_contexts=[strategy], 
                        peer_ae_title=self.dest_ae_title
                    )
                    log.error("All C-STORE strategies failed", 
                             final_error=str(e),
                             recommendation=recommendation)
                    raise
                    
                # For other attempts, continue to next strategy
                continue
        
        # This should never be reached due to the raise above, but just in case
        raise StorageBackendError("All C-STORE attempts failed - no successful strategy found")

    def _get_negotiation_strategies(self) -> List[str]:
        """Get list of strategies to try in order of preference."""
        base_strategy = self.transfer_syntax_strategy
        
        # Define fallback sequence based on configured strategy
        if base_strategy == "universal":
            return ["universal", "conservative"]
        elif base_strategy == "conservative":
            return ["conservative", "universal"]
        elif base_strategy == "standard":
            return ["standard", "conservative", "universal"]
        elif base_strategy == "compression":
            return ["compression", "standard", "conservative", "universal"]
        elif base_strategy == "extended":
            return ["extended", "compression", "standard", "conservative", "universal"]
        else:
            # Unknown strategy, use safe defaults
            return ["conservative", "universal"]

    def _attempt_store_with_strategy(
        self, 
        modified_ds: Dataset, 
        strategy: str, 
        log: Any
    ) -> Dict[str, Any]:
        """Attempt C-STORE with a specific transfer syntax strategy."""
        
        ae = AE(ae_title=self.calling_ae_title)
        ae.network_timeout = self.network_timeout
        ae.acse_timeout = self.acse_timeout
        ae.dimse_timeout = self.dimse_timeout
        ae.maximum_pdu_size = self.max_pdu_size

        # Create presentation contexts using the strategy
        sop_class_uid = str(modified_ds.SOPClassUID)
        contexts = create_presentation_contexts_with_fallback(
            sop_class_uid=sop_class_uid,
            strategies=[strategy],
            max_contexts_per_strategy=5  # Limit to avoid too many contexts
        )
        
        # Add all contexts to the AE
        ae.requested_contexts = contexts
        
        log.debug("Created presentation contexts for strategy",
                 context_count=len(contexts),
                 strategy=strategy)

        assoc: Optional[Association] = None
        ssl_context: Optional[ssl.SSLContext] = None
        temp_tls_files: List[str] = []
        
        try:
            # TLS setup if enabled
            if self.tls_enabled:
                try:
                    ca_cert_file, client_cert_file, client_key_file = self._get_tls_files()
                    temp_tls_files.extend(filter(None, [ca_cert_file, client_cert_file, client_key_file]))
                    if not ca_cert_file: 
                        raise StorageBackendError("Failed to get CA cert file path for TLS.")
                    
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ssl_context.check_hostname = False 
                    ssl_context.verify_mode = ssl.CERT_REQUIRED
                    ssl_context.load_verify_locations(cafile=ca_cert_file)
                    if client_cert_file and client_key_file:
                        ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
                    
                    _tls_hostname_for_associate = self.dest_host if ssl_context and ssl_context.check_hostname else None
                    assoc = ae.associate(
                        self.dest_host,
                        self.dest_port,
                        ae_title=self.dest_ae_title,
                        tls_args=(ssl_context, _tls_hostname_for_associate)
                    )
                except StorageBackendError as tls_prep_err: 
                    raise
                except ssl.SSLError as ssl_err: 
                    raise StorageBackendError(f"SSLContext config error: {ssl_err}") from ssl_err
                except Exception as e: 
                    raise StorageBackendError(f"Unexpected TLS setup error: {e}") from e
            else:
                assoc = ae.associate(
                    self.dest_host,
                    self.dest_port,
                    ae_title=self.dest_ae_title
                )

            if not assoc or not assoc.is_established:
                if assoc and assoc.is_rejected:
                    reason_source = getattr(assoc, 'result_source', 'N/A')
                    reason_code = getattr(assoc, 'result_reason', 'N/A')
                    raise StorageBackendError(
                        f"Association rejected by {self.dest_ae_title}. Reason: {reason_source} ({reason_code})"
                    )
                else:
                    raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")

            # Analyze accepted contexts
            context_analysis = analyze_accepted_contexts(assoc)
            log.debug("Association established - analyzing contexts", 
                     accepted_count=context_analysis["accepted_count"],
                     rejected_count=context_analysis["rejected_count"])

            # Find compatible presentation context for this dataset
            compatible_result = find_compatible_transfer_syntax(
                dataset=modified_ds,
                accepted_contexts=assoc.accepted_contexts,
                sop_class_uid=sop_class_uid
            )
            
            if not compatible_result:
                # Collect transfer syntaxes properly handling list format
                accepted_ts = []
                for ctx in assoc.accepted_contexts:
                    if ctx.abstract_syntax == sop_class_uid:
                        if isinstance(ctx.transfer_syntax, list):
                            accepted_ts.extend(ctx.transfer_syntax)
                        else:
                            accepted_ts.append(ctx.transfer_syntax)
                            
                raise StorageBackendError(
                    f"No compatible presentation context for '{sop_class_uid}'. "
                    f"Peer accepted transfer syntaxes: {accepted_ts}"
                )
                
            chosen_context, selection_reason = compatible_result
            
            # Handle transfer syntax display (might be a list)
            ctx_ts = chosen_context.transfer_syntax
            if isinstance(ctx_ts, list):
                ts_for_display = ctx_ts[0] if ctx_ts else "Unknown"
            else:
                ts_for_display = ctx_ts
                
            log.info("Selected presentation context",
                    context_id=chosen_context.context_id,
                    transfer_syntax=ts_for_display,
                    reason=selection_reason)

            # Perform C-STORE (pynetdicom automatically selects the right context)
            status_store = assoc.send_c_store(modified_ds)
            
            if status_store:
                status_hex = f"0x{status_store.Status:04X}"
                if status_store.Status == 0x0000:
                    log.info("C-STORE completed successfully", 
                           remote_status=status_hex,
                           context_id=chosen_context.context_id)
                    return {
                        "status": "success",
                        "message": f"C-STORE to {self.dest_ae_title} successful.",
                        "remote_status": status_hex,
                        "transfer_syntax": ts_for_display,
                        "context_analysis": context_analysis
                    }
                else:
                    raise StorageBackendError(
                        f"C-STORE failed: {self.dest_ae_title} status {status_hex}",
                        status_code=status_store.Status
                    )
            else:
                raise StorageBackendError(f"No C-STORE response from {self.dest_ae_title}")

        except StorageBackendError:
            raise
        except ssl.SSLError as e: 
            raise StorageBackendError(f"TLS/SSL Error with {self.dest_ae_title}: {e}") from e
        except socket.timeout: 
            raise StorageBackendError(f"Network timeout connecting to {self.dest_ae_title}")
        except ConnectionRefusedError: 
            raise StorageBackendError(f"Connection refused by {self.dest_ae_title}")
        except OSError as e: 
            raise StorageBackendError(f"Network/OS error with {self.dest_ae_title}: {e}") from e
        except Exception as e: 
            raise StorageBackendError(f"Unexpected error C-STORE: {e}") from e
        finally:
            # Cleanup
            for file_path in temp_tls_files:
                try:
                    if file_path and os.path.exists(file_path): 
                        os.remove(file_path)
                except OSError: 
                    pass
            if assoc and assoc.is_established:
                assoc.release()
            elif assoc and not assoc.is_released and not assoc.is_aborted: 
                assoc.abort()

    def __repr__(self) -> str:
        return (f"<{self.__class__.__name__}(name='{self.name}', ae='{self.dest_ae_title}', "
                f"host='{self.dest_host}:{self.dest_port}', tls={self.tls_enabled})>")
