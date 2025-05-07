# backend/app/services/storage_backends/dicom_cstore.py
import structlog
from app.core.config import settings
import logging
import socket
import tempfile
import os
import ssl
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from pydicom.dataset import Dataset
from pynetdicom import AE, Association
from pynetdicom._globals import ALL_TRANSFER_SYNTAXES

from .base_backend import BaseStorageBackend, StorageBackendError

try:
    from app.core import gcp_utils
    from app.core.gcp_utils import SecretManagerError, SecretNotFoundError, PermissionDeniedError
    GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY = True
except ImportError:
    structlog.get_logger(__name__).error(
        "dicom_cstore.py: CRITICAL - Failed to import app.core.gcp_utils. Secret Manager functionality will be UNAVAILABLE."
    )
    GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY = False
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass
    # Define a dummy _get_sm_client and GCP_SECRET_MANAGER_AVAILABLE if gcp_utils itself is missing
    # to prevent NameErrors later if GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY is True but gcp_utils members are used.
    # This is defensive; the main check is GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY.
    if not GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY: # Redundant but explicit
        class gcp_utils_dummy: # type: ignore
            GCP_SECRET_MANAGER_AVAILABLE = False
            def _get_sm_client(self):
                raise SecretManagerError("gcp_utils module not imported.")
            def get_secret(self, *args, **kwargs):
                raise SecretManagerError("gcp_utils module not imported, cannot get_secret.")
        gcp_utils = gcp_utils_dummy() # type: ignore


logger = structlog.get_logger(__name__)

class CStoreStorage(BaseStorageBackend):
    def __init__(self, config: Dict[str, Any]):
        log = logger.bind(config_received_name=config.get("name", "UnnamedCStore"))
        log.debug("Initializing CStoreStorage backend...")

        self.backend_type = "cstore"
        self.name = config.get("name", "Unnamed CStore Backend")
        self.dest_ae_title = config.get("remote_ae_title")
        self.dest_host = config.get("remote_host")
        self.dest_port_str = config.get("remote_port") # Will be parsed to int
        self.calling_ae_title = config.get("local_ae_title", "AXIOM_SCU")

        self.network_timeout = config.get("network_timeout", 30)
        self.acse_timeout = config.get("acse_timeout", 30)
        self.dimse_timeout = config.get("dimse_timeout", 60)
        self.max_pdu_size = config.get("max_pdu_size", 0)

        self.tls_enabled = config.get("tls_enabled", False)
        self.tls_ca_cert_secret_name = config.get("tls_ca_cert_secret_name")
        self.tls_client_cert_secret_name = config.get("tls_client_cert_secret_name")
        self.tls_client_key_secret_name = config.get("tls_client_key_secret_name")

        self._validate_config() # This will now use gcp_utils internal state
        self.dest_port = self._parse_port(self.dest_port_str) # Parse after validation
        
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

    def _validate_config(self):
        log = logger.bind(backend_name=self.name)
        errors: List[str] = []
        if not self.dest_ae_title: errors.append("'remote_ae_title' is required.")
        if not self.dest_host: errors.append("'remote_host' is required.")
        # Port parsing will raise ValueError if port is invalid/missing before this.

        if self.tls_enabled:
            if not GCP_UTILS_MODULE_IMPORTED_SUCCESSFULLY:
                errors.append("TLS is enabled, but the GCP Utils module (for Secret Manager) could not be imported. Cannot fetch secrets.")
            else:
                # Attempt to get/initialize the SM client and check its availability
                try:
                    gcp_utils._get_sm_client() # This attempts initialization if not already done
                    if not gcp_utils.GCP_SECRET_MANAGER_AVAILABLE:
                         errors.append("TLS is enabled, but GCP Secret Manager client is not available or failed to initialize.")
                except gcp_utils.SecretManagerError as e: # Catch specific init error from _get_sm_client()
                     errors.append(f"TLS is enabled, but Secret Manager client initialization failed: {e}")
            
            if not self.tls_ca_cert_secret_name: # CA is always required for server cert verification by client
                 errors.append("TLS is enabled, but 'tls_ca_cert_secret_name' is missing (CA required to verify remote peer).")
            
            has_client_cert = bool(self.tls_client_cert_secret_name)
            has_client_key = bool(self.tls_client_key_secret_name)
            if has_client_cert != has_client_key:
                 errors.append("For mTLS, both client cert and key secrets must be provided, or neither.")

        if errors:
             log.error("CStoreStorage configuration validation failed.", validation_errors=errors)
             raise ValueError(f"[{self.name}] Configuration errors: {'; '.join(errors)}")
        log.debug("CStoreStorage configuration validated successfully.")

    def get_redacted_config(self) -> Dict[str, Any]:
        return {
            "name": self.name, "backend_type": self.backend_type,
            "remote_ae_title": self.dest_ae_title, "remote_host": self.dest_host, "remote_port": self.dest_port,
            "local_ae_title": self.calling_ae_title, "tls_enabled": self.tls_enabled,
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

    def store(
        self, dataset_to_send: Dataset, original_filepath: Optional[Path]=None,
        filename_context: Optional[str]=None, source_identifier: Optional[str]=None, **kwargs: Any
    ) -> Dict[str, Any]:
        log = logger.bind(backend_name=self.name, source_identifier=source_identifier)
        log_identifier = filename_context; sop_instance_uid="Unknown"; sop_class_uid="Unknown"
        try:
            sop_instance_uid=dataset_to_send.SOPInstanceUID; sop_class_uid=dataset_to_send.SOPClassUID
            if not log_identifier: log_identifier=f"SOP {sop_instance_uid}"
            log=log.bind(sop_instance_uid=sop_instance_uid,sop_class_uid=sop_class_uid)
        except AttributeError:
            if not log_identifier: log_identifier="Dataset with missing UIDs"
        log=log.bind(log_identifier=log_identifier)

        ae=AE(ae_title=self.calling_ae_title); ae.network_timeout=self.network_timeout
        ae.acse_timeout=self.acse_timeout; ae.dimse_timeout=self.dimse_timeout
        ae.maximum_pdu_size=self.max_pdu_size
        ae.add_requested_context(sop_class_uid, transfer_syntax=ALL_TRANSFER_SYNTAXES)

        assoc: Optional[Association]=None; ssl_context: Optional[ssl.SSLContext]=None; temp_tls_files: List[str]=[]
        try:
            if self.tls_enabled:
                try:
                    ca_cert_file, client_cert_file, client_key_file = self._get_tls_files()
                    temp_tls_files.extend(filter(None, [ca_cert_file, client_cert_file, client_key_file]))
                    if not ca_cert_file: raise StorageBackendError("Failed to get CA cert file path for TLS.")
                    
                    ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ssl_context.check_hostname=False # Typically requires hostnames in certs, adjust if needed
                    ssl_context.verify_mode=ssl.CERT_REQUIRED
                    ssl_context.load_verify_locations(cafile=ca_cert_file)
                    if client_cert_file and client_key_file:
                        ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
                    assoc = ae.associate(self.dest_host, self.dest_port, ae_title=self.dest_ae_title, tls_args=(ssl_context, None))
                except StorageBackendError as tls_prep_err: raise
                except ssl.SSLError as ssl_err: raise StorageBackendError(f"SSLContext config error: {ssl_err}") from ssl_err
                except Exception as e: raise StorageBackendError(f"Unexpected TLS setup error: {e}") from e
            else:
                assoc = ae.associate(self.dest_host, self.dest_port, ae_title=self.dest_ae_title)

            if assoc and assoc.is_established:
                status_store = assoc.send_c_store(dataset_to_send)
                assoc.release()
                if status_store:
                    status_hex = f"0x{status_store.Status:04X}"
                    if status_store.Status == 0x0000:
                        return {"status":"success","message":f"C-STORE to {self.dest_ae_title} successful.","remote_status":status_hex}
                    else:
                        raise StorageBackendError(f"C-STORE failed: {self.dest_ae_title} status {status_hex}",status_code=status_store.Status)
                else:
                    raise StorageBackendError(f"No C-STORE response from {self.dest_ae_title}")
            elif assoc and assoc.is_rejected:
                raise StorageBackendError(f"Association rejected by {self.dest_ae_title}. Reason: {assoc.result_source} ({assoc.result_reason})")
            else:
                raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")
        except StorageBackendError as sbe: raise
        except ssl.SSLError as e: raise StorageBackendError(f"TLS/SSL Error with {self.dest_ae_title}: {e}") from e
        except socket.timeout: raise StorageBackendError(f"Network timeout connecting to {self.dest_ae_title}")
        except ConnectionRefusedError: raise StorageBackendError(f"Connection refused by {self.dest_ae_title}")
        except OSError as e: raise StorageBackendError(f"Network/OS error with {self.dest_ae_title}: {e}") from e
        except Exception as e: raise StorageBackendError(f"Unexpected error C-STORE: {e}") from e
        finally:
            for file_path in temp_tls_files:
                try:
                    if file_path and os.path.exists(file_path): os.remove(file_path)
                except OSError: pass # Logged in _fetch if needed
            if assoc and not assoc.is_released and not assoc.is_aborted: assoc.abort()

    def __repr__(self) -> str:
        return (f"<{self.__class__.__name__}(name='{self.name}', ae='{self.dest_ae_title}', "
                f"host='{self.dest_host}:{self.dest_port}', tls={self.tls_enabled})>")
