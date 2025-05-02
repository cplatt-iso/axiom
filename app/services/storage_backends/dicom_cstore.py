# app/services/storage_backends/dicom_cstore.py

import logging
import socket
import tempfile
import os
import ssl
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from pydicom.dataset import Dataset
from pynetdicom import AE, Association
from pynetdicom._globals import ALL_TRANSFER_SYNTAXES

from .base_backend import BaseStorageBackend, StorageBackendError

try:
    from app.core.gcp_utils import (
        fetch_secret_content,
        SecretManagerError,
        SecretNotFoundError,
        PermissionDeniedError
    )
    SECRET_MANAGER_ENABLED = True
except ImportError:
    logging.getLogger(__name__).warning("GCP Utils / Secret Manager not available.")
    SECRET_MANAGER_ENABLED = False
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass
    def fetch_secret_content(*args, **kwargs): raise NotImplementedError("Secret Manager fetch unavailable")

logger = logging.getLogger(__name__)

class CStoreStorage(BaseStorageBackend):

    def __init__(self, config: Dict[str, Any]):
        self.backend_type = config.get("type", "cstore")
        self.name = config.get("name", "Unnamed CStore Backend")
        ae_title = config.get("remote_ae_title")
        host = config.get("host")
        port = config.get("remote_port")
        self.calling_ae_title = config.get("local_ae_title", "AXIOM_SCU")
        self.network_timeout = config.get("network_timeout", 30)
        self.acse_timeout = config.get("acse_timeout", 30)
        self.dimse_timeout = config.get("dimse_timeout", 60)
        self.max_pdu_size = config.get("max_pdu_size", 0)
        self.tls_enabled = config.get("tls_enabled", False)
        self.tls_ca_cert_secret_name = config.get("tls_ca_cert_secret_name")
        self.tls_client_cert_secret_name = config.get("tls_client_cert_secret_name")
        self.tls_client_key_secret_name = config.get("tls_client_key_secret_name")

        if not ae_title or not host or port is None:
            raise ValueError("C-STORE config requires 'remote_ae_title', 'host', 'remote_port'.")
        try:
            self.port_int = int(port)
            if not (0 < self.port_int < 65536): raise ValueError("Port out of range")
        except (TypeError, ValueError):
             raise ValueError(f"Invalid port number: {port}")

        self.dest_ae_title = ae_title
        self.dest_host = host
        self.dest_port = self.port_int
        self._validate_config()

    def _validate_config(self):
        if self.tls_enabled:
            if not SECRET_MANAGER_ENABLED: raise ValueError("TLS enabled but SM unavailable.")
            if not self.tls_ca_cert_secret_name: raise ValueError("TLS requires 'tls_ca_cert_secret_name'.")
            if bool(self.tls_client_cert_secret_name) != bool(self.tls_client_key_secret_name):
                 raise ValueError("mTLS requires both client cert/key secrets, or neither.")

    def _get_tls_files(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        log = logger.bind(dest_ae=self.dest_ae_title)
        ca_cert_path, client_cert_path, client_key_path = None, None, None
        temp_files_created = []
        try:
            if not self.tls_ca_cert_secret_name: raise SecretManagerError("CA secret name missing.")
            log.debug("Fetching CA cert", secret_name=self.tls_ca_cert_secret_name)
            ca_cert_bytes = fetch_secret_content(self.tls_ca_cert_secret_name)
            tf_ca = tempfile.NamedTemporaryFile(delete=False, suffix="-ca.pem", prefix="cstore_scu_")
            tf_ca.write(ca_cert_bytes); tf_ca.close(); ca_cert_path = tf_ca.name; temp_files_created.append(ca_cert_path)
            log.debug("CA cert temp file", path=ca_cert_path)
            if self.tls_client_cert_secret_name and self.tls_client_key_secret_name:
                log.debug("Fetching client cert/key for mTLS...")
                client_cert_bytes = fetch_secret_content(self.tls_client_cert_secret_name)
                tf_cert = tempfile.NamedTemporaryFile(delete=False, suffix="-cert.pem", prefix="cstore_scu_")
                tf_cert.write(client_cert_bytes); tf_cert.close(); client_cert_path = tf_cert.name; temp_files_created.append(client_cert_path)
                client_key_bytes = fetch_secret_content(self.tls_client_key_secret_name)
                tf_key = tempfile.NamedTemporaryFile(delete=False, suffix="-key.pem", prefix="cstore_scu_")
                tf_key.write(client_key_bytes); tf_key.close(); client_key_path = tf_key.name; temp_files_created.append(client_key_path)
                try: os.chmod(client_key_path, 0o600)
                except OSError as chmod_err: log.warning(f"Could not chmod temp key file {client_key_path}: {chmod_err}")
                log.debug("Client cert/key temp files", cert_path=client_cert_path, key_path=client_key_path)
            return ca_cert_path, client_cert_path, client_key_path
        except (SecretManagerError, SecretNotFoundError, PermissionDeniedError, IOError, OSError) as e:
            log.error(f"Error fetching/writing TLS secrets: {e}", exc_info=True)
            for path in temp_files_created:
                 try:
                     if os.path.exists(path): os.remove(path)
                 except OSError as rm_err: log.warning(f"Failed cleanup {path}: {rm_err}")
            raise StorageBackendError(f"Failed prepare TLS config: {e}") from e

    def store(
        self,
        dataset_to_send: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        try: sop_instance_uid = dataset_to_send.SOPInstanceUID; sop_class_uid = dataset_to_send.SOPClassUID; log_identifier = f"SOP {sop_instance_uid}"
        except AttributeError as e: raise StorageBackendError(f"Dataset missing required UID: {e}")
        ae = AE(ae_title=self.calling_ae_title); ae.network_timeout = self.network_timeout; ae.acse_timeout = self.acse_timeout; ae.dimse_timeout = self.dimse_timeout; ae.maximum_pdu_size = self.max_pdu_size
        ae.add_requested_context(sop_class_uid, transfer_syntax=ALL_TRANSFER_SYNTAXES)
        logger.debug(f"Attempting C-STORE for {log_identifier} to {self.dest_ae_title}@{self.dest_host}:{self.dest_port}")
        assoc: Optional[Association] = None
        ca_cert_file, client_cert_file, client_key_file = None, None, None
        ssl_context: Optional[ssl.SSLContext] = None
        temp_files_created : List[str] = []

        try:
            if self.tls_enabled:
                log.info(f"TLS enabled for C-STORE to {self.dest_ae_title}. Preparing context...")
                ca_cert_file, client_cert_file, client_key_file = self._get_tls_files()
                temp_files_created = [f for f in [ca_cert_file, client_cert_file, client_key_file] if f]
                ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_file)
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                if client_cert_file and client_key_file:
                    ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
                log.info("SSLContext configured for C-STORE.")
                assoc = ae.associate(self.dest_host, self.dest_port, ae_title=self.dest_ae_title, tls_args=(ssl_context, None))
            else:
                log.debug("Requesting non-TLS association...")
                assoc = ae.associate(self.dest_host, self.dest_port, ae_title=self.dest_ae_title)

            if assoc.is_established:
                # ***** CORRECTED F-STRING *****
                logger.info(f"Association established with {self.dest_ae_title} for {log_identifier} {'(TLS)' if self.tls_enabled else ''}")
                # ******************************
                accepted_contexts = [ctx for ctx in assoc.accepted_contexts if ctx.abstract_syntax == sop_class_uid]
                if not accepted_contexts: assoc.release(); raise StorageBackendError(f"Remote AE {self.dest_ae_title} rejected context for {sop_class_uid}")
                log.debug(f"Sending C-STORE for {log_identifier}...")
                status_store = assoc.send_c_store(dataset_to_send)
                if status_store:
                     status_hex = f"0x{status_store.Status:04X}"
                     log.info(f"C-STORE response for {log_identifier}: Status {status_hex}")
                     assoc.release(); log.debug(f"Association released.")
                     if status_store.Status == 0x0000: return {"status": "success", "message": f"C-STORE successful for {log_identifier}", "remote_status": status_hex}
                     else: raise StorageBackendError(f"C-STORE rejected by {self.dest_ae_title} status {status_hex}", remote_status=status_hex) # Pass status_hex correctly
                else: assoc.abort(); raise StorageBackendError(f"C-STORE to {self.dest_ae_title} failed - No response")
            else: raise StorageBackendError(f"Association rejected/aborted by {self.dest_ae_title}")

        except (socket.timeout, TimeoutError) as e: raise StorageBackendError(f"Timeout C-STORE to {self.dest_ae_title}") from e
        except ConnectionRefusedError as e: raise StorageBackendError(f"Connection refused C-STORE to {self.dest_ae_title}") from e
        except OSError as e: raise StorageBackendError(f"Network/OS error C-STORE to {self.dest_ae_title}: {e}") from e
        except ssl.SSLError as e: log.error(f"TLS Error C-STORE: {e}", exc_info=True); raise StorageBackendError(f"TLS Handshake Error: {e}") from e
        except StorageBackendError: raise
        except Exception as e: log.error(f"Unexpected C-STORE failure to {self.dest_ae_title}: {e}", exc_info=True); raise StorageBackendError(f"Unexpected C-STORE error: {e}") from e
        finally:
            for file_path in temp_files_created:
                 try:
                     if os.path.exists(file_path): os.remove(file_path); logger.debug(f"Cleaned temp TLS file: {file_path}")
                 except OSError as rm_err: logger.warning(f"Failed cleanup {file_path}: {rm_err}")
            if assoc and not assoc.is_released and not assoc.is_aborted: logger.warning("Aborting unclosed assoc."); assoc.abort()

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(ae={self.dest_ae_title}, host={self.dest_host}:{self.dest_port}, tls={self.tls_enabled})>"
