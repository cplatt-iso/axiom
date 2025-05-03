# app/services/storage_backends/dicom_cstore.py

import logging
import socket
import tempfile
import os
import ssl
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List # <-- Added List

from pydicom.dataset import Dataset
from pynetdicom import AE, Association
# Use specific AE/Association types if available/needed, but this is typical
from pynetdicom._globals import ALL_TRANSFER_SYNTAXES # <-- Keep this unless you have specific needs

# Import the base error and backend class
from .base_backend import BaseStorageBackend, StorageBackendError

# Conditional Secret Manager Import
try:
    from app.core.gcp_utils import (
        fetch_secret_content,
        SecretManagerError,
        SecretNotFoundError,
        PermissionDeniedError
    )
    SECRET_MANAGER_ENABLED = True
except ImportError:
    # Fallback if GCP utils are not available (e.g., local testing without GCP access)
    logging.getLogger(__name__).warning(
        "GCP Utils / Secret Manager not available. TLS secret fetching will fail."
    )
    SECRET_MANAGER_ENABLED = False
    # Define dummy exceptions so the 'except' blocks don't crash
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass
    # Define dummy fetch function
    def fetch_secret_content(*args, **kwargs):
        raise NotImplementedError("Secret Manager fetch unavailable")

# Use standard logging for services for now, unless structlog is mandated everywhere
# If structlog IS mandated, replace this with structlog.get_logger()
logger = logging.getLogger(__name__) # Standard logging

class CStoreStorage(BaseStorageBackend):
    """
    Storage backend that sends DICOM datasets via C-STORE.
    Handles both non-TLS and TLS connections using Google Secret Manager for certs/keys.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the C-STORE backend.

        Args:
            config: A dictionary derived from the StorageBackendConfig model,
                    containing flattened attributes like 'name', 'remote_ae_title',
                    'remote_host', 'remote_port', 'tls_enabled', etc.
        """
        # No super().__init__(config) needed if BaseStorageBackend.__init__ is simple
        # self.config = config # Store raw config if needed, but prefer explicit attrs

        # --- Extract config values with defaults ---
        self.backend_type = "cstore" # Explicitly set type
        self.name = config.get("name", "Unnamed CStore Backend")
        self.dest_ae_title = config.get("remote_ae_title")
        self.dest_host = config.get("remote_host") # Renamed from 'host' for clarity
        self.dest_port_str = config.get("remote_port") # Get as string first
        self.calling_ae_title = config.get("local_ae_title", "AXIOM_SCU") # Default calling AE

        # Network timeouts
        self.network_timeout = config.get("network_timeout", 30)
        self.acse_timeout = config.get("acse_timeout", 30)
        self.dimse_timeout = config.get("dimse_timeout", 60)
        self.max_pdu_size = config.get("max_pdu_size", 0) # 0 means use pynetdicom default

        # TLS configuration
        self.tls_enabled = config.get("tls_enabled", False)
        self.tls_ca_cert_secret_name = config.get("tls_ca_cert_secret_name")
        self.tls_client_cert_secret_name = config.get("tls_client_cert_secret_name")
        self.tls_client_key_secret_name = config.get("tls_client_key_secret_name")

        # --- Initial Validation ---
        # Call validation method AFTER setting attributes
        self._validate_config()

        # Store parsed port
        self.dest_port = self._parse_port(self.dest_port_str)

        logger.debug(f"Initialized CStoreStorage backend '{self.name}'", config=self.get_redacted_config())


    def _parse_port(self, port_val: Any) -> int:
        """Parses and validates the port number."""
        if port_val is None:
             raise ValueError(f"[{self.name}] 'remote_port' is required.")
        try:
            port_int = int(port_val)
            if not (0 < port_int < 65536):
                raise ValueError(f"Port number {port_int} is out of the valid range (1-65535).")
            return port_int
        except (TypeError, ValueError) as e:
             raise ValueError(f"[{self.name}] Invalid port number '{port_val}': {e}")


    def _validate_config(self):
        """Validates the essential configuration parameters."""
        if not self.dest_ae_title: raise ValueError(f"[{self.name}] 'remote_ae_title' is required.")
        if not self.dest_host: raise ValueError(f"[{self.name}] 'remote_host' is required.")
        # Port validation happens in _parse_port after __init__ sets self.name

        if self.tls_enabled:
            if not SECRET_MANAGER_ENABLED:
                raise ValueError(
                    f"[{self.name}] TLS is enabled, but Secret Manager integration is "
                    "unavailable (gcp_utils might be missing or failed import)."
                )
            if not self.tls_ca_cert_secret_name:
                 raise ValueError(
                     f"[{self.name}] TLS is enabled, but 'tls_ca_cert_secret_name' is missing. "
                     "CA certificate is required to verify the remote server."
                 )
            # Check if *both* or *neither* client cert/key are provided for mTLS
            has_client_cert = bool(self.tls_client_cert_secret_name)
            has_client_key = bool(self.tls_client_key_secret_name)
            if has_client_cert != has_client_key:
                 raise ValueError(
                     f"[{self.name}] For mutual TLS (mTLS), both 'tls_client_cert_secret_name' "
                     "and 'tls_client_key_secret_name' must be provided, or neither."
                 )
        logger.debug(f"[{self.name}] Configuration validated successfully.")

    def get_redacted_config(self) -> Dict[str, Any]:
        """Returns a dictionary representation suitable for logging (secrets redacted)."""
        # Basic example, expand as needed
        return {
            "name": self.name,
            "backend_type": self.backend_type,
            "remote_ae_title": self.dest_ae_title,
            "remote_host": self.dest_host,
            "remote_port": self.dest_port,
            "local_ae_title": self.calling_ae_title,
            "tls_enabled": self.tls_enabled,
            "tls_ca_cert_secret_name": self.tls_ca_cert_secret_name,
            "tls_client_cert_secret_name": "[REDACTED]" if self.tls_client_cert_secret_name else None,
            "tls_client_key_secret_name": "[REDACTED]" if self.tls_client_key_secret_name else None,
        }


    def _fetch_and_write_secret(self, secret_name: str, suffix: str) -> str:
        """Fetches a secret and writes it to a secure temporary file."""
        # This internal helper centralizes the temp file creation logic
        log = logger.bind(secret_name=secret_name) # Use bind if using structlog
        log.debug(f"Fetching secret '{secret_name}'...")
        try:
            secret_bytes = fetch_secret_content(secret_name)
            # Use NamedTemporaryFile for automatic cleanup potential, but delete=False means we manage it
            tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="cstore_scu_tls_")
            tf.write(secret_bytes)
            tf.close() # Close the file handle immediately
            temp_path = tf.name
            log.debug(f"Secret '{secret_name}' written to temp file: {temp_path}")
            # Set permissions for key files
            if suffix == "-key.pem":
                try:
                    os.chmod(temp_path, 0o600)
                    log.debug(f"Set permissions for key file: {temp_path}")
                except OSError as chmod_err:
                    # Log warning but proceed; ssl context might handle it
                    log.warning(f"Could not set permissions on temp key file {temp_path}: {chmod_err}")
            return temp_path
        except (SecretManagerError, SecretNotFoundError, PermissionDeniedError) as sm_err:
            log.error(f"Failed to fetch secret '{secret_name}': {sm_err}", exc_info=True)
            raise StorageBackendError(f"Failed to fetch required TLS secret '{secret_name}': {sm_err}") from sm_err
        except (IOError, OSError) as file_err:
            log.error(f"Failed to write secret '{secret_name}' to temp file: {file_err}", exc_info=True)
            raise StorageBackendError(f"Failed to write TLS secret '{secret_name}' to file: {file_err}") from file_err

    def _get_tls_files(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Fetches TLS certificates/keys from Secret Manager and saves them to temporary files.

        Returns:
            A tuple containing paths to (ca_cert_file, client_cert_file, client_key_file).
            Paths are None if not configured or applicable.

        Raises:
            StorageBackendError: If fetching or writing secrets fails.
        """
        # Note: This method now raises exceptions directly if fetching/writing fails.
        # The caller (`store`) is responsible for cleanup.
        ca_cert_path: Optional[str] = None
        client_cert_path: Optional[str] = None
        client_key_path: Optional[str] = None

        # Fetch CA Certificate (Mandatory if TLS enabled)
        if self.tls_ca_cert_secret_name:
            ca_cert_path = self._fetch_and_write_secret(self.tls_ca_cert_secret_name, "-ca.pem")
        else:
            # This case should be caught by _validate_config, but check defensively
            raise StorageBackendError("TLS CA certificate secret name is required but missing.")

        # Fetch Client Certificate and Key for mTLS (Optional)
        if self.tls_client_cert_secret_name and self.tls_client_key_secret_name:
            logger.info(f"[{self.name}] Attempting mTLS: Fetching client certificate and key...")
            client_cert_path = self._fetch_and_write_secret(self.tls_client_cert_secret_name, "-cert.pem")
            client_key_path = self._fetch_and_write_secret(self.tls_client_key_secret_name, "-key.pem")
            logger.info(f"[{self.name}] Client certificate and key fetched for mTLS.")
        else:
            logger.info(f"[{self.name}] Client certificate/key not configured, proceeding without mTLS.")

        return ca_cert_path, client_cert_path, client_key_path

    def store(
        self,
        dataset_to_send: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None, # filename_context often IS the UID
        source_identifier: Optional[str] = None,
        **kwargs: Any # Absorb extra kwargs
    ) -> Dict[str, Any]:
        """
        Sends the DICOM dataset using C-STORE.

        Args:
            dataset_to_send: The pydicom Dataset to send.
            original_filepath: Path to the original file (optional, for logging).
            filename_context: Context for logging (e.g., SOPInstanceUID).
            source_identifier: Identifier of the data source (optional, for logging).
            **kwargs: Extra arguments (ignored).

        Returns:
            A dictionary with 'status' ('success' or 'error') and 'message'.

        Raises:
            StorageBackendError: If the C-STORE operation fails for any reason
                                (configuration, network, TLS, DICOM protocol).
        """
        # Use filename_context or extract UID for logging
        log_identifier = filename_context
        sop_instance_uid = "Unknown"
        sop_class_uid = "Unknown"
        try:
            sop_instance_uid = dataset_to_send.SOPInstanceUID
            sop_class_uid = dataset_to_send.SOPClassUID
            if not log_identifier:
                log_identifier = f"SOP {sop_instance_uid}"
        except AttributeError:
            # Log warning but allow attempt if basic dataset structure seems okay
            logger.warning(f"[{self.name}] Dataset missing SOPInstanceUID or SOPClassUID. Attempting send anyway.", dataset_info=str(dataset_to_send)[:200])
            if not log_identifier: log_identifier = "Dataset with missing UIDs"

        ae = AE(ae_title=self.calling_ae_title)
        ae.network_timeout = self.network_timeout
        ae.acse_timeout = self.acse_timeout
        ae.dimse_timeout = self.dimse_timeout
        ae.maximum_pdu_size = self.max_pdu_size

        # Add requested presentation context for the specific SOP Class
        # Using ALL_TRANSFER_SYNTAXES is common for broad compatibility
        ae.add_requested_context(sop_class_uid, transfer_syntax=ALL_TRANSFER_SYNTAXES)

        logger.debug(f"Attempting C-STORE for {log_identifier} to {self.dest_ae_title}@{self.dest_host}:{self.dest_port} (TLS: {self.tls_enabled})")

        assoc: Optional[Association] = None
        # --- TLS Setup ---
        ssl_context: Optional[ssl.SSLContext] = None
        temp_tls_files: List[str] = [] # Keep track of files to delete

        try:
            # --- Establish Association ---
            if self.tls_enabled:
                logger.info(f"[{self.name}] TLS enabled. Preparing TLS context for {log_identifier}...")
                try:
                    # Fetch secrets and get temp file paths
                    ca_cert_file, client_cert_file, client_key_file = self._get_tls_files()
                    # Add any created file paths to the cleanup list
                    temp_tls_files.extend(filter(None, [ca_cert_file, client_cert_file, client_key_file]))

                    if not ca_cert_file: # Should be caught earlier, but double-check
                        raise StorageBackendError("Failed to get CA certificate file path for TLS.")

                    # Create SSL context for client-side verification of the server
                    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ssl_context.check_hostname = False # Often needed for DICOM AE title mismatch
                    ssl_context.verify_mode = ssl.CERT_REQUIRED # Require server cert validation
                    logger.debug(f"[{self.name}] Loading CA cert for verification: {ca_cert_file}")
                    ssl_context.load_verify_locations(cafile=ca_cert_file)

                    # Load client cert/key if provided (for mTLS)
                    if client_cert_file and client_key_file:
                        logger.debug(f"[{self.name}] Loading client cert chain: cert={client_cert_file}, key={client_key_file}")
                        ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
                    logger.info(f"[{self.name}] SSLContext configured. Requesting TLS association...")

                    # Associate using tls_args
                    # The second element of tls_args is hostname check override (None uses context default)
                    assoc = ae.associate(self.dest_host, self.dest_port, ae_title=self.dest_ae_title, tls_args=(ssl_context, None))

                except StorageBackendError as tls_prep_err:
                     # Error during _get_tls_files or context setup
                     logger.error(f"[{self.name}] TLS preparation failed for {log_identifier}: {tls_prep_err}", exc_info=True)
                     raise # Re-raise the specific error
                except ssl.SSLError as ssl_err:
                     # Error during context creation/loading
                     logger.error(f"[{self.name}] SSLContext configuration error for {log_identifier}: {ssl_err}", exc_info=True)
                     raise StorageBackendError(f"Failed to configure SSLContext: {ssl_err}") from ssl_err
                except Exception as e:
                    # Catch-all for unexpected TLS setup issues
                    logger.error(f"[{self.name}] Unexpected error during TLS setup for {log_identifier}: {e}", exc_info=True)
                    raise StorageBackendError(f"Unexpected TLS setup error: {e}") from e

            else: # Non-TLS Association
                logger.debug(f"[{self.name}] Requesting non-TLS association for {log_identifier}...")
                assoc = ae.associate(self.dest_host, self.dest_port, ae_title=self.dest_ae_title)

            # --- Process Association Result ---
            if assoc and assoc.is_established:
                tls_status = '(TLS)' if self.tls_enabled else '(non-TLS)'
                logger.info(f"[{self.name}] Association established with {self.dest_ae_title} for {log_identifier} {tls_status}")

                # Check if the required context was accepted
                accepted_contexts = [ctx for ctx in assoc.accepted_contexts if ctx.abstract_syntax == sop_class_uid]
                if not accepted_contexts:
                    logger.error(f"[{self.name}] Association established, but context for SOP Class {sop_class_uid} was rejected by {self.dest_ae_title}.")
                    assoc.release()
                    raise StorageBackendError(f"Remote AE {self.dest_ae_title} rejected required context for {sop_class_uid}")

                # Preferred context logic (optional, usually pynetdicom handles negotiation well)
                # chosen_context = accepted_contexts[0] # Simplest: use the first accepted
                # logger.debug(f"Using accepted context: {chosen_context.abstract_syntax} / {chosen_context.transfer_syntax}")

                # Send C-STORE Request
                logger.debug(f"[{self.name}] Sending C-STORE request for {log_identifier}...")
                status_store = assoc.send_c_store(dataset_to_send) # Use the chosen context if specific negotiation needed

                if status_store:
                    status_hex = f"0x{status_store.Status:04X}"
                    logger.info(f"[{self.name}] C-STORE response received for {log_identifier}: Status {status_hex}")
                    assoc.release()
                    logger.debug(f"[{self.name}] Association released.")

                    if status_store.Status == 0x0000:
                        logger.info(f"[{self.name}] C-STORE successful for {log_identifier}.")
                        return {"status": "success", "message": f"C-STORE successful to {self.dest_ae_title}", "remote_status": status_hex}
                    else:
                        # Include specific DICOM status code in the error
                        error_message = f"C-STORE failed: Remote AE {self.dest_ae_title} returned status {status_hex}"
                        logger.error(f"[{self.name}] {error_message} for {log_identifier}.")
                        raise StorageBackendError(error_message, status_code=status_store.Status)
                else:
                    # No response received, usually indicates a lower-level network issue or timeout
                    logger.error(f"[{self.name}] No C-STORE response received from {self.dest_ae_title} for {log_identifier}. Aborting association.")
                    assoc.abort() # Abort if no status is returned
                    raise StorageBackendError(f"No C-STORE response from {self.dest_ae_title}")

            elif assoc and assoc.is_rejected:
                 # Log specific rejection reason if available
                 reason_code = assoc.result_reason
                 reason_str = assoc.result_source # These might be numeric codes
                 logger.error(f"[{self.name}] Association rejected by {self.dest_ae_title} for {log_identifier}. Reason: {reason_str} ({reason_code})")
                 raise StorageBackendError(f"Association rejected by {self.dest_ae_title}. Reason: {reason_str} ({reason_code})")
            else:
                 # Handle other failure cases (aborted, negotiated failed, etc.)
                 logger.error(f"[{self.name}] Association failed to establish with {self.dest_ae_title} for {log_identifier}. Result: {assoc.result if assoc else 'No Association Object'}")
                 raise StorageBackendError(f"Failed to establish association with {self.dest_ae_title}")

        # --- Exception Handling ---
        # Order matters: More specific exceptions first
        except StorageBackendError as sbe:
            logger.error(f"[{self.name}] Storage Backend Error during C-STORE for {log_identifier}: {sbe}", exc_info=settings.DEBUG) # Log stacktrace only in debug
            raise # Re-raise the specific error
        except ssl.SSLError as e:
            # Catch TLS-specific errors during association/handshake
            logger.error(f"[{self.name}] TLS/SSL Error during C-STORE association for {log_identifier}: {e}", exc_info=True)
            raise StorageBackendError(f"TLS Handshake/Error with {self.dest_ae_title}: {e}") from e
        except socket.timeout:
            logger.error(f"[{self.name}] Network timeout during C-STORE for {log_identifier} to {self.dest_ae_title}")
            raise StorageBackendError(f"Network timeout connecting to {self.dest_ae_title}@{self.dest_host}:{self.dest_port}")
        except ConnectionRefusedError:
            logger.error(f"[{self.name}] Connection refused by {self.dest_ae_title}@{self.dest_host}:{self.dest_port} for {log_identifier}")
            raise StorageBackendError(f"Connection refused by {self.dest_ae_title}@{self.dest_host}:{self.dest_port}")
        except OSError as e:
            # Catch broader network/OS errors (e.g., Host not found, network unreachable)
            logger.error(f"[{self.name}] Network/OS Error during C-STORE for {log_identifier} to {self.dest_ae_title}: {e}", exc_info=True)
            raise StorageBackendError(f"Network/OS error connecting to {self.dest_ae_title}: {e}") from e
        except Exception as e:
            # Catch any other unexpected errors during the process
            logger.exception(f"[{self.name}] Unexpected error during C-STORE for {log_identifier} to {self.dest_ae_title}: {e}") # Use logger.exception to include stack trace
            raise StorageBackendError(f"Unexpected error during C-STORE: {e}") from e

        # --- Cleanup ---
        finally:
            # Ensure temporary TLS files are deleted
            if temp_tls_files:
                logger.debug(f"[{self.name}] Cleaning up temporary TLS files: {temp_tls_files}")
                for file_path in temp_tls_files:
                    try:
                        if file_path and os.path.exists(file_path):
                            os.remove(file_path)
                            logger.debug(f"Removed temp TLS file: {file_path}")
                    except OSError as rm_err:
                        # Log error but continue cleanup
                        logger.warning(f"[{self.name}] Failed to remove temporary TLS file {file_path}: {rm_err}")

            # Ensure association is properly closed if it exists and wasn't already
            if assoc and not assoc.is_released and not assoc.is_aborted:
                logger.warning(f"[{self.name}] Association for {log_identifier} was left open. Aborting.")
                assoc.abort()

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"<{self.__class__.__name__}(name='{self.name}', "
            f"ae='{self.dest_ae_title}', host='{self.dest_host}:{self.dest_port}', "
            f"tls={self.tls_enabled})>"
        )
