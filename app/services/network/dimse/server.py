# filename: backend/app/services/network/dimse/server.py

import logging
import sys
import time
import socket
import threading
import signal
import os
import re
import ssl
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple, List

# Third-party imports
from pynetdicom.ae import ApplicationEntity as AE # MODIFIED
from pynetdicom.presentation import PresentationContext, build_context, StoragePresentationContexts # MODIFIED
from pynetdicom.sop_class import Verification # MODIFIED (was VerificationServiceClass, add type ignore) # type: ignore[attr-defined]
from pynetdicom import evt
from pynetdicom._globals import ALL_TRANSFER_SYNTAXES # MODIFIED (as per Pylance suggestion, though typically internal)
import structlog # type: ignore

# SQLAlchemy Core imports
from sqlalchemy import select, update as sql_update, func

# Application imports
from app.core.config import settings
from app.services.network.dimse.handlers import handle_store, handle_echo, set_current_listener_context
from app.db.session import SessionLocal, Session
from app.crud import crud_dimse_listener_state, crud_dimse_listener_config
from app.db import models

# --- CORRECTED: GCP Secret Manager Imports ---
# Import the main utility module and the specific exceptions we might handle
try:
    from app.core import gcp_utils # Import the module
    from app.core.gcp_utils import ( # Import specific exceptions if needed for handling
        SecretManagerError as RealSecretManagerError, # Use aliases to avoid name clash
        SecretNotFoundError as RealSecretNotFoundError,
        PermissionDeniedError as RealPermissionDeniedError
    )
    # Make the real exceptions available under the standard names if import succeeds
    SecretManagerError = RealSecretManagerError # type: ignore[no-redef]
    SecretNotFoundError = RealSecretNotFoundError # type: ignore[no-redef]
    PermissionDeniedError = RealPermissionDeniedError # type: ignore[no-redef]
    GCP_UTILS_IMPORT_SUCCESS = True
except ImportError as import_err:
    logging.getLogger("dicom_listener_startup").critical(
        f"Failed to import GCP Utils (app.core.gcp_utils). TLS secret fetching via GCP will be unavailable. Error: {import_err}",
        exc_info=True # Log traceback for import errors
        )
    GCP_UTILS_IMPORT_SUCCESS = False
    # Define dummy exceptions if import failed
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass
    
    # Define a dummy gcp_utils object
    class _DummyGcpUtils:
        def get_secret(self, *args, **kwargs):
            raise RuntimeError("gcp_utils module not imported, cannot get_secret.")
        # Add other methods if they are called and need dummy versions
    gcp_utils = _DummyGcpUtils()


# --- Logging Setup with Structlog (Keep Existing) ---
# Determine log level from settings first
log_level_str = getattr(settings, 'LOG_LEVEL', "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

def configure_logging():
    """Configures logging using structlog, adding a JSON handler to the root logger."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
    )
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)
    pynetdicom_logger = logging.getLogger('pynetdicom')
    pynetdicom_log_level = logging.INFO if settings.DEBUG else logging.WARNING
    pynetdicom_logger.setLevel(pynetdicom_log_level)
    pynetdicom_logger.propagate = True
    logger_instance = structlog.get_logger("dicom_listener")
    logger_instance.info(
        "Structlog logging configured for DICOM Listener",
        configured_log_level=log_level_str,
        pynetdicom_level=logging.getLevelName(pynetdicom_log_level)
    )
    return logger_instance

try:
    logger = configure_logging()
except Exception as log_config_error:
     logging.basicConfig(level=logging.ERROR)
     logging.getLogger("dicom_listener_startup").critical(
         f"CRITICAL: Failed to configure structlog logging: {log_config_error}",
         exc_info=True
     )
     raise log_config_error

# --- Configuration (Keep Existing) ---
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)
HEARTBEAT_INTERVAL_SECONDS = 30

# --- Presentation Contexts (Keep Existing) ---
SUPPORTED_TRANSFER_SYNTAXES = [
    '1.2.840.10008.1.2.1', '1.2.840.10008.1.2', '1.2.840.10008.1.2.2',
    '1.2.840.10008.1.2.5', '1.2.840.10008.1.2.4.50', '1.2.840.10008.1.2.4.70',
    '1.2.840.10008.1.2.4.90', '1.2.840.10008.1.2.4.91',
]
contexts = []
for default_context in StoragePresentationContexts:
    sop_class_uid = default_context.abstract_syntax
    if sop_class_uid: # MODIFIED: Ensure sop_class_uid is not None
        new_context = build_context(sop_class_uid, SUPPORTED_TRANSFER_SYNTAXES)
        contexts.append(new_context)
contexts.append(build_context(Verification, SUPPORTED_TRANSFER_SYNTAXES)) # MODIFIED: Use Verification (SOPClass instance)

# --- Event Handlers (Keep Existing) ---
def log_assoc_event(event, msg_prefix):
    """Logs details about an association event using structlog."""
    try:
        assoc = event.assoc
        log = logger.bind( # Use the module-level logger configured by configure_logging()
            event_type=event.event.name,
            assoc_id=assoc.native_id,
            remote_ae=assoc.requestor.ae_title,
            remote_addr=assoc.requestor.address,
            remote_port=assoc.requestor.port,
            local_ae=assoc.acceptor.ae_title,
        )
        log.info(msg_prefix)
    except Exception as e:
        logger.error("Failed to log association event details", event_type=event.event.name, error=str(e), exc_info=False)
        logger.info(msg_prefix, raw_event_assoc=str(event.assoc))


HANDLERS = [
    (evt.EVT_C_STORE, handle_store),
    (evt.EVT_C_ECHO, handle_echo),
    (evt.EVT_ACCEPTED, lambda event: log_assoc_event(event, "Association Accepted")),
    (evt.EVT_ESTABLISHED, lambda event: log_assoc_event(event, "Association Established")),
    (evt.EVT_REJECTED, lambda event: log_assoc_event(event, "Association Rejected")),
    (evt.EVT_RELEASED, lambda event: log_assoc_event(event, "Association Released")),
    (evt.EVT_ABORTED, lambda event: log_assoc_event(event, "Association Aborted")),
    (evt.EVT_CONN_CLOSE, lambda event: log_assoc_event(event, "Connection Closed")),
]

# --- Global State (Keep Existing) ---
shutdown_event = threading.Event()
server_thread_exception = None

# --- CORRECTED: Helper: Fetch Secret to Temp File ---
def _fetch_and_write_secret(secret_id: str, version: str, project_id: Optional[str], suffix: str, log_context) -> str:
    """Fetches a secret using gcp_utils.get_secret and writes it to a secure temporary file."""
    # Check if import succeeded first (gcp_utils will be the dummy if it failed)
    if not GCP_UTILS_IMPORT_SUCCESS: # This check is still good for clarity and explicit error
        raise RuntimeError("Cannot fetch secret: GCP Utils module failed to import.")

    log = log_context.bind(secret_id=secret_id, version=version, project_id=project_id)
    log.debug("Fetching secret via gcp_utils.get_secret...")
    try:
        # Call gcp_utils.get_secret (will be real or dummy)
        secret_string = gcp_utils.get_secret(
            secret_id=secret_id,
            version=version,
            project_id=project_id
        )
        secret_bytes = secret_string.encode('utf-8')

        # Use a more explicit directory if possible, e.g., settings.TEMP_DIR
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="dimse_scp_tls_")
        tf.write(secret_bytes)
        tf.close()
        temp_path = tf.name
        log.debug("Secret written to temp file", path=temp_path)

        # Set permissions only for private key files
        if suffix == "-key.pem":
            try:
                os.chmod(temp_path, 0o600)
                log.debug("Set permissions for key file")
            except OSError as chmod_err:
                log.warning("Could not set permissions on temp key file", error=str(chmod_err))
        return temp_path
    except (SecretManagerError, SecretNotFoundError, PermissionDeniedError, ValueError) as sm_err: # MODIFIED: These will now correctly catch real or dummy
        log.error("Failed to fetch secret using gcp_utils.get_secret", error=str(sm_err), exc_info=True)
        raise RuntimeError(f"Failed to fetch required TLS secret '{secret_id}': {sm_err}") from sm_err
    except (IOError, OSError) as file_err:
        log.error("Failed to write secret to temp file", error=str(file_err), exc_info=True)
        raise RuntimeError(f"Failed to write TLS secret '{secret_id}' to file: {file_err}") from file_err
    except Exception as e:
         log.error("Unexpected error during secret fetching/writing", error=str(e), exc_info=True)
         raise RuntimeError(f"Unexpected error handling secret '{secret_id}': {e}") from e


# --- Helper: Prepare TLS Context (Uses Corrected Fetch Helper) ---
def _prepare_server_tls_context(config: models.DimseListenerConfig) -> Tuple[Optional[ssl.SSLContext], List[str]]:
    """Prepares the SSLContext for the server using config."""
    log = logger.bind(config_name=config.name, config_id=config.id, listener_instance_id=config.instance_id)
    temp_files_created: List[str] = []
    server_cert_file: Optional[str] = None
    server_key_file: Optional[str] = None
    ca_cert_file: Optional[str] = None

    # Determine project ID (Needed by get_secret if not using ADC project discovery)
    # Use the project ID from global settings
    gcp_project_id_for_secrets = settings.VERTEX_AI_PROJECT # Or dedicated settings.GCP_PROJECT_ID
    if not gcp_project_id_for_secrets:
         log.error("Cannot determine GCP Project ID for fetching TLS secrets (e.g., VERTEX_AI_PROJECT setting is missing).")
         raise RuntimeError("GCP Project ID setting is required for fetching TLS secrets but is not configured.")

    try:
        # Use secret IDs (which are now just the names) from the config object
        cert_secret_id = config.tls_cert_secret_name
        key_secret_id = config.tls_key_secret_name
        ca_secret_id = config.tls_ca_cert_secret_name # Can be None

        if not cert_secret_id or not key_secret_id:
             raise ValueError("Server certificate and key secret names/IDs are required for TLS.")

        log.info("Fetching server certificate and key for TLS...", gcp_project_id=gcp_project_id_for_secrets)
        # Call the corrected helper which now calls gcp_utils.get_secret
        server_cert_file = _fetch_and_write_secret(cert_secret_id, "latest", gcp_project_id_for_secrets, "-cert.pem", log)
        temp_files_created.append(server_cert_file)
        server_key_file = _fetch_and_write_secret(key_secret_id, "latest", gcp_project_id_for_secrets, "-key.pem", log)
        temp_files_created.append(server_key_file)
        log.info("Server certificate and key fetched.")

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        log.debug("Loading server certificate chain...")
        ssl_context.load_cert_chain(certfile=server_cert_file, keyfile=server_key_file)

        if ca_secret_id:
            log.info("Client certificate verification requested. Fetching CA certificate...")
            ca_cert_file = _fetch_and_write_secret(ca_secret_id, "latest", gcp_project_id_for_secrets, "-ca.pem", log)
            temp_files_created.append(ca_cert_file)

            log.debug("Loading CA certificate for client verification...")
            ssl_context.load_verify_locations(cafile=ca_cert_file)
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            log.info("TLS context configured to REQUIRE client certificates.")
        else:
            ssl_context.verify_mode = ssl.CERT_NONE
            log.info("TLS context configured to NOT require client certificates.")

        # TODO: Add cipher configuration, min TLS version etc. if needed
        # ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
        # ssl_context.set_ciphers(...)

        log.info("Server TLS context prepared successfully.")
        return ssl_context, temp_files_created

    except (ValueError, RuntimeError, FileNotFoundError, ssl.SSLError) as e:
        # Catch expected errors during TLS setup
        log.error("Failed to prepare server TLS context", error=str(e), exc_info=True)
         # Clean up any files created before the error
        for path in temp_files_created:
             try:
                 if path and os.path.exists(path): os.remove(path)
             except OSError as rm_err: log.warning(f"Failed cleanup {path}: {rm_err}")
        return None, [] # Return None context and empty list on failure

# --- Server Thread (Keep Existing) ---
def _run_server_thread(ae: AE, address: tuple, ssl_context: Optional[ssl.SSLContext] = None):
    """Target function run in a separate thread."""
    global server_thread_exception
    thread_log = logger.bind(thread_name="pynetdicom_server")
    try:
        tls_status = "with TLS" if ssl_context else "without TLS"
        thread_log.info(f"Starting pynetdicom AE server {tls_status}...")
        ae.start_server(address, evt_handlers=HANDLERS, block=True, ssl_context=ssl_context)
        thread_log.info("Pynetdicom server stopped normally.")
    except Exception as e:
        thread_log.error("Exception in server thread", error=str(e), exc_info=True)
        server_thread_exception = e
    finally:
        thread_log.info("Server thread finished.")
        shutdown_event.set()

# --- Main Execution Function (Use Corrected TLS Prep) ---
def run_dimse_server():
    """Configures and runs the DICOM DIMSE SCP server with structured logging and TLS support."""
    global shutdown_event, server_thread_exception
    db: Optional[Session] = None
    listener_config: Optional[models.DimseListenerConfig] = None
    ae_title = "DEFAULT_AE"
    port = 11112
    config_name = "UNKNOWN_CONFIG"
    instance_id_for_handler = "UNKNOWN_INSTANCE"
    ssl_context: Optional[ssl.SSLContext] = None
    temp_tls_files: List[str] = []
    tls_enabled_in_config = False

    log = logger

    listener_id = os.environ.get("AXIOM_INSTANCE_ID")
    if not listener_id:
        log.critical("CRITICAL: AXIOM_INSTANCE_ID environment variable not set! Listener cannot start.")
        sys.exit(1)
    instance_id_for_handler = listener_id
    log = log.bind(listener_instance_id=listener_id)
    log.info("Listener process starting.")

    try:
        db = SessionLocal()
        log.info("Loading configuration from DB...")
        listener_config = crud_dimse_listener_config.get_by_instance_id(db, instance_id=listener_id)
        if not listener_config:
            log.error("Config Error: No matching config found for instance ID. Listener cannot start.", required_instance_id=listener_id)
            sys.exit(1)

        if not listener_config.is_enabled:
            log.warning("Config Disabled: Listener is disabled in config. Listener will not start.", config_name=listener_config.name)
            sys.exit(0) # Exit cleanly if disabled

        # Extract config details
        ae_title = listener_config.ae_title
        port = listener_config.port
        config_name = listener_config.name
        config_id = listener_config.id
        tls_enabled_in_config = listener_config.tls_enabled

        log = log.bind(config_name=config_name, config_id=config_id, ae_title=ae_title, port=port, tls_enabled=tls_enabled_in_config)
        log.info("Listener configuration loaded.")

        # Prepare TLS context if enabled
        if tls_enabled_in_config:
            log.info("TLS is enabled in configuration. Preparing server TLS context...")
            if not GCP_UTILS_IMPORT_SUCCESS: # Check if import succeeded
                log.critical("TLS enabled but GCP Utils failed to import. Cannot fetch secrets for TLS.")
                sys.exit(1)
            # Call the helper function which now uses gcp_utils.get_secret
            ssl_context, temp_tls_files = _prepare_server_tls_context(listener_config)
            if ssl_context is None:
                log.critical("Failed to prepare TLS context. Listener cannot start securely.")
                sys.exit(1)
            log.info("Server TLS context prepared successfully.")
        else:
            log.info("TLS is not enabled in configuration.")

        # Set context for handlers
        set_current_listener_context(config_name, instance_id_for_handler)
        log.info("Loaded config and set handler context.")

    except SystemExit:
        raise # Allow clean exits
    except Exception as e:
        log.error("DB Error or Config Validation Error during startup", error=str(e), exc_info=True)
        sys.exit(1) # Exit on other startup errors
    finally:
        if db: db.close()

    # --- Server Setup (Update initial status in DB) ---
    initial_status = "starting"
    tls_info = " (TLS enabled)" if tls_enabled_in_config else ""
    initial_message = f"Listener initializing on port {port}{tls_info} using config '{config_name}'."
    db = None # Reset db variable
    try:
        db = SessionLocal()
        log.info("Setting initial status in DB", status=initial_status)
        crud_dimse_listener_state.update_listener_state(
            db=db, listener_id=listener_id, status=initial_status,
            status_message=initial_message, host=settings.LISTENER_HOST,
            port=port, ae_title=ae_title
        )
        db.commit()
        log.info("Initial status committed.")
    except Exception as e:
        log.error("Failed set initial status in DB", error=str(e), exc_info=True)
        if db and db.is_active: db.rollback()
    finally:
        if db: db.close()


    # --- AE Setup and Thread Start ---
    ae = AE(ae_title=ae_title)
    log.info("Configuring AE", storage_contexts_count=len(contexts), verification_context=True)
    ae.supported_contexts = contexts
    # Ensure Verification context uses all transfer syntaxes if needed, or restrict
    # ae.add_supported_context(Verification, transfer_syntax=SUPPORTED_TRANSFER_SYNTAXES) # Be explicit
    address = (settings.LISTENER_HOST, port)
    log = log.bind(listen_address=f"{settings.LISTENER_HOST}:{port}")
    log.info("Starting DICOM Listener server thread", heartbeat_interval_s=HEARTBEAT_INTERVAL_SECONDS)

    server_thread = threading.Thread(
        target=_run_server_thread,
        args=(ae, address, ssl_context), # Pass the prepared ssl_context
        daemon=True # Ensure thread exits if main process dies unexpectedly
    )
    server_thread.start()
    log.info("Server thread launched.")
    time.sleep(1) # Give thread a moment to start

    # --- Main Loop ---
    last_heartbeat_update_time = time.monotonic()
    current_status = initial_status
    status_message = initial_message
    first_successful_run_completed = False

    try:
        while not shutdown_event.is_set():
            is_thread_alive = server_thread.is_alive()
            target_status = current_status
            target_message = status_message

            # Determine target status based on thread state
            if current_status not in ("error", "stopped"):
                if is_thread_alive:
                    base_msg = "Listener active."
                    tls_msg_part = " (TLS enabled)" if tls_enabled_in_config else ""
                    target_status = "running"
                    target_message = f"{base_msg}{tls_msg_part}"
                elif server_thread_exception is not None:
                    log.error("Pynetdicom server thread terminated unexpectedly!", server_thread_exception=str(server_thread_exception))
                    target_status = "error"
                    error_details = f"Error: {server_thread_exception}"
                    target_message = f"Server thread stopped unexpectedly. {error_details}"
                else: # Thread not alive, no exception recorded
                    log.warning("Server thread is not alive, but no exception recorded. Assuming stopped normally but unexpectedly.")
                    target_status = "stopped"
                    target_message = "Server thread stopped." # Simplified message

            # Check if DB update is needed
            now_monotonic = time.monotonic()
            interval_elapsed = (now_monotonic - last_heartbeat_update_time >= HEARTBEAT_INTERVAL_SECONDS)
            status_changed = (target_status != current_status)
            should_update_db = status_changed or (target_status == "running" and interval_elapsed) or (target_status == "running" and not first_successful_run_completed)

            # Perform DB Update if needed
            if should_update_db:
                db = None
                update_attempted = False
                db_update_type = "none"
                try:
                    db = SessionLocal()
                    is_already_running = (current_status == "running" and target_status == "running")

                    if is_already_running and interval_elapsed:
                        # Update heartbeat only
                        db_update_type = "heartbeat"
                        log.debug("Updating heartbeat only...")
                        update_attempted = True
                        rows_affected = crud_dimse_listener_state.update_listener_heartbeat(db=db, listener_id=listener_id)
                        if rows_affected == 0: log.warning("Heartbeat update statement affected 0 rows.")
                        else: db.commit(); log.debug("Committed heartbeat update."); last_heartbeat_update_time = now_monotonic
                    elif status_changed or (target_status == "running" and not first_successful_run_completed):
                        # Update full state
                        db_update_type = "full_state"
                        log.debug("Performing full state update", target_status=target_status, target_message=target_message)
                        update_attempted = True
                        crud_dimse_listener_state.update_listener_state(
                            db=db, listener_id=listener_id, status=target_status, status_message=target_message,
                            host=settings.LISTENER_HOST, port=port, ae_title=ae_title
                        )
                        db.commit()
                        log.debug("Committed full state update.")
                        current_status = target_status
                        status_message = target_message
                        last_heartbeat_update_time = now_monotonic # Reset heartbeat timer on full update too
                        if current_status == "running" and not first_successful_run_completed:
                            first_successful_run_completed = True; log.info("Listener confirmed running and status updated in DB.")
                    # else: # No update needed log
                    #      log.debug("DB update not required this cycle.")

                except Exception as e:
                    log.error("Failed DB transaction for listener status/heartbeat", update_type=db_update_type, error=str(e), exc_info=settings.DEBUG)
                    if db and db.is_active: db.rollback()
                finally:
                    if db: db.close()

            # Check if status requires loop exit
            if current_status not in ("running", "starting"):
                log.info("Listener status is terminal, exiting main loop", final_db_status=current_status)
                shutdown_event.set() # Ensure shutdown event is set
                break # Exit loop

            # Double check thread status after potential DB update delay
            if not server_thread.is_alive() and current_status == "running":
                 log.error("Detected server thread died after status update cycle!", recorded_exception=str(server_thread_exception))
                 current_status = "error" # Force error state
                 status_message = f"Server thread stopped unexpectedly after status check. Error: {server_thread_exception or 'Unknown Reason'}"
                 shutdown_event.set() # Trigger shutdown
                 break # Exit loop

            # Wait for next cycle or shutdown signal
            shutdown_event.wait(timeout=1.0)

    # --- Shutdown Sequence (Keep Existing) ---
    except KeyboardInterrupt:
        log.warning("Shutdown signal (KeyboardInterrupt) received.")
        current_status = "stopped"; status_message = "Listener stopped by signal."; shutdown_event.set()
    except SystemExit as se:
        log.info("SystemExit called, listener shutting down.", exit_code=se.code)
        # Determine final status based on exit reason if possible
        if "Failed to prepare TLS context" in status_message or "Config Error" in status_message or "GCP Utils failed to import" in status_message:
             current_status = "error"
             status_message = "Listener failed to start due to configuration or dependency error."
        else:
             current_status = "stopped"
             status_message = "Listener stopped due to explicit exit call."
        shutdown_event.set()
    except Exception as main_loop_exc:
        log.error("Unexpected error in main listener loop", error=str(main_loop_exc), exc_info=True)
        current_status = "error"; status_message = f"Main loop failed: {main_loop_exc}"; shutdown_event.set()
    finally:
        log.info("Initiating final shutdown", final_status=current_status)
        # Update final status in DB
        db = None
        try:
            db = SessionLocal()
            final_message = status_message if status_message else f"Listener final status: {current_status}"
            crud_dimse_listener_state.update_listener_state(
                db=db, listener_id=listener_id, status=current_status, status_message=final_message,
                host=settings.LISTENER_HOST, port=port, ae_title=ae_title
            )
            db.commit()
            log.info("Final status committed.")
        except Exception as e:
            log.error("Failed final status update", error=str(e), exc_info=True)
            if db and db.is_active: db.rollback()
        finally:
            if db: db.close()

        # Shutdown AE and thread
        if 'ae' in locals() and ae:
            log.info("Shutting down pynetdicom AE...")
            ae.shutdown()
            log.info("Pynetdicom AE shutdown complete.")
        if 'server_thread' in locals() and server_thread.is_alive():
            log.info("Waiting for server thread to join...")
            server_thread.join(timeout=5)
            if server_thread.is_alive(): log.warning("Server thread did not exit cleanly after AE shutdown.")
            else: log.info("Server thread joined successfully.")
        else: log.info("Server thread not running or already finished.")

        # Cleanup temp files
        if temp_tls_files:
            log.info("Cleaning up temporary TLS files...")
            for file_path in temp_tls_files:
                try:
                    if file_path and os.path.exists(file_path): os.remove(file_path); log.debug(f"Removed temp TLS file: {file_path}")
                except OSError as rm_err: log.warning("Failed to remove temporary TLS file", path=file_path, error=str(rm_err))
            log.info("Temporary TLS file cleanup finished.")

        log.info("Listener service shut down complete.")


# --- Signal Handler (Keep Existing) ---
def handle_signal(signum, frame):
    """Handles SIGINT and SIGTERM for graceful shutdown."""
    signal_name = signal.Signals(signum).name
    logger.warning(f"Received signal, triggering shutdown...", signal_name=signal_name, signal_number=signum)
    shutdown_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# --- Script Entry Point (Keep Existing) ---
if __name__ == "__main__":
    logger.info("Starting DICOM Listener Service directly...")
    run_dimse_server()
