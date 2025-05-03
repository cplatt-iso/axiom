# app/services/network/dimse/server.py

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
from pynetdicom import AE, StoragePresentationContexts, evt, ALL_TRANSFER_SYNTAXES, build_context
from pynetdicom.sop_class import Verification
from pynetdicom.presentation import PresentationContext
import structlog # type: ignore

# SQLAlchemy Core imports
from sqlalchemy import select, update as sql_update, func

# Application imports
from app.core.config import settings
from app.services.network.dimse.handlers import handle_store, handle_echo, set_current_listener_context
from app.db.session import SessionLocal, Session
from app.crud import crud_dimse_listener_state, crud_dimse_listener_config
from app.db import models

# --- GCP Secret Manager Imports ---
try:
    from app.core.gcp_utils import (
        fetch_secret_content,
        SecretManagerError,
        SecretNotFoundError,
        PermissionDeniedError,
        GCP_SECRET_MANAGER_AVAILABLE
    )
except ImportError:
    # Log using standard logging initially if structlog isn't set up yet
    logging.getLogger("dicom_listener_startup").critical("Failed to import GCP Utils. TLS secret fetching will be unavailable.")
    GCP_SECRET_MANAGER_AVAILABLE = False
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass
    def fetch_secret_content(*args, **kwargs): raise NotImplementedError("GCP Utils unavailable")

# --- Logging Setup with Structlog ---

# Determine log level from settings first
log_level_str = getattr(settings, 'LOG_LEVEL', "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

def configure_logging():
    """Configures logging using structlog, adding a JSON handler to the root logger."""

    # Prevent basicConfig from running if already configured elsewhere
    # logging.basicConfig(handlers=[logging.NullHandler()], level=log_level, force=True) # force=True might be needed

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            # Use ProcessorFormatter to format events for stdlib handler
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Create the formatter for stdlib handler
    formatter = structlog.stdlib.ProcessorFormatter(
        # The actual formatting to JSON happens here
        processor=structlog.processors.JSONRenderer(),
        # foreign_pre_chain might be needed if other logs use stdlib directly
        # foreign_pre_chain=structlog.stdlib.ProcessorFormatter.get_default_processors(),
    )

    # Get the root logger
    root_logger = logging.getLogger()

    # Remove existing handlers attached *directly* to the root logger
    # This prevents duplicate logs if the script/environment adds default handlers
    for handler in list(root_logger.handlers): # Iterate over a copy
        root_logger.removeHandler(handler)

    # Create and configure the stream handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    # Add the single handler to the root logger
    root_logger.addHandler(handler)

    # Set the overall level for the root logger
    root_logger.setLevel(log_level)

    # Configure pynetdicom's logger level (it will use the root handler via propagation)
    pynetdicom_logger = logging.getLogger('pynetdicom')
    pynetdicom_log_level = logging.INFO if settings.DEBUG else logging.WARNING
    pynetdicom_logger.setLevel(pynetdicom_log_level)
    # Ensure pynetdicom logs *do* propagate to the root handler
    pynetdicom_logger.propagate = True

    # Get our specific application logger instance using structlog
    # It will inherit the handlers and level setting from the root logger configuration
    logger_instance = structlog.get_logger("dicom_listener")

    # Log the configuration success using the structlog instance
    logger_instance.info(
        "Structlog logging configured for DICOM Listener",
        configured_log_level=log_level_str,
        pynetdicom_level=logging.getLevelName(pynetdicom_log_level)
    )

    return logger_instance # Return the structlog logger instance

# --- Call logging configuration AT MODULE LEVEL ---
# This line might be the source of the error if configuration fails
try:
    logger = configure_logging()
except Exception as log_config_error:
     # Fallback to basic logging if structlog setup fails catastrophically
     logging.basicConfig(level=logging.ERROR)
     logging.getLogger("dicom_listener_startup").critical(
         f"CRITICAL: Failed to configure structlog logging: {log_config_error}",
         exc_info=True
     )
     # Re-raise the error to halt execution, as logging is fundamental
     raise log_config_error


# --- Configuration ---
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)
HEARTBEAT_INTERVAL_SECONDS = 30

# --- Presentation Contexts ---
SUPPORTED_TRANSFER_SYNTAXES = [
    '1.2.840.10008.1.2.1', '1.2.840.10008.1.2', '1.2.840.10008.1.2.2',
    '1.2.840.10008.1.2.5', '1.2.840.10008.1.2.4.50', '1.2.840.10008.1.2.4.70',
    '1.2.840.10008.1.2.4.90', '1.2.840.10008.1.2.4.91',
]
contexts = []
for default_context in StoragePresentationContexts:
    sop_class_uid = default_context.abstract_syntax
    new_context = build_context(sop_class_uid, SUPPORTED_TRANSFER_SYNTAXES)
    contexts.append(new_context)

# --- Event Handlers ---
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

# --- Global State ---
shutdown_event = threading.Event()
server_thread_exception = None

# --- Helper: Fetch Secret to Temp File ---
def _fetch_and_write_secret(secret_name: str, suffix: str, log_context) -> str:
    """Fetches a secret and writes it to a secure temporary file."""
    log = log_context.bind(secret_name=secret_name)
    log.debug("Fetching secret...")
    try:
        secret_bytes = fetch_secret_content(secret_name)
        # Use a more explicit directory if possible, e.g., settings.TEMP_DIR
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="dimse_scp_tls_")
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
        raise RuntimeError(f"Failed to fetch required TLS secret '{secret_name}': {sm_err}") from sm_err
    except (IOError, OSError) as file_err:
        log.error("Failed to write secret to temp file", error=str(file_err), exc_info=True)
        raise RuntimeError(f"Failed to write TLS secret '{secret_name}' to file: {file_err}") from file_err

# --- Helper: Prepare TLS Context ---
def _prepare_server_tls_context(config: models.DimseListenerConfig) -> Tuple[Optional[ssl.SSLContext], List[str]]:
    """Prepares the SSLContext for the server using config."""
    # Use the module-level logger, binding config context
    log = logger.bind(config_name=config.name, config_id=config.id, listener_instance_id=config.instance_id)
    temp_files_created: List[str] = []
    server_cert_file: Optional[str] = None
    server_key_file: Optional[str] = None
    ca_cert_file: Optional[str] = None

    try:
        if not config.tls_cert_secret_name or not config.tls_key_secret_name:
             raise ValueError("Server certificate and key secret names are required for TLS.")

        log.info("Fetching server certificate and key for TLS...")
        server_cert_file = _fetch_and_write_secret(config.tls_cert_secret_name, "-cert.pem", log)
        temp_files_created.append(server_cert_file)
        server_key_file = _fetch_and_write_secret(config.tls_key_secret_name, "-key.pem", log)
        temp_files_created.append(server_key_file)
        log.info("Server certificate and key fetched.")

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        log.debug("Loading server certificate chain...")
        ssl_context.load_cert_chain(certfile=server_cert_file, keyfile=server_key_file)

        if config.tls_ca_cert_secret_name:
            log.info("Client certificate verification requested. Fetching CA certificate...")
            ca_cert_file = _fetch_and_write_secret(config.tls_ca_cert_secret_name, "-ca.pem", log)
            temp_files_created.append(ca_cert_file)

            log.debug("Loading CA certificate for client verification...")
            ssl_context.load_verify_locations(cafile=ca_cert_file)
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            log.info("TLS context configured to REQUIRE client certificates.")
        else:
            ssl_context.verify_mode = ssl.CERT_NONE
            log.info("TLS context configured to NOT require client certificates.")

        log.info("Server TLS context prepared successfully.")
        return ssl_context, temp_files_created

    except (ValueError, RuntimeError, SecretManagerError, FileNotFoundError, ssl.SSLError) as e:
        log.error("Failed to prepare server TLS context", error=str(e), exc_info=True)
        for path in temp_files_created:
            try:
                if path and os.path.exists(path): os.remove(path)
            except OSError as rm_err: log.warning(f"Failed cleanup {path}: {rm_err}")
        return None, []


# --- Server Thread ---
def _run_server_thread(ae: AE, address: tuple, ssl_context: Optional[ssl.SSLContext] = None):
    """Target function run in a separate thread."""
    global server_thread_exception
    # Use the module-level logger, binding thread context
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

# --- Main Execution Function ---
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

    # Use the already configured module-level logger
    log = logger # Alias for convenience

    listener_id = os.environ.get("AXIOM_INSTANCE_ID")
    if not listener_id:
        log.critical("CRITICAL: AXIOM_INSTANCE_ID environment variable not set! Listener cannot start.")
        sys.exit(1)
    instance_id_for_handler = listener_id
    log = log.bind(listener_instance_id=listener_id) # Bind instance ID
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
            sys.exit(0)

        ae_title = listener_config.ae_title
        port = listener_config.port
        config_name = listener_config.name
        config_id = listener_config.id
        tls_enabled_in_config = listener_config.tls_enabled

        log = log.bind(config_name=config_name, config_id=config_id, ae_title=ae_title, port=port, tls_enabled=tls_enabled_in_config)
        log.info("Listener configuration loaded.")

        if tls_enabled_in_config:
            log.info("TLS is enabled in configuration. Preparing server TLS context...")
            if not GCP_SECRET_MANAGER_AVAILABLE:
                log.critical("TLS enabled but Secret Manager is unavailable. Cannot start TLS listener.")
                sys.exit(1)
            ssl_context, temp_tls_files = _prepare_server_tls_context(listener_config)
            if ssl_context is None:
                log.critical("Failed to prepare TLS context. Listener cannot start securely.")
                sys.exit(1)
            log.info("Server TLS context prepared successfully.")
        else:
            log.info("TLS is not enabled in configuration.")

        set_current_listener_context(config_name, instance_id_for_handler)
        log.info("Loaded config and set handler context.")

    except SystemExit:
        raise
    except Exception as e:
        log.error("DB Error or Config Validation Error during startup", error=str(e), exc_info=True)
        sys.exit(1)
    finally:
        if db: db.close()

    # --- Server Setup ---
    initial_status = "starting"
    tls_info = " (TLS enabled)" if tls_enabled_in_config else ""
    initial_message = f"Listener initializing on port {port}{tls_info} using config '{config_name}'."
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
        if db: db.rollback()
    finally:
        if db: db.close()

    ae = AE(ae_title=ae_title)
    log.info("Configuring AE", storage_contexts_count=len(contexts), verification_context=True)
    ae.supported_contexts = contexts
    ae.add_supported_context(Verification)
    address = (settings.LISTENER_HOST, port)
    log = log.bind(listen_address=f"{settings.LISTENER_HOST}:{port}")
    log.info("Starting DICOM Listener server thread", heartbeat_interval_s=HEARTBEAT_INTERVAL_SECONDS)

    server_thread = threading.Thread(
        target=_run_server_thread,
        args=(ae, address, ssl_context),
        daemon=True
    )
    server_thread.start()
    log.info("Server thread launched.")
    time.sleep(1)

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
                elif current_status != "starting":
                     log.warning("Server thread is not alive, but no exception recorded. Assuming stopped.")
                     target_status = "stopped"
                     target_message = "Server thread stopped for unknown reason."


            now_monotonic = time.monotonic()
            interval_elapsed = (now_monotonic - last_heartbeat_update_time >= HEARTBEAT_INTERVAL_SECONDS)
            status_changed = (target_status != current_status)
            # Update DB only if status changes, or if it's running and interval passed, or first time running
            should_update_db = status_changed or (target_status == "running" and interval_elapsed) or (target_status == "running" and not first_successful_run_completed)

            if should_update_db:
                db = None
                update_attempted = False
                db_update_type = "none"
                try:
                    db = SessionLocal()
                    is_already_running = (current_status == "running" and target_status == "running")

                    if is_already_running and interval_elapsed:
                        db_update_type = "heartbeat"
                        log.debug("Updating heartbeat only...")
                        update_attempted = True
                        rows_affected = crud_dimse_listener_state.update_listener_heartbeat(db=db, listener_id=listener_id)
                        if rows_affected == 0:
                            log.warning("Heartbeat update statement affected 0 rows.")
                        else:
                             db.commit()
                             log.debug("Committed heartbeat update.")
                             last_heartbeat_update_time = now_monotonic

                    elif status_changed or (target_status == "running" and not first_successful_run_completed):
                        db_update_type = "full_state"
                        log.debug("Performing full state update", target_status=target_status, target_message=target_message)
                        update_attempted = True
                        crud_dimse_listener_state.update_listener_state(
                            db=db, listener_id=listener_id, status=target_status,
                            status_message=target_message,
                            host=settings.LISTENER_HOST, port=port, ae_title=ae_title
                        )
                        db.commit()
                        log.debug("Committed full state update.")
                        current_status = target_status
                        status_message = target_message
                        last_heartbeat_update_time = now_monotonic
                        if current_status == "running" and not first_successful_run_completed:
                            first_successful_run_completed = True
                            log.info("Listener confirmed running and status updated in DB.")
                    else:
                         log.debug("DB update not required this cycle.", current_status=current_status, target_status=target_status, interval_elapsed=interval_elapsed, first_run=not first_successful_run_completed)


                except Exception as e:
                    log.error("Failed DB transaction for listener status/heartbeat", update_type=db_update_type, error=str(e), exc_info=settings.DEBUG)
                    if db and db.is_active: db.rollback()
                finally:
                    if db: db.close()


            if current_status not in ("running", "starting"):
                log.info("Listener status is terminal, exiting main loop", final_db_status=current_status)
                shutdown_event.set()
                break

            if not server_thread.is_alive() and current_status == "running":
                 log.error("Detected server thread died after status update cycle!", recorded_exception=str(server_thread_exception))
                 current_status = "error"
                 status_message = f"Server thread stopped unexpectedly after status check. Error: {server_thread_exception or 'Unknown Reason'}"


            shutdown_event.wait(timeout=1.0) # Main loop wait

    # --- Shutdown Sequence ---
    except KeyboardInterrupt:
        log.warning("Shutdown signal (KeyboardInterrupt) received.")
        current_status = "stopped"; status_message = "Listener stopped by signal."; shutdown_event.set()
    except SystemExit as se:
         log.info("SystemExit called, listener shutting down.", exit_code=se.code)
         if "Failed to prepare TLS context" in status_message or "Config Error" in status_message:
              current_status = "error"
              status_message = "Listener failed to start due to configuration or TLS setup error."
         else:
              current_status = "stopped"
              status_message = "Listener stopped due to explicit exit call."
         shutdown_event.set()
    except Exception as main_loop_exc:
        log.error("Unexpected error in main listener loop", error=str(main_loop_exc), exc_info=True)
        current_status = "error"; status_message = f"Main loop failed: {main_loop_exc}"; shutdown_event.set()
    finally:
        log.info("Initiating final shutdown", final_status=current_status)
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

        if temp_tls_files:
            log.info("Cleaning up temporary TLS files...")
            for file_path in temp_tls_files:
                try:
                    if file_path and os.path.exists(file_path):
                        os.remove(file_path)
                        log.debug(f"Removed temp TLS file: {file_path}")
                except OSError as rm_err:
                    log.warning("Failed to remove temporary TLS file", path=file_path, error=str(rm_err))
            log.info("Temporary TLS file cleanup finished.")

        log.info("Listener service shut down complete.")

# --- Signal Handler ---
def handle_signal(signum, frame):
    signal_name = signal.Signals(signum).name
    logger.warning(f"Received signal, triggering shutdown...", signal_name=signal_name, signal_number=signum)
    shutdown_event.set()
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# --- Script Entry Point ---
if __name__ == "__main__":
    logger.info("Starting DICOM Listener Service directly...")
    run_dimse_server()
