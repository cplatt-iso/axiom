# app/services/network/dimse/server.py

import logging
import sys
import time
import socket
import threading
import signal
import os
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

# Third-party imports
from pynetdicom import AE, StoragePresentationContexts, evt, ALL_TRANSFER_SYNTAXES, build_context
from pynetdicom.sop_class import Verification
from pynetdicom.presentation import PresentationContext
import structlog # type: ignore <-- ADDED: Import structlog

# SQLAlchemy Core imports
from sqlalchemy import select, update as sql_update, func

# Application imports
from app.core.config import settings
# Make sure handlers.py also uses structlog!
from app.services.network.dimse.handlers import handle_store, handle_echo, set_current_listener_context
from app.db.session import SessionLocal, Session
from app.crud import crud_dimse_listener_state, crud_dimse_listener_config
from app.db import models

# --- Logging Setup with Structlog ---
# Clear existing handlers from the root logger first
logging.basicConfig(handlers=[logging.NullHandler()])

log_level_str = getattr(settings, 'LOG_LEVEL', "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

def configure_logging():
    """Configures logging using structlog for the listener process."""
    # Same configuration as used in main.py and celery_app.py
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
        foreign_pre_chain=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
        ],
    )

    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    # Configure pynetdicom logger to use our handler/level
    pynetdicom_logger = logging.getLogger('pynetdicom')
    # Remove its default handlers if any were added
    for h in pynetdicom_logger.handlers[:]:
        pynetdicom_logger.removeHandler(h)
    # Add our handler
    pynetdicom_logger.addHandler(handler)
    # Set its level (often good to keep it less verbose than our app)
    pynetdicom_log_level = logging.INFO if settings.DEBUG else logging.WARNING
    pynetdicom_logger.setLevel(pynetdicom_log_level)
    pynetdicom_logger.propagate = False # Prevent logs going to root logger again

    logger = structlog.get_logger("dicom_listener") # Get our main logger instance
    logger.info(
        "Structlog logging configured for DICOM Listener",
        log_level=log_level_str,
        pynetdicom_log_level=logging.getLevelName(pynetdicom_log_level)
    )
    return logger

# Configure logging early and get the main logger instance
logger = configure_logging()

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
        log = logger.bind(
            event_type=event.event.name,
            assoc_id=assoc.native_id,
            remote_ae=assoc.requestor.ae_title,
            remote_addr=assoc.requestor.address,
            remote_port=assoc.requestor.port,
            local_ae=assoc.acceptor.ae_title,
        )
        log.info(msg_prefix) # Message is simple, details are in kwargs
    except Exception as e:
        # Fallback logging if accessing assoc properties fails
        logger.error("Failed to log association event details", event_type=event.event.name, error=str(e), exc_info=False)
        logger.info(msg_prefix, raw_event_assoc=str(event.assoc))

# Ensure handlers.py also uses structlog logger instances
HANDLERS = [
    (evt.EVT_C_STORE, handle_store), # handle_store needs structlog
    (evt.EVT_C_ECHO, handle_echo),   # handle_echo needs structlog
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

# --- Server Thread ---
def _run_server_thread(ae: AE, address: tuple):
    """Target function run in a separate thread."""
    global server_thread_exception
    thread_log = logger.bind(thread_name="pynetdicom_server") # Bind thread context
    try:
        thread_log.info("Starting pynetdicom AE server...")
        ae.start_server(address, evt_handlers=HANDLERS, block=True)
        thread_log.info("Pynetdicom server stopped normally.")
    except Exception as e:
        thread_log.error("Exception in server thread", error=str(e), exc_info=True)
        server_thread_exception = e
    finally:
        thread_log.info("Server thread finished.")
        shutdown_event.set() # Ensure main loop knows thread exited

# --- Main Execution Function ---
def run_dimse_server():
    """Configures and runs the DICOM DIMSE SCP server with structured logging."""
    global shutdown_event, server_thread_exception
    db: Optional[Session] = None
    listener_config: Optional[models.DimseListenerConfig] = None
    ae_title = "DEFAULT_AE"
    port = 11112
    config_name = "UNKNOWN_CONFIG"
    instance_id_for_handler = "UNKNOWN_INSTANCE"

    listener_id = os.environ.get("AXIOM_INSTANCE_ID")
    if not listener_id:
        logger.critical("CRITICAL: AXIOM_INSTANCE_ID environment variable not set! Listener cannot start.")
        sys.exit(1)
    instance_id_for_handler = listener_id
    # Bind instance ID early for consistent context
    log = logger.bind(listener_instance_id=listener_id)
    log.info("Listener process starting.")

    try:
        db = SessionLocal()
        log.info("Loading configuration from DB...")
        listener_config = crud_dimse_listener_config.get_by_instance_id(db, instance_id=listener_id)
        if not listener_config:
            log.error("Config Error: No config found. Listener cannot start.")
            sys.exit(1)
        if not listener_config.is_enabled:
            log.warning("Config Disabled: Listener is disabled in config. Listener will not start.", config_name=listener_config.name)
            sys.exit(1)

        ae_title = listener_config.ae_title
        port = listener_config.port
        config_name = listener_config.name
        config_id = listener_config.id
        log = log.bind(config_name=config_name, config_id=config_id, ae_title=ae_title, port=port) # Update context

        set_current_listener_context(config_name, instance_id_for_handler)
        log.info("Loaded config and set handler context.")

    except Exception as e:
        log.error("DB Error: Failed to load config", error=str(e), exc_info=True)
        sys.exit(1)
    finally:
        if db: db.close()

    initial_status = "starting"
    initial_message = f"Listener initializing using config '{config_name}'."
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
    log = log.bind(listen_address=f"{settings.LISTENER_HOST}:{port}") # Add listen address
    log.info("Starting DICOM Listener server thread", heartbeat_interval_s=HEARTBEAT_INTERVAL_SECONDS)

    server_thread = threading.Thread(target=_run_server_thread, args=(ae, address), daemon=True)
    server_thread.start()
    log.info("Server thread launched.")
    time.sleep(1)

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
                    target_status = "running"
                    target_message = "Listener active."
                elif current_status != "starting":
                    log.error("Pynetdicom server thread terminated unexpectedly!", server_thread_exception=str(server_thread_exception) if server_thread_exception else "Unknown")
                    target_status = "error"
                    error_details = f"Error: {server_thread_exception or 'Unknown Reason'}"
                    target_message = f"Server thread stopped unexpectedly. {error_details}"

            now_monotonic = time.monotonic()
            interval_elapsed = (now_monotonic - last_heartbeat_update_time >= HEARTBEAT_INTERVAL_SECONDS)
            status_changed = (target_status != current_status)
            should_update_db = status_changed or (target_status == "running" and interval_elapsed) or (target_status == "running" and not first_successful_run_completed)

            if should_update_db:
                db = None
                update_attempted = False
                db_update_type = "none" # For logging
                try:
                    db = SessionLocal()
                    is_already_running = (current_status == "running" and target_status == "running")

                    if is_already_running and interval_elapsed:
                        db_update_type = "heartbeat"
                        log.debug("Updating heartbeat only...")
                        update_attempted = True
                        updated_obj = crud_dimse_listener_state.update_listener_heartbeat(db=db, listener_id=listener_id)
                        if updated_obj is None:
                            log.warning("Heartbeat update statement failed to find listener.")
                            update_attempted = False
                        if update_attempted:
                             db.commit()
                             log.debug("Committed heartbeat update.")

                    elif status_changed or (target_status == "running" and not first_successful_run_completed):
                        db_update_type = "full_state"
                        log.debug("Performing full state update", target_status=target_status)
                        update_attempted = True
                        crud_dimse_listener_state.update_listener_state(
                            db=db, listener_id=listener_id, status=target_status,
                            status_message=target_message if target_status != "running" else "Listener active.",
                            host=settings.LISTENER_HOST, port=port, ae_title=ae_title
                        )
                        db.commit()
                        log.debug("Committed full state update.")
                    else:
                         log.warning("Reached unexpected state in DB update logic.")

                    if update_attempted and db.is_active:
                        current_status = target_status
                        status_message = target_message if target_status != "running" else "Listener active."
                        last_heartbeat_update_time = now_monotonic
                        if current_status == "running" and not first_successful_run_completed:
                            first_successful_run_completed = True
                            log.info("Listener confirmed running in DB.")
                        log.debug("DB update cycle complete", update_type=db_update_type, internal_status=current_status)

                except Exception as e:
                    log.error("Failed DB transaction for listener status/heartbeat", update_type=db_update_type, error=str(e), exc_info=settings.DEBUG)
                    if db: db.rollback()
                finally:
                    if db: db.close()

            if current_status not in ("running", "starting"):
                log.info("Listener status is terminal, exiting main loop", final_db_status=current_status)
                break

            shutdown_event.wait(timeout=1.0)
    except KeyboardInterrupt:
        log.warning("Shutdown signal (KeyboardInterrupt) received.")
        current_status = "stopped"; status_message = "Listener stopped by signal."; shutdown_event.set()
    except Exception as main_loop_exc:
        log.error("Unexpected error in main listener loop", error=str(main_loop_exc), exc_info=True)
        current_status = "error"; status_message = f"Main loop failed: {main_loop_exc}"; shutdown_event.set()
    finally:
        log.info("Initiating final shutdown", final_status=current_status)
        db = None
        try:
            db = SessionLocal()
            crud_dimse_listener_state.update_listener_state(
                db=db, listener_id=listener_id, status=current_status, status_message=status_message,
                host=settings.LISTENER_HOST, port=port, ae_title=ae_title
            )
            db.commit()
            log.info("Final status committed.")
        except Exception as e:
            log.error("Failed final status update", error=str(e), exc_info=True)
            if db: db.rollback()
        finally:
            if db: db.close()

        if 'ae' in locals() and ae: ae.shutdown()
        if 'server_thread' in locals() and server_thread.is_alive():
            log.info("Waiting for server thread..."); server_thread.join(timeout=5)
            if server_thread.is_alive(): log.warning("Server thread did not exit cleanly.")
            else: log.info("Server thread joined.")
        else: log.info("Server thread not running or already finished.")
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
    logger.info("Starting DICOM Listener Service directly...") # This log uses the configured logger
    run_dimse_server()
