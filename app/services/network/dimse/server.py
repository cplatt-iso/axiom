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

# SQLAlchemy Core imports
from sqlalchemy import select, update as sql_update, func

# Application imports
from app.core.config import settings # Still needed for INCOMING_DIR, DEBUG, LISTENER_HOST
# --- MODIFIED IMPORTS ---
from app.services.network.dimse.handlers import handle_store, handle_echo, set_current_listener_name # Import new function
from app.db.session import SessionLocal, Session
from app.crud import crud_dimse_listener_state, crud_dimse_listener_config # Import new CRUD
from app.db import models
# --- END MODIFIED IMPORTS ---

# --- Logging Setup (Keep as is) ---
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("dicom_listener")
logger.setLevel(logging.INFO if not settings.DEBUG else logging.DEBUG)
pynetdicom_logger = logging.getLogger('pynetdicom')
pynetdicom_logger.setLevel(logging.INFO if settings.DEBUG else logging.WARNING)
if not logger.handlers:
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
if not pynetdicom_logger.handlers:
    pynetdicom_stream_handler = logging.StreamHandler(sys.stdout)
    pynetdicom_stream_handler.setFormatter(log_formatter)
    pynetdicom_logger.addHandler(pynetdicom_stream_handler)

# --- Configuration from Settings (REDUCED) ---
# HOSTNAME = settings.LISTENER_HOST # Bind address is still global
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)

# --- Listener State Configuration ---
# LISTENER_ID is retrieved from ENV within run_dimse_server now
HEARTBEAT_INTERVAL_SECONDS = 30

# --- Presentation Contexts (Keep as is) ---
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
    """Logs details about an association event."""
    try:
        remote_ae = event.assoc.requestor.ae_title
        remote_addr = event.assoc.requestor.address
        remote_port = event.assoc.requestor.port
        assoc_id = event.assoc.native_id
        logger.info(f"{msg_prefix}: {remote_ae} @ {remote_addr}:{remote_port} (Assoc ID: {assoc_id})")
    except Exception:
        logger.info(f"{msg_prefix}: {event.assoc}")

# Map pynetdicom event types to corresponding handler functions or logging callbacks
HANDLERS = [
    (evt.EVT_C_STORE, handle_store),
    (evt.EVT_C_ECHO, handle_echo),
    (evt.EVT_ACCEPTED, lambda event: log_assoc_event(event, "Association Accepted from")),
    (evt.EVT_ESTABLISHED, lambda event: log_assoc_event(event, "Association Established with")),
    (evt.EVT_REJECTED, lambda event: log_assoc_event(event, "Association Rejected by")),
    (evt.EVT_RELEASED, lambda event: log_assoc_event(event, "Association Released by")),
    (evt.EVT_ABORTED, lambda event: log_assoc_event(event, "Association Aborted by")),
    (evt.EVT_CONN_CLOSE, lambda event: log_assoc_event(event, "Connection Closed by")),
]

# --- Global State for Shutdown Coordination ---
shutdown_event = threading.Event()
server_thread_exception = None

# --- Server Thread Target Function ---
def _run_server_thread(ae: AE, address: tuple):
    """Target function run in a separate thread to host the pynetdicom server."""
    global server_thread_exception
    try:
        logger.info("Server thread started, starting pynetdicom AE server...")
        ae.start_server(address, evt_handlers=HANDLERS, block=True)
        logger.info("pynetdicom server stopped normally in thread.")
    except Exception as e:
        logger.error(f"Exception in pynetdicom server thread: {e}", exc_info=True)
        server_thread_exception = e
    finally:
        logger.info("Server thread finished.")
        shutdown_event.set()

# --- Main Execution Function (Major Refactor) ---
def run_dimse_server():
    """Configures and runs the DICOM DIMSE SCP server based on DB config, including DB status updates."""
    global shutdown_event, server_thread_exception
    db: Optional[Session] = None

    # 1. Get Listener Instance ID from Environment
    listener_id = os.environ.get("AXIOM_INSTANCE_ID")
    if not listener_id:
        # Fallback using hostname/default port, but log critical error
        fallback_id = f"dimse_{socket.getfqdn()}_unknownport"
        logger.critical("CRITICAL: AXIOM_INSTANCE_ID environment variable is not set!")
        logger.critical(f"Listener cannot reliably load its configuration. Using fallback ID: {fallback_id}")
        logger.critical("Set AXIOM_INSTANCE_ID in the environment (e.g., docker-compose.yml) for this listener.")
        # Decide whether to exit or try to continue with a potentially wrong config
        # For safety, let's exit if the ID isn't set explicitly.
        sys.exit("Listener startup failed: AXIOM_INSTANCE_ID not set.")
        # listener_id = fallback_id # Use this line instead of sys.exit if you want to attempt to run

    logger.info(f"Listener Instance ID: {listener_id}")

    # 2. Load Configuration from Database
    listener_config: Optional[models.DimseListenerConfig] = None
    try:
        db = SessionLocal()
        logger.info(f"Loading configuration for listener instance ID: {listener_id}")
        listener_config = crud_dimse_listener_config.get_by_instance_id(db, instance_id=listener_id)

        if not listener_config:
            logger.error(f"No DIMSE listener configuration found in database for instance_id '{listener_id}'. Listener cannot start.")
            sys.exit(f"Configuration Error: No config found for instance {listener_id}")

        if not listener_config.is_enabled:
            logger.warning(f"Listener configuration '{listener_config.name}' (ID: {listener_config.id}) found for instance_id '{listener_id}', but it is disabled. Listener will not start.")
            sys.exit(f"Configuration Disabled: Listener {listener_id} is disabled in config.")

        # Configuration loaded successfully
        ae_title = listener_config.ae_title
        port = listener_config.port
        config_name = listener_config.name # Store the name for use as source_identifier
        config_id = listener_config.id

        # --- Pass the loaded name to the handlers module ---
        set_current_listener_name(config_name)
        # --- End Passing Name ---

        logger.info(f"Loaded configuration '{config_name}' (ID: {config_id}): AE='{ae_title}', Port={port}")

    except Exception as e:
        logger.error(f"Failed to load listener configuration from database for instance_id '{listener_id}': {e}", exc_info=True)
        sys.exit(f"Database Error: Could not load config for {listener_id}")
    finally:
        if db: db.close()

    # 3. Attempt Initial DB Status Update (using loaded config details)
    initial_status = "starting"
    initial_message = f"Listener process initializing using config '{config_name}'."
    try:
        db = SessionLocal()
        logger.info(f"Updating DIMSE listener state for '{listener_id}' to '{initial_status}'...")
        # Use the separate STATE update function, passing loaded config details
        crud_dimse_listener_state.update_listener_state(
            db=db, listener_id=listener_id, status=initial_status,
            status_message=initial_message,
            host=settings.LISTENER_HOST, # Use bind host from settings
            port=port, # Use port from loaded config
            ae_title=ae_title # Use AE Title from loaded config
        )
        db.commit()
        logger.info(f"Initial status '{initial_status}' committed for '{listener_id}'.")
    except Exception as e:
        logger.error(f"Failed to set initial '{initial_status}' status in DB for '{listener_id}': {e}", exc_info=True)
        if db: db.rollback()
    finally:
        if db: db.close()

    # 4. Configure and Start AE (using loaded config)
    ae = AE(ae_title=ae_title) # Use AE Title from config
    logger.info(f"Setting {len(contexts)} supported Storage presentation contexts...")
    ae.supported_contexts = contexts
    logger.info("Adding Verification context (C-ECHO)...")
    ae.add_supported_context(Verification)

    logger.info(f"Starting DICOM DIMSE Listener Service:")
    logger.info(f"  Config Name: {config_name} (ID: {config_id})")
    logger.info(f"  Listener ID: {listener_id}")
    logger.info(f"  AE Title: {ae_title}")
    logger.info(f"  Bind Address: {settings.LISTENER_HOST}:{port}") # Use port from config
    logger.info(f"  DB Heartbeat Interval: {HEARTBEAT_INTERVAL_SECONDS}s")

    address = (settings.LISTENER_HOST, port) # Use loaded port
    server_thread = threading.Thread(target=_run_server_thread, args=(ae, address), daemon=True)
    server_thread.start()
    logger.info("Server thread launched.")
    time.sleep(1)

    # 5. Main Loop (Keep the logic from the previous working version)
    last_heartbeat_update_time = time.monotonic()
    current_status = initial_status
    status_message = initial_message
    first_successful_run_completed = False

    try:
        while not shutdown_event.is_set():
            if not server_thread.is_alive() and current_status != "error":
                logger.error("Pynetdicom server thread terminated unexpectedly!")
                current_status = "error"
                error_details = f"Error: {server_thread_exception or 'Unknown Reason'}"
                status_message = f"Server thread stopped unexpectedly. {error_details}"

            now_monotonic = time.monotonic()

            should_update_db = False
            if current_status == "error":
                 should_update_db = True
            elif not first_successful_run_completed:
                 should_update_db = True
            elif now_monotonic - last_heartbeat_update_time >= HEARTBEAT_INTERVAL_SECONDS:
                 should_update_db = True

            if should_update_db:
                db = None
                try:
                    db = SessionLocal()
                    update_committed = False

                    target_status = current_status
                    target_message = status_message
                    if current_status != "error" and current_status != "stopped":
                         if server_thread.is_alive():
                              target_status = "running"
                              target_message = "Listener active and accepting associations."
                         else:
                              target_status = "error"
                              target_message = f"Server thread stopped unexpectedly. {server_thread_exception or 'Unknown Reason'}"

                    logger.debug(f"DB Update Check: Current Internal Status='{current_status}', Target DB Status='{target_status}', Thread Alive={server_thread.is_alive()}, First Run Flag={first_successful_run_completed}")

                    # Always use the full update method to ensure status consistency
                    logger.debug(f"Attempting full state update for '{listener_id}' to target status '{target_status}'...")
                    updated_obj = crud_dimse_listener_state.update_listener_state(
                         db=db,
                         listener_id=listener_id, # Use the instance ID for the state record
                         status=target_status,
                         status_message=target_message if target_status != "running" else "Listener active.",
                         host=settings.LISTENER_HOST, # Use bind host
                         port=port, # Use port from loaded config
                         ae_title=ae_title # Use AE Title from loaded config
                    )

                    db.commit()
                    update_committed = True
                    current_status = target_status
                    status_message = target_message if target_status != "running" else "Listener active."

                    if current_status == "running" and not first_successful_run_completed:
                         first_successful_run_completed = True
                         logger.info(f"Listener '{listener_id}' status successfully set to 'running' in DB.")

                    last_heartbeat_update_time = now_monotonic
                    logger.debug(f"DB transaction committed for '{listener_id}'. Status written: '{current_status}'.")

                except Exception as e:
                    logger.error(f"Failed DB transaction for listener status/heartbeat: {e}", exc_info=settings.DEBUG)
                    if db:
                         try: db.rollback()
                         except Exception as rb_exc: logger.error(f"Error during rollback attempt: {rb_exc}")
                finally:
                    if db:
                        try: db.close()
                        except Exception as close_exc: logger.error(f"Error closing DB session: {close_exc}")

                if current_status != "running" and current_status != "starting":
                    logger.info(f"Listener status is terminal ('{current_status}'), exiting main loop.")
                    break

            shutdown_event.wait(timeout=1.0)

    except KeyboardInterrupt:
        logger.info("Shutdown signal (KeyboardInterrupt) received.")
        current_status = "stopped"
        status_message = "Listener stopped by user signal."
        shutdown_event.set()

    except Exception as main_loop_exc:
         logger.error(f"Unexpected error in main listener loop: {main_loop_exc}", exc_info=True)
         current_status = "error"
         status_message = f"Main loop failed unexpectedly: {main_loop_exc}"
         shutdown_event.set()

    finally:
        # --- Final actions (Keep existing logic, use loaded config details) ---
        logger.info(f"Initiating final shutdown sequence (final status: '{current_status}')...")
        db = None
        try:
            db = SessionLocal()
            # Use loaded config details for the final state update
            crud_dimse_listener_state.update_listener_state(
                db=db, listener_id=listener_id, status=current_status,
                status_message=status_message,
                host=settings.LISTENER_HOST,
                port=port if listener_config else None, # Use loaded port if available
                ae_title=ae_title if listener_config else None # Use loaded AE if available
            )
            db.commit()
            logger.info(f"Final DIMSE listener status update committed for '{listener_id}'.")
        except Exception as e:
            logger.error(f"Failed to set final status '{current_status}' in DB: {e}", exc_info=True)
            if db: db.rollback()
        finally:
            if db: db.close()

        if 'ae' in locals() and ae: ae.shutdown()
        if 'server_thread' in locals() and server_thread.is_alive():
            logger.info("Waiting for server thread to finish...")
            server_thread.join(timeout=10)
            if server_thread.is_alive(): logger.warning("Server thread did not exit cleanly within timeout.")
            else: logger.info("Server thread joined successfully.")
        else: logger.info("Server thread was not running or already finished.")
        logger.info("Listener service shut down complete.")


# --- Signal Handler (Keep as is) ---
def handle_signal(signum, frame):
    signal_name = signal.Signals(signum).name
    logger.warning(f"Received signal {signal_name} ({signum}). Triggering graceful shutdown...")
    shutdown_event.set()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# --- Script Entry Point (Keep as is) ---
if __name__ == "__main__":
    logger.info("Starting DICOM Listener Service directly...")
    run_dimse_server()
