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
from app.core.config import settings
from app.services.network.dimse.handlers import handle_store, handle_echo, set_current_listener_context # Correct function name
from app.db.session import SessionLocal, Session
from app.crud import crud_dimse_listener_state, crud_dimse_listener_config
from app.db import models

# --- Logging Setup ---
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
    """Logs details about an association event."""
    try:
        remote_ae = event.assoc.requestor.ae_title
        remote_addr = event.assoc.requestor.address
        remote_port = event.assoc.requestor.port
        assoc_id = event.assoc.native_id
        logger.info(f"{msg_prefix}: {remote_ae} @ {remote_addr}:{remote_port} (Assoc ID: {assoc_id})")
    except Exception:
        logger.info(f"{msg_prefix}: {event.assoc}")

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

# --- Global State ---
shutdown_event = threading.Event()
server_thread_exception = None

# --- Server Thread ---
def _run_server_thread(ae: AE, address: tuple):
    """Target function run in a separate thread."""
    global server_thread_exception
    try:
        logger.info("Server thread: starting pynetdicom AE server...")
        # Changed evt_handlers= to handlers= as per pynetdicom 1.5+ start_server API
        ae.start_server(address, evt_handlers=HANDLERS, block=True)
        logger.info("Server thread: pynetdicom server stopped normally.")
    except Exception as e:
        logger.error(f"Server thread: Exception: {e}", exc_info=True)
        server_thread_exception = e
    finally:
        logger.info("Server thread: finished.")
        shutdown_event.set()

# --- Main Execution Function ---
def run_dimse_server():
    """Configures and runs the DICOM DIMSE SCP server based on DB config."""
    global shutdown_event, server_thread_exception
    db: Optional[Session] = None
    listener_config: Optional[models.DimseListenerConfig] = None
    ae_title = "DEFAULT_AE"
    port = 11112 # Default fallback port
    config_name = "UNKNOWN_CONFIG"
    instance_id_for_handler = "UNKNOWN_INSTANCE"

    # 1. Get Listener Instance ID
    listener_id = os.environ.get("AXIOM_INSTANCE_ID")
    if not listener_id:
        logger.critical("CRITICAL: AXIOM_INSTANCE_ID environment variable not set! Listener cannot start.")
        sys.exit(1)
    instance_id_for_handler = listener_id
    logger.info(f"Listener Instance ID: {listener_id}")

    # 2. Load Configuration from DB
    try:
        db = SessionLocal()
        logger.info(f"Loading configuration for listener instance ID: {listener_id}")
        listener_config = crud_dimse_listener_config.get_by_instance_id(db, instance_id=listener_id)
        if not listener_config:
            logger.error(f"Config Error: No config found for instance {listener_id}. Listener cannot start.")
            sys.exit(1)
        if not listener_config.is_enabled:
            logger.warning(f"Config Disabled: Listener {listener_id} is disabled in config. Listener will not start.")
            sys.exit(1)

        ae_title = listener_config.ae_title
        port = listener_config.port
        config_name = listener_config.name
        config_id = listener_config.id # For logging

        # Set context for handlers
        set_current_listener_context(config_name, instance_id_for_handler) # Use CORRECT function name

        logger.info(f"Loaded config '{config_name}' (ID: {config_id}): AE='{ae_title}', Port={port}")

    except Exception as e:
        logger.error(f"DB Error: Failed to load config for {listener_id}: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if db: db.close()

    # 3. Initial DB Status Update
    initial_status = "starting"
    initial_message = f"Listener initializing using config '{config_name}'."
    try:
        db = SessionLocal()
        logger.info(f"Setting initial status '{initial_status}' for '{listener_id}'...")
        # Use update_listener_state which handles create/update and requires commit outside
        crud_dimse_listener_state.update_listener_state(
            db=db, listener_id=listener_id, status=initial_status,
            status_message=initial_message, host=settings.LISTENER_HOST,
            port=port, ae_title=ae_title
        )
        db.commit() # Commit the initial state
        logger.info(f"Initial status '{initial_status}' committed for '{listener_id}'.")
    except Exception as e:
        logger.error(f"Failed set initial status in DB for '{listener_id}': {e}", exc_info=True)
        if db: db.rollback()
    finally:
        if db: db.close()

    # 4. Configure and Start AE
    ae = AE(ae_title=ae_title)
    logger.info(f"Setting {len(contexts)} Storage contexts...")
    ae.supported_contexts = contexts
    logger.info("Adding Verification context...")
    ae.add_supported_context(Verification)
    logger.info(f"Starting DICOM Listener: Config='{config_name}', Instance='{listener_id}', AE='{ae_title}', Addr={settings.LISTENER_HOST}:{port}, Heartbeat={HEARTBEAT_INTERVAL_SECONDS}s")
    address = (settings.LISTENER_HOST, port)
    server_thread = threading.Thread(target=_run_server_thread, args=(ae, address), daemon=True)
    server_thread.start()
    logger.info("Server thread launched.")
    time.sleep(1) # Give thread a moment to start

    # 5. Main Loop
    last_heartbeat_update_time = time.monotonic()
    current_status = initial_status # Tracks status last known to be committed
    status_message = initial_message
    first_successful_run_completed = False

    try:
        while not shutdown_event.is_set():
            # Check thread status first
            is_thread_alive = server_thread.is_alive()
            target_status = current_status
            target_message = status_message

            if current_status not in ("error", "stopped"):
                if is_thread_alive:
                    target_status = "running"
                    target_message = "Listener active."
                elif current_status != "starting": # Only transition to error if not already starting
                    logger.error("Pynetdicom server thread terminated unexpectedly!")
                    target_status = "error"
                    error_details = f"Error: {server_thread_exception or 'Unknown Reason'}"
                    target_message = f"Server thread stopped unexpectedly. {error_details}"
            # --- End thread status check ---

            # Determine if DB update needed
            now_monotonic = time.monotonic()
            interval_elapsed = (now_monotonic - last_heartbeat_update_time >= HEARTBEAT_INTERVAL_SECONDS)
            status_changed = (target_status != current_status)
            should_update_db = status_changed or (target_status == "running" and interval_elapsed) or (target_status == "running" and not first_successful_run_completed)

            if should_update_db:
                db = None
                update_attempted = False # Track if we tried to update DB
                try:
                    db = SessionLocal()
                    is_already_running = (current_status == "running" and target_status == "running")

                    if is_already_running and interval_elapsed:
                        logger.debug(f"Updating heartbeat only for '{listener_id}'...")
                        update_attempted = True
                        # Call the heartbeat function (which executes UPDATE but doesn't commit)
                        updated_obj = crud_dimse_listener_state.update_listener_heartbeat(db=db, listener_id=listener_id)
                        if updated_obj is None:
                            logger.warning(f"Heartbeat update statement failed to find listener '{listener_id}'.")
                            update_attempted = False # Don't try to commit if update failed
                        # <<< COMMIT NEEDED HERE for heartbeat path >>>
                        if update_attempted:
                             db.commit()
                             logger.debug(f"Committed heartbeat update for '{listener_id}'.")

                    elif status_changed or (target_status == "running" and not first_successful_run_completed): # Status change or initial run confirmation
                        logger.debug(f"Performing full state update for '{listener_id}' to target '{target_status}'...")
                        update_attempted = True
                        # update_listener_state prepares the change but does NOT commit
                        crud_dimse_listener_state.update_listener_state(
                            db=db, listener_id=listener_id, status=target_status,
                            status_message=target_message if target_status != "running" else "Listener active.",
                            host=settings.LISTENER_HOST, port=port, ae_title=ae_title
                        )
                        db.commit() # Commit the state change here
                        logger.debug(f"Committed full state update for '{listener_id}'.")
                    else:
                         # This case should not be reached if should_update_db logic is correct
                         logger.warning("Reached unexpected state in DB update logic.")


                    # Update internal state ONLY if DB update was successful
                    if update_attempted and db.is_active: # Check if transaction is still active (commit succeeded)
                        current_status = target_status
                        status_message = target_message if target_status != "running" else "Listener active."
                        last_heartbeat_update_time = now_monotonic # Reset timer only on successful update/commit
                        if current_status == "running" and not first_successful_run_completed:
                            first_successful_run_completed = True
                            logger.info(f"Listener '{listener_id}' confirmed running in DB.")
                        logger.debug(f"DB update cycle complete for '{listener_id}'. Internal status: '{current_status}'")

                except Exception as e:
                    logger.error(f"Failed DB transaction for listener status/heartbeat: {e}", exc_info=settings.DEBUG)
                    if db: db.rollback()
                finally:
                    if db: db.close()

            # Check if status is now terminal after the potential update attempt
            if current_status not in ("running", "starting"):
                logger.info(f"Listener status '{current_status}', exiting main loop.")
                break # Exit loop if stopped or error

            shutdown_event.wait(timeout=1.0) # Wait for signal or next loop interval
    except KeyboardInterrupt:
        logger.info("Shutdown signal (KeyboardInterrupt) received.")
        current_status = "stopped"; status_message = "Listener stopped by signal."; shutdown_event.set()
    except Exception as main_loop_exc:
        logger.error(f"Unexpected error in main listener loop: {main_loop_exc}", exc_info=True)
        current_status = "error"; status_message = f"Main loop failed: {main_loop_exc}"; shutdown_event.set()
    finally:
        # Final actions
        logger.info(f"Initiating final shutdown (final status: '{current_status}')...")
        db = None
        try:
            db = SessionLocal()
            # Use update_listener_state to handle create/update logic
            crud_dimse_listener_state.update_listener_state(
                db=db, listener_id=listener_id, status=current_status, status_message=status_message,
                host=settings.LISTENER_HOST, port=port, ae_title=ae_title
            )
            db.commit() # Commit final status
            logger.info(f"Final status committed for '{listener_id}'.")
        except Exception as e:
            logger.error(f"Failed final status update for '{listener_id}': {e}", exc_info=True)
            if db: db.rollback()
        finally:
            if db: db.close()

        if 'ae' in locals() and ae: ae.shutdown()
        if 'server_thread' in locals() and server_thread.is_alive():
            logger.info("Waiting for server thread..."); server_thread.join(timeout=5)
            if server_thread.is_alive(): logger.warning("Server thread did not exit cleanly.")
            else: logger.info("Server thread joined.")
        else: logger.info("Server thread not running or already finished.")
        logger.info("Listener service shut down complete.")

# --- Signal Handler ---
def handle_signal(signum, frame):
    signal_name = signal.Signals(signum).name
    logger.warning(f"Received {signal_name} ({signum}). Triggering shutdown...")
    shutdown_event.set()
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# --- Script Entry Point ---
if __name__ == "__main__":
    logger.info("Starting DICOM Listener Service directly...")
    run_dimse_server()
