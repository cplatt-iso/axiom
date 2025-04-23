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
from pynetdicom.presentation import PresentationContext # Import for type hint if needed

# SQLAlchemy Core imports
from sqlalchemy import select, update as sql_update, func

# Application imports
from app.core.config import settings
from app.services.network.dimse.handlers import handle_store, handle_echo
from app.db.session import SessionLocal, Session # Import Session type
from app.crud import crud_dimse_listener_state
from app.db import models # Import models for the direct query check

# --- Logging Setup ---
# Configure logging for this listener service
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("dicom_listener")
logger.setLevel(logging.INFO if not settings.DEBUG else logging.DEBUG)

# Configure pynetdicom's internal logger separately
pynetdicom_logger = logging.getLogger('pynetdicom')
pynetdicom_logger.setLevel(logging.INFO if settings.DEBUG else logging.WARNING) # INFO logs associations, WARNING is quieter

# Add stream handlers if not already configured (e.g., by a central logging setup)
if not logger.handlers:
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
if not pynetdicom_logger.handlers:
    pynetdicom_stream_handler = logging.StreamHandler(sys.stdout)
    pynetdicom_stream_handler.setFormatter(log_formatter)
    pynetdicom_logger.addHandler(pynetdicom_stream_handler)

# --- Configuration from Settings ---
AE_TITLE = settings.DICOM_LISTENER_AET
PORT = settings.DICOM_LISTENER_PORT
HOSTNAME = settings.LISTENER_HOST # Address to bind to (e.g., 0.0.0.0 for all interfaces)
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)
INCOMING_DIR.mkdir(parents=True, exist_ok=True) # Ensure storage path exists

# --- Listener State Configuration ---
# Unique ID for this listener instance, used for DB status tracking
LISTENER_ID = os.environ.get("AXIOM_INSTANCE_ID", f"dimse_{socket.getfqdn()}_{PORT}")
if "unknown" in LISTENER_ID or "localhost" in LISTENER_ID:
     logger.warning(f"Using potentially non-unique listener ID: {LISTENER_ID}")
     logger.warning("Set AXIOM_INSTANCE_ID environment variable for a stable unique ID if running multiple listeners.")
HEARTBEAT_INTERVAL_SECONDS = 30 # Frequency (seconds) for updating DB status/heartbeat

# --- Presentation Contexts ---
# Define the transfer syntaxes this SCP will support for storage operations
SUPPORTED_TRANSFER_SYNTAXES = [
    '1.2.840.10008.1.2.1', # Explicit VR Little Endian (Preferred)
    '1.2.840.10008.1.2',   # Implicit VR Little Endian
    '1.2.840.10008.1.2.2', # Explicit VR Big Endian
    '1.2.840.10008.1.2.5', # RLE Lossless
    '1.2.840.10008.1.2.4.50', # JPEG Baseline
    '1.2.840.10008.1.2.4.70', # JPEG Lossless, Non-Hierarchical
    '1.2.840.10008.1.2.4.90', # JPEG 2000 Image Compression Lossless Only
    '1.2.840.10008.1.2.4.91', # JPEG 2000 Image Compression
]
# Build a list of supported presentation contexts using the default storage SOP classes
contexts = []
for default_context in StoragePresentationContexts: # Iterate through pynetdicom's default storage contexts
    sop_class_uid = default_context.abstract_syntax # Get the SOP Class UID
    # Create a new context for this SOP class with our supported transfer syntaxes
    new_context = build_context(sop_class_uid, SUPPORTED_TRANSFER_SYNTAXES)
    contexts.append(new_context)

# --- Event Handlers ---
# Helper to log association-related events
def log_assoc_event(event, msg_prefix):
    """Logs details about an association event."""
    try:
        # Attempt to get detailed remote AE info
        remote_ae = event.assoc.requestor.ae_title
        remote_addr = event.assoc.requestor.address
        remote_port = event.assoc.requestor.port
        assoc_id = event.assoc.native_id
        logger.info(f"{msg_prefix}: {remote_ae} @ {remote_addr}:{remote_port} (Assoc ID: {assoc_id})")
    except Exception:
        # Fallback if association details aren't fully available
        logger.info(f"{msg_prefix}: {event.assoc}")

# Map pynetdicom event types to corresponding handler functions or logging callbacks
HANDLERS = [
    (evt.EVT_C_STORE, handle_store),        # Handle incoming C-STORE requests
    (evt.EVT_C_ECHO, handle_echo),          # Handle incoming C-ECHO requests
    (evt.EVT_ACCEPTED, lambda event: log_assoc_event(event, "Association Accepted from")),
    (evt.EVT_ESTABLISHED, lambda event: log_assoc_event(event, "Association Established with")),
    (evt.EVT_REJECTED, lambda event: log_assoc_event(event, "Association Rejected by")),
    (evt.EVT_RELEASED, lambda event: log_assoc_event(event, "Association Released by")),
    (evt.EVT_ABORTED, lambda event: log_assoc_event(event, "Association Aborted by")),
    (evt.EVT_CONN_CLOSE, lambda event: log_assoc_event(event, "Connection Closed by")),
]

# --- Global State for Shutdown Coordination ---
shutdown_event = threading.Event() # Event to signal threads to stop
server_thread_exception = None # To store any exception from the server thread

# --- Server Thread Target Function ---
def _run_server_thread(ae: AE, address: tuple):
    """Target function run in a separate thread to host the pynetdicom server."""
    global server_thread_exception
    try:
        logger.info("Server thread started, starting pynetdicom AE server...")
        # ae.start_server is blocking until ae.shutdown() is called or an error occurs
        ae.start_server(address, evt_handlers=HANDLERS, block=True)
        logger.info("pynetdicom server stopped normally in thread.")
    except Exception as e:
        logger.error(f"Exception in pynetdicom server thread: {e}", exc_info=True)
        server_thread_exception = e # Make exception available to main thread
    finally:
        logger.info("Server thread finished.")
        # Ensure main thread knows the server stopped, regardless of reason
        shutdown_event.set()


# --- Main Execution Function ---
def run_dimse_server():
    """Configures and runs the DICOM DIMSE SCP server, including DB status updates."""
    global shutdown_event, server_thread_exception
    db: Optional[Session] = None

    # Attempt initial DB status update before starting the server
    initial_status = "starting"
    initial_message = "Listener process initializing."
    try:
        db = SessionLocal()
        logger.info(f"Updating DIMSE listener state for '{LISTENER_ID}' to '{initial_status}'...")
        crud_dimse_listener_state.update_listener_state(
            db=db, listener_id=LISTENER_ID, status=initial_status,
            status_message=initial_message,
            host=HOSTNAME, port=PORT, ae_title=AE_TITLE
        )
        db.commit()
        logger.info(f"Initial status '{initial_status}' committed for '{LISTENER_ID}'.")
    except Exception as e:
        logger.error(f"Failed to set initial '{initial_status}' status in DB for '{LISTENER_ID}': {e}", exc_info=True)
        if db: db.rollback()
    finally:
        if db: db.close()

    # Configure the Application Entity (AE)
    ae = AE(ae_title=AE_TITLE)
    logger.info(f"Setting {len(contexts)} supported Storage presentation contexts...")
    ae.supported_contexts = contexts
    logger.info("Adding Verification context (C-ECHO)...")
    ae.add_supported_context(Verification)

    logger.info(f"Starting DICOM DIMSE Listener Service:")
    logger.info(f"  Listener ID: {LISTENER_ID}")
    logger.info(f"  AE Title: {AE_TITLE}")
    logger.info(f"  Bind Address: {HOSTNAME}:{PORT}")
    logger.info(f"  DB Heartbeat Interval: {HEARTBEAT_INTERVAL_SECONDS}s")

    # Start the pynetdicom server in a background thread
    address = (HOSTNAME, PORT)
    server_thread = threading.Thread(target=_run_server_thread, args=(ae, address), daemon=True)
    server_thread.start()
    logger.info("Server thread launched.")
    time.sleep(1)

    # Main loop: Monitor server thread health and send periodic DB heartbeats
    last_heartbeat_update_time = time.monotonic()
    current_status = initial_status # Start with the status we hopefully wrote initially
    status_message = initial_message
    first_successful_run_completed = False # Flag to track if we've updated to 'running' yet

    try:
        while not shutdown_event.is_set():
            # Check if server thread died unexpectedly
            if not server_thread.is_alive() and current_status != "error":
                logger.error("Pynetdicom server thread terminated unexpectedly!")
                current_status = "error"
                error_details = f"Error: {server_thread_exception or 'Unknown Reason'}"
                status_message = f"Server thread stopped unexpectedly. {error_details}"

            now_monotonic = time.monotonic()

            # Determine if DB update is needed
            should_update_db = False
            if current_status == "error": # Always try to update if error occurred
                 should_update_db = True
            elif not first_successful_run_completed: # Always try to update until first success
                 should_update_db = True
            elif now_monotonic - last_heartbeat_update_time >= HEARTBEAT_INTERVAL_SECONDS: # Update on interval
                 should_update_db = True

            # Perform DB Update
            if should_update_db:
                db = None
                try:
                    db = SessionLocal()
                    update_committed = False

                    # Determine target status based on current state and thread health
                    target_status = current_status
                    target_message = status_message
                    if current_status != "error" and current_status != "stopped": # If not already in terminal state
                         if server_thread.is_alive():
                              target_status = "running" # Assume running if thread is alive
                              target_message = "Listener active and accepting associations."
                         else: # Thread died but status wasn't 'error' yet
                              target_status = "error"
                              target_message = f"Server thread stopped unexpectedly. {server_thread_exception or 'Unknown Reason'}"

                    logger.debug(f"DB Update Check: Current Internal Status='{current_status}', Target DB Status='{target_status}', Thread Alive={server_thread.is_alive()}, First Run Flag={first_successful_run_completed}")

                    # Always use the full update method to ensure status consistency
                    logger.debug(f"Attempting full state update for '{LISTENER_ID}' to target status '{target_status}'...")
                    updated_obj = crud_dimse_listener_state.update_listener_state(
                         db=db,
                         listener_id=LISTENER_ID,
                         status=target_status,
                         status_message=target_message if target_status != "running" else "Listener active.", # Set specific message only if not running
                         host=HOSTNAME,
                         port=PORT,
                         ae_title=AE_TITLE
                    )

                    db.commit() # Commit the state change
                    update_committed = True
                    current_status = target_status # Update internal status *after* successful commit
                    status_message = target_message if target_status != "running" else "Listener active."

                    # Set flag only AFTER successful commit of 'running' state
                    if current_status == "running" and not first_successful_run_completed:
                         first_successful_run_completed = True
                         logger.info(f"Listener '{LISTENER_ID}' status successfully set to 'running' in DB.")

                    last_heartbeat_update_time = now_monotonic # Reset timer
                    logger.debug(f"DB transaction committed for '{LISTENER_ID}'. Status written: '{current_status}'.")

                except Exception as e:
                    logger.error(f"Failed DB transaction for listener status/heartbeat: {e}", exc_info=settings.DEBUG)
                    if db:
                         try: db.rollback()
                         except Exception as rb_exc: logger.error(f"Error during rollback attempt: {rb_exc}")
                finally:
                    if db:
                        try: db.close()
                        except Exception as close_exc: logger.error(f"Error closing DB session: {close_exc}")

                # Exit loop if status is terminal (error or stopped)
                if current_status != "running" and current_status != "starting": # Also exit if still 'starting' after update attempt fails? No, let it retry.
                    logger.info(f"Listener status is terminal ('{current_status}'), exiting main loop.")
                    break

            # Wait efficiently for the next check or shutdown signal
            shutdown_event.wait(timeout=1.0) # Check every second

    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        logger.info("Shutdown signal (KeyboardInterrupt) received.")
        current_status = "stopped"
        status_message = "Listener stopped by user signal."
        shutdown_event.set() # Ensure shutdown event is set

    except Exception as main_loop_exc:
         # Catch any other unexpected errors in this monitoring loop
         logger.error(f"Unexpected error in main listener loop: {main_loop_exc}", exc_info=True)
         current_status = "error"
         status_message = f"Main loop failed unexpectedly: {main_loop_exc}"
         shutdown_event.set() # Trigger shutdown on unexpected main loop failure

    finally:
        # Final actions before the process exits
        logger.info(f"Initiating final shutdown sequence (final status: '{current_status}')...")
        db = None
        try:
            db = SessionLocal()
            crud_dimse_listener_state.update_listener_state(
                db=db, listener_id=LISTENER_ID, status=current_status,
                status_message=status_message
            )
            db.commit()
            logger.info(f"Final DIMSE listener status update committed for '{LISTENER_ID}'.")
        except Exception as e:
            logger.error(f"Failed to set final status '{current_status}' in DB: {e}", exc_info=True)
            if db: db.rollback()
        finally:
            if db: db.close()

        if 'ae' in locals() and ae: # Check if ae exists
            ae.shutdown()

        if 'server_thread' in locals() and server_thread.is_alive(): # Check if thread exists and started
            logger.info("Waiting for server thread to finish...")
            server_thread.join(timeout=10) # Give it time to exit
            if server_thread.is_alive():
                 logger.warning("Server thread did not exit cleanly within timeout.")
            else:
                 logger.info("Server thread joined successfully.")
        else:
             logger.info("Server thread was not running or already finished.")

        logger.info("Listener service shut down complete.")

# --- Signal Handler ---
def handle_signal(signum, frame):
    """Callback function to handle OS signals for graceful shutdown."""
    signal_name = signal.Signals(signum).name
    logger.warning(f"Received signal {signal_name} ({signum}). Triggering graceful shutdown...")
    # Set the event to break the main loop and signal the server thread
    shutdown_event.set()

# Register handlers for common termination signals
signal.signal(signal.SIGINT, handle_signal)  # Ctrl+C
signal.signal(signal.SIGTERM, handle_signal) # kill, docker stop

# --- Script Entry Point ---
if __name__ == "__main__":
    # Allows running the listener directly, e.g., python -m app.services.network.dimse.server
    logger.info("Starting DICOM Listener Service directly...")
    run_dimse_server()
