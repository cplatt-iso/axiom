# app/services/network/dimse/server.py

import logging
import sys
import time
import socket # Import socket to get hostname
import threading # Import threading
import signal # Import signal for graceful shutdown
import os
from pathlib import Path
from datetime import datetime,timezone # Import datetime

# --- Imports ---
from pynetdicom import AE, StoragePresentationContexts, evt, ALL_TRANSFER_SYNTAXES
from pynetdicom.sop_class import Verification
# --- Removed Redis Import ---
# import redis
from app.core.config import settings
from app.services.network.dimse.handlers import handle_store, handle_echo

# --- DB Imports (NEW) ---
from app.db.session import SessionLocal
from app.crud import crud_dimse_listener_state
# --- End DB Imports ---

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("dicom_listener")
logger.setLevel(logging.INFO if not settings.DEBUG else logging.DEBUG)
if not logger.handlers:
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
pynetdicom_logger = logging.getLogger('pynetdicom')
# Set pynetdicom log level (optional, INFO logs association details, WARNING is less verbose)
pynetdicom_logger.setLevel(logging.INFO if settings.DEBUG else logging.WARNING)
if not pynetdicom_logger.handlers:
    pynetdicom_stream_handler = logging.StreamHandler(sys.stdout)
    pynetdicom_stream_handler.setFormatter(log_formatter)
    pynetdicom_logger.addHandler(pynetdicom_stream_handler)


# --- Configuration ---
AE_TITLE = settings.LISTENER_AE_TITLE
PORT = settings.LISTENER_PORT
HOSTNAME = settings.LISTENER_HOST # Address to bind to (e.g., 0.0.0.0)
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)

# --- Listener State Config (NEW) ---
LISTENER_ID = os.environ.get("AXIOM_INSTANCE_ID", "dimse_listener_unknown") # Use Fully Qualified Domain Name as default ID
if LISTENER_ID == "dimse_listener_unknown":
     logger.warning("Environment variable AXIOM_INSTANCE_ID not set! Using default ID. Multiple listeners might overwrite status.")
HEARTBEAT_INTERVAL_SECONDS = 30 # How often to update DB heartbeat

# --- Presentation Contexts Preparation ---
# (storage_contexts and contexts setup remains the same)
SUPPORTED_TRANSFER_SYNTAXES = [
    '1.2.840.10008.1.2',   # Implicit VR Little Endian
    '1.2.840.10008.1.2.1', # Explicit VR Little Endian
    '1.2.840.10008.1.2.2', # Explicit VR Big Endian (less common)
    # Add other commonly supported transfer syntaxes (JPEG, J2K, etc.) if needed
    '1.2.840.10008.1.2.5', # RLE Lossless
    '1.2.840.10008.1.2.4.50', # JPEG Baseline
    '1.2.840.10008.1.2.4.70', # JPEG Lossless, Non-Hierarchical
    '1.2.840.10008.1.2.4.90', # JPEG 2000 Image Compression Lossless Only
    '1.2.840.10008.1.2.4.91', # JPEG 2000 Image Compression
]
# Use default StoragePresentationContexts and add our transfer syntaxes
contexts = []
for context in StoragePresentationContexts:
    # Create a new context object to avoid modifying the defaults globally
    new_context = context # Or copy if needed: new_context = context.copy()
    new_context.transfer_syntax = SUPPORTED_TRANSFER_SYNTAXES
    contexts.append(new_context)

# --- Event Handlers Mapping ---
# (HANDLERS list remains the same, maybe add more logging)
def log_assoc_event(event, msg_prefix):
    """Helper to log association events."""
    try:
        remote_info = f"{event.assoc.requestor.ae_title} @ {event.assoc.requestor.address}:{event.assoc.requestor.port}"
        logger.info(f"{msg_prefix}: {remote_info} (Assoc ID: {event.assoc.native_id})")
    except Exception:
        logger.info(f"{msg_prefix}: {event.assoc}") # Fallback logging

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

# --- Global variable to signal shutdown ---
shutdown_event = threading.Event()
server_thread_exception = None # Store exception from server thread if it fails

# --- Server Thread Function ---
def _run_server_thread(ae, address):
    """Target function for the server thread."""
    global server_thread_exception
    try:
        # start_server is blocking when block=True (default)
        logger.info("Server thread started, starting pynetdicom server...")
        ae.start_server(address, evt_handlers=HANDLERS, block=True)
        logger.info("pynetdicom server stopped normally in thread.")
    except Exception as e:
        logger.error(f"Exception in server thread: {e}", exc_info=True)
        server_thread_exception = e # Store exception for main thread
    finally:
        logger.info("Server thread finished.")
        # Signal main thread that server stopped (expectedly or unexpectedly)
        shutdown_event.set() # Ensure main loop exits if server stops


# --- Main Execution Function ---
def run_dimse_server():
    """Configures and runs the DICOM DIMSE SCP server with DB heartbeat."""
    global shutdown_event, server_thread_exception
    db = None # Initialize db to None

    # --- Initial DB Status Update (Keep as is, using update_listener_state which handles create) ---
    try:
        db = SessionLocal()
        logger.info(f"Updating DIMSE listener state for '{LISTENER_ID}' to 'starting'...")
        crud_dimse_listener_state.update_listener_state(
            db=db,
            listener_id=LISTENER_ID,
            status="starting",
            status_message="Listener process initializing.",
            host=HOSTNAME,
            port=PORT,
            ae_title=AE_TITLE
        )
        db.commit()  # <-- COMMIT after initial update
        logger.info(f"Initial status 'starting' committed for '{LISTENER_ID}'.")
    except Exception as e:
        logger.error(f"Failed to set initial 'starting' status in DB for '{LISTENER_ID}': {e}", exc_info=True)
        if db: db.rollback() # <-- ROLLBACK on error
        # Continue starting server, but log the DB error
    finally:
        if db: db.close() # Close session after initial update

    # --- Configure AE ---
    # (Keep AE configuration as is)
    ae = AE(ae_title=AE_TITLE)
    logger.info(f"Adding {len(contexts)} Storage contexts...")
    for context in contexts:
        ae.add_supported_context(context.abstract_syntax, transfer_syntax=context.transfer_syntax)
    logger.info("Adding Verification context (C-ECHO)...")
    ae.add_supported_context(Verification)
    logger.info(f"Starting DICOM DIMSE Listener Service:")
    logger.info(f"  Listener ID: {LISTENER_ID}")
    logger.info(f"  AE Title: {AE_TITLE}")
    logger.info(f"  Bind Address: {HOSTNAME}:{PORT}")
    logger.info(f"  DB Heartbeat Enabled (Interval: {HEARTBEAT_INTERVAL_SECONDS}s)")

    # --- Start Server Thread ---
    # (Keep server thread start as is)
    address = (HOSTNAME, PORT)
    server_thread = threading.Thread(target=_run_server_thread, args=(ae, address), daemon=True)
    server_thread.start()
    logger.info("Server thread launched.")
    time.sleep(2) # Give thread a moment to start server and potentially error early

    # --- Main Loop for Heartbeat ---
    last_heartbeat_update = time.time()
    current_status = "running" # Assume running after successful start
    status_message = "Listener active and accepting associations." # Default message

    try:
        while not shutdown_event.is_set():
            # Check if server thread is still alive
            if not server_thread.is_alive() and current_status == "running":
                logger.error("Server thread terminated unexpectedly!")
                current_status = "error"
                status_message = f"Server thread stopped unexpectedly. Error: {server_thread_exception or 'Unknown'}"
                # Use current time for comparison below to force immediate DB update attempt
                now = time.time()
            else:
                # Still running (or just detected error), proceed with potential heartbeat update
                now = time.time()


            # Periodic Heartbeat Update OR Immediate Update on Error
            # Update if interval passed OR if status just changed to non-running
            if (now - last_heartbeat_update >= HEARTBEAT_INTERVAL_SECONDS) or current_status != "running":
                db = None
                try:
                    db = SessionLocal()
                    # --- REVISED UPDATE LOGIC ---
                    # 1. Get the existing record
                    db_obj = crud_dimse_listener_state.get_listener_state(db=db, listener_id=LISTENER_ID)

                    if db_obj:
                        # 2. Update attributes directly
                        db_obj.status = current_status
                        # Only set status_message if not 'running' to avoid clearing error messages accidentally
                        db_obj.status_message = status_message if current_status != "running" else None
                        db_obj.last_heartbeat = datetime.now(timezone.utc) # Update heartbeat timestamp
                        # Host/Port/AE Title likely don't change, no need to update unless desired

                        # 3. Commit the session
                        db.commit() # Commit the changes made to db_obj
                        last_heartbeat_update = now # Update time only on successful commit
                        logger.debug(f"DIMSE Listener DB status committed for '{LISTENER_ID}': '{current_status}'")
                    else:
                        # This case should ideally not happen after initial start, but log if it does
                        logger.error(f"Listener state record not found for ID '{LISTENER_ID}' during heartbeat update. Attempting re-creation.")
                        # Attempt to recreate the record using the update function (which handles create)
                        crud_dimse_listener_state.update_listener_state(
                             db=db, listener_id=LISTENER_ID, status=current_status,
                             status_message=status_message if current_status != "running" else None,
                             host=HOSTNAME, port=PORT, ae_title=AE_TITLE
                        )
                        db.commit() # Commit the recreation attempt
                        last_heartbeat_update = now
                        logger.info(f"Recreated listener state record for '{LISTENER_ID}' with status '{current_status}'.")

                    # --- END REVISED UPDATE LOGIC ---

                except Exception as e:
                    logger.error(f"Failed to update/commit listener status in DB: {e}", exc_info=settings.DEBUG)
                    if db: db.rollback() # Rollback on any exception during the block
                finally:
                    if db: db.close() # Always close the session

            # Exit loop immediately if status is no longer 'running'
            if current_status != "running":
                 break

            # Sleep until next heartbeat check or shutdown signal
            shutdown_event.wait(timeout=1.0) # Wait for 1 sec or until event is set

    except KeyboardInterrupt:
        logger.info("Shutdown signal (KeyboardInterrupt) received.")
        current_status = "stopped"
        status_message = "Listener stopped by user signal."
        shutdown_event.set() # Signal server thread to stop

    except Exception as main_loop_exc: # Catch unexpected errors in main loop
         logger.error(f"Unexpected error in main listener loop: {main_loop_exc}", exc_info=True)
         current_status = "error"
         status_message = f"Main loop failed: {main_loop_exc}"
         shutdown_event.set() # Attempt graceful shutdown

    finally:
        logger.info(f"Initiating final shutdown sequence (final status: '{current_status}')...")

        # --- Final DB Status Update (using revised fetch-update-commit logic) ---
        db = None
        try:
            db = SessionLocal()
            # 1. Get the existing record
            db_obj = crud_dimse_listener_state.get_listener_state(db=db, listener_id=LISTENER_ID)
            if db_obj:
                # 2. Update attributes
                db_obj.status = current_status
                db_obj.status_message = status_message # Set final message
                db_obj.last_heartbeat = datetime.now(timezone.utc) # Update heartbeat one last time
                # 3. Commit
                db.commit()
                logger.info(f"Final DIMSE listener status update committed for '{LISTENER_ID}'.")
            else:
                # Log if record couldn't be found for final update
                logger.warning(f"Could not find listener state record for '{LISTENER_ID}' during final shutdown update.")

        except Exception as e:
            logger.error(f"Failed to set final status '{current_status}' in DB: {e}", exc_info=True)
            if db: db.rollback()
        finally:
            if db: db.close()

        # --- Shutdown pynetdicom Server ---
        logger.info("Waiting for server thread to finish...")
        server_thread.join(timeout=10) # Wait for thread to exit
        if server_thread.is_alive():
             logger.warning("Server thread did not exit cleanly after 10 seconds.")
        else:
             logger.info("Server thread joined successfully.")

        logger.info("Listener service shut down complete.")

# --- Signal Handler ---
def handle_signal(signum, frame):
    """Handle termination signals gracefully."""
    logger.warning(f"Received signal {signum}. Triggering graceful shutdown...")
    shutdown_event.set()

# Register signal handlers for SIGINT (Ctrl+C) and SIGTERM (kill/docker stop)
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

# --- Run Main Function ---
if __name__ == "__main__":
    run_dimse_server()
