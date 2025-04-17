# app/services/network/dimse/server.py

import logging
import sys
import time
from pathlib import Path

# --- Imports ---
from pynetdicom import AE, StoragePresentationContexts, evt, ALL_TRANSFER_SYNTAXES
from pynetdicom.sop_class import Verification
import redis
from app.core.config import settings
from app.services.network.dimse.handlers import handle_store, handle_echo

# --- Logging Setup ---
# ... (logging setup remains the same) ...
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("dicom_listener")
logger.setLevel(logging.INFO if not settings.DEBUG else logging.DEBUG)
if not logger.handlers:
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)
pynetdicom_logger = logging.getLogger('pynetdicom')
pynetdicom_logger.setLevel(logging.WARNING)
if not pynetdicom_logger.handlers:
    pynetdicom_stream_handler = logging.StreamHandler(sys.stdout)
    pynetdicom_stream_handler.setFormatter(log_formatter)
    pynetdicom_logger.addHandler(pynetdicom_stream_handler)


# --- Configuration ---
# ... (AE_TITLE, PORT, HOSTNAME, INCOMING_DIR remain the same) ...
AE_TITLE = settings.LISTENER_AE_TITLE
PORT = settings.LISTENER_PORT
HOSTNAME = settings.LISTENER_HOST
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)

# --- Redis Configuration ---
# ... (redis config remains the same) ...
redis_client = None
LISTENER_HEARTBEAT_KEY = "axiom_flow:listener:heartbeat"
HEARTBEAT_INTERVAL_SECONDS = 15
try:
    logger.info(f"Attempting to connect to Redis at {settings.REDIS_URL} for listener heartbeat...")
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_connect_timeout=5)
    redis_client.ping()
    logger.info(f"Redis connection successful for listener heartbeat (Key: {LISTENER_HEARTBEAT_KEY}).")
except Exception as e:
    logger.error(f"Failed to connect to Redis for listener heartbeat: {e}", exc_info=settings.DEBUG)
    redis_client = None

# --- Presentation Contexts Preparation ---
# ... (contexts setup remains the same) ...
SUPPORTED_TRANSFER_SYNTAXES = [ '1.2.840.10008.1.2', '1.2.840.10008.1.2.1', '1.2.840.10008.1.2.2', '1.2.840.10008.1.2.5', '1.2.840.10008.1.2.4.50', '1.2.840.10008.1.2.4.51', '1.2.840.10008.1.2.4.57', '1.2.840.10008.1.2.4.70', '1.2.840.10008.1.2.4.80', '1.2.840.10008.1.2.4.81', '1.2.840.10008.1.2.4.90', '1.2.840.10008.1.2.4.91', ]
storage_contexts = list(StoragePresentationContexts)
contexts = []
for context in storage_contexts: context.transfer_syntax = SUPPORTED_TRANSFER_SYNTAXES; contexts.append(context)

# --- Event Handlers Mapping ---
# ... (HANDLERS list remains the same) ...
HANDLERS = [ (evt.EVT_C_STORE, handle_store), (evt.EVT_C_ECHO, handle_echo), (evt.EVT_ACCEPTED, lambda event: logger.info(f"Association Accepted: {event.assoc}")), (evt.EVT_ESTABLISHED, lambda event: logger.info(f"Association Established: {event.assoc}")), (evt.EVT_REJECTED, lambda event: logger.warning(f"Association Rejected: {event.assoc}")), (evt.EVT_RELEASED, lambda event: logger.info(f"Association Released: {event.assoc}")), (evt.EVT_ABORTED, lambda event: logger.warning(f"Association Aborted: {event.assoc}")), (evt.EVT_CONN_CLOSE, lambda event: logger.info(f"Connection Closed: {event.assoc}")), ]


# --- Main Execution ---
def run_dimse_server():
    """Configures and runs the DICOM DIMSE SCP server with heartbeat."""
    ae = AE(ae_title=AE_TITLE)

    # Add supported presentation contexts
    # ... (context setup remains the same) ...
    logger.info(f"Adding {len(contexts)} Storage contexts...")
    for context in contexts: ae.add_supported_context(context.abstract_syntax, transfer_syntax=context.transfer_syntax)
    logger.info("Adding Verification context (C-ECHO)...")
    ae.add_supported_context(Verification)

    logger.info(f"Starting DICOM DIMSE Server:")
    # ... (logging remains the same) ...
    logger.info(f"  AE Title: {AE_TITLE}")
    logger.info(f"  Host: {HOSTNAME}")
    logger.info(f"  Port: {PORT}")
    if redis_client: logger.info(f"  Redis Heartbeat Enabled...")
    else: logger.warning("  Redis Heartbeat Disabled...")


    # Start the server non-blockingly
    server = ae.start_server((HOSTNAME, PORT), block=False, evt_handlers=HANDLERS)
    logger.info("Server started in background, listening for associations...")

    last_heartbeat_time = 0

    try:
        # Main loop for heartbeat and keeping the process alive
        while True:
            # --- REMOVED check for server.is_running ---
            # if not server: # Simple check if server object exists
            #     logger.warning("Server object is invalid. Exiting loop.")
            #     break
            # --- END REMOVED check ---

            # Periodic Heartbeat
            current_time = time.time()
            if redis_client and (current_time - last_heartbeat_time >= HEARTBEAT_INTERVAL_SECONDS):
                try:
                    redis_client.set(LISTENER_HEARTBEAT_KEY, int(current_time), ex=HEARTBEAT_INTERVAL_SECONDS * 3)
                    last_heartbeat_time = current_time
                    logger.debug(f"Listener heartbeat sent ({int(current_time)}).")
                except redis.exceptions.ConnectionError as redis_conn_err:
                     logger.error(f"Redis connection error during heartbeat: {redis_conn_err}")
                except Exception as e:
                    logger.error(f"Failed to send listener heartbeat to Redis: {e}", exc_info=settings.DEBUG)

            # Sleep for a short duration before the next check/heartbeat
            time.sleep(1) # Check status/send heartbeat roughly every second

    except KeyboardInterrupt:
        logger.info("Server received shutdown signal (KeyboardInterrupt).")
    finally:
        # Ensure proper shutdown
        logger.info("Shutting down server...")
        if server: # Check if server object was successfully created
            server.shutdown() # Tell the background server threads to stop
        else:
             logger.warning("Server object was not created, cannot call shutdown().")

        if redis_client:
            try: redis_client.close(); logger.info("Redis connection closed.")
            except Exception as e: logger.error(f"Error closing Redis connection: {e}", exc_info=settings.DEBUG)
        logger.info("Server shut down complete.")


if __name__ == "__main__":
    run_dimse_server()
