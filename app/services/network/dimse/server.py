# app/services/network/dimse/server.py

import logging
import sys
from pathlib import Path

from pynetdicom import AE, StoragePresentationContexts, evt, ALL_TRANSFER_SYNTAXES
# No longer importing VerificationSOPClass directly
# Add other SOP classes as needed, e.g., StudyRootQueryRetrieveInformationModelFind

from app.core.config import settings
from app.services.network.dimse.handlers import handle_store, handle_echo # Import handlers
# from app.services.network.dimse.find_handler import handle_find # Example for later

# --- Logging Setup ---
# Configure logging specifically for this service/process
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("dicom_listener") # Main logger for the server
logger.setLevel(logging.INFO) # Set level (e.g., INFO, DEBUG)

# Ensure logs go to standard output (for Docker)
# Avoid adding handlers multiple times if module reloads
if not logger.handlers:
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(log_formatter)
    logger.addHandler(stream_handler)

# Also configure level for pynetdicom library logger if needed
pynetdicom_logger = logging.getLogger('pynetdicom')
pynetdicom_logger.setLevel(logging.WARNING) # Set to INFO or DEBUG for more details
if not pynetdicom_logger.handlers:
    pynetdicom_stream_handler = logging.StreamHandler(sys.stdout)
    pynetdicom_stream_handler.setFormatter(log_formatter) # Use same formatter for consistency
    pynetdicom_logger.addHandler(pynetdicom_stream_handler) # Send its logs to stdout too


# --- Configuration ---
AE_TITLE = settings.DICOM_SCP_AE_TITLE
PORT = settings.DICOM_SCP_PORT
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)

# Ensure the incoming directory exists (redundant with config.py, but safe)
INCOMING_DIR.mkdir(parents=True, exist_ok=True)

# --- Presentation Contexts Preparation (Storage Only Initially) ---
# Define transfer syntaxes explicitly (recommended over ALL_TRANSFER_SYNTAXES)
SUPPORTED_TRANSFER_SYNTAXES = [
    '1.2.840.10008.1.2',        # Implicit VR Little Endian
    '1.2.840.10008.1.2.1',      # Explicit VR Little Endian
    '1.2.840.10008.1.2.2',      # Explicit VR Big Endian (Less common)
    # --- Compressed Syntaxes (Require additional libraries) ---
    '1.2.840.10008.1.2.5',      # RLE Lossless (Requires pylibjpeg-rle)
    '1.2.840.10008.1.2.4.50',   # JPEG Baseline (Process 1) (Requires pylibjpeg)
    '1.2.840.10008.1.2.4.51',   # JPEG Extended (Process 2 & 4) (Requires pylibjpeg)
    '1.2.840.10008.1.2.4.57',   # JPEG Lossless, Non-Hierarchical (Process 14) (Requires pylibjpeg)
    '1.2.840.10008.1.2.4.70',   # JPEG Lossless, Non-Hierarchical, First-Order Prediction (Process 14, Sel 1) (Requires pylibjpeg)
    '1.2.840.10008.1.2.4.80',   # JPEG-LS Lossless Image Compression (Requires pylibjpeg & jpeg_ls)
    '1.2.840.10008.1.2.4.81',   # JPEG-LS Lossy (Near-Lossless) Image Compression (Requires pylibjpeg & jpeg_ls)
    '1.2.840.10008.1.2.4.90',   # JPEG 2000 Image Compression (Lossless Only) (Requires pylibjpeg & pillow)
    '1.2.840.10008.1.2.4.91',   # JPEG 2000 Image Compression (Requires pylibjpeg & pillow)
    # --- Deflated Syntax ---
    # '1.2.840.10008.1.2.1.99', # Deflated Explicit VR Little Endian (Supported by pydicom >= 1.3)
    # Add others as needed and ensure dependencies are installed
    # ... (other transfer syntaxes as before)
]

# Define supported Abstract Syntaxes (SOP Classes) for Storage
# Use pynetdicom's StoragePresentationContexts as a base
storage_contexts = list(StoragePresentationContexts)

# Filter contexts and apply transfer syntaxes
contexts = []
for context in storage_contexts:
    # Filter out unwanted SOP classes if necessary
    # if context.abstract_syntax == SomeUnwantedSOPClassUID: continue
    context.transfer_syntax = SUPPORTED_TRANSFER_SYNTAXES
    contexts.append(context)

# --- Event Handlers Mapping ---
# Map event constants to handler functions imported from handlers.py etc.
HANDLERS = [
    # Storage Service Class
    (evt.EVT_C_STORE, handle_store),
    # Verification Service Class
    (evt.EVT_C_ECHO, handle_echo),
    # Query Service Class (example for future)
    # (evt.EVT_C_FIND, handle_find),
    # Association control events
    (evt.EVT_ACCEPTED, lambda event: logger.info(f"Association Accepted: {event.assoc}")),
    (evt.EVT_ESTABLISHED, lambda event: logger.info(f"Association Established: {event.assoc}")),
    (evt.EVT_REJECTED, lambda event: logger.warning(f"Association Rejected: {event.assoc}")),
    (evt.EVT_RELEASED, lambda event: logger.info(f"Association Released: {event.assoc}")),
    (evt.EVT_ABORTED, lambda event: logger.warning(f"Association Aborted: {event.assoc}")),
    (evt.EVT_CONN_CLOSE, lambda event: logger.info(f"Connection Closed: {event.assoc}")),
    (evt.EVT_CONN_OPEN, lambda event: logger.info(f"Connection Opened: {event.assoc}")),
]


# --- Main Execution ---
def run_dimse_server():
    """Configures and runs the DICOM DIMSE SCP server."""
    ae = AE(ae_title=AE_TITLE)

    # Add supported presentation contexts for Storage
    logger.info(f"Adding {len(contexts)} Storage contexts...")
    for context in contexts:
        ae.add_supported_context(context.abstract_syntax, transfer_syntax=context.transfer_syntax)

    # Add Verification context directly using its UID
    # Default transfer syntaxes [Implicit VR LE, Explicit VR LE] are used by default
    logger.info("Adding Verification context (C-ECHO)...")
    ae.add_supported_context('1.2.840.10008.1.1')

    # Add required C-FIND response presentation contexts if implementing C-FIND (Example for future)
    # find_response_contexts = [cx for cx in QueryRetrievePresentationContexts if cx.abstract_syntax == StudyRootQueryRetrieveInformationModelFind]
    # for cx in find_response_contexts:
    #     ae.add_required_context(cx.abstract_syntax, transfer_syntax=SUPPORTED_TRANSFER_SYNTAXES)


    logger.info(f"Starting DICOM DIMSE Server:")
    logger.info(f"  AE Title: {AE_TITLE}")
    logger.info(f"  Port: {PORT}")
    logger.info(f"  Incoming Directory: {INCOMING_DIR}")
    logger.info(f"  Supported Abstract Syntaxes: {len(ae.supported_contexts)} total")
    logger.info(f"  Transfer Syntaxes (typical): {', '.join(SUPPORTED_TRANSFER_SYNTAXES)}")

    # Start listening for incoming connections
    ae.start_server(
        address=('0.0.0.0', PORT),
        block=True, # Keep server running until interrupted
        evt_handlers=HANDLERS,
        # backlog= # Optional: listen queue size
        # acse_timeout= # Optional: network timeouts
        # dimse_timeout= # Optional: network timeouts
        # max_pdu_size= # Optional: Max PDU size negotiation
    )

if __name__ == "__main__":
    run_dimse_server()
