# app/services/network/dimse/handlers.py

import logging
import os
import uuid
from pathlib import Path

from pydicom import dcmread
from pydicom.errors import InvalidDicomError

from app.core.config import settings # Import settings
from app.worker.tasks import process_dicom_file_task # Task definition
from app.worker.celery_app import app as current_celery_app # Import Celery app instance

# Use a sub-logger name for clarity
logger = logging.getLogger("dicom_listener.handlers")

# Get path from settings loaded by the server process
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)


# --- C-ECHO Handler ---
def handle_echo(event):
    """Handle a C-ECHO request event."""
    logger.info(f"Received C-ECHO request from {event.assoc.requestor.ae_title} "
                f"on association {event.assoc.native_id}")
    # Return Success status
    return 0x0000

# --- C-STORE Handler ---
def handle_store(event):
    """Handle a C-STORE request event."""
    filepath: Optional[Path] = None # Define filepath variable for broader scope
    assoc_id = event.assoc.native_id # Get association ID early for logging
    calling_ae = event.assoc.requestor.ae_title # Get calling AE early for logging

    try:
        # Decode the C-STORE request's Data Set
        ds = event.dataset
        # Add the file meta information
        ds.file_meta = event.file_meta

        sop_instance_uid = ds.get("SOPInstanceUID", None)

        # Generate a unique filename for storage
        if sop_instance_uid:
            # Sanitize UID for filename
            filename_sop = re.sub(r'[^\w.-]', '_', sop_instance_uid) # Replace non-alphanumeric/dot/hyphen with underscore
            # Add truncation if UIDs could be too long for filesystem
            filename_sop = filename_sop[:100] # Example truncation
            filename = f"{filename_sop}.dcm"
        else:
             # Fallback to UUID if SOP Instance UID is missing
             filename = f"{uuid.uuid4()}.dcm"

        filepath = INCOMING_DIR / filename
        logger.info(f"C-STORE from {calling_ae} [Assoc: {assoc_id}]: "
                    f"SOP Instance {sop_instance_uid or 'Unknown'}")

        # --- Optional: Quick Validation ---
        # ... (keep optional validation as is) ...

        # Save the dataset to the incoming directory
        logger.debug(f"Saving received object to {filepath} [Assoc: {assoc_id}]")
        # Ensure the directory exists just before saving
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        # Use dcmwrite via dataset.save_as()
        ds.save_as(filepath, write_like_original=False) # write_like_original=False is generally safer
        logger.info(f"Successfully saved DICOM file: {filepath} [Assoc: {assoc_id}]")

        # Dispatch the processing task to Celery
        logger.debug(f"Attempting to dispatch processing task for: {filepath} [Assoc: {assoc_id}]")
        try:
            # --- MODIFIED TASK DISPATCH ---
            # Get the source identifier for this listener from settings
            source_id = settings.DEFAULT_SCP_SOURCE_ID
            logger.debug(f"Dispatching task with source_identifier: '{source_id}' [Assoc: {assoc_id}]")

            # Send task explicitly using the imported app instance and task name
            # Pass filepath as first arg, source_identifier as kwarg
            current_celery_app.send_task(
                 'process_dicom_file_task',      # Task name registered with Celery
                 args=[str(filepath)],           # Positional arguments (must be serializable)
                 kwargs={'source_identifier': source_id}, # Keyword arguments (must be serializable)
                 # queue='dicom_processing',     # Optionally specify a queue
                 # priority=5,                 # Optionally specify priority
            )

            logger.info(f"Task dispatched successfully for {filepath} from source '{source_id}' [Assoc: {assoc_id}]")
            # --- END MODIFIED TASK DISPATCH ---

        except Exception as celery_err:
            logger.error(f"Failed to dispatch Celery task for {filepath}: {celery_err} "
                         f"[Assoc: {assoc_id}]", exc_info=True)
            # Return a failure status indicating processing couldn't be initiated
            # Clean up saved file if queueing failed
            if filepath and filepath.exists():
                 try:
                     filepath.unlink()
                     logger.info(f"Cleaned up file {filepath} after failed Celery dispatch [Assoc: {assoc_id}]")
                 except OSError as unlink_err:
                     logger.error(f"Could not delete file {filepath} after failed Celery dispatch [Assoc: {assoc_id}]: {unlink_err}")
            # Return C... status code - Service Class specific codes are better if defined
            # 0xC001 - Unable to process might fit
            # 0xA700 - Out of Resources might also fit
            return 0xA700 # Using Out of Resources to imply queue/broker issue


        # Return a Success status to the C-STORE SCU
        return 0x0000 # Status: Success

    except InvalidDicomError as e:
        logger.error(f"Invalid DICOM data received from {calling_ae} "
                     f"[Assoc: {assoc_id}]: {e}", exc_info=False) # Don't need full traceback for invalid data
        # Bad data received
        return 0xC000 # Status: Cannot understand
    except Exception as e:
        logger.error(f"Unexpected error handling C-STORE request from {calling_ae} "
                     f"[Assoc: {assoc_id}]: {e}", exc_info=True)
        # Clean up potentially partially saved file if error occurred after filepath assigned
        if filepath and filepath.exists():
            try:
                filepath.unlink()
                logger.info(f"Cleaned up potentially partial file {filepath} after error [Assoc: {assoc_id}]")
            except OSError as unlink_err:
                logger.error(f"Could not delete file {filepath} after error [Assoc: {assoc_id}]: {unlink_err}")
        # General processing failure
        return 0xA900 # Status: Processing Failure

# --- Other Handlers (Add C-FIND, C-MOVE etc. here later) ---
# Example placeholder:
# def handle_find(event):
#     logger.info(f"Received C-FIND request from {event.assoc.requestor.ae_title} [Assoc: {event.assoc.native_id}]")
#     # Logic to query database based on event.identifier (query dataset)
#     # For each match:
#     #   yield 0xFF00, matching_dataset  # Status Pending + Match
#     # Finally:
#     #   yield 0x0000, None # Status Success, no more matches
#     return 0xFE00 # Status: Cancelled (Example - implement proper status)

# def handle_move(event):
#     logger.info(f"Received C-MOVE request from {event.assoc.requestor.ae_title} [Assoc: {event.assoc.native_id}]")
#     # Logic to identify datasets based on event.identifier
#     # Logic to initiate C-STORE sub-operations to event.move_destination
#     # Yield status updates (Pending, Completed, Failed)
#     return 0x0000 # Status: Success (Example - implement proper status)
