# app/services/network/dimse/handlers.py

import logging
import os
import uuid
from pathlib import Path

from pydicom import dcmread
from pydicom.errors import InvalidDicomError
# NOTE: Ensure VerificationSOPClass import is REMOVED

from app.core.config import settings
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
    try:
        # Decode the C-STORE request's Data Set
        ds = event.dataset
        # Add the file meta information back to the dataset (pynetdicom >= 1.5)
        # Required for pydicom.dcmwrite to work correctly
        ds.file_meta = event.file_meta

        sop_instance_uid = ds.get("SOPInstanceUID", None)
        calling_ae = event.assoc.requestor.ae_title
        assoc_id = event.assoc.native_id

        # Generate a unique filename for storage
        if sop_instance_uid:
            # Sanitize UID for filename (replace '.' with '_', maybe check length)
            filename_sop = sop_instance_uid.replace('.', '_')
            # Add truncation or hashing if UIDs could be too long for filesystem
            # filename_sop = filename_sop[:100] # Example truncation
            filename = f"{filename_sop}.dcm"
        else:
             # Fallback to UUID if SOP Instance UID is missing (should be rare)
             filename = f"{uuid.uuid4()}.dcm"

        filepath = INCOMING_DIR / filename
        logger.info(f"C-STORE from {calling_ae} [Assoc: {assoc_id}]: "
                    f"SOP Instance {sop_instance_uid or 'Unknown'}")

        # --- Optional: Quick Validation ---
        # Example: check for essential tags before saving/queueing
        # if not sop_instance_uid or not ds.get("PatientID"):
        #    logger.warning(f"Missing SOPInstanceUID or PatientID from {calling_ae}. Rejecting. [Assoc: {assoc_id}]")
        #    return 0xA900 # Status: Processing Failure

        # Save the dataset to the incoming directory
        logger.debug(f"Saving received object to {filepath} [Assoc: {assoc_id}]")
        # Ensure the directory exists just before saving (extra safety)
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        # We use dcmwrite via dataset.save_as()
        ds.save_as(filepath, write_like_original=False) # write_like_original=False is safer
        logger.info(f"Successfully saved DICOM file: {filepath} [Assoc: {assoc_id}]")

        # Dispatch the processing task to Celery
        logger.debug(f"Attempting to dispatch processing task for: {filepath} [Assoc: {assoc_id}]")
        try:
            # --- MODIFIED TASK DISPATCH ---
            # Log the broker URL Celery is configured with right now
            broker_url_in_handler = current_celery_app.conf.broker_url
            logger.debug(f"Celery app broker URL in handler: {broker_url_in_handler} [Assoc: {assoc_id}]")

            # Send task explicitly using the imported app instance and task name
            current_celery_app.send_task(
                 'process_dicom_file_task', # Task name registered with Celery
                 args=[str(filepath)]       # Arguments must be serializable (string path)
                 # kwargs={},              # Optional keyword arguments
                 # queue='dicom_processing', # Optionally specify a queue
                 # priority=5,             # Optionally specify priority
            )

            logger.info(f"Task dispatched successfully for {filepath} [Assoc: {assoc_id}]")
            # --- END MODIFIED TASK DISPATCH ---

        except Exception as celery_err:
            logger.error(f"Failed to dispatch Celery task for {filepath}: {celery_err} "
                         f"[Assoc: {assoc_id}]", exc_info=True)
            # Decide how to handle this: Maybe retry? Or move file to an error queue?
            # For now, we return failure to the sender as the processing pipeline failed
            try:
                # Attempt to clean up saved file if queueing failed
                filepath.unlink(missing_ok=True) # missing_ok=True ignores error if file already gone
            except OSError:
                logger.error(f"Could not delete file {filepath} after failed Celery dispatch [Assoc: {assoc_id}].")
            # Return a failure status indicating processing couldn't be initiated
            return 0xC001 # Status: Unable to Process - Queue Full (or similar C... code)

        # Return a Success status to the C-STORE SCU
        return 0x0000 # Status: Success

    except InvalidDicomError as e:
        logger.error(f"Invalid DICOM data received from {event.assoc.requestor.ae_title} "
                     f"[Assoc: {event.assoc.native_id}]: {e}", exc_info=True)
        # Bad data received
        return 0xC000 # Status: Cannot understand
    except Exception as e:
        logger.error(f"Unexpected error handling C-STORE request from {event.assoc.requestor.ae_title} "
                     f"[Assoc: {event.assoc.native_id}]: {e}", exc_info=True)
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
