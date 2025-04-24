# app/services/network/dimse/handlers.py

import logging
import os
import uuid
import re
from pathlib import Path
from typing import Optional

from pydicom import dcmread
from pydicom.errors import InvalidDicomError

from app.core.config import settings
from app.worker.tasks import process_dicom_file_task
from app.worker.celery_app import app as current_celery_app
# --- ADDED: Import CRUD for listener state ---
from app import crud # Import top-level crud package
from app.db.session import SessionLocal # Need session for DB operations
# --- END ADDED ---
from app.db.models import ProcessedStudySourceType # Import Enum for clarity

logger = logging.getLogger("dicom_listener.handlers")
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)

_current_listener_name: Optional[str] = None
_current_listener_instance_id: Optional[str] = None # Store instance ID too

# --- MODIFIED: Accept and store instance_id ---
# Note: This function must be called by server.py at startup
def set_current_listener_context(name: str, instance_id: str):
    """Sets the name and instance ID of the currently running listener configuration."""
    global _current_listener_name, _current_listener_instance_id
    _current_listener_name = name
    _current_listener_instance_id = instance_id
    logger.info(f"Handler context set for listener name: '{name}', instance ID: '{instance_id}'")
# --- END MODIFIED ---


# --- C-ECHO Handler (Keep as is) ---
def handle_echo(event):
    """Handle a C-ECHO request event."""
    logger.info(f"Received C-ECHO request from {event.assoc.requestor.ae_title} "
                f"on association {event.assoc.native_id}")
    return 0x0000

# --- C-STORE Handler (Modified for metrics) ---
def handle_store(event):
    """Handle a C-STORE request event."""
    filepath: Optional[Path] = None
    assoc_id = event.assoc.native_id
    calling_ae = event.assoc.requestor.ae_title

    # Use the configured listener name/id set at startup
    source_identifier = _current_listener_name or f"dimse_listener_{settings.LISTENER_HOST}"
    listener_instance_id = _current_listener_instance_id # Get the instance ID

    if not _current_listener_name or not listener_instance_id:
         logger.warning("Handler could not determine specific listener name or instance ID, using fallback source identifier. Metrics will not be incremented.")
         # Set to None to prevent incorrect increment calls later
         listener_instance_id = None

    try:
        ds = event.dataset
        ds.file_meta = event.file_meta
        sop_instance_uid = ds.get("SOPInstanceUID", None)

        if sop_instance_uid:
            # Simple sanitization and length limiting for filename
            filename_sop = re.sub(r'[^\w.-]', '_', sop_instance_uid)
            filename_sop = filename_sop[:100] # Limit length for safety
            filename = f"{filename_sop}.dcm"
        else:
             # Generate UUID if SOP Instance UID is missing (should be rare)
             filename = f"no_sopuid_{uuid.uuid4()}.dcm"
             logger.warning(f"Received instance from {calling_ae} missing SOPInstanceUID. Using filename: {filename}")


        filepath = INCOMING_DIR / filename
        logger.info(f"C-STORE from {calling_ae} [Assoc: {assoc_id}, Source: {source_identifier}]: "
                    f"SOP Instance {sop_instance_uid or 'Unknown'}")

        # Optional quick validation here...
        # e.g., check required tags before saving

        logger.debug(f"Saving received object to {filepath} [Assoc: {assoc_id}]")
        INCOMING_DIR.mkdir(parents=True, exist_ok=True) # Ensure dir exists
        ds.save_as(filepath, write_like_original=False)
        logger.info(f"Successfully saved DICOM file: {filepath} [Assoc: {assoc_id}]")

        # --- ADDED: Increment Received Count ---
        if listener_instance_id: # Only increment if we know the instance ID
            db_session = None
            increment_succeeded = False
            try:
                db_session = SessionLocal()
                # Use correct crud object
                increment_succeeded = crud.crud_dimse_listener_state.increment_received_count(
                    db=db_session,
                    listener_id=listener_instance_id,
                    count=1 # Increment by one for this instance
                )
                # CRUD function commits internally
                if not increment_succeeded:
                     logger.warning(f"Failed to increment received count via CRUD for listener {listener_instance_id}.")
            except Exception as db_err:
                 logger.error(f"Error during received count increment for listener {listener_instance_id}: {db_err}", exc_info=True)
                 # Rollback handled by CRUD
            finally:
                 if db_session: db_session.close() # Ensure session is closed
        else:
             logger.warning("Cannot increment received count: Listener instance ID is unknown.")
        # --- END ADDED ---

        logger.debug(f"Attempting to dispatch processing task for: {filepath} [Assoc: {assoc_id}]")
        try:
            # --- Ensure correct arguments are passed to task ---
            source_type_str = ProcessedStudySourceType.DIMSE_LISTENER.value
            if not listener_instance_id:
                 logger.error(f"Cannot dispatch task for {filepath} - Listener instance ID is unknown. File will not be processed.")
                 # Return failure to SCU
                 return 0xA700 # Or another appropriate error code

            logger.debug(f"Dispatching task with source_type: '{source_type_str}', source_id: '{listener_instance_id}' [Assoc: {assoc_id}]")
            current_celery_app.send_task(
                 'process_dicom_file_task', # Match task name in tasks.py
                 args=[str(filepath)],
                 kwargs={
                      'source_type': source_type_str,
                      'source_db_id_or_instance_id': listener_instance_id # Pass the string instance ID
                      }
            )
            # --- END Ensure correct arguments ---
            logger.info(f"Task dispatched successfully for {filepath} from source '{source_identifier}' [Assoc: {assoc_id}]")

        except Exception as celery_err:
            logger.error(f"Failed to dispatch Celery task for {filepath}: {celery_err} "
                         f"[Assoc: {assoc_id}]", exc_info=True)
            # If dispatch fails, the file is saved but won't be processed.
            # Consider moving to an error/retry directory?
            # For now, just log the error and return failure status to SCU.
            # Optional: Attempt to clean up file
            if filepath and filepath.exists():
                 try: filepath.unlink(); logger.info(f"Cleaned up file {filepath} after failed Celery dispatch [Assoc: {assoc_id}]")
                 except OSError as unlink_err: logger.error(f"Could not delete file {filepath} after failed Celery dispatch [Assoc: {assoc_id}]: {unlink_err}")
            return 0xA700 # Out of Resources: Failed to queue for processing

        # If file save and task dispatch succeed
        return 0x0000 # Success

    except InvalidDicomError as e:
        logger.error(f"Invalid DICOM data received from {calling_ae} "
                     f"[Assoc: {assoc_id}]: {e}", exc_info=False)
        # Don't increment count for invalid files
        return 0xC000 # Cannot understand
    except Exception as e:
        logger.error(f"Unexpected error handling C-STORE request from {calling_ae} "
                     f"[Assoc: {assoc_id}]: {e}", exc_info=True)
        # Don't increment count for general processing errors before saving
        if filepath and filepath.exists():
            try:
                filepath.unlink()
                logger.info(f"Cleaned up potentially partial file {filepath} after error [Assoc: {assoc_id}]")
            except OSError as unlink_err:
                logger.error(f"Could not delete file {filepath} after error [Assoc: {assoc_id}]: {unlink_err}")
        return 0xA900 # Processing Failure
