# app/services/network/dimse/handlers.py

import logging
import os
import uuid
import re # Added re
from pathlib import Path
from typing import Optional # Added Optional

from pydicom import dcmread
from pydicom.errors import InvalidDicomError

from app.core.config import settings # Still needed for INCOMING_DIR
from app.worker.tasks import process_dicom_file_task
from app.worker.celery_app import app as current_celery_app

logger = logging.getLogger("dicom_listener.handlers")
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)

# --- ADDED Global Variable ---
# This will be set by the server.py script at startup
_current_listener_name: Optional[str] = None

def set_current_listener_name(name: str):
    """Sets the name of the currently running listener configuration."""
    global _current_listener_name
    _current_listener_name = name
    logger.info(f"Handler context set for listener name: '{name}'")
# --- END ADDED ---


# --- C-ECHO Handler (Keep as is) ---
def handle_echo(event):
    """Handle a C-ECHO request event."""
    logger.info(f"Received C-ECHO request from {event.assoc.requestor.ae_title} "
                f"on association {event.assoc.native_id}")
    return 0x0000

# --- C-STORE Handler (Modified) ---
def handle_store(event):
    """Handle a C-STORE request event."""
    filepath: Optional[Path] = None
    assoc_id = event.assoc.native_id
    calling_ae = event.assoc.requestor.ae_title

    # --- USE CONFIGURED LISTENER NAME ---
    # Use the name set at startup, fallback if somehow not set
    source_identifier = _current_listener_name or f"dimse_listener_{settings.LISTENER_HOST}" # Fallback ID
    if not _current_listener_name:
         logger.warning("Handler could not determine specific listener name, using fallback source identifier.")
    # --- END USE NAME ---

    try:
        ds = event.dataset
        ds.file_meta = event.file_meta
        sop_instance_uid = ds.get("SOPInstanceUID", None)

        if sop_instance_uid:
            filename_sop = re.sub(r'[^\w.-]', '_', sop_instance_uid)
            filename_sop = filename_sop[:100]
            filename = f"{filename_sop}.dcm"
        else:
             filename = f"{uuid.uuid4()}.dcm"

        filepath = INCOMING_DIR / filename
        logger.info(f"C-STORE from {calling_ae} [Assoc: {assoc_id}, Source: {source_identifier}]: " # Added Source ID
                    f"SOP Instance {sop_instance_uid or 'Unknown'}")

        # Optional quick validation here...

        logger.debug(f"Saving received object to {filepath} [Assoc: {assoc_id}]")
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        ds.save_as(filepath, write_like_original=False)
        logger.info(f"Successfully saved DICOM file: {filepath} [Assoc: {assoc_id}]")

        logger.debug(f"Attempting to dispatch processing task for: {filepath} [Assoc: {assoc_id}]")
        try:
            # --- Pass the determined source_identifier ---
            logger.debug(f"Dispatching task with source_identifier: '{source_identifier}' [Assoc: {assoc_id}]")
            current_celery_app.send_task(
                 'process_dicom_file_task',
                 args=[str(filepath)],
                 kwargs={'source_identifier': source_identifier}, # Pass the loaded name
            )
            logger.info(f"Task dispatched successfully for {filepath} from source '{source_identifier}' [Assoc: {assoc_id}]")
            # --- End Pass source_identifier ---

        except Exception as celery_err:
            logger.error(f"Failed to dispatch Celery task for {filepath}: {celery_err} "
                         f"[Assoc: {assoc_id}]", exc_info=True)
            if filepath and filepath.exists():
                 try:
                     filepath.unlink()
                     logger.info(f"Cleaned up file {filepath} after failed Celery dispatch [Assoc: {assoc_id}]")
                 except OSError as unlink_err:
                     logger.error(f"Could not delete file {filepath} after failed Celery dispatch [Assoc: {assoc_id}]: {unlink_err}")
            return 0xA700 # Out of Resources

        return 0x0000 # Success

    except InvalidDicomError as e:
        logger.error(f"Invalid DICOM data received from {calling_ae} "
                     f"[Assoc: {assoc_id}]: {e}", exc_info=False)
        return 0xC000 # Cannot understand
    except Exception as e:
        logger.error(f"Unexpected error handling C-STORE request from {calling_ae} "
                     f"[Assoc: {assoc_id}]: {e}", exc_info=True)
        if filepath and filepath.exists():
            try:
                filepath.unlink()
                logger.info(f"Cleaned up potentially partial file {filepath} after error [Assoc: {assoc_id}]")
            except OSError as unlink_err:
                logger.error(f"Could not delete file {filepath} after error [Assoc: {assoc_id}]: {unlink_err}")
        return 0xA900 # Processing Failure
