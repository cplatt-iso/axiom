# app/services/network/dimse/handlers.py

import logging
import os
import uuid
import re
from pathlib import Path
from typing import Optional, Dict, Any # Added Dict, Any

from pydicom import dcmread
from pydicom.errors import InvalidDicomError

from app.core.config import settings
from app.worker.tasks import process_dicom_file_task
from app.worker.celery_app import app as current_celery_app
from app import crud
from app.db.session import SessionLocal
from app.db.models import ProcessedStudySourceType

logger = logging.getLogger("dicom_listener.handlers")
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)

_current_listener_name: Optional[str] = None
_current_listener_instance_id: Optional[str] = None

def set_current_listener_context(name: str, instance_id: str):
    """Sets the name and instance ID of the currently running listener configuration."""
    global _current_listener_name, _current_listener_instance_id
    _current_listener_name = name
    _current_listener_instance_id = instance_id
    logger.info(f"Handler context set for listener name: '{name}', instance ID: '{instance_id}'")

# --- C-ECHO Handler ---
def handle_echo(event):
    """Handle a C-ECHO request event."""
    logger.info(f"Received C-ECHO request from {event.assoc.requestor.ae_title} "
                f"on association {event.assoc.native_id}")
    return 0x0000

# --- C-STORE Handler ---
def handle_store(event):
    """Handle a C-STORE request event."""
    filepath: Optional[Path] = None
    assoc_id = event.assoc.native_id
    calling_ae = event.assoc.requestor.ae_title
    called_ae = event.assoc.acceptor.ae_title # Get our AE title
    source_ip = event.assoc.requestor.address # Get source IP

    # Use the configured listener name/id set at startup
    source_identifier = _current_listener_name or f"dimse_listener_{settings.LISTENER_HOST}"
    listener_instance_id = _current_listener_instance_id

    if not listener_instance_id:
         logger.warning("Handler could not determine specific listener instance ID, using fallback source identifier. Metrics will not be incremented.")

    # --- Prepare Association Info Dictionary ---
    association_info: Dict[str, Any] = {
        "calling_ae_title": calling_ae,
        "called_ae_title": called_ae,
        "source_ip": source_ip,
    }
    # --- END Prepare ---

    try:
        ds = event.dataset
        ds.file_meta = event.file_meta
        sop_instance_uid = ds.get("SOPInstanceUID", None)

        if sop_instance_uid:
            filename_sop = re.sub(r'[^\w.-]', '_', sop_instance_uid)
            filename_sop = filename_sop[:100]
            filename = f"{filename_sop}.dcm"
        else:
             filename = f"no_sopuid_{uuid.uuid4()}.dcm"
             logger.warning(f"Received instance from {calling_ae} missing SOPInstanceUID. Using filename: {filename}")

        filepath = INCOMING_DIR / filename
        logger.info(f"C-STORE from {calling_ae} @ {source_ip} -> {called_ae} "
                    f"[Assoc: {assoc_id}, Source: {source_identifier}]: "
                    f"SOP Instance {sop_instance_uid or 'Unknown'}")

        logger.debug(f"Saving received object to {filepath} [Assoc: {assoc_id}]")
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        ds.save_as(filepath, write_like_original=False)
        logger.info(f"Successfully saved DICOM file: {filepath} [Assoc: {assoc_id}]")

        # Increment Received Count
        if listener_instance_id:
            db_session = None
            increment_succeeded = False
            try:
                db_session = SessionLocal()
                increment_succeeded = crud.crud_dimse_listener_state.increment_received_count(
                    db=db_session, listener_id=listener_instance_id, count=1
                )
                if not increment_succeeded:
                     logger.warning(f"Failed to increment received count via CRUD for listener {listener_instance_id}.")
            except Exception as db_err:
                 logger.error(f"Error during received count increment for listener {listener_instance_id}: {db_err}", exc_info=True)
            finally:
                 if db_session: db_session.close()
        else:
             logger.warning("Cannot increment received count: Listener instance ID is unknown.")

        logger.debug(f"Attempting to dispatch processing task for: {filepath} [Assoc: {assoc_id}]")
        try:
            source_type_str = ProcessedStudySourceType.DIMSE_LISTENER.value
            if not listener_instance_id:
                 logger.error(f"Cannot dispatch task for {filepath} - Listener instance ID is unknown. File will not be processed.")
                 return 0xA700 # Out of Resources

            logger.debug(f"Dispatching task with source_type: '{source_type_str}', source_id: '{listener_instance_id}', assoc: {association_info} [Assoc: {assoc_id}]")
            current_celery_app.send_task(
                 'process_dicom_file_task',
                 args=[str(filepath)],
                 kwargs={
                      'source_type': source_type_str,
                      'source_db_id_or_instance_id': listener_instance_id,
                      # --- Pass association_info ---
                      'association_info': association_info
                      # --- END Pass ---
                      }
            )
            logger.info(f"Task dispatched successfully for {filepath} from source '{source_identifier}' [Assoc: {assoc_id}]")

        except Exception as celery_err:
            logger.error(f"Failed to dispatch Celery task for {filepath}: {celery_err} "
                         f"[Assoc: {assoc_id}]", exc_info=True)
            if filepath and filepath.exists():
                 try: filepath.unlink(); logger.info(f"Cleaned up file {filepath} after failed Celery dispatch [Assoc: {assoc_id}]")
                 except OSError as unlink_err: logger.error(f"Could not delete file {filepath} after failed Celery dispatch [Assoc: {assoc_id}]: {unlink_err}")
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
            try: filepath.unlink(); logger.info(f"Cleaned up potentially partial file {filepath} after error [Assoc: {assoc_id}]")
            except OSError as unlink_err: logger.error(f"Could not delete file {filepath} after error [Assoc: {assoc_id}]: {unlink_err}")
        return 0xA900 # Processing Failure
