# app/services/network/dimse/handlers.py

import os
import uuid
import re
from pathlib import Path
from typing import Optional, Dict, Any

# Use structlog for logging
import structlog # type: ignore

from pydicom import dcmread
from pydicom.errors import InvalidDicomError

from app.core.config import settings
from app.worker.tasks import process_dicom_file_task
from app.worker.celery_app import app as current_celery_app
from app import crud
from app.db.session import SessionLocal
from app.schemas.enums import ProcessedStudySourceType

# Get logger instance (should inherit config from server.py)
logger = structlog.get_logger("dicom_listener.handlers")
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)

_current_listener_name: Optional[str] = None
_current_listener_instance_id: Optional[str] = None

def set_current_listener_context(name: str, instance_id: str):
    """Sets the name and instance ID of the currently running listener configuration."""
    global _current_listener_name, _current_listener_instance_id
    _current_listener_name = name
    _current_listener_instance_id = instance_id
    # Bind context for subsequent handler logs if needed, or just log directly
    log = logger.bind(listener_name=name, listener_instance_id=instance_id)
    log.info("Handler context set for listener")

# --- C-ECHO Handler ---
def handle_echo(event):
    """Handle a C-ECHO request event with structured logging."""
    log = logger.bind(
        assoc_id=event.assoc.native_id,
        calling_ae=event.assoc.requestor.ae_title,
        called_ae=event.assoc.acceptor.ae_title,
        source_ip=event.assoc.requestor.address,
        event_type="C-ECHO"
    )
    log.info("Received C-ECHO request")
    return 0x0000 # Success

# --- C-STORE Handler ---
def handle_store(event):
    """Handle a C-STORE request event with structured logging."""
    filepath: Optional[Path] = None
    assoc_id = event.assoc.native_id
    calling_ae = event.assoc.requestor.ae_title
    called_ae = event.assoc.acceptor.ae_title
    source_ip = event.assoc.requestor.address

    source_identifier = _current_listener_name or f"dimse_listener_{settings.LISTENER_HOST}"
    listener_instance_id = _current_listener_instance_id

    # Bind context for this specific C-STORE operation
    log = logger.bind(
        assoc_id=assoc_id,
        calling_ae=calling_ae,
        called_ae=called_ae,
        source_ip=source_ip,
        listener_name=source_identifier, # Use the resolved name
        listener_instance_id=listener_instance_id,
        event_type="C-STORE"
    )

    if not listener_instance_id:
         log.warning("Handler could not determine specific listener instance ID. Metrics will not be incremented.")

    association_info: Dict[str, Any] = {
        "calling_ae_title": calling_ae,
        "called_ae_title": called_ae,
        "source_ip": source_ip,
    }

    try:
        ds = event.dataset
        ds.file_meta = event.file_meta
        sop_instance_uid = ds.get("SOPInstanceUID", None)
        log = log.bind(instance_uid=sop_instance_uid or "Unknown") # Add UID to context

        if sop_instance_uid:
            # Sanitize SOP Instance UID for use in filename
            filename_sop = re.sub(r'[^\w.-]', '_', str(sop_instance_uid)) # Ensure it's a string
            filename_sop = filename_sop[:100] # Limit length
            filename = f"{filename_sop}.dcm"
        else:
             # Generate a unique filename if SOPInstanceUID is missing
             unique_id = uuid.uuid4()
             filename = f"no_sopuid_{unique_id}.dcm"
             log.warning("Received instance missing SOPInstanceUID. Using generated filename.", generated_filename=filename)

        filepath = INCOMING_DIR / filename
        log = log.bind(target_filepath=str(filepath)) # Add target path to context
        log.info("Received C-STORE request") # Simple message, details in context

        log.debug("Saving received object")
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        ds.save_as(filepath, write_like_original=False) # Consider performance implications
        log.info("Successfully saved DICOM file")

        # Increment Received Count
        if listener_instance_id:
            db_session = None
            increment_succeeded = False
            try:
                db_session = SessionLocal()
                increment_succeeded = crud.crud_dimse_listener_state.increment_received_count(
                    db=db_session, listener_id=listener_instance_id, count=1
                )
                db_session.commit() # Commit the increment here
                if not increment_succeeded:
                     # This might happen if the state row didn't exist initially
                     log.warning("Received count increment did not affect any rows (state record might be missing initially?).")
                else:
                     log.debug("Incremented received count successfully.")
            except Exception as db_err:
                 log.error("Error during received count increment", database_error=str(db_err), exc_info=True)
                 if db_session and db_session.is_active: db_session.rollback()
            finally:
                 if db_session: db_session.close()
        else:
             log.warning("Cannot increment received count: Listener instance ID is unknown.")

        log.debug("Attempting to dispatch processing task...")
        try:
            source_type_str = ProcessedStudySourceType.DIMSE_LISTENER.value
            if not listener_instance_id:
                 log.error("Cannot dispatch task - Listener instance ID is unknown. File will not be processed.")
                 return 0xA700 # Out of Resources

            log.debug("Dispatching task", task_name='process_dicom_file_task', source_type=source_type_str)
            current_celery_app.send_task(
                 'process_dicom_file_task',
                 args=[str(filepath)],
                 kwargs={
                      'source_type': source_type_str,
                      'source_db_id_or_instance_id': listener_instance_id,
                      'association_info': association_info
                      }
            )
            log.info("Task dispatched successfully")

        except Exception as celery_err:
            log.error("Failed to dispatch Celery task", error=str(celery_err), exc_info=True)
            if filepath and filepath.exists():
                 try: filepath.unlink(); log.info("Cleaned up file after failed Celery dispatch")
                 except OSError as unlink_err: log.error("Could not delete file after failed Celery dispatch", error=str(unlink_err))
            return 0xA700 # Out of Resources - Cannot Queue

        return 0x0000 # Success

    except InvalidDicomError as e:
        log.error("Invalid DICOM data received", error=str(e), exc_info=False)
        return 0xC000 # Cannot understand
    except Exception as e:
        log.error("Unexpected error handling C-STORE request", error=str(e), exc_info=True)
        if filepath and filepath.exists():
            try: filepath.unlink(); log.info("Cleaned up potentially partial file after error")
            except OSError as unlink_err: log.error("Could not delete file after error", error=str(unlink_err))
        return 0xA900 # Processing Failure
