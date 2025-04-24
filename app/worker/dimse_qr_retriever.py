# app/worker/dimse_qr_retriever.py
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Pynetdicom imports
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove # Import MOVE SOP class
from pydicom.dataset import Dataset

# Application imports
from sqlalchemy.orm import Session
from celery import shared_task, Task # Import Task for potential retry logic customization
from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models
from app.crud import crud_dimse_qr_source

# Configure logging
logger = logging.getLogger(__name__)
# Optional: Enable pynetdicom logging
# debug_logger()

# Define specific error for C-MOVE failures if needed
class DimseMoveError(Exception):
    """Custom exception for C-MOVE errors."""
    pass

# --- Celery Task for C-MOVE Initiation ---
@shared_task(
    bind=True, # Allows access to self.request
    name="trigger_dimse_cmove_task",
    acks_late=True,
    max_retries=settings.CELERY_TASK_MAX_RETRIES, # Use global settings
    default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
    autoretry_for=(ConnectionRefusedError, TimeoutError, OSError, DimseMoveError), # Retry on network/specific errors
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True
)
def trigger_dimse_cmove_task(self: Task, source_id: int, study_instance_uid: str):
    """
    Initiates a C-MOVE request for a specific study from a configured DIMSE Q/R source.

    Args:
        source_id: The database ID of the DimseQueryRetrieveSource config.
        study_instance_uid: The StudyInstanceUID to retrieve.
    """
    task_id = self.request.id
    logger.info(f"Task {task_id}: Received request to trigger C-MOVE for Study UID {study_instance_uid} from source ID {source_id}.")

    db: Optional[Session] = None
    config: Optional[models.DimseQueryRetrieveSource] = None

    try:
        db = SessionLocal()
        # Fetch the source configuration
        config = crud_dimse_qr_source.get(db, id=source_id)
        if not config:
            logger.error(f"Task {task_id}: Cannot initiate C-MOVE. Configuration for source ID {source_id} not found.")
            # Do not retry if config is missing
            return {"status": "error", "message": f"Configuration for source ID {source_id} not found."}

        if not config.is_enabled:
             logger.warning(f"Task {task_id}: Source ID {source_id} ('{config.name}') is disabled. Skipping C-MOVE.")
             return {"status": "skipped", "message": "Source is disabled."}

        if not config.move_destination_ae_title:
            logger.error(f"Task {task_id}: Cannot initiate C-MOVE for source '{config.name}' (ID: {source_id}). 'move_destination_ae_title' is not configured.")
            # Do not retry if destination AE is missing
            return {"status": "error", "message": "C-MOVE destination AE Title not configured for this source."}

        # Configuration checks passed, proceed with C-MOVE
        logger.info(f"Task {task_id}: Initiating C-MOVE for Study {study_instance_uid} from {config.name} ({config.remote_ae_title}) to {config.move_destination_ae_title}")

        # 1. Initialize AE
        ae = AE(ae_title=config.local_ae_title)
        # Add MOVE SOP Class context (Study Root)
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)
        # Optional: Add storage contexts if the remote SCP requires negotiation for C-STORE sub-ops (usually not needed for C-MOVE SCU)

        # 2. Associate
        assoc = None
        try:
            logger.debug(f"Task {task_id}: Requesting association with {config.remote_ae_title} for C-MOVE...")
            assoc = ae.associate(
                config.remote_host,
                config.remote_port,
                ae_title=config.remote_ae_title
            )

            if assoc.is_established:
                logger.info(f"Task {task_id}: Association established with {config.remote_ae_title} for C-MOVE.")

                # 3. Build Identifier (only Study UID needed for Study Root Move)
                identifier = Dataset()
                identifier.QueryRetrieveLevel = "STUDY" # Or match config.query_level if needed? Study is safest.
                identifier.StudyInstanceUID = study_instance_uid

                # 4. Send C-MOVE Request
                responses = assoc.send_c_move(
                    identifier,
                    config.move_destination_ae_title,
                    StudyRootQueryRetrieveInformationModelMove
                )

                # 5. Process Responses
                final_move_status = -1 # Default to error/unknown
                for status, response_identifier in responses:
                    if status and hasattr(status, 'Status'):
                        status_int = int(status.Status)
                        final_move_status = status_int
                        logger.debug(f"Task {task_id}: Received C-MOVE response status 0x{status_int:04X}")

                        # Log number of remaining/completed/failed/warning sub-ops if available
                        remaining = status.get("NumberOfRemainingSuboperations", "N/A")
                        completed = status.get("NumberOfCompletedSuboperations", "N/A")
                        failed = status.get("NumberOfFailedSuboperations", "N/A")
                        warning = status.get("NumberOfWarningSuboperations", "N/A")
                        logger.info(f"  -> Status: 0x{status_int:04X}, Remaining: {remaining}, Completed: {completed}, Failed: {failed}, Warning: {warning}")

                        if status.Status == 0x0000: # Final Success
                            logger.info(f"Task {task_id}: C-MOVE completed successfully for Study {study_instance_uid}.")
                            # Update DB state for successful move
                            crud_dimse_qr_source.update_move_status(db=db, source_id=config.id, last_successful_move=datetime.now(timezone.utc))
                            # db.commit() handled by CRUD function
                            break # Exit loop
                        elif status.Status in (0xFF00, 0xFF01): # Pending
                            continue # Continue receiving updates
                        elif status.Status == 0xFE00: # Cancel
                             logger.warning(f"Task {task_id}: C-MOVE for Study {study_instance_uid} cancelled by remote AE.")
                             raise DimseMoveError(f"C-MOVE Cancelled by Remote AE (Status: 0xFE00)")
                        else: # Failure status
                             error_comment = status.get('ErrorComment', 'No comment')
                             logger.error(f"Task {task_id}: C-MOVE failed for Study {study_instance_uid} with status 0x{status_int:04X}. Comment: {error_comment}")
                             raise DimseMoveError(f"C-MOVE Failed (Status: 0x{status_int:04X} - {error_comment})")
                    else:
                         logger.error(f"Task {task_id}: Received invalid or missing status in C-MOVE response for Study {study_instance_uid}.")
                         raise DimseMoveError("Invalid/Missing Status in C-MOVE response")

                # Check final status after loop
                if final_move_status != 0x0000:
                     # This case might happen if the generator finishes without a final success/failure status?
                     logger.error(f"Task {task_id}: C-MOVE for Study {study_instance_uid} finished without final success status (Last Status: 0x{final_move_status:04X}).")
                     raise DimseMoveError(f"C-MOVE finished without final success status (Last: 0x{final_move_status:04X})")

                # Release association
                assoc.release()
                logger.info(f"Task {task_id}: Association released with {config.remote_ae_title}.")
                return {"status": "success", "message": f"C-MOVE successful for Study {study_instance_uid}"}

            else:
                # Association failed
                logger.error(f"Task {task_id}: Association rejected/aborted by {config.remote_ae_title} for C-MOVE.")
                raise DimseMoveError(f"Association failed with {config.remote_ae_title}") # Trigger retry

        except Exception as move_exc:
            # Catch errors during association or C-MOVE sending/processing
            logger.error(f"Task {task_id}: Error during C-MOVE operation for Study {study_instance_uid}: {move_exc}", exc_info=True)
            if assoc and assoc.is_established:
                assoc.abort()
            # Update DB state for error
            crud_dimse_qr_source.update_move_status(
                 db=db,
                 source_id=config.id,
                 last_error_time=datetime.now(timezone.utc),
                 last_error_message=f"C-MOVE Error: {str(move_exc)[:500]}" # Store truncated error
            )
            # db.commit() handled by CRUD function
            # Re-raise specific error types to allow Celery auto-retry
            if isinstance(move_exc, (ConnectionRefusedError, TimeoutError, OSError, DimseMoveError)):
                 raise move_exc # Let Celery handle retry
            else:
                 # For unexpected errors, maybe don't retry automatically
                 return {"status": "error", "message": f"Unexpected C-MOVE error: {move_exc}"}

    except Exception as e:
        # Catch errors during DB fetching or general setup
        logger.critical(f"Task {task_id}: CRITICAL - Unhandled exception: {e}", exc_info=True)
        if db and config: # Attempt to update DB error state if possible
            try:
                crud_dimse_qr_source.update_move_status(
                    db=db, source_id=config.id,
                    last_error_time=datetime.now(timezone.utc),
                    last_error_message=f"Unhandled Task Error: {str(e)[:500]}"
                )
                # db.commit() handled by CRUD
            except Exception as db_err:
                 logger.error(f"Task {task_id}: Failed to update error status in DB during critical failure handling: {db_err}")
                 if db: db.rollback()
        elif db:
             db.rollback() # Rollback if config fetch failed

        # Re-raise non-retryable error or return error status?
        # For critical setup errors, maybe return failure immediately
        return {"status": "error", "message": f"Critical task error: {e}"}
    finally:
        if db:
            db.close()
