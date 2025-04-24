# app/worker/dimse_qr_poller.py
import logging
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, Optional, Generator, Tuple
import re

# Pynetdicom imports
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelFind,
)
from pydicom.dataset import Dataset
from pydicom.tag import Tag

# Application imports
from sqlalchemy.orm import Session
from celery import shared_task
from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models
from app.db.models import ProcessedStudySourceType # Import Enum
# --- Use top-level crud ---
from app import crud
# --- End Use top-level ---
from .dimse_qr_retriever import trigger_dimse_cmove_task

# Configure logging
logger = logging.getLogger(__name__)
# debug_logger()

class DimseQrPollingError(Exception):
    """Custom exception for DIMSE Q/R polling errors."""
    pass

# --- Constants ---
QR_LEVEL_SOP_CLASSES = {
    "PATIENT": PatientRootQueryRetrieveInformationModelFind,
    "STUDY": StudyRootQueryRetrieveInformationModelFind,
}

# --- Helper function _resolve_dynamic_date_filter (Corrected try/except) ---
def _resolve_dynamic_date_filter(value: Any) -> Optional[str]:
    """
    Resolves special date strings into DICOM DA or range format.
    Returns None if the value is invalid or cannot be resolved.
    """
    if not isinstance(value, str):
        logger.debug(f"Date filter value is not a string ({type(value)}), cannot resolve dynamically.")
        return None

    val_upper = value.strip().upper()
    today = date.today()

    if val_upper == "TODAY":
        return today.strftime('%Y%m%d')
    if val_upper == "YESTERDAY":
        yesterday = today - timedelta(days=1)
        return yesterday.strftime('%Y%m%d')

    # Check for -<N>d format (N days ago until now) -> YYYYMMDD-
    match_days_ago = re.match(r"^-(\d+)D$", val_upper)
    if match_days_ago:
        try:
            days = int(match_days_ago.group(1))
            if days >= 0:
                start_date = today - timedelta(days=days)
                return f"{start_date.strftime('%Y%m%d')}-" # Correct return placement
            else:
                 logger.warning(f"Invalid negative number of days in date filter: {value}")
                 # Fall through to return None if days < 0
        except (ValueError, OverflowError):
            logger.warning(f"Invalid number of days in date filter: {value}")
            # Fall through to return None if exception occurs
        # Return None ONLY if try failed or days < 0
        return None

    # Check for existing DICOM formats (YYYYMMDD, YYYYMMDD-, YYYYMMDD-YYYYMMDD)
    if re.match(r"^\d{8}$", val_upper): return val_upper
    if re.match(r"^\d{8}-$", val_upper): return val_upper
    if re.match(r"^\d{8}-\d{8}$", val_upper): return val_upper

    logger.warning(f"Unrecognized dynamic date format: {value}")
    return None


# --- Main Celery Beat Task ---
@shared_task(name="poll_all_dimse_qr_sources")
def poll_all_dimse_qr_sources() -> Dict[str, Any]:
    """Celery Beat task to poll all enabled DIMSE Q/R sources using C-FIND."""
    logger.info("Starting DIMSE Q/R polling cycle...")
    processed_sources = 0; failed_sources = 0; db: Optional[Session] = None
    try:
        db = SessionLocal();
        # --- Use correct crud object ---
        enabled_sources = crud.crud_dimse_qr_source.get_enabled_sources(db)
        # --- END Use correct crud object ---
        logger.info(f"Found {len(enabled_sources)} enabled DIMSE Q/R sources to check.")
        for source_config in enabled_sources:
            try: _poll_single_dimse_source(db, source_config); processed_sources += 1
            except Exception as e:
                logger.error(f"Polling failed for DIMSE Q/R source '{source_config.name}' (ID: {source_config.id}): {e}", exc_info=True); failed_sources += 1
                try:
                    # --- Use correct crud object ---
                    crud.crud_dimse_qr_source.update_query_status( db=db, source_id=source_config.id, last_error_time=datetime.now(timezone.utc), last_error_message=str(e)[:1024] )
                    # --- END Use correct crud object ---
                except Exception as db_err: logger.error(f"Failed to update error status for {source_config.name}: {db_err}", exc_info=True)
        # Commit all updates after loop
        if db: db.commit(); logger.debug("Committed DB changes after DIMSE polling cycle.")
    except Exception as e:
        logger.error(f"Critical error during DIMSE Q/R polling cycle: {e}", exc_info=True);
        if db: db.rollback() # Rollback on critical error
    finally:
        # Ensure the database session is always closed
        if db:
            db.close()
            logger.debug("Database session closed.")

    logger.info("DIMSE Q/R polling cycle finished.")
    return {"status": "cycle_complete", "processed": processed_sources, "failed": failed_sources}


# --- Helper to Poll a Single Source ---
def _poll_single_dimse_source(db: Session, config: models.DimseQueryRetrieveSource):
    """Polls a single DIMSE Q/R source using C-FIND and queues C-MOVE if needed."""
    logger.info(f"Polling DIMSE Q/R source: '{config.name}' (ID: {config.id}) -> {config.remote_ae_title}@{config.remote_host}:{config.remote_port}")
    ae = AE(ae_title=config.local_ae_title)
    ae.add_requested_context('1.2.840.10008.1.1') # Verification
    find_sop_class = QR_LEVEL_SOP_CLASSES.get(config.query_level.upper());
    if not find_sop_class: raise ValueError(f"Unsupported query level: {config.query_level}")
    ae.add_requested_context(find_sop_class)

    identifier = Dataset(); identifier.QueryRetrieveLevel = config.query_level.upper(); filter_keys_used = set()
    if isinstance(config.query_filters, dict):
        for key, value in config.query_filters.items():
            resolved_value = value;
            if key.upper() == 'STUDYDATE': resolved_value = _resolve_dynamic_date_filter(value)
            if resolved_value is None and key.upper() == 'STUDYDATE': logger.warning(f"Skipping StudyDate filter: {value}"); continue
            try: tag = Tag(key); keyword = tag.keyword or key
            except Exception: keyword = key
            try: setattr(identifier, keyword, resolved_value); filter_keys_used.add(keyword); logger.debug(f" Filter: {keyword}={resolved_value}")
            except Exception as e: logger.warning(f"Error setting filter {keyword}={resolved_value}: {e}")
    default_return_keys = [];
    if config.query_level.upper() == "STUDY": default_return_keys = ["PatientID", "PatientName", "StudyInstanceUID", "StudyDate", "StudyTime", "AccessionNumber", "ModalitiesInStudy"]
    for key in default_return_keys:
        if key not in filter_keys_used:
            try: setattr(identifier, key, '')
            except Exception as e: logger.warning(f"Could not set return key {key}: {e}")
    logger.debug(f"Final C-FIND Identifier:\n{identifier}")

    assoc = None
    studies_queued_this_run = 0
    found_study_uids_this_run = set()
    source_id_for_task = config.id # Get the source ID
    # --- Get source ID as string for logging ---
    source_id_str = str(config.id)
    # --- END Get source ID as string ---
    try:
        assoc = ae.associate( config.remote_host, config.remote_port, ae_title=config.remote_ae_title )
        if assoc.is_established:
            logger.info(f"Assoc established with {config.remote_ae_title} for C-FIND.")
            responses: Generator[Tuple[Dataset, Optional[Dataset]], Any, None] = assoc.send_c_find(identifier, find_sop_class)
            response_count = 0; success_or_pending_received = False
            for status, result_identifier in responses:
                if status and hasattr(status, 'Status'):
                     status_int = int(status.Status)
                     if status.Status == 0x0000: # Success
                         success_or_pending_received = True; logger.info(f"C-FIND success '{config.name}'. {response_count} results."); break
                     elif status.Status in (0xFF00, 0xFF01): # Pending
                         success_or_pending_received = True
                         if result_identifier:
                             response_count += 1; study_uid = result_identifier.get("StudyInstanceUID", None)
                             if study_uid:
                                 found_study_uids_this_run.add(study_uid) # Add found UID
                                 pat_id=result_identifier.get("PatientID", "N/A"); study_date=result_identifier.get("StudyDate", "N/A"); logger.info(f" -> Found Study: {study_uid}, PID={pat_id}, Date={study_date} [{response_count}]")
                                 # --- Use correct crud object AND pass source_id as string ---
                                 already_processed = crud.crud_processed_study_log.check_exists(
                                     db=db,
                                     source_type=ProcessedStudySourceType.DIMSE_QR,
                                     source_id=source_id_str, # <<< Pass string ID
                                     study_instance_uid=study_uid
                                 )
                                 # --- END Use correct crud object AND pass source_id as string ---
                                 if already_processed: logger.debug(f"Study {study_uid} already logged. Skipping.")
                                 elif config.move_destination_ae_title:
                                     try:
                                         logger.debug(f"Queueing C-MOVE: {study_uid} -> {config.move_destination_ae_title}")
                                         trigger_dimse_cmove_task.delay(source_id=source_id_for_task, study_instance_uid=study_uid)
                                         # --- Use correct crud object AND pass source_id as string ---
                                         log_created = crud.crud_processed_study_log.create_log_entry(
                                             db=db,
                                             source_type=ProcessedStudySourceType.DIMSE_QR,
                                             source_id=source_id_str, # <<< Pass string ID
                                             study_instance_uid=study_uid,
                                             commit=False # Commit handled outside loop
                                         )
                                         # --- END Use correct crud object AND pass source_id as string ---
                                         if log_created:
                                             studies_queued_this_run += 1 # Increment local counter
                                             logger.info(f"Logged {study_uid} for queueing.")
                                         else: logger.error(f"Failed log {study_uid} after queueing.")
                                     except Exception as queue_err: logger.error(f"Failed queue C-MOVE {study_uid}. Error: {queue_err}", exc_info=True)
                                 else: logger.warning(f"Study {study_uid} found but no move dest configured.")
                             else: logger.warning(f"Pending Result [{response_count}] missing StudyUID.")
                         else: logger.warning(f"Pending status 0x{status_int:04X} no identifier.")
                         continue
                     elif status.Status == 0xFE00: raise DimseQrPollingError(f"C-FIND Cancelled (0xFE00)")
                     else: error_comment = status.get('ErrorComment', 'No comment'); raise DimseQrPollingError(f"C-FIND Failed (0x{status_int:04X} - {error_comment})")
                else: raise DimseQrPollingError("Invalid/Missing Status in C-FIND response")
            if not success_or_pending_received: raise DimseQrPollingError("No valid C-FIND status received")

            # --- Increment Counts AFTER successful loop ---
            if len(found_study_uids_this_run) > 0:
                 try:
                     # Use correct crud object
                     if crud.crud_dimse_qr_source.increment_found_study_count(db=db, source_id=source_id_for_task, count=len(found_study_uids_this_run)):
                          logger.info(f"Incremented found study count for source ID {source_id_for_task} by {len(found_study_uids_this_run)}")
                     else:
                          logger.warning(f"Failed to increment found study count for source ID {source_id_for_task}")
                 except Exception as inc_err:
                       logger.error(f"DB Error incrementing found study count for source ID {source_id_for_task}: {inc_err}")

            if studies_queued_this_run > 0:
                 try:
                     # Use correct crud object
                     if crud.crud_dimse_qr_source.increment_move_queued_count(db=db, source_id=source_id_for_task, count=studies_queued_this_run):
                          logger.info(f"Incremented move queued count for source ID {source_id_for_task} by {studies_queued_this_run}")
                     else:
                           logger.warning(f"Failed to increment move queued count for source ID {source_id_for_task}")
                 except Exception as inc_err:
                       logger.error(f"DB Error incrementing move queued count for source ID {source_id_for_task}: {inc_err}")
            # --- End Increment ---

            # Update last successful query timestamp (commit handled outside)
            # --- Use correct crud object ---
            crud.crud_dimse_qr_source.update_query_status(db=db, source_id=config.id, last_successful_query=datetime.now(timezone.utc))
            # --- END Use correct crud object ---

            assoc.release(); logger.info(f"Assoc released. Found {len(found_study_uids_this_run)} studies, queued {studies_queued_this_run} new moves.")
        else: raise DimseQrPollingError(f"Association failed with {config.remote_ae_title}")
    except Exception as e:
        logger.error(f"Error C-FIND '{config.name}': {e}", exc_info=True)
        if assoc and assoc.is_established: assoc.abort(); logger.warning(f"Aborted assoc with {config.remote_ae_title}.")
        raise e # Re-raise to be caught by main task loop
