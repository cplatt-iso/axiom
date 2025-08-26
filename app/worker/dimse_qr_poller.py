# app/worker/dimse_qr_poller.py
import logging
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, Optional, Generator, Tuple, List
import re

# Pynetdicom imports
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind, # type: ignore[attr-defined]
    StudyRootQueryRetrieveInformationModelFind, # type: ignore[attr-defined]
    SOPClass # Import base type if needed for type hints elsewhere
)
from pydicom.dataset import Dataset
from pydicom.tag import Tag

# Application imports
from sqlalchemy.orm import Session
from celery import shared_task
from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models
from app.schemas.enums import ProcessedStudySourceType
from app import crud
from .dimse_qr_retriever import trigger_dimse_cmove_task
# Import the SCU service
from app.services.network.dimse.scu_service import (
    find_studies,
    DimseScuError,
    AssociationError,
    DimseCommandError,
    TlsConfigError
)


# Configure logging
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = structlog.get_logger(__name__)
    logger.warning("structlog not found, using standard logging. Bind method will not work.")


class DimseQrPollingError(Exception):
    """Custom exception for DIMSE Q/R polling errors."""
    pass

# Constants
QR_LEVEL_SOP_CLASSES = {
    "PATIENT": PatientRootQueryRetrieveInformationModelFind,
    "STUDY": StudyRootQueryRetrieveInformationModelFind,
}

# Helper function _resolve_dynamic_date_filter (Unchanged)
def _resolve_dynamic_date_filter(value: Any) -> Optional[str]:
    if not isinstance(value, str): return None
    val_upper = value.strip().upper()
    today = date.today()
    if val_upper == "TODAY": return today.strftime('%Y%m%d')
    if val_upper == "YESTERDAY": return (today - timedelta(days=1)).strftime('%Y%m%d')
    match_days_ago = re.match(r"^-(\d+)D$", val_upper)
    if match_days_ago:
        try:
            days = int(match_days_ago.group(1))
            if days >= 0: return (today - timedelta(days=days)).strftime('%Y%m%d') + "-"
        except (ValueError, OverflowError): pass
        return None
    if re.match(r"^\d{8}$", val_upper): return val_upper
    if re.match(r"^\d{8}-$", val_upper): return val_upper
    if re.match(r"^\d{8}-\d{8}$", val_upper): return val_upper
    logger.warning("Unrecognized dynamic date format", unrecognized_value=value) # type: ignore[call-arg]
    return None


# Main Celery Beat Task (Unchanged except for how _poll_single handles errors)
@shared_task(name="poll_all_dimse_qr_sources")
def poll_all_dimse_qr_sources() -> Dict[str, Any]:
    """
    Celery Beat task. Checks all active DIMSE QR sources and polls ONLY those
    whose polling_interval_seconds has elapsed since their last_successful_query.
    """
    logger.info("Starting DIMSE Q/R interval check cycle...")
    polls_attempted = 0
    successful_polls = 0
    failed_polls = 0
    db: Optional[Session] = None

    try:
        db = SessionLocal()
        # Get all potentially active sources
        all_sources = crud.crud_dimse_qr_source.get_multi(db, limit=settings.DIMSE_QR_POLLER_MAX_SOURCES)

        # Filter for sources that are enabled, active, AND due for polling
        sources_due_for_poll: List[models.DimseQueryRetrieveSource] = []
        now = datetime.now(timezone.utc)

        for source in all_sources:
            log = logger.bind(source_id=source.id, source_name=source.name) if hasattr(logger, 'bind') else logger # type: ignore[attr-defined]
            if not source.is_enabled or not source.is_active:
                log.debug("Source is disabled or inactive, skipping interval check.")
                continue

            # Check if interval has passed
            interval_met = False
            interval_seconds = source.polling_interval_seconds or 0 # Handle None case

            if interval_seconds <= 0:
                log.debug("Source has zero or negative polling interval, skipping timed poll.")
                continue # Don't poll if interval is zero or less

            if source.last_successful_query is None:
                # Never polled successfully before, consider it due
                interval_met = True
                log.debug("Source never polled successfully, scheduling poll.")
            else:
                # Ensure last_successful_query is timezone-aware (should be from DB)
                last_success_aware = source.last_successful_query
                if last_success_aware.tzinfo is None:
                    # This shouldn't happen if DB stores TZ, but handle defensively
                    log.warning("last_successful_query is timezone naive, assuming UTC.")
                    last_success_aware = last_success_aware.replace(tzinfo=timezone.utc)

                time_since_last_poll = now - last_success_aware
                if time_since_last_poll >= timedelta(seconds=interval_seconds):
                    interval_met = True
                    log.debug("Polling interval met.", interval_duration_sec=interval_seconds, time_since_last_poll_sec=time_since_last_poll.total_seconds()) # type: ignore[call-arg]
                else:
                    log.debug("Polling interval not met yet.", interval_duration_sec=interval_seconds, time_since_last_poll_sec=time_since_last_poll.total_seconds()) # type: ignore[call-arg]

            if interval_met:
                sources_due_for_poll.append(source)

        logger.info(f"Found {len(sources_due_for_poll)} sources due for polling based on their intervals.")

        # Poll only the sources that are due
        for source_config in sources_due_for_poll:
            # Re-bind logger for the specific poll attempt
            poll_log = logger.bind(source_id=source_config.id, source_name=source_config.name) if hasattr(logger, 'bind') else logger # type: ignore[attr-defined]
            polls_attempted += 1
            try:
                 # Call the existing function that handles the actual poll logic
                 poll_log.info("Attempting poll for due source...")
                 _poll_single_dimse_source(db, source_config)
                 successful_polls += 1
                 # Note: _poll_single_dimse_source now handles committing its own success updates
            except (DimseQrPollingError, TlsConfigError, AssociationError, DimseCommandError, DimseScuError) as poll_err:
                poll_log.error("Polling failed for DIMSE Q/R source", specific_error=str(poll_err), exc_info=True) # type: ignore[call-arg]
                failed_polls += 1
                try:
                    # Update error status in DB
                    crud.crud_dimse_qr_source.update_query_status(
                         db=db, source_id=source_config.id,
                         last_error_time=datetime.now(timezone.utc),
                         last_error_message=str(poll_err)[:1024]
                    )
                    db.commit() # Commit error update separately
                except Exception as db_err:
                    poll_log.error("Failed to update error status in DB", db_update_error=str(db_err), exc_info=True) # type: ignore[call-arg]
                    db.rollback() # Rollback only the failed status update
            except Exception as e:
                poll_log.error("Unexpected error during polling", general_error=str(e), exc_info=True) # type: ignore[call-arg]
                failed_polls += 1
                try:
                    # Update error status in DB
                    crud.crud_dimse_qr_source.update_query_status(
                         db=db, source_id=source_config.id,
                         last_error_time=datetime.now(timezone.utc),
                         last_error_message=f"Unexpected: {str(e)[:1000]}"
                    )
                    db.commit() # Commit error update separately
                except Exception as db_err:
                    poll_log.error("Failed to update error status in DB after unexpected error", db_update_error=str(db_err), exc_info=True) # type: ignore[call-arg]
                    db.rollback()
    except Exception as e:
        logger.error("Critical error during DIMSE Q/R interval check cycle", cycle_error=str(e), exc_info=True); # type: ignore[call-arg]
        if db: db.rollback() # Rollback any potential partial commits if outer loop fails
    finally:
        if db:
            db.close()
            logger.debug("Database session closed.")

    logger.info("DIMSE Q/R interval check cycle finished.", num_polls_attempted=polls_attempted, num_successful_polls=successful_polls, num_failed_polls=failed_polls) # type: ignore[call-arg]
    return {
        "status": "cycle_complete",
        "polls_attempted": polls_attempted,
        "successful_polls": successful_polls,
        "failed_polls": failed_polls
    }

# Helper to Poll a Single Source (Refactored)
def _poll_single_dimse_source(db: Session, config: models.DimseQueryRetrieveSource):
    """Polls a single DIMSE Q/R source using scu_service.find_studies."""
    log = logger.bind(source_name=config.name, source_id=config.id, remote_ae=config.remote_ae_title) if hasattr(logger, 'bind') else logger # type: ignore[attr-defined]
    log.info("Polling DIMSE Q/R source using scu_service")

    # Determine SOP Class Object
    find_sop_class = QR_LEVEL_SOP_CLASSES.get(config.query_level.upper())
    if not find_sop_class:
        raise ValueError(f"Unsupported query level: {config.query_level}")

    # --- Build C-FIND Identifier ---
    identifier = Dataset()
    identifier.QueryRetrieveLevel = config.query_level.upper()
    filter_keys_used = set()

    if isinstance(config.query_filters, dict):
        for key, value in config.query_filters.items():
            resolved_value = value
            if key.upper() == 'STUDYDATE':
                resolved_value = _resolve_dynamic_date_filter(value)
            
            if resolved_value is None and key.upper() == 'STUDYDATE':
                log.warning(f"Skipping StudyDate filter", original_filter_value=value) # type: ignore[call-arg]
                continue
            
            try:
                tag = Tag(key)
                keyword = (tag.keyword # type: ignore[attr-defined]
                          ) or key
            except Exception:
                keyword = key
            
            try:
                setattr(identifier, keyword, resolved_value)
                filter_keys_used.add(keyword)
                log.debug(f" Filter: {keyword}={resolved_value}")
            except Exception as e:
                log.warning(f"Error setting filter", filter_keyword=keyword, filter_value_set=resolved_value, filter_set_error=str(e)) # type: ignore[call-arg]
    
    default_return_keys: List[str] = []
    if config.query_level.upper() == "STUDY":
        default_return_keys = ["PatientID", "PatientName", "StudyInstanceUID", "StudyDate", "StudyTime", "AccessionNumber", "ModalitiesInStudy"]
    
    for key in default_return_keys:
        if key not in filter_keys_used:
            try:
                setattr(identifier, key, '')
            except Exception as e:
                log.warning("Could not set default return key", return_key_name=key, set_key_error=str(e)) # type: ignore[call-arg]
    
    log.debug(f"Final C-FIND Identifier:\n{identifier}")

    # --- Prepare config dict for scu_service ---
    scu_config_dict = {
        "remote_host": config.remote_host,
        "remote_port": config.remote_port,
        "remote_ae_title": config.remote_ae_title,
        "local_ae_title": config.local_ae_title,
        "tls_enabled": config.tls_enabled,
        "tls_ca_cert_secret_name": config.tls_ca_cert_secret_name,
        "tls_client_cert_secret_name": config.tls_client_cert_secret_name,
        "tls_client_key_secret_name": config.tls_client_key_secret_name,
    }

    # --- Execute C-FIND using scu_service ---
    found_results: List[Dataset] = []
    try:
        log.info("Calling scu_service.find_studies...")
        # Pass the SOPClass object itself
        found_results = find_studies(
            config=scu_config_dict,
            identifier=identifier,
            find_sop_class=find_sop_class # Pass the object
        )
        log.info("scu_service.find_studies completed", num_results_found=len(found_results)) # type: ignore[call-arg]
    except (TlsConfigError, AssociationError, DimseCommandError, DimseScuError, ValueError) as scu_err:
        log.error("C-FIND failed via scu_service", scu_service_error=str(scu_err), exc_info=True) # type: ignore[call-arg]
        raise DimseQrPollingError(f"C-FIND via service failed: {scu_err}") from scu_err
    except Exception as e:
        log.error("Unexpected error calling scu_service.find_studies", general_find_error=str(e), exc_info=True) # type: ignore[call-arg]
        raise DimseQrPollingError(f"Unexpected error during C-FIND: {e}") from e


    # --- Process Results ---
    studies_queued_this_run = 0
    found_study_uids_this_run = set()
    source_id_for_task = config.id
    source_id_str = str(config.id)

    log.info(f"Processing {len(found_results)} found results...")
    for result_identifier in found_results:
        study_uid = result_identifier.get("StudyInstanceUID", None)
        if study_uid:
            found_study_uids_this_run.add(study_uid)
            pat_id=result_identifier.get("PatientID", "N/A"); study_date=result_identifier.get("StudyDate", "N/A")
            log.info(f" -> Processing Study: {study_uid}, PID={pat_id}, Date={study_date}")

            already_processed = crud.crud_processed_study_log.check_exists(
                db=db,
                source_type=ProcessedStudySourceType.DIMSE_QR,
                source_id=source_id_str, # type: ignore[arg-type]
                study_instance_uid=study_uid
            )
            if already_processed: log.debug("Study already logged. Skipping C-MOVE queue.", current_study_uid=study_uid) # type: ignore[call-arg]
            elif config.move_destination_ae_title:
                try:
                    log.debug("Queueing C-MOVE task", current_study_uid=study_uid, move_destination_target=config.move_destination_ae_title) # type: ignore[call-arg]
                    trigger_dimse_cmove_task.delay(source_id=source_id_for_task, study_instance_uid=study_uid) # type: ignore[attr-defined]
                    log_created = crud.crud_processed_study_log.create_log_entry(
                        db=db,
                        source_type=ProcessedStudySourceType.DIMSE_QR,
                        source_id=source_id_str, # type: ignore[arg-type]
                        study_instance_uid=study_uid,
                        commit=False
                    )
                    if log_created:
                        studies_queued_this_run += 1
                        log.info("Logged study for C-MOVE queueing.", current_study_uid=study_uid) # type: ignore[call-arg]
                    else: log.error("Failed to log study after queueing C-MOVE task.", current_study_uid=study_uid) # type: ignore[call-arg]
                except Exception as queue_err: log.error("Failed queue C-MOVE task", current_study_uid=study_uid, c_move_queue_error=str(queue_err), exc_info=True) # type: ignore[call-arg]
            else: log.warning("Study found but no move destination configured.", current_study_uid=study_uid) # type: ignore[call-arg]
        else: log.warning("Found result missing StudyInstanceUID.")

    # --- Update DB Counts ---
    commit_needed = False
    if len(found_study_uids_this_run) > 0:
         try:
             rows_affected = crud.crud_dimse_qr_source.increment_found_study_count(db=db, source_id=source_id_for_task, count=len(found_study_uids_this_run))
             if rows_affected > 0: log.info("Incremented found study count", increment_amount=len(found_study_uids_this_run)); commit_needed = True # type: ignore[call-arg]
             else: log.warning("Failed to increment found study count (rows affected 0)")
         except Exception as inc_err:
               log.error("DB Error incrementing found study count", db_increment_error=str(inc_err)) # type: ignore[call-arg]

    if studies_queued_this_run > 0:
         try:
             rows_affected = crud.crud_dimse_qr_source.increment_move_queued_count(db=db, source_id=source_id_for_task, count=studies_queued_this_run)
             if rows_affected > 0: log.info("Incremented move queued count", increment_amount=studies_queued_this_run); commit_needed = True # type: ignore[call-arg]
             else: log.warning("Failed to increment move queued count (rows affected 0)")
         except Exception as inc_err:
               log.error("DB Error incrementing move queued count", db_increment_error=str(inc_err)) # type: ignore[call-arg]

    # Update last successful query timestamp
    try:
         crud.crud_dimse_qr_source.update_query_status(db=db, source_id=config.id, last_successful_query=datetime.now(timezone.utc))
         commit_needed = True
    except Exception as status_err:
         log.error("DB Error updating last successful query timestamp", db_status_update_error=str(status_err)) # type: ignore[call-arg]

    # Commit successful updates
    if commit_needed:
         try:
             db.commit()
             log.info("Committed DB updates for successful poll.")
         except Exception as commit_err:
              log.error("Failed to commit DB updates after successful poll", db_commit_error=str(commit_err)) # type: ignore[call-arg]
              db.rollback()
    else:
         log.info("No database changes to commit for this poll.")

    log.info("Finished processing poll", num_found_studies_in_run=len(found_study_uids_this_run), num_queued_moves_in_run=studies_queued_this_run) # type: ignore[call-arg]