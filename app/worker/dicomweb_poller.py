# app/worker/dicomweb_poller.py
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List

from sqlalchemy.orm import Session
from celery import shared_task

from app.core.config import settings
from app.db.session import SessionLocal
from app.db.models import DicomWebSourceState, ProcessedStudySourceType

from app import crud
from app.services import dicomweb_client
from app.worker.tasks import process_dicomweb_metadata_task

from app.core.config import settings

logger = logging.getLogger(__name__)

@shared_task(name="poll_all_dicomweb_sources")
def poll_all_dicomweb_sources() -> Dict[str, Any]:
    """
    Celery Beat task to poll all enabled DICOMweb sources for new studies/instances.
    Includes deduplication check and metrics increments.
    """
    logger.info("Starting DICOMweb polling cycle...")
    processed_sources = 0
    failed_sources = 0
    db: Optional[Session] = None # Initialize db as Optional

    try:
        db = SessionLocal()
        # --- Use alias crud_dicomweb_state ---
        all_sources = crud.dicomweb_state.get_all(db, limit=settings.DICOMWEB_POLLER_MAX_SOURCES)
        # --- END Use alias ---
        enabled_sources = [s for s in all_sources if s.is_enabled]
        logger.info(f"Found {len(enabled_sources)} enabled DICOMweb sources in database to check.")

        for source_config in enabled_sources:
             try:
                 # Process each enabled source individually
                 poll_source(db, source_config) # Pass the db session
                 processed_sources += 1
             except Exception as e:
                 # Log failure for this specific source and update its error state
                 logger.error(f"Polling failed for source '{source_config.source_name}' (ID: {source_config.id}): {e}", exc_info=True)
                 failed_sources += 1
                 try:
                      # --- Use alias crud_dicomweb_state ---
                      crud.dicomweb_state.update_run_status(
                          db,
                          source_name=source_config.source_name,
                          last_error_run=datetime.now(timezone.utc),
                          last_error_message=str(e)[:1024] # Store truncated error
                      )
                      # --- END Use alias ---
                      # No commit here, handled by the main try/finally block
                 except Exception as db_err:
                      logger.error(f"Failed to update error status for {source_config.source_name} in DB: {db_err}", exc_info=True)

        # Commit all state/error/metric/log updates after processing all sources
        if db: # Check if db was successfully initialized
            db.commit()
            logger.debug("Committed DB changes after DICOMweb polling cycle.")

    except Exception as e:
         logger.error(f"Critical error during DICOMweb polling cycle setup or commit: {e}", exc_info=True)
         if db: db.rollback()
    finally:
        if db: db.close()

    logger.info("DICOMweb polling cycle finished.")
    return {"status": "cycle_complete", "processed": processed_sources, "failed": failed_sources}


def poll_source(db: Session, config: DicomWebSourceState):
    """
    Polls a single DICOMweb source for new instances based on configuration and last run state.
    Includes deduplication using ProcessedStudyLog and increments metrics.
    """
    logger.info(f"Polling DICOMweb source: '{config.source_name}' (ID: {config.id})")
    client = dicomweb_client
    last_successful_run_for_update: Optional[datetime] = None
    last_processed_in_batch: Optional[datetime] = None
    newly_queued_instance_count = 0
    found_instances_in_run = set() # Track unique instance UIDs found in *this* run
    total_instances_found_this_run_for_db = 0
    # --- Get source ID as string for logging ---
    source_id_str = str(config.id)
    # --- END Get source ID as string ---

    try:
        # --- Determine query parameters (logic remains the same) ---
        polling_custom_params: Dict[str, Any] = {}
        study_date_in_config = False; config_study_date = None
        if isinstance(config.search_filters, dict):
             for key in config.search_filters:
                 if key.lower() == 'studydate': study_date_in_config = True; config_study_date = config.search_filters[key]; break
        if not study_date_in_config:
            now_utc = datetime.now(timezone.utc)
            if config.last_successful_run: overlap = timedelta(minutes=settings.DICOMWEB_POLLER_OVERLAP_MINUTES); query_since = config.last_successful_run - overlap; logger.info(f"{config.source_name}: Querying from {query_since.isoformat()} (+ {overlap})")
            else: fallback_days = settings.DICOMWEB_POLLER_DEFAULT_FALLBACK_DAYS; query_since = now_utc - timedelta(days=fallback_days); logger.info(f"{config.source_name}: First run. Querying from {query_since.isoformat()} ({fallback_days} days ago).")
            study_date_param = f"{query_since.strftime('%Y%m%d')}-{now_utc.strftime('%Y%m%d')}"; polling_custom_params["StudyDate"] = study_date_param; logger.info(f"{config.source_name}: Calculated StudyDate: {study_date_param}")
        else: logger.info(f"{config.source_name}: Using StudyDate from config: '{config_study_date}'")
        # --- End Parameter Determination ---

        # Perform the QIDO query for studies
        logger.debug(f"{config.source_name}: Calling query_qido (STUDY) with custom_params: {polling_custom_params}")
        studies = client.query_qido(config, level="STUDY", custom_params=polling_custom_params)
        logger.info(f"{config.source_name}: Found {len(studies)} studies matching criteria.")

        if not studies:
            last_successful_run_for_update = datetime.now(timezone.utc)
            total_instances_found_this_run_for_db = 0
        else:
            all_processed_timestamps = []

            for study in studies:
                study_uid = study.get("0020000D", {}).get("Value", [None])[0]
                if not study_uid: logger.warning(f"{config.source_name}: Skipping study missing StudyInstanceUID: {study}"); continue
                logger.info(f"{config.source_name}: Processing Study UID: {study_uid}")

                # Deduplication Check
                # --- Use correct crud object AND pass source_id as string ---
                already_processed = crud.crud_processed_study_log.check_exists(
                    db=db,
                    source_type=ProcessedStudySourceType.DICOMWEB,
                    source_id=source_id_str, # <<< Pass string ID
                    study_instance_uid=study_uid
                )
                # --- END Use correct crud object AND pass source_id as string ---
                if already_processed: logger.debug(f"{config.source_name}: Study UID {study_uid} already logged. Skipping."); continue

                try:
                    # Query instances for this specific study
                    instances = client.query_qido(config, level="INSTANCE", custom_params={"StudyInstanceUID": study_uid})
                    logger.info(f"{config.source_name}: Found {len(instances)} instances for study {study_uid}.")

                    # --- Increment Found Count ---
                    if len(instances) > 0:
                        total_instances_found_this_run_for_db += len(instances)
                        found_instances_in_run.update(inst.get("00080018", {}).get("Value", [None])[0] for inst in instances if inst.get("00080018", {}).get("Value", [None])[0])
                    # --- End Increment ---

                    queued_study_in_loop = False
                    for instance in instances:
                        series_uid = instance.get("0020000E", {}).get("Value", [None])[0]
                        instance_uid = instance.get("00080018", {}).get("Value", [None])[0]
                        instance_dt_str = instance.get("00080015", {}).get("Value", [None])[0]
                        if not series_uid or not instance_uid: logger.warning(f"Skipping instance missing UIDs: Study={study_uid}"); continue

                        # Timestamp check
                        instance_datetime: Optional[datetime] = None; process_instance = True
                        if instance_dt_str:
                            try:
                                dt_part=instance_dt_str.split('+')[0].split('-')[0].split('&')[0]; dt_format='%Y%m%d%H%M%S';
                                if '.' in dt_part: dt_format += '.%f'
                                instance_datetime = datetime.strptime(dt_part, dt_format).replace(tzinfo=timezone.utc)
                                all_processed_timestamps.append(instance_datetime)
                                if config.last_processed_timestamp and instance_datetime <= config.last_processed_timestamp: process_instance = False; logger.debug(f"Skipping {instance_uid} by timestamp.")
                            except: logger.warning(f"Parse fail DT '{instance_dt_str}'. Processing {instance_uid}."); all_processed_timestamps.append(datetime.now(timezone.utc))
                        else: logger.warning(f"Instance {instance_uid} missing DT. Processing."); all_processed_timestamps.append(datetime.now(timezone.utc))

                        if process_instance:
                            logger.info(f"{config.source_name}: Queueing instance {instance_uid}.")
                            try:
                                # Task call already uses config.id (integer) which is correct for this task
                                process_dicomweb_metadata_task.delay(
                                    source_id=config.id, # Pass the integer source ID
                                    study_uid=study_uid,
                                    series_uid=series_uid,
                                    instance_uid=instance_uid
                                )
                                newly_queued_instance_count += 1 # Increment local counter for QUEUED
                                queued_study_in_loop = True
                            except Exception as queue_err: logger.error(f"Failed queue task instance {instance_uid}. Error: {queue_err}", exc_info=True)

                    # Log study after attempting to queue its instances
                    if queued_study_in_loop:
                        try:
                            # --- Use correct crud object AND pass source_id as string ---
                            log_created = crud.crud_processed_study_log.create_log_entry(
                                db=db,
                                source_type=ProcessedStudySourceType.DICOMWEB,
                                source_id=source_id_str, # <<< Pass string ID
                                study_instance_uid=study_uid,
                                commit=False
                            )
                            # --- END Use correct crud object AND pass source_id as string ---
                            if log_created: logger.info(f"Logged Study UID {study_uid} to ProcessedStudyLog.")
                            else: logger.error(f"Failed create ProcessedStudyLog entry for {study_uid}.")
                        except Exception as log_err: logger.error(f"Exception creating ProcessedStudyLog entry for {study_uid}: {log_err}", exc_info=True)

                except client.DicomWebClientError as q_err: logger.error(f"Failed query instances {study_uid}: {q_err}")
                except Exception as inst_err: logger.error(f"Unexpected error processing instances {study_uid}: {inst_err}", exc_info=True)

            # --- Increment Found Count (Total for Run) ---
            if total_instances_found_this_run_for_db > 0:
                 try:
                     # Use alias crud_dicomweb_state
                     if crud.dicomweb_state.increment_found_count(db=db, source_name=config.source_name, count=total_instances_found_this_run_for_db):
                         logger.info(f"Incremented found count for '{config.source_name}' by {total_instances_found_this_run_for_db}")
                     else:
                          logger.warning(f"Failed to increment found count for '{config.source_name}'")
                 except Exception as inc_err:
                      logger.error(f"DB Error incrementing found count for '{config.source_name}': {inc_err}")
                      # Continue processing state update even if counter fails
            # --- End Increment ---

            if all_processed_timestamps: last_processed_in_batch = max(all_processed_timestamps); logger.info(f"{config.source_name}: Latest timestamp: {last_processed_in_batch.isoformat()}")
            last_successful_run_for_update = datetime.now(timezone.utc)

        # Update source state
        logger.info(f"{config.source_name}: Poll cycle finished. Found {len(found_instances_in_run)} unique instances. Queued {newly_queued_instance_count} new instances.")
        update_payload = { "last_successful_run": last_successful_run_for_update, "last_error_message": None, "last_error_run": None }
        if last_processed_in_batch and (not config.last_processed_timestamp or last_processed_in_batch > config.last_processed_timestamp): update_payload["last_processed_timestamp"] = last_processed_in_batch; logger.info(f"Advancing timestamp.")
        elif last_processed_in_batch: logger.info(f"Not advancing timestamp.")

        # --- Increment Queued Count ---
        if newly_queued_instance_count > 0:
             try:
                 # Use alias crud_dicomweb_state
                 if crud.dicomweb_state.increment_queued_count(db=db, source_name=config.source_name, count=newly_queued_instance_count):
                     logger.info(f"Incremented queued count for '{config.source_name}' by {newly_queued_instance_count}")
                 else:
                      logger.warning(f"Failed to increment queued count for '{config.source_name}'")
             except Exception as inc_err:
                  logger.error(f"DB Error incrementing queued count for '{config.source_name}': {inc_err}")
                  # Continue processing state update even if counter fails
        # --- End Increment ---

        # Use alias crud_dicomweb_state
        crud.dicomweb_state.update_run_status(db, source_name=config.source_name, **update_payload) # Use alias

    except client.DicomWebClientError as client_err: logger.error(f"{config.source_name}: Client error: {client_err}"); raise client_err
    except Exception as poll_err: logger.error(f"{config.source_name}: Unexpected error: {poll_err}", exc_info=True); raise poll_err
