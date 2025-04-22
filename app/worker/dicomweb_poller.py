# app/worker/dicomweb_poller.py
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from sqlalchemy.orm import Session
from celery import shared_task

from app.core.config import settings
from app.db.session import SessionLocal
from app.db.models import DicomWebSourceState
from app.services import dicomweb_client
# Import the specific instance for state updates
from app.crud.crud_dicomweb_source_state import dicomweb_state as crud_state_updater
# Import the task to be queued
from app.worker.tasks import process_dicomweb_metadata_task

logger = logging.getLogger(__name__)

@shared_task(name="poll_all_dicomweb_sources")
def poll_all_dicomweb_sources() -> Dict[str, Any]:
    """
    Celery Beat task to poll all enabled DICOMweb sources for new studies/instances.
    """
    logger.info("Starting DICOMweb polling cycle...")
    processed_sources = 0
    failed_sources = 0
    db: Session = SessionLocal()
    try:
        # Fetch all source configurations/states from the database
        all_sources = crud_state_updater.get_all(db, limit=settings.DICOMWEB_POLLER_MAX_SOURCES)
        enabled_sources = [s for s in all_sources if s.is_enabled]
        logger.info(f"Found {len(enabled_sources)} enabled DICOMweb sources in database to check.")

        for source_config in enabled_sources:
             try:
                 # Process each enabled source individually
                 poll_source(db, source_config)
                 processed_sources += 1
             except Exception as e:
                 # Log failure for this specific source and update its error state
                 logger.error(f"Polling failed for source '{source_config.source_name}' (ID: {source_config.id}): {e}", exc_info=True)
                 failed_sources += 1
                 try:
                      # Record the error state in the database for monitoring
                      crud_state_updater.update_run_status(
                          db,
                          source_name=source_config.source_name,
                          last_error_run=datetime.now(timezone.utc),
                          last_error_message=str(e)[:1024] # Store truncated error
                      )
                      db.commit()
                 except Exception as db_err:
                      # Log critical error if DB update fails
                      logger.error(f"Failed to update error status for {source_config.source_name} in DB: {db_err}", exc_info=True)
                      db.rollback()

    except Exception as e:
         # Catch errors during the initial source fetching phase
         logger.error(f"Critical error during DICOMweb polling cycle setup: {e}", exc_info=True)
    finally:
        # Ensure the database session is always closed
        db.close()

    logger.info("DICOMweb polling cycle finished.")
    return {"status": "cycle_complete", "processed": processed_sources, "failed": failed_sources}


def poll_source(db: Session, config: DicomWebSourceState):
    """
    Polls a single DICOMweb source for new instances based on configuration and last run state.

    Args:
        db: The database session.
        config: The DicomWebSourceState object containing configuration and state.
    """
    logger.info(f"Polling DICOMweb source: '{config.source_name}' (ID: {config.id})")
    client = dicomweb_client # Use the imported client module
    last_successful_run_for_update: Optional[datetime] = None
    last_processed_in_batch: Optional[datetime] = None
    processed_instance_count = 0

    try:
        # Determine the query parameters for QIDO-RS
        # Start with potential custom parameters needed by the poller logic
        polling_custom_params: Dict[str, Any] = {}

        # Check if StudyDate is already defined in the source's persistent search_filters
        study_date_in_config = False
        config_study_date = None
        if isinstance(config.search_filters, dict):
             for key in config.search_filters:
                 if key.lower() == 'studydate':
                     study_date_in_config = True
                     config_study_date = config.search_filters[key]
                     break

        # Calculate a dynamic StudyDate range ONLY if not specified in the config
        if not study_date_in_config:
            logger.info(f"{config.source_name}: No StudyDate filter found in configuration, calculating based on last run.")
            now_utc = datetime.now(timezone.utc)
            # Determine the start time for the query
            if config.last_successful_run:
                # Use last successful run time, maybe with overlap
                overlap = timedelta(minutes=settings.DICOMWEB_POLLER_OVERLAP_MINUTES)
                query_since = config.last_successful_run - overlap
                logger.info(f"{config.source_name}: Querying from last successful run: {query_since.isoformat()} (includes {overlap} overlap)")
            else:
                # Fallback for the first run or if last run time is missing
                fallback_days = settings.DICOMWEB_POLLER_DEFAULT_FALLBACK_DAYS
                query_since = now_utc - timedelta(days=fallback_days)
                logger.info(f"{config.source_name}: First run or no previous timestamp. Querying from {query_since.isoformat()} ({fallback_days} days ago).")

            # Format the StudyDate range parameter: YYYYMMDD-YYYYMMDD
            study_date_param = f"{query_since.strftime('%Y%m%d')}-{now_utc.strftime('%Y%m%d')}"
            polling_custom_params["StudyDate"] = study_date_param
            logger.info(f"{config.source_name}: Calculated StudyDate range parameter: {study_date_param}")
        else:
            logger.info(f"{config.source_name}: Using StudyDate filter specified in configuration: '{config_study_date}'")
            # The client will use the StudyDate from config.search_filters


        # Perform the QIDO query for studies matching the combined criteria
        # The client now handles merging config filters and these custom params correctly.
        logger.debug(f"{config.source_name}: Calling query_qido with polling_custom_params: {polling_custom_params}")
        studies = client.query_qido(config, level="STUDY", custom_params=polling_custom_params)
        study_count = len(studies)
        logger.info(f"{config.source_name}: Found {study_count} studies matching criteria.")

        if not studies:
            # No studies found, still update the last successful run time
            last_successful_run_for_update = datetime.now(timezone.utc)
        else:
            # Process each found study to find its instances
            all_processed_timestamps = []
            for study in studies:
                study_uid = study.get("0020000D", {}).get("Value", [None])[0]
                if not study_uid:
                    logger.warning(f"{config.source_name}: Skipping study with missing StudyInstanceUID in QIDO response: {study}")
                    continue

                logger.info(f"{config.source_name}: Processing Study UID: {study_uid}")
                try:
                    # Query all instances for the current study
                    # Pass StudyInstanceUID to narrow the search at the instance level
                    instances = client.query_qido(config, level="INSTANCE", custom_params={"StudyInstanceUID": study_uid})
                    logger.info(f"{config.source_name}: Found {len(instances)} instances for study {study_uid}.")

                    for instance in instances:
                        series_uid = instance.get("0020000E", {}).get("Value", [None])[0]
                        instance_uid = instance.get("00080018", {}).get("Value", [None])[0]
                        # Attempt to get Instance Creation DateTime for timestamp tracking
                        instance_dt_str = instance.get("00080015", {}).get("Value", [None])[0]

                        if not series_uid or not instance_uid:
                             logger.warning(f"{config.source_name}: Skipping instance with missing Series or SOP Instance UID: Study={study_uid}, InstanceData={instance}")
                             continue

                        # Check if this instance was already processed based on timestamp
                        instance_datetime: Optional[datetime] = None
                        if instance_dt_str:
                             try:
                                 # Parse DICOM DT format (YYYYMMDDHHMMSS.FFFFFF)
                                 # Assume UTC if no timezone offset present
                                 dt_part = instance_dt_str.split('+')[0].split('-')[0].split('&')[0] # Handle potential offsets/separators
                                 dt_format = '%Y%m%d%H%M%S'
                                 if '.' in dt_part:
                                     dt_format += '.%f'
                                 instance_datetime = datetime.strptime(dt_part, dt_format)
                                 instance_datetime = instance_datetime.replace(tzinfo=timezone.utc) # Assume UTC
                                 all_processed_timestamps.append(instance_datetime)

                                 # Compare with the source's last known processed timestamp
                                 if config.last_processed_timestamp and instance_datetime <= config.last_processed_timestamp:
                                      logger.debug(f"{config.source_name}: Skipping instance {instance_uid} (Timestamp {instance_datetime.isoformat()} <= Last Processed {config.last_processed_timestamp.isoformat()})")
                                      continue # Skip already processed instance
                             except (ValueError, TypeError) as dt_err:
                                 logger.warning(f"{config.source_name}: Could not parse Instance Creation DateTime '{instance_dt_str}' for {instance_uid}. Error: {dt_err}. Will process anyway.")
                                 # Use current time if timestamp is invalid/missing, ensuring it gets processed
                                 all_processed_timestamps.append(datetime.now(timezone.utc))
                        else:
                            logger.warning(f"{config.source_name}: Instance {instance_uid} missing InstanceCreationDateTime (0008,0015). Cannot use timestamp check. Will process.")
                            all_processed_timestamps.append(datetime.now(timezone.utc))


                        # Queue the task to fetch full metadata and process the instance
                        logger.info(f"{config.source_name}: Queueing instance {instance_uid} for metadata processing.")
                        process_dicomweb_metadata_task.delay(
                            source_id=config.id,
                            study_uid=study_uid,
                            series_uid=series_uid,
                            instance_uid=instance_uid
                        )
                        processed_instance_count += 1

                except client.DicomWebClientError as qido_inst_err:
                    # Log error querying instances for *this* study, but continue polling other studies
                    logger.error(f"{config.source_name}: Failed to query instances for study {study_uid}: {qido_inst_err}")
                except Exception as inst_err:
                     # Log unexpected error processing instances for *this* study, continue polling
                     logger.error(f"{config.source_name}: Unexpected error processing instances for study {study_uid}: {inst_err}", exc_info=True)

            # After iterating through all studies, find the latest timestamp processed in this batch
            if all_processed_timestamps:
                last_processed_in_batch = max(all_processed_timestamps)
                logger.info(f"{config.source_name}: Latest instance timestamp processed in this batch: {last_processed_in_batch.isoformat()}")

            # Mark the overall poll run as successful (even if individual instance lookups failed)
            last_successful_run_for_update = datetime.now(timezone.utc)


        # Update the source state in the database after the poll cycle
        logger.info(f"{config.source_name}: Poll cycle finished. Processed {processed_instance_count} new instances. Last timestamp in batch: {last_processed_in_batch.isoformat() if last_processed_in_batch else 'N/A'}.")
        update_payload = {
            "last_successful_run": last_successful_run_for_update,
            "last_error_message": None, # Clear previous error on successful run
            "last_error_run": None,
        }
        # Only advance last_processed_timestamp if a newer timestamp was found in this batch
        if last_processed_in_batch and (not config.last_processed_timestamp or last_processed_in_batch > config.last_processed_timestamp):
             update_payload["last_processed_timestamp"] = last_processed_in_batch
             logger.info(f"{config.source_name}: Advancing last_processed_timestamp to {last_processed_in_batch.isoformat()}")
        elif last_processed_in_batch:
             logger.info(f"{config.source_name}: Not advancing last_processed_timestamp ({last_processed_in_batch.isoformat()} is not newer than existing {config.last_processed_timestamp.isoformat() if config.last_processed_timestamp else 'None'})")

        crud_state_updater.update_run_status(db, source_name=config.source_name, **update_payload)
        db.commit() # Commit the successful state update

    except client.DicomWebClientError as client_err:
         # Let the main task loop handle logging and DB state update for client errors
         logger.error(f"{config.source_name}: DICOMweb client error during polling: {client_err}", exc_info=False)
         raise client_err # Re-raise to be caught by poll_all_dicomweb_sources
    except Exception as poll_err:
         # Let the main task loop handle logging and DB state update for unexpected errors
         logger.error(f"{config.source_name}: Unexpected error during poll_source function: {poll_err}", exc_info=True)
         raise poll_err # Re-raise to be caught by poll_all_dicomweb_sources
