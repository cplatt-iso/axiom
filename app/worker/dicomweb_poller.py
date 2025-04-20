# app/worker/dicomweb_poller.py

import logging
import json # Need json for logging dicts potentially
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List

from celery import shared_task
from pydicom.uid import generate_uid # Although not strictly needed here anymore
from celery.utils.log import get_task_logger

# Database and CRUD
from app.db.session import SessionLocal
from app.crud import dicomweb_state # Assuming this uses the CRUD object instance

# Settings and DICOMweb client
from app.core.config import settings, DicomWebSourceConfig
from app.services.dicomweb_client import query_qido, retrieve_instance_metadata, DicomWebClientError

# Import the task that will actually process the retrieved metadata
from app.worker.tasks import process_dicomweb_metadata_task

# Get task-specific logger
logger = get_task_logger(__name__)

# How far back to look on the very first run if no state exists
INITIAL_LOOKBACK_DAYS = 3 # Adjusted based on previous testing
# How much to overlap queries to avoid missing items due to timing
QUERY_OVERLAP_SECONDS = 60


def _parse_dicom_datetime(date_str: Optional[str], time_str: Optional[str]) -> Optional[datetime]:
    """Helper to parse DICOM date and time into a timezone-aware UTC datetime."""
    if not date_str: return None
    try:
        dt_part = date_str
        tm_part = time_str.split('.')[0] if time_str else "000000" # Handle missing/fractional time
        if len(tm_part) < 6: tm_part = tm_part.ljust(6, '0') # Pad if needed
        ts = datetime.strptime(f"{dt_part}{tm_part}", '%Y%m%d%H%M%S')
        # Assume UTC if no timezone info is available in DICOM tags
        return ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        # Fallback to date only if possible
        try:
            ts = datetime.strptime(date_str, '%Y%m%d')
            return ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
             logger.warning(f"Could not parse StudyDate/Time '{date_str}'/'{time_str}'.")
             return None

@shared_task(name="poll_all_dicomweb_sources")
def poll_all_dicomweb_sources_task():
    """
    Iterates through configured DICOMweb sources and triggers polling for each enabled one.
    Intended to be run periodically by Celery Beat.
    """
    logger.info("Starting DICOMweb polling cycle...")
    active_source_configs: list[DicomWebSourceConfig] = [
        src for src in settings.DICOMWEB_SOURCES if src.is_enabled
    ]

    if not active_source_configs:
        logger.info("No enabled DICOMweb sources configured. Skipping poll cycle.")
        return {"status": "no_enabled_sources"}

    logger.info(f"Found {len(active_source_configs)} enabled DICOMweb sources to check.")

    db = SessionLocal()
    try:
        for source_config in active_source_configs:
            source_name = source_config.name
            try:
                # Check if this source is due for polling based on its interval and last run
                db_state = dicomweb_state.get(db, source_name=source_name)

                # Initialize state if it doesn't exist
                if not db_state:
                    logger.info(f"Initializing state for new DICOMweb source: {source_name}")
                    db_state = dicomweb_state.create_or_update(db, source_name=source_name, is_enabled=source_config.is_enabled)
                    db.commit() # Commit initial state creation explicitly here
                    logger.info(f"State created for {source_name}. Will poll on next suitable cycle.")
                    continue # Skip to next source config, poll this one on the next beat cycle

                if not db_state.is_enabled:
                    logger.debug(f"Skipping poll for {source_name}: disabled in database state.")
                    continue

                # Check polling interval
                now = datetime.now(timezone.utc)
                interval_seconds = source_config.polling_interval_seconds
                # Consider last attempt time (success or error) to prevent rapid retries on error
                last_run = db_state.last_successful_run or db_state.last_error_run

                if last_run and (now - last_run) < timedelta(seconds=interval_seconds):
                     logger.debug(f"Skipping poll for {source_name}: interval ({interval_seconds}s) not yet passed since last run at {last_run}.")
                     continue

                logger.info(f"Polling DICOMweb source: {source_name}")

                # --- Perform Polling Logic ---
                last_processed_ts = db_state.last_processed_timestamp

                if last_processed_ts:
                    start_dt = last_processed_ts - timedelta(seconds=QUERY_OVERLAP_SECONDS)
                    logger.info(f"{source_name}: Querying from {start_dt} (last processed: {last_processed_ts})")
                else:
                    start_dt = now - timedelta(days=INITIAL_LOOKBACK_DAYS)
                    logger.info(f"{source_name}: First run or no previous timestamp. Querying from {start_dt}.")
                end_dt = now # Query up to 'now'

                # Use StudyDate range for initial query
                query_start_date = start_dt.strftime('%Y%m%d')
                query_end_date = end_dt.strftime('%Y%m%d')
                study_date_range = f"{query_start_date}-{query_end_date}"
                study_query_params = {"StudyDate": study_date_range, "limit": 5000}

                logger.debug(f"{source_name}: Querying studies with params: {study_query_params}")
                studies: List[Dict[str, Any]] = query_qido(source_config, level="STUDY", custom_params=study_query_params)

                new_instances_found = 0
                latest_study_ts_in_batch: Optional[datetime] = None

                logger.debug(f"{source_name}: Processing {len(studies)} studies found by initial query.")
                for study_detail in studies:
                    # Ensure study_detail is a dictionary before proceeding
                    if not isinstance(study_detail, dict):
                         logger.warning(f"{source_name}: Study result item is not a dictionary: {study_detail}. Skipping.")
                         continue

                    try: # Wrap processing for a single study entry
                        study_uid = study_detail.get("0020000D", {}).get("Value", [None])[0]
                        study_dt_str = study_detail.get("00080020", {}).get("Value", [None])[0]
                        study_tm_str = study_detail.get("00080030", {}).get("Value", [None])[0]
                    except AttributeError as e:
                        logger.error(f"{source_name}: Error accessing keys in study detail dictionary: {study_detail}. Error: {e}. Skipping study.")
                        continue # Skip this malformed study entry

                    if not study_uid:
                        logger.warning(f"{source_name}: Study detail dictionary missing StudyInstanceUID. Skipping study.")
                        continue

                    study_ts = _parse_dicom_datetime(study_dt_str, study_tm_str)

                    # Update latest timestamp seen in this batch
                    if study_ts:
                         if latest_study_ts_in_batch is None or study_ts > latest_study_ts_in_batch:
                             latest_study_ts_in_batch = study_ts
                    else:
                         # If timestamp cannot be parsed, we cannot reliably filter based on time
                         logger.warning(f"{source_name}: Could not determine timestamp for Study {study_uid}. Cannot use time filter.")
                         # If we have a previous timestamp, we MUST skip to avoid reprocessing old data indefinitely
                         if last_processed_ts:
                              logger.warning(f"{source_name}: Skipping study {study_uid} due to unparsable timestamp and existing last_processed_ts.")
                              continue
                         # Otherwise (first run), process it anyway but don't update latest_study_ts_in_batch

                    # Skip study if timestamp is not newer than last processed
                    if study_ts and last_processed_ts and study_ts <= last_processed_ts:
                         logger.debug(f"{source_name}: Skipping study {study_uid} (Timestamp {study_ts}): not newer than last processed ({last_processed_ts}).")
                         continue

                    logger.info(f"{source_name}: Processing potentially new study {study_uid} (Timestamp {study_ts or 'Unknown'}).")

                    # --- Query for Instances within the Study ---
                    try:
                        logger.debug(f"{source_name}: Querying instances for study {study_uid}...")
                        instance_query_params = {"StudyInstanceUID": study_uid} # Query ONLY by Study UID
                        instances: List[Dict[str, Any]] = query_qido(source_config, level="INSTANCE", custom_params=instance_query_params)
                        logger.debug(f"{source_name}: Found {len(instances)} instances for study {study_uid}.")

                        for instance in instances:
                            if not isinstance(instance, dict):
                                 logger.warning(f"{source_name}: Instance result is not a dictionary: {instance}. Skipping instance.")
                                 continue

                            instance_uid = instance.get("00080018", {}).get("Value", [None])[0]
                            series_uid = instance.get("0020000E", {}).get("Value", [None])[0]

                            if not instance_uid or not series_uid:
                                logger.warning(f"{source_name}: Instance result missing SOP/Series UID in Study {study_uid}. Instance: {instance}. Skipping.")
                                continue

                            # --- Retrieve Metadata ---
                            logger.debug(f"{source_name}: Retrieving metadata for instance {instance_uid}...")
                            metadata = retrieve_instance_metadata(source_config, study_uid, series_uid, instance_uid)

                            if metadata:
                                logger.info(f"{source_name}: Found instance {instance_uid}. Submitting for processing.")
                                new_instances_found += 1
                                # --- Submit to processing task ---
                                try:
                                    # Convert keys from "GGGG,EEEE" to "GGGGeeee" for from_json
                                    # compact_metadata = {
                                    #    tag.replace(',', ''): value
                                    #    for tag, value in metadata.items()
                                        # Basic check to avoid non-tag keys if any exist
                                    #    if isinstance(tag, str) and len(tag) == 9 and tag[4] == ','
                                    #}
                                    #if not compact_metadata:
                                    #     logger.error(f"{source_name}: Failed to convert metadata keys to compact format for instance {instance_uid}. Original metadata: {metadata}")
                                    #     continue # Skip if conversion failed

                                    process_dicomweb_metadata_task.delay(
                                        source_identifier=source_name,
                                        dicom_metadata=metadata 
                                    )
                                except Exception as dispatch_err:
                                     logger.error(f"{source_name}: Failed to dispatch processing task for instance {instance_uid}: {dispatch_err}", exc_info=True)
                                     # Don't raise here, just log and continue; handle overall poll state later
                            else:
                                 logger.warning(f"{source_name}: Failed to retrieve metadata for instance {instance_uid}.")

                        # --- End instance loop ---

                    except DicomWebClientError as qido_inst_err:
                         logger.error(f"{source_name}: Failed to query/process instances for study {study_uid}: {qido_inst_err}")
                         # Log error but continue to next study to avoid getting stuck
                         pass # Error logged, state handled after loop

                    # --- End processing single study ---

                # --- Update State After Processing Batch ---
                update_kwargs = {"last_successful_run": now, "last_error_message": None, "last_error_run": None}
                if latest_study_ts_in_batch:
                    current_last_ts = db_state.last_processed_timestamp
                    if current_last_ts is None or latest_study_ts_in_batch > current_last_ts:
                         logger.info(f"{source_name}: Poll cycle finished. Processed {new_instances_found} new instances. Updating last processed timestamp to {latest_study_ts_in_batch}.")
                         update_kwargs["last_processed_timestamp"] = latest_study_ts_in_batch
                    else:
                         logger.info(f"{source_name}: Poll cycle finished. Processed {new_instances_found} new instances. Latest timestamp {latest_study_ts_in_batch} is not newer than stored {current_last_ts}. Not advancing timestamp.")
                else:
                     # No new studies with valid timestamps found/processed in this run
                     logger.info(f"{source_name}: Poll cycle finished. Processed {new_instances_found} new instances. No valid timestamp found in batch to advance state. Last processed TS remains {last_processed_ts}.")
                     # Only update last_successful_run

                dicomweb_state.update_run_status(db, source_name=source_name, **update_kwargs)
                db.commit() # Commit successful state update for this source

            except DicomWebClientError as poll_err:
                logger.error(f"Polling failed for {source_name}: {poll_err}", exc_info=False)
                try:
                    dicomweb_state.update_run_status(db, source_name=source_name, last_error_run=now, last_error_message=str(poll_err))
                    db.commit() # Commit error state
                except Exception as db_err:
                    logger.error(f"Failed to update error state for {source_name} after polling error: {db_err}", exc_info=True)
                    db.rollback() # Rollback DB state update attempt on secondary failure
            except Exception as e:
                logger.error(f"Unexpected error during polling for {source_name}: {e}", exc_info=True)
                try:
                    dicomweb_state.update_run_status(db, source_name=source_name, last_error_run=now, last_error_message=f"Unexpected error: {e}")
                    db.commit() # Commit error state
                except Exception as db_err:
                    logger.error(f"Failed to update error state for {source_name} after unexpected error: {db_err}", exc_info=True)
                    db.rollback()

            # Loop continues to the next source config
            # --- End processing for one source ---

    except Exception as outer_exc:
         logger.error(f"Error in outer DICOMweb polling loop: {outer_exc}", exc_info=True)
         db.rollback() # Rollback any potential uncommitted changes
    finally:
        logger.info("DICOMweb polling cycle finished.")
        db.close()

    return {"status": "cycle_complete"}
