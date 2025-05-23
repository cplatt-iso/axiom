# app/worker/google_healthcare_poller.py

import asyncio
from typing import Dict, Any, Optional, List

try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

from celery import shared_task
from app.db.session import SessionLocal
from app import crud
from app.services.storage_backends.google_healthcare import GoogleHealthcareDicomStoreStorage, StorageBackendError
from app.db.models.processed_study_log import ProcessedStudyLog
from app.schemas.enums import ProcessedStudySourceType
from app.db.models.google_healthcare_source import GoogleHealthcareSource
from app.worker.tasks import process_google_healthcare_metadata_task


@shared_task(acks_late=True, name="poll_google_healthcare_source")
async def poll_google_healthcare_source(source_id: int):
    """
    Celery task to poll a single configured Google Healthcare DICOM store for new studies,
    check against processed log, and trigger metadata processing for new studies.
    """
    log = logger.bind(source_id=source_id, source_type=ProcessedStudySourceType.GOOGLE_HEALTHCARE.value) # type: ignore[attr-defined]
    log.info("Starting Google Healthcare poll task")

    db = SessionLocal()
    if not db:
        log.error("Failed to get database session.")
        return

    source_config: Optional[GoogleHealthcareSource] = None
    try:
        source_config = crud.google_healthcare_source.get(db, id=source_id)

        if not source_config:
            log.warning("Source configuration not found.")
            db.close()
            return
        if not source_config.is_enabled or not source_config.is_active:
            log.info("Source is not enabled or not active, skipping poll.")
            db.close()
            return

        log = log.bind(source_name=source_config.name) # type: ignore[attr-defined]
        log.info("Found active source configuration, proceeding with poll.")

        backend_config_dict: Dict[str, Any] = {
            "type": "google_healthcare",
            "name": f"Poller_{source_config.name}",
            "gcp_project_id": source_config.gcp_project_id,
            "gcp_location": source_config.gcp_location,
            "gcp_dataset_id": source_config.gcp_dataset_id,
            "gcp_dicom_store_id": source_config.gcp_dicom_store_id,
        }

        ghc_backend: Optional[GoogleHealthcareDicomStoreStorage] = None
        try:
            ghc_backend = GoogleHealthcareDicomStoreStorage(config=backend_config_dict)
            # await ghc_backend.initialize_client() # REMOVED: initialize_client does not exist on sync backend

            query_params = source_config.query_filters if isinstance(source_config.query_filters, dict) else None
            log.debug("Using query filters", filters=query_params)

            found_studies_raw: List[Dict[str, Any]] = await asyncio.to_thread( # MODIFIED: Use asyncio.to_thread
                ghc_backend.search_studies,
                query_params=query_params,
                limit=1000
            )

            study_uids_found: List[str] = [
                 study.get("0020000D", {}).get("Value", [None])[0]
                 for study in found_studies_raw
                 if study.get("0020000D", {}).get("Value", [None])[0] is not None
            ]
            log = log.bind(studies_found_count=len(study_uids_found)) # type: ignore[attr-defined]
            log.info(f"Poll query found {len(study_uids_found)} studies.")

            if not study_uids_found:
                log.debug("No studies returned by query.")
                db.close()
                return

            log.debug("Checking found studies against processed log...")
            processed_uids = crud.crud_processed_study_log.get_processed_study_uids_for_source(
                db=db,
                source_id=str(source_id), # MODIFIED: Cast to str
                source_type=ProcessedStudySourceType.GOOGLE_HEALTHCARE,
                study_uids_to_check=study_uids_found
            )
            processed_uids_set = set(processed_uids)
            log.debug(f"Found {len(processed_uids_set)} already processed studies in log.")

            new_study_uids = [uid for uid in study_uids_found if uid not in processed_uids_set]
            log = log.bind(new_studies_count=len(new_study_uids)) # type: ignore[attr-defined]

            if not new_study_uids:
                log.info("No new studies found after checking processed log.")
                db.close()
                return

            log.info(f"Found {len(new_study_uids)} new studies to process.", new_study_uids=new_study_uids)

            processed_count_for_log = 0
            for study_uid in new_study_uids:
                log.debug("Dispatching metadata processing task for new study", study_uid=study_uid)
                process_google_healthcare_metadata_task.delay(source_id, study_uid) # type: ignore[operator]
                try:
                    crud.crud_processed_study_log.create_log_entry( # MODIFIED: Renamed from log_processed_study
                        db=db,
                        study_instance_uid=study_uid,
                        source_id=str(source_id), # MODIFIED: Cast to str
                        source_type=ProcessedStudySourceType.GOOGLE_HEALTHCARE
                    )
                    processed_count_for_log += 1
                except Exception as log_exc:
                    log.error(
                        "Failed to log study UID to processed log after dispatching task",
                        study_uid=study_uid,
                        error=str(log_exc),
                        exc_info=True
                    )
            db.commit()
            log.info(f"Successfully logged {processed_count_for_log} new studies as processed.")


        except StorageBackendError as e:
            log.error(f"Storage backend error during poll: {e}", exc_info=True)
        except Exception as e:
            log.error(f"Unexpected error during poll task execution: {e}", exc_info=True)

    finally:
        if db:
            db.close()
            log.debug("Database session closed.")


@shared_task(name="poll_all_google_healthcare_sources")
def poll_all_google_healthcare_sources():
    """
    Celery Beat task to find all active Google Healthcare sources
    and dispatch individual polling tasks for each.
    """
    log = logger.bind(task_name="poll_all_google_healthcare_sources_scheduler") # type: ignore[attr-defined]
    log.info("Scheduler task started: Finding active Google Healthcare sources...")
    db = SessionLocal()
    active_sources: List[GoogleHealthcareSource] = []
    try:
        active_sources = crud.google_healthcare_source.get_multi_active(db)
        log.info(f"Found {len(active_sources)} active Google Healthcare source(s) to poll.")

        for source in active_sources:
            log.debug(f"Dispatching poll task for source ID: {source.id}, Name: {source.name}")
            poll_google_healthcare_source.delay(source.id) # type: ignore[operator]

    except Exception as e:
        log.error(f"Error during scheduling Google Healthcare poll tasks: {e}", exc_info=True)
    finally:
        if db:
            db.close()
    log.info(f"Finished scheduling poll tasks for {len(active_sources)} source(s).")
