# app/worker/executors/ghc_executor.py

import asyncio # Crucial for this async function
from pathlib import Path # Though not directly used for file paths by GHC task itself for input
from typing import Optional, List, Dict, Any, Union, Tuple, cast
import uuid
from datetime import datetime, timezone

import anyio.abc
import pydicom
from pydicom.errors import InvalidDicomError # Though less likely here, good for consistency
from pydicom.dataset import Dataset, FileMetaDataset # For dataset manipulation
from sqlalchemy.orm import Session
import structlog
from structlog.stdlib import BoundLogger as StdlibBoundLogger

from app.db import models as db_models
from app import crud
from app.schemas.enums import ProcessedStudySourceType, ExceptionProcessingStage, ExceptionStatus
from app.db.models.storage_backend_config import StorageBackendConfig as DBStorageBackendConfigModel
from app.services.storage_backends import get_storage_backend, StorageBackendError
# Specific to GHC
from app.services.storage_backends.google_healthcare import GoogleHealthcareDicomStoreStorage
from app.core.config import settings

from app.worker.processing_orchestrator import process_instance_against_rules
from app.worker.task_utils import (
    build_storage_backend_config_dict,
    create_exception_log_entry,
    _serialize_result,
    _safe_get_dicom_value
)
from app.worker.task_utils import EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS

async def execute_ghc_task(
    task_context_log: StdlibBoundLogger,
    db_session: Session,
    source_id: int,
    study_uid: str,
    task_id: str,
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset], Optional[str], str]:
    log: StdlibBoundLogger = task_context_log.bind(ghc_exec_source_id=source_id, ghc_exec_study_uid=study_uid)
    log.info("Executor started: execute_ghc_task")

    instance_uid_res: Optional[str] = f"GHC_UNKNOWN_SOP_FOR_STUDY_{study_uid}"
    source_name_res = f"GHC_src_exec_{source_id}"
    original_ds: Optional[pydicom.Dataset] = None
    source_type_enum_for_exc_log = ProcessedStudySourceType.GOOGLE_HEALTHCARE

    source_config_model = await asyncio.to_thread(crud.google_healthcare_source.get, db_session, id=source_id)
    if not source_config_model:
        log.error("GHC source config not found.")
        await asyncio.to_thread(
            create_exception_log_entry,
            db=db_session, exc=ValueError(f"GHC Source config ID {source_id} not found."),
            processing_stage=ExceptionProcessingStage.INGESTION,
            original_source_type=source_type_enum_for_exc_log,
            original_source_identifier=f"GHC_Source_ID_{source_id}",
            study_instance_uid_str=study_uid,
            celery_task_id=task_id, initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED,
            retryable=False, commit_on_success=True
        )
        raise ValueError(f"GHC Source config ID {source_id} not found.")
    source_name_res = source_config_model.name
    # Rebind log with the correct source_identifier_for_matching
    log: StdlibBoundLogger = task_context_log.bind(
        ghc_exec_source_id=source_id, ghc_exec_study_uid=study_uid, # Keep original context
        source_identifier_for_matching=source_name_res # Add the resolved name
    )


    try:
        # ... (GHC metadata fetch logic - UNCHANGED, assume original_ds and instance_uid_res are populated) ...
        backend_config_dict = {
            "type": "google_healthcare", "name": f"GHC_Fetcher_{source_config_model.name}",
            "gcp_project_id": source_config_model.gcp_project_id, "gcp_location": source_config_model.gcp_location,
            "gcp_dataset_id": source_config_model.gcp_dataset_id, "gcp_dicom_store_id": source_config_model.gcp_dicom_store_id,
        }
        ghc_fetch_backend = GoogleHealthcareDicomStoreStorage(config=backend_config_dict)
        series_list = await asyncio.to_thread(ghc_fetch_backend.search_series, study_instance_uid=study_uid, limit=1) # type: ignore
        if not series_list or not series_list[0].get("0020000E", {}).get("Value"):
            log.info("No series found for GHC study in executor.") # type: ignore
            return False, False, "success_no_series_ghc", "No series in GHC study", [], {}, None, instance_uid_res, source_name_res
        first_series_uid = series_list[0]["0020000E"]["Value"][0]
        instance_metadata_list = await asyncio.to_thread(
            ghc_fetch_backend.search_instances, # type: ignore
            study_instance_uid=study_uid, series_instance_uid=first_series_uid, limit=1
        )
        if not instance_metadata_list or not instance_metadata_list[0].get("00080018", {}).get("Value"):
            log.info("No instance metadata found for GHC series in executor.") # type: ignore
            return False, False, "success_no_instances_ghc", "No instances", [], {}, None, instance_uid_res, source_name_res
        instance_metadata_json = instance_metadata_list[0]
        temp_instance_uid_res = instance_metadata_json["00080018"]["Value"][0]
        if temp_instance_uid_res: instance_uid_res = temp_instance_uid_res
        log = log.bind(instance_uid=instance_uid_res) # Rebind log with actual instance UID
        original_ds = pydicom.Dataset.from_json(instance_metadata_json)
        if not hasattr(original_ds, 'file_meta') or not original_ds.file_meta:
            fm = FileMetaDataset()
            fm.MediaStorageSOPClassUID = original_ds.SOPClassUID # type: ignore
            fm.MediaStorageSOPInstanceUID = original_ds.SOPInstanceUID # type: ignore
            fm.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian # type: ignore
            original_ds.file_meta = fm
    # ... (exception handling for fetch - UNCHANGED) ...
    except StorageBackendError as ghc_fetch_err:
        log.warning("GHC fetch error during metadata retrieval.", error_msg=str(ghc_fetch_err)) # type: ignore
        await asyncio.to_thread(
            create_exception_log_entry,
            db=db_session, exc=ghc_fetch_err, processing_stage=ExceptionProcessingStage.INGESTION,
            original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
            study_instance_uid_str=study_uid, celery_task_id=task_id, dataset=None,
            initial_status=ExceptionStatus.RETRY_PENDING, retryable=True, commit_on_success=True
        )
        raise
    except Exception as e:
        log.error("Unexpected error fetching/parsing GHC metadata.", error_msg=str(e), exc_info=True) # type: ignore
        await asyncio.to_thread(
            create_exception_log_entry,
            db=db_session, exc=e, processing_stage=ExceptionProcessingStage.INGESTION,
            original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
            study_instance_uid_str=study_uid, celery_task_id=task_id, dataset=None,
            initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
        )
        raise


    if original_ds is None:
        # ... (error handling for original_ds being None - UNCHANGED) ...
        log.error("original_ds is None before GHC rule processing (after fetch attempt).") # type: ignore
        await asyncio.to_thread(
            create_exception_log_entry,
            db=db_session, exc=RuntimeError("Dataset is None after GHC fetch attempt"),
            processing_stage=ExceptionProcessingStage.INGESTION, dataset=None,
            original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
            study_instance_uid_str=study_uid, celery_task_id=task_id,
            initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
        )
        return False, False, "error_ghc_no_dataset", "Dataset not loaded", [], {}, None, instance_uid_res, source_name_res


    active_rulesets_list = await asyncio.to_thread(crud.ruleset.get_active_ordered, db_session)
    processed_ds_rules, applied_rules, unique_dest_dicts = await asyncio.to_thread(
        process_instance_against_rules, original_ds, active_rulesets_list,
        source_name_res, db_session, None, ai_portal
    )

    modifications_made = (processed_ds_rules is not original_ds and processed_ds_rules is not None)
    rules_matched_triggered = bool(applied_rules or unique_dest_dicts)
    dataset_to_send = processed_ds_rules if processed_ds_rules else original_ds

    if not rules_matched_triggered:
        # ... (return logic - UNCHANGED) ...
        return rules_matched_triggered, modifications_made, "success_no_matching_rules_ghc", \
               f"No matching rules for GHC {source_name_res}.", applied_rules, {}, \
               dataset_to_send, instance_uid_res, source_name_res


    final_dest_statuses = {}
    all_dest_ok = True

    if not unique_dest_dicts:
        # ... (return logic - UNCHANGED) ...
         return rules_matched_triggered, modifications_made, "success_no_destinations_ghc", \
               "Rules matched, no destinations.", applied_rules, {}, \
               dataset_to_send, instance_uid_res, source_name_res

    log.info(f"Processing {len(unique_dest_dicts)} destinations for GHC instance {instance_uid_res}.") # type: ignore

    for i, dest_info in enumerate(unique_dest_dicts):
        dest_id = cast(Optional[int], dest_info.get("id"))
        # --- CORRECTED: Use dest_name for logging context BEFORE dest_log is rebound ---
        dest_name = cast(str, dest_info.get("name", f"UnknownDest_ID_{dest_id}"))
        # Bind dest_log for THIS iteration, using the correct dest_name
        dest_log_iter: StdlibBoundLogger = log.bind(dest_idx=i + 1, dest_id_loop=dest_id, dest_name_loop=dest_name)


        if dest_id is None:
            final_dest_statuses[f"MalformedDest_GHC_{i+1}"] = {"status": "error", "message": "Missing ID"}
            all_dest_ok = False; continue

        db_model_sync: Optional[DBStorageBackendConfigModel] = await asyncio.to_thread(crud.crud_storage_backend_config.get, db_session, id=dest_id)
        if not db_model_sync:
            dest_log_iter.warning("GHC Destination DB config not found.") # Use dest_log_iter
            final_dest_statuses[dest_name] = {"status": "error", "message": "DB config not found"}
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=ValueError(f"Dest config ID {dest_id} ({dest_name}) not found."), # Use dest_name
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=dataset_to_send,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=None, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue
        if not db_model_sync.is_enabled:
            final_dest_statuses[dest_name] = {"status": "skipped_disabled", "message": "Disabled"}
            continue

        actual_storage_config = build_storage_backend_config_dict(db_model_sync, task_id)
        if not actual_storage_config:
            dest_log_iter.warning("Failed to build GHC destination storage config.") # Use dest_log_iter
            final_dest_statuses[dest_name] = {"status": "error", "message": "Failed to build storage config"}
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=RuntimeError(f"Failed to build GHC dest config '{dest_name}' (ID: {dest_id})."), # Use dest_name
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=dataset_to_send,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=db_model_sync, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue

        if dataset_to_send is None: # Defensive
            dest_log_iter.critical("dataset_to_send is None unexpectedly before GHC store attempt.") # Use dest_log_iter
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=RuntimeError(f"Critical: dataset_to_send is None for GHC dest '{dest_name}'"), # Use dest_name
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=None,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=db_model_sync, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue

        storage_backend_instance = get_storage_backend(actual_storage_config)
        filename_ctx_loop = f"{instance_uid_res}.dcm" if instance_uid_res else f"UNKNOWN_SOP_GHC_{uuid.uuid4().hex[:8]}.dcm"

        try:
            store_result: Any
            store_result = await asyncio.to_thread( # GHC store is sync
                storage_backend_instance.store,
                dataset_to_send, None, filename_ctx_loop, source_name_res
            )
            status_key = "duplicate" if store_result == "duplicate" else "success"
            final_dest_statuses[dest_name] = {"status": status_key, "result": _serialize_result(store_result)} # Use dest_name
            dest_log_iter.info(f"Store to GHC dest {dest_name} reported: {status_key}") # Use dest_log_iter and dest_name
        except StorageBackendError as sbe:
            # ... (Staging logic and create_exception_log_entry call as corrected previously, using dest_log_iter and dest_name)
            dest_log_iter.warning(f"StorageBackendError for GHC dest {dest_name}", error_msg=str(sbe)) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": str(sbe)}
            all_dest_ok = False
            is_sbe_retryable = any(isinstance(sbe, retryable_exc_type) for retryable_exc_type in EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS) # type: ignore
            staged_filepath_for_retry: Optional[str] = None
            if is_sbe_retryable and dataset_to_send:
                try:
                    def _save_to_stage_sync_ghc(): # Renamed for clarity
                        sop_uid_for_stage = _safe_get_dicom_value(dataset_to_send, "SOPInstanceUID", f"NO_SOP_UID_GHC_{uuid.uuid4().hex[:8]}")
                        timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')
                        staged_filename = f"retry_ghc_{task_id}_{sop_uid_for_stage}_{dest_id}_{timestamp_str}.dcm"
                        settings.DICOM_RETRY_STAGING_PATH.mkdir(parents=True, exist_ok=True)
                        staged_file_full_path = settings.DICOM_RETRY_STAGING_PATH / staged_filename
                        dataset_to_send.save_as(str(staged_file_full_path), write_like_original=False) # type: ignore
                        return str(staged_file_full_path)
                    staged_filepath_for_retry = await asyncio.to_thread(_save_to_stage_sync_ghc)
                    dest_log_iter.info("Saved processed GHC dataset to staging for retry.", staged_path=staged_filepath_for_retry) # type: ignore
                except Exception as stage_save_err:
                    dest_log_iter.error("Failed to save processed GHC dataset to staging for retry!", error=str(stage_save_err), exc_info=True) # type: ignore
                    is_sbe_retryable = False
                    staged_filepath_for_retry = None
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=sbe, processing_stage=ExceptionProcessingStage.DESTINATION_SEND,
                dataset=dataset_to_send, failed_filepath=staged_filepath_for_retry,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=db_model_sync, celery_task_id=task_id,
                initial_status=ExceptionStatus.RETRY_PENDING if is_sbe_retryable else ExceptionStatus.MANUAL_REVIEW_REQUIRED,
                retryable=is_sbe_retryable, commit_on_success=True
            )
        except Exception as e:
            # ... (Generic exception logging using dest_log_iter and dest_name) ...
            dest_log_iter.error(f"Unexpected error for GHC dest {dest_name}", error_msg=str(e), exc_info=True) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": f"Unexpected: {e}"}
            all_dest_ok = False
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=e, processing_stage=ExceptionProcessingStage.DESTINATION_SEND,
                dataset=dataset_to_send, failed_filepath=None,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=db_model_sync, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )

    status_code_res = "success_all_destinations_ghc" if all_dest_ok else "partial_failure_destinations_ghc"
    msg_res = f"GHC processing complete for study {study_uid}. All destinations OK: {all_dest_ok}."

    try:
        study_logged_successfully = await asyncio.to_thread(
            crud.google_healthcare_source.log_processed_study,
            db_session, source_id=source_id, study_uid=study_uid
        )
        if study_logged_successfully:
            await asyncio.to_thread(db_session.commit)
            log.info("GHC processed study logged successfully.", ghc_source_id=source_id, study_uid_processed=study_uid) # type: ignore
        else:
            log.info("GHC processed study was already logged or logging failed.", ghc_source_id=source_id, study_uid_processed=study_uid) # type: ignore
    except Exception as e_inc:
        log.error("DB error during GHC processed study logging.", error_msg=str(e_inc), exc_info=True) # type: ignore
        if db_session.is_active: await asyncio.to_thread(db_session.rollback)

    # --- CORRECTED RETURN TUPLE ORDER ---
    return (
        rules_matched_triggered,
        modifications_made,
        status_code_res,
        msg_res,
        applied_rules,          # List[str]
        final_dest_statuses,    # Dict[str, Dict[str, Any]]
        dataset_to_send,        # Optional[pydicom.Dataset]
        instance_uid_res,       # Optional[str]
        source_name_res         # str
    )