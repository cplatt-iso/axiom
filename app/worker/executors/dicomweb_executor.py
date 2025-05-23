# app/worker/executors/dicomweb_executor.py

import asyncio # Crucial
from pathlib import Path # Not directly used for input files here
from typing import Optional, List, Dict, Any, Union, Tuple, cast
import uuid
from datetime import datetime, timezone

import anyio.abc
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.dataset import Dataset, FileMetaDataset
from sqlalchemy.orm import Session
import structlog
from structlog.stdlib import BoundLogger as StdlibBoundLogger

from app.db import models as db_models
from app import crud
from app.schemas.enums import ProcessedStudySourceType, ExceptionProcessingStage, ExceptionStatus
from app.db.models.storage_backend_config import StorageBackendConfig as DBStorageBackendConfigModel
from app.services.storage_backends import get_storage_backend, StorageBackendError
# Specific to DICOMweb
from app.services import dicomweb_client # For retrieve_instance_metadata
from app.core.config import settings

from app.worker.processing_orchestrator import process_instance_against_rules
from app.worker.task_utils import (
    build_storage_backend_config_dict,
    create_exception_log_entry,
    _serialize_result
)
from app.worker.task_utils import EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS


async def execute_dicomweb_task(
    task_context_log: StdlibBoundLogger,
    db_session: Session,
    source_id: int,
    study_uid: str,
    series_uid: str,
    instance_uid: str, # This is the specific instance being processed (and is str)
    task_id: str,
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset], Optional[str], str]: # Return type for instance_uid is Optional[str]
    # instance_uid is the specific one we're fetching, so it's known and str.
    # However, the *dataset* might fail to parse, so processed_ds is Optional.
    # The instance_uid_res in the return will be the instance_uid passed in.
    
    log: StdlibBoundLogger = task_context_log.bind(
        dicomweb_exec_source_id=source_id, dicomweb_exec_study_uid=study_uid,
        dicomweb_exec_series_uid=series_uid, dicomweb_exec_instance_uid=instance_uid
    )
    log.info("Executor started: execute_dicomweb_task")

    source_name_res = f"DICOMweb_src_exec_{source_id}" # Default
    original_ds: Optional[pydicom.Dataset] = None
    processed_ds_final: Optional[pydicom.Dataset] = None # To hold the dataset through processing
    source_type_enum_for_exc_log = ProcessedStudySourceType.DICOMWEB

    retrieved_config: Optional[db_models.DicomWebSourceState] = await asyncio.to_thread(crud.dicomweb_source.get, db_session, id=source_id) # type: ignore
    if not retrieved_config:
        log.error("DICOMweb source config not found.") # type: ignore
        await asyncio.to_thread(
            create_exception_log_entry,
            db=db_session, exc=ValueError(f"DICOMweb source config ID {source_id} not found."),
            processing_stage=ExceptionProcessingStage.INGESTION,
            original_source_type=source_type_enum_for_exc_log,
            original_source_identifier=f"DICOMweb_Source_ID_{source_id}",
            study_instance_uid_str=study_uid, series_instance_uid_str=series_uid, sop_instance_uid_str=instance_uid,
            celery_task_id=task_id, initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED,
            retryable=False, commit_on_success=True
        )
        raise ValueError(f"DICOMweb source config ID {source_id} not found.")
    source_config_typed: db_models.DicomWebSourceState = retrieved_config # type: ignore
    source_name_res = cast(str, source_config_typed.source_name)
    log = log.bind(source_identifier_for_matching=source_name_res) # type: ignore

    try:
        # ... (DICOMweb metadata fetch logic - UNCHANGED from your last version) ...
        # This should populate original_ds
        log.debug("Fetching instance metadata via DICOMweb.") # type: ignore
        metadata_json_list_raw = await asyncio.to_thread(
            dicomweb_client.retrieve_instance_metadata, # type: ignore
            config=source_config_typed, study_uid=study_uid, series_uid=series_uid, instance_uid=instance_uid
        )
        metadata_json_list: List[Dict[str, Any]] = []
        if metadata_json_list_raw is None: metadata_json_list = []
        elif isinstance(metadata_json_list_raw, dict): metadata_json_list = [metadata_json_list_raw]
        else: metadata_json_list = metadata_json_list_raw # type: ignore

        if not metadata_json_list:
            log.info("Instance metadata not found via DICOMweb (possibly deleted).") # type: ignore
            # This is not an "error" for exception log, just no data.
            return (False, False, "success_metadata_not_found", "Instance metadata not found.",
                    [], {}, None, instance_uid, source_name_res) # instance_uid is known here
        original_ds = pydicom.Dataset.from_json(metadata_json_list[0])
        if not hasattr(original_ds, 'file_meta') or not original_ds.file_meta:
            fm = FileMetaDataset()
            if "SOPClassUID" not in original_ds or "SOPInstanceUID" not in original_ds: # type: ignore
                log.error("SOPClassUID or SOPInstanceUID missing from DICOMweb metadata.") # type: ignore
                raise ValueError("Incomplete DICOMweb metadata for dataset construction.")
            fm.MediaStorageSOPClassUID = original_ds.SOPClassUID # type: ignore
            fm.MediaStorageSOPInstanceUID = original_ds.SOPInstanceUID # type: ignore
            fm.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian # type: ignore
            original_ds.file_meta = fm

    except dicomweb_client.DicomWebClientError as fetch_exc: # type: ignore
        # ... (Error handling for fetch_exc with corrected retryable logic and create_exception_log_entry call - UNCHANGED)
        log.warning("DICOMweb client error fetching metadata.", error_msg=str(fetch_exc)) # type: ignore
        is_retryable_fetch = True
        fetch_exc_str = str(fetch_exc).lower()
        non_retryable_keywords = ["not found", "404", "unauthorized", "401", "forbidden", "403", "bad request", "400", "invalid study uid", "invalid series uid", "invalid instance uid"]
        if any(kw in fetch_exc_str for kw in non_retryable_keywords): is_retryable_fetch = False
        elif "timeout" in fetch_exc_str or "connection" in fetch_exc_str or "service unavailable" in fetch_exc_str or "503" in fetch_exc_str: is_retryable_fetch = True
        await asyncio.to_thread(
            create_exception_log_entry,
            db=db_session, exc=fetch_exc, processing_stage=ExceptionProcessingStage.INGESTION,
            original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
            dataset=None, study_instance_uid_str=study_uid, series_instance_uid_str=series_uid, sop_instance_uid_str=instance_uid,
            celery_task_id=task_id,
            initial_status=ExceptionStatus.RETRY_PENDING if is_retryable_fetch else ExceptionStatus.MANUAL_REVIEW_REQUIRED,
            retryable=is_retryable_fetch, commit_on_success=True
        )
        raise fetch_exc
    except Exception as e:
        # ... (Generic exception handling for fetch - UNCHANGED) ...
        log.error("Failed to fetch or parse DICOMweb metadata.", error_msg=str(e), exc_info=True) # type: ignore
        await asyncio.to_thread(
            create_exception_log_entry,
            db=db_session, exc=e, processing_stage=ExceptionProcessingStage.INGESTION,
            original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
            dataset=None, study_instance_uid_str=study_uid, series_instance_uid_str=series_uid, sop_instance_uid_str=instance_uid,
            celery_task_id=task_id, initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED,
            retryable=False, commit_on_success=True
        )
        raise

    if original_ds is None:
        # ... (Error handling if original_ds is None - UNCHANGED) ...
        log.error("original_ds is None before DICOMweb rule processing.") # type: ignore
        await asyncio.to_thread(
            create_exception_log_entry,
            db=db_session, exc=RuntimeError("Dataset became None before DICOMweb rule processing"),
            processing_stage=ExceptionProcessingStage.UNKNOWN, dataset=None,
            original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
            study_instance_uid_str=study_uid, series_instance_uid_str=series_uid, sop_instance_uid_str=instance_uid, # Pass all known UIDs
            celery_task_id=task_id, initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED,
            retryable=False, commit_on_success=True
        )
        return False, False, "error_dicomweb_no_dataset", "Dataset not loaded", [], {}, None, instance_uid, source_name_res


    active_rulesets_list_dw = await asyncio.to_thread(crud.ruleset.get_active_ordered, db_session)
    processed_ds_rules, applied_rules, unique_dest_dicts = await asyncio.to_thread(
        process_instance_against_rules, original_ds, active_rulesets_list_dw,
        source_name_res, db_session, None, ai_portal
    )

    modifications_made = (processed_ds_rules is not original_ds and processed_ds_rules is not None)
    rules_matched_triggered = bool(applied_rules or unique_dest_dicts)
    dataset_to_send = processed_ds_rules if processed_ds_rules else original_ds # Ensure not None
    processed_ds_final = dataset_to_send # Assign to processed_ds_final for clarity in return

    if not rules_matched_triggered:
        # ... (Return logic, including incrementing processed count - UNCHANGED) ...
        log.info(f"No matching DICOMweb rules triggered actions for {source_name_res}, instance {instance_uid}.") # type: ignore
        try:
            inc_res = await asyncio.to_thread(crud.dicomweb_state.increment_processed_count, db_session, source_name=source_name_res, count=1) # type: ignore
            if inc_res: await asyncio.to_thread(db_session.commit)
        except Exception as e_inc: log.error("Failed to inc processed count (no match DW)", error=str(e_inc)) # type: ignore
        return (rules_matched_triggered, modifications_made, "success_no_matching_rules_dicomweb",
               f"No matching rules for DICOMweb {source_name_res}.", applied_rules, {},
               processed_ds_final, instance_uid, source_name_res)


    final_dest_statuses: Dict[str, Dict[str, Any]] = {}
    all_dest_ok = True

    if not unique_dest_dicts:
        # ... (Return logic, including incrementing processed count - UNCHANGED) ...
         log.info("DICOMweb rules matched, but no destinations configured.") # type: ignore
         try:
            inc_res = await asyncio.to_thread(crud.dicomweb_state.increment_processed_count, db_session, source_name=source_name_res, count=1) # type: ignore
            if inc_res: await asyncio.to_thread(db_session.commit)
         except Exception as e_inc: log.error("Failed to inc processed count (no dest DW)", error=str(e_inc)) # type: ignore
         return (rules_matched_triggered, modifications_made, "success_no_destinations_dicomweb",
               "Rules matched, no destinations.", applied_rules, {},
               processed_ds_final, instance_uid, source_name_res)

    log.info(f"Processing {len(unique_dest_dicts)} destinations for DICOMweb instance {instance_uid}.") # type: ignore

    for i, dest_info in enumerate(unique_dest_dicts):
        dest_id = cast(Optional[int], dest_info.get("id"))
        # --- CORRECTED: Use dest_name consistently for logging context ---
        dest_name = cast(str, dest_info.get("name", f"UnknownDest_ID_{dest_id}"))
        dest_log_iter: StdlibBoundLogger = log.bind(dest_idx=i + 1, dest_id_loop=dest_id, dest_name_loop=dest_name) # type: ignore

        if dest_id is None:
            final_dest_statuses[f"MalformedDest_DW_{i+1}"] = {"status": "error", "message": "Missing ID"}
            all_dest_ok = False; continue

        db_storage_model: Optional[DBStorageBackendConfigModel] = await asyncio.to_thread(crud.crud_storage_backend_config.get, db_session, id=dest_id)
        if not db_storage_model:
            dest_log_iter.warning("DICOMweb Destination DB config not found.")
            final_dest_statuses[dest_name] = {"status": "error", "message": "DB config not found"}
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=ValueError(f"Dest config ID {dest_id} ({dest_name}) not found."),
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=dataset_to_send,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=None, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue
        if not db_storage_model.is_enabled:
            final_dest_statuses[dest_name] = {"status": "skipped_disabled", "message": "Disabled"}
            continue

        actual_storage_config = build_storage_backend_config_dict(db_storage_model, task_id)
        if not actual_storage_config:
            dest_log_iter.warning("Failed to build DICOMweb destination storage config.")
            final_dest_statuses[dest_name] = {"status": "error", "message": "Failed to build storage config"}
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=RuntimeError(f"Failed to build DW dest config '{dest_name}' (ID: {dest_id})."),
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=dataset_to_send,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue

        if dataset_to_send is None: # Defensive
            dest_log_iter.critical("dataset_to_send is None unexpectedly before DW store attempt.")
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=RuntimeError(f"Critical: dataset_to_send is None for DW dest '{dest_name}'"),
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=None,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue

        storage_backend_instance = get_storage_backend(actual_storage_config)
        # instance_uid is the specific one for this task, known and str
        filename_ctx_loop = f"{instance_uid}.dcm"

        try:
            store_result_dw: Any
            if asyncio.iscoroutinefunction(storage_backend_instance.store):
                store_result_dw = await storage_backend_instance.store(
                    dataset_to_send, None, filename_ctx_loop, source_name_res
                )
            else:
                store_result_dw = await asyncio.to_thread(
                    storage_backend_instance.store,
                    dataset_to_send, None, filename_ctx_loop, source_name_res
                )
            status_key = "duplicate" if store_result_dw == "duplicate" else "success"
            final_dest_statuses[dest_name] = {"status": status_key, "result": _serialize_result(store_result_dw)}
            dest_log_iter.info(f"Store to DW dest {dest_name} reported: {status_key}")
        except StorageBackendError as sbe_dw:
            dest_log_iter.warning(f"StorageBackendError for DW dest {dest_name}", error_msg=str(sbe_dw)) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": str(sbe_dw)}
            all_dest_ok = False

            is_sbe_retryable_dw = any(isinstance(sbe_dw, retryable_exc_type) for retryable_exc_type in EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS) # type: ignore
            staged_filepath_for_retry: Optional[str] = None
            if is_sbe_retryable_dw and dataset_to_send:
                try:
                    def _save_to_stage_sync_dw(): # Renamed for clarity
                        # instance_uid is the specific instance for this DICOMweb task
                        sop_uid_for_stage = instance_uid if instance_uid else f"NO_SOP_UID_DW_{uuid.uuid4().hex[:8]}"
                        timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')
                        staged_filename = f"retry_dw_{task_id}_{sop_uid_for_stage}_{dest_id}_{timestamp_str}.dcm"
                        settings.DICOM_RETRY_STAGING_PATH.mkdir(parents=True, exist_ok=True)
                        staged_file_full_path = settings.DICOM_RETRY_STAGING_PATH / staged_filename
                        dataset_to_send.save_as(str(staged_file_full_path), write_like_original=False) # type: ignore
                        return str(staged_file_full_path)
                    staged_filepath_for_retry = await asyncio.to_thread(_save_to_stage_sync_dw)
                    dest_log_iter.info("Saved processed DW dataset to staging for retry.", staged_path=staged_filepath_for_retry) # type: ignore
                except Exception as stage_save_err:
                    dest_log_iter.error("Failed to save processed DW dataset to staging for retry!", error=str(stage_save_err), exc_info=True) # type: ignore
                    is_sbe_retryable_dw = False
                    staged_filepath_for_retry = None
            
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=sbe_dw, processing_stage=ExceptionProcessingStage.DESTINATION_SEND,
                dataset=dataset_to_send, failed_filepath=staged_filepath_for_retry,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.RETRY_PENDING if is_sbe_retryable_dw else ExceptionStatus.MANUAL_REVIEW_REQUIRED,
                retryable=is_sbe_retryable_dw, commit_on_success=True
            )
        except Exception as e_dw:
            dest_log_iter.error(f"Unexpected error for DW dest {dest_name}", error_msg=str(e_dw), exc_info=True) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": f"Unexpected: {e_dw}"}
            all_dest_ok = False
            await asyncio.to_thread(
                create_exception_log_entry, db=db_session, exc=e_dw, processing_stage=ExceptionProcessingStage.DESTINATION_SEND,
                dataset=dataset_to_send, failed_filepath=None,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_name_res,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )

    current_status_code = "success_all_destinations_dicomweb" if all_dest_ok else "partial_failure_destinations_dicomweb"
    current_msg = f"DICOMweb task processing complete for instance {instance_uid}. All destinations OK: {all_dest_ok}."

    try:
        inc_res = await asyncio.to_thread(crud.dicomweb_state.increment_processed_count, db_session, source_name=source_name_res, count=1) # type: ignore
        if inc_res: await asyncio.to_thread(db_session.commit)
        else: log.warning("Failed to increment DICOMweb processed count post-destinations.") # type: ignore
    except Exception as e_inc:
        log.error("DB error incrementing DICOMweb count post-destinations.", error_msg=str(e_inc), exc_info=True) # type: ignore
        if db_session.is_active: await asyncio.to_thread(db_session.rollback)

    # Corrected return tuple order
    return (
        rules_matched_triggered,
        modifications_made,
        current_status_code,
        current_msg,
        applied_rules,          # List[str]
        final_dest_statuses,    # Dict[str, Dict[str, Any]]
        processed_ds_final,     # Optional[pydicom.Dataset]
        instance_uid,           # str (this is the input instance_uid, which is known and str)
        source_name_res         # str
    )
