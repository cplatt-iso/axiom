# app/worker/executors/stow_executor.py

import asyncio # Though not heavily used by this sync function, good for consistency if ai_portal is ever truly async
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple, cast # Keep cast if used
import uuid # For unique filenames if needed for staging
from datetime import datetime, timezone # For timestamping staged files

import anyio.abc # For ai_portal type hint
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.dataset import Dataset, FileMetaDataset # FileMetaDataset might not be directly used
from sqlalchemy.orm import Session
import structlog
from structlog.stdlib import BoundLogger as StdlibBoundLogger

from app.db import models as db_models
from app import crud
from app.schemas.enums import ProcessedStudySourceType, ExceptionProcessingStage, ExceptionStatus
from app.db.models.storage_backend_config import StorageBackendConfig as DBStorageBackendConfigModel
from app.services.storage_backends import get_storage_backend, StorageBackendError
from app.core.config import settings

from app.worker.processing_orchestrator import process_instance_against_rules
# Assuming these are now in task_utils
from app.worker.task_utils import (
    build_storage_backend_config_dict,
    create_exception_log_entry,
    _serialize_result,
    _safe_get_dicom_value
)

# This might be specific to how you handle retryable SBEs
from app.worker.task_utils import EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS

def execute_stow_task(
    task_context_log: StdlibBoundLogger,
    db_session: Session,
    temp_filepath_str: str, # This is the path to the initially received STOW file
    source_ip: Optional[str],
    task_id: str,
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset], Optional[str], str]: # Return type for instance_uid is Optional[str]
    temp_filepath = Path(temp_filepath_str)
    source_identifier = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}"
    instance_uid: Optional[str] = "UnknownSOPInstanceUID_StowTaskExec" # Initialize as Optional[str]
    log: StdlibBoundLogger = task_context_log.bind(
        stow_temp_filepath=str(temp_filepath),
        stow_source_ip=source_ip,
        source_identifier_for_matching=source_identifier
    )

    source_type_enum_for_exc_log = ProcessedStudySourceType.STOW_RS
    original_ds: Optional[pydicom.Dataset] = None

    try:
        log.debug("Reading temporary DICOM file for STOW processing.")
        original_ds = pydicom.dcmread(str(temp_filepath), force=True)
        instance_uid = _safe_get_dicom_value(original_ds, "SOPInstanceUID", instance_uid)
        log = log.bind(instance_uid=instance_uid)
    except InvalidDicomError as e:
        log.error("Invalid DICOM file from STOW source in executor.", error_msg=str(e))
        raise # Let Celery task log this via its top-level handler
    except Exception as read_exc:
        log.error("Error reading temporary DICOM file for STOW in executor.", error_msg=str(read_exc), exc_info=True)
        raise # Let Celery task log this

    if original_ds is None:
        log.error("original_ds is None after read attempt in STOW task.")
        create_exception_log_entry(
            db=db_session, exc=RuntimeError("Dataset is None after read in STOW task"),
            processing_stage=ExceptionProcessingStage.INGESTION, failed_filepath=str(temp_filepath),
            original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
            celery_task_id=task_id, initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, commit_on_success=True
        )
        return False, False, "error_stow_no_dataset", "Dataset not loaded", [], {}, None, instance_uid, source_identifier

    active_rulesets = crud.ruleset.get_active_ordered(db_session)
    processed_ds_rules, applied_rules, unique_dest_dicts = process_instance_against_rules(
        original_ds=original_ds, active_rulesets=active_rulesets,
        source_identifier=source_identifier, db_session=db_session,
        association_info=None, ai_portal=ai_portal
    )

    modifications_made = (processed_ds_rules is not original_ds and processed_ds_rules is not None)
    rules_matched_triggered = bool(applied_rules or unique_dest_dicts)
    dataset_to_send = processed_ds_rules if processed_ds_rules else original_ds

    if not rules_matched_triggered:
        log.info(f"No matching STOW rules triggered actions for {source_identifier}.")
        return rules_matched_triggered, modifications_made, "success_no_matching_rules", \
               f"No matching rules for STOW source.", applied_rules, {}, dataset_to_send, instance_uid, source_identifier

    final_dest_statuses: Dict[str, Dict[str, Any]] = {}
    all_dest_ok = True

    if not unique_dest_dicts:
         log.info("STOW rules matched, but no destinations configured.")
         return rules_matched_triggered, modifications_made, "success_no_destinations", \
               "STOW rules matched, no destinations.", applied_rules, {}, dataset_to_send, instance_uid, source_identifier

    log.info(f"Processing {len(unique_dest_dicts)} destinations for STOW task.")
    instance_uid_for_filename = _safe_get_dicom_value(dataset_to_send, "SOPInstanceUID", instance_uid)

    for i, dest_info in enumerate(unique_dest_dicts):
        dest_id = cast(Optional[int], dest_info.get("id"))
        dest_name = cast(str, dest_info.get("name", f"UnknownDest_ID_{dest_id}"))
        dest_log: StdlibBoundLogger = log.bind(dest_idx=i + 1, dest_id_loop=dest_id, dest_name_loop=dest_name)

        if dest_id is None:
            final_dest_statuses[f"MalformedDest_STOW_{i+1}"] = {"status": "error", "message": "Missing ID"}
            all_dest_ok = False; continue

        db_storage_model: Optional[DBStorageBackendConfigModel] = crud.crud_storage_backend_config.get(db_session, id=dest_id)
        if not db_storage_model: # Config not found
            # ... (Log with create_exception_log_entry - similar to execute_file_based_task) ...
            dest_log.warning("STOW Destination DB config not found.")
            final_dest_statuses[dest_name] = {"status": "error", "message": "DB config not found"}
            create_exception_log_entry(
                db=db_session, exc=ValueError(f"Dest config ID {dest_id} ({dest_name}) not found."),
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=dataset_to_send,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                target_destination_db_model=None, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue
        if not db_storage_model.is_enabled: # Disabled
            dest_log.info("STOW Destination disabled.")
            final_dest_statuses[dest_name] = {"status": "skipped_disabled", "message": "Disabled"}
            continue

        actual_storage_config = build_storage_backend_config_dict(db_storage_model, task_id)
        if not actual_storage_config: # Failed build
            # ... (Log with create_exception_log_entry - similar to execute_file_based_task) ...
            dest_log.warning("Failed to build STOW destination storage config.")
            final_dest_statuses[dest_name] = {"status": "error", "message": "Failed to build storage config"}
            create_exception_log_entry(
                db=db_session, exc=RuntimeError(f"Failed to build STOW dest config '{dest_name}' (ID: {dest_id})."),
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=dataset_to_send,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue
        
        if dataset_to_send is None: # Defensive
            # ... (Log with create_exception_log_entry - similar to execute_file_based_task) ...
            dest_log.critical("dataset_to_send is None unexpectedly before STOW store attempt.")
            create_exception_log_entry(
                db=db_session, exc=RuntimeError(f"Critical: dataset_to_send is None for STOW dest '{dest_name}'"),
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=None,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue


        try:
            storage_backend = get_storage_backend(actual_storage_config)
            filename_ctx = f"{instance_uid_for_filename}.dcm" # type: ignore
            if asyncio.iscoroutinefunction(storage_backend.store):
                 raise TypeError(f"Async store method for {dest_name} from sync STOW executor.")

            # For STOW, the `original_filepath` context for storage_backend.store
            # might be the `temp_filepath` if the backend uses it for context,
            # or None if it purely relies on `filename_context`.
            # Here, `temp_filepath` represents the received file.
            store_result = storage_backend.store(
                dataset_to_send, temp_filepath, filename_ctx, source_identifier
            )
            status_key = "duplicate" if store_result == "duplicate" else "success"
            final_dest_statuses[dest_name] = {"status": status_key, "result": _serialize_result(store_result)}
            dest_log.info(f"Store to STOW dest {dest_name} reported: {status_key}")
        except StorageBackendError as sbe:
            dest_log.warning(f"StorageBackendError for STOW dest {dest_name}", error_msg=str(sbe))
            final_dest_statuses[dest_name] = {"status": "error", "message": str(sbe)}
            all_dest_ok = False

            is_sbe_retryable = any(isinstance(sbe, retryable_exc_type) for retryable_exc_type in EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS) # type: ignore
            staged_filepath_for_retry: Optional[str] = None
            if is_sbe_retryable and dataset_to_send:
                try:
                    sop_uid_for_stage = _safe_get_dicom_value(dataset_to_send, "SOPInstanceUID", f"NO_SOP_UID_{uuid.uuid4().hex[:8]}")
                    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')
                    staged_filename = f"retry_stow_{task_id}_{sop_uid_for_stage}_{dest_id}_{timestamp_str}.dcm"
                    settings.DICOM_RETRY_STAGING_PATH.mkdir(parents=True, exist_ok=True)
                    staged_file_full_path = settings.DICOM_RETRY_STAGING_PATH / staged_filename
                    dataset_to_send.save_as(str(staged_file_full_path), write_like_original=False)
                    staged_filepath_for_retry = str(staged_file_full_path)
                    dest_log.info("Saved processed STOW dataset to staging for retry.", staged_path=staged_filepath_for_retry)
                except Exception as stage_save_err:
                    dest_log.error("Failed to save processed STOW dataset to staging for retry!", error=str(stage_save_err), exc_info=True)
                    is_sbe_retryable = False
                    staged_filepath_for_retry = None
            
            create_exception_log_entry(
                db=db_session, exc=sbe, processing_stage=ExceptionProcessingStage.DESTINATION_SEND,
                dataset=dataset_to_send, failed_filepath=staged_filepath_for_retry,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                # No association_info for STOW generally
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.RETRY_PENDING if is_sbe_retryable else ExceptionStatus.MANUAL_REVIEW_REQUIRED,
                retryable=is_sbe_retryable, commit_on_success=True
            )
        except Exception as e:
            dest_log.error(f"Unexpected error for STOW dest {dest_name}", error_msg=str(e), exc_info=True)
            final_dest_statuses[dest_name] = {"status": "error", "message": f"Unexpected: {e}"}
            all_dest_ok = False
            create_exception_log_entry(
                db=db_session, exc=e, processing_stage=ExceptionProcessingStage.DESTINATION_SEND,
                dataset=dataset_to_send, failed_filepath=None,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )

    current_status_code = "success_all_destinations" if all_dest_ok else "partial_failure_destinations"
    current_msg = f"STOW task processing complete. All destinations OK: {all_dest_ok}."
    return (rules_matched_triggered, modifications_made, current_status_code, current_msg,
           applied_rules, final_dest_statuses, dataset_to_send, instance_uid, source_identifier)