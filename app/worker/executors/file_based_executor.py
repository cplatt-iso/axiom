# app/worker/executors/file_based_executor.py

import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple, cast
import uuid # For unique filenames if needed for staging
from datetime import datetime, timezone # For timestamping staged files

import anyio.abc # For ai_portal type hint
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.dataset import Dataset, FileMetaDataset # FileMetaDataset might not be directly used but good for context
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
# Assuming _safe_get_dicom_value, create_exception_log_entry, _serialize_result are in task_utils
from app.worker.task_utils import build_storage_backend_config_dict, create_exception_log_entry, _serialize_result, _safe_get_dicom_value

# This might be specific to how you handle retryable SBEs
from app.worker.task_utils import EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS

def execute_file_based_task(
    task_context_log: StdlibBoundLogger,
    db_session: Session,
    dicom_filepath_str: str,
    source_type_str: str,
    source_db_id_or_instance_id: Union[int, str],
    task_id: str,
    association_info: Optional[Dict[str, str]] = None,
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset], Optional[str], str]:
    original_filepath = Path(dicom_filepath_str)
    instance_uid = "UnknownSOPInstanceUID_FileTaskExec"
    default_source_identifier = f"{source_type_str}_{source_db_id_or_instance_id}"
    source_identifier = default_source_identifier
    log: StdlibBoundLogger = task_context_log

    try:
        source_type_enum_for_exc_log = ProcessedStudySourceType(source_type_str)
    except ValueError:
        log.warning("Invalid source_type_str in execute_file_based_task, cannot map to enum.", received_source_type_str=source_type_str)
        source_type_enum_for_exc_log = ProcessedStudySourceType.UNKNOWN

    original_ds: Optional[pydicom.Dataset] = None
    try:
        # --- Determine source_identifier_for_matching (UNCHANGED FROM YOUR LAST VERSION) ---
        log.debug("Attempting to resolve source identifier for matching.",
                  source_type=source_type_str, source_id_or_inst=str(source_db_id_or_instance_id))
        source_type_enum_for_rules = ProcessedStudySourceType(source_type_str) # Used for rule matching logic
        if source_type_enum_for_rules == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(source_db_id_or_instance_id, str):
            listener_config = crud.crud_dimse_listener_config.get_by_instance_id(db_session, instance_id=source_db_id_or_instance_id)
            if listener_config and listener_config.name: source_identifier = listener_config.name
        elif source_type_enum_for_rules == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
            qr_config = crud.crud_dimse_qr_source.get(db_session, id=source_db_id_or_instance_id)
            if qr_config and qr_config.name: source_identifier = qr_config.name
        elif source_type_enum_for_rules == ProcessedStudySourceType.FILE_UPLOAD:
            source_identifier = "FILE_UPLOAD"
        log = log.bind(source_identifier_for_matching=source_identifier)

        log.debug("Reading DICOM file.")
        original_ds = pydicom.dcmread(str(original_filepath), force=True)
        instance_uid = _safe_get_dicom_value(original_ds, "SOPInstanceUID", instance_uid) # type: ignore
        log = log.bind(instance_uid=instance_uid)

    except InvalidDicomError as e:
        log.error("Invalid DICOM file format in executor.", error_msg=str(e))
        raise # Let Celery task's top-level handler log this
    except Exception as init_exc:
        log.error("Error during executor initialization.", error_msg=str(init_exc), exc_info=True)
        raise # Let Celery task's top-level handler log this

    if original_ds is None:
        log.error("original_ds is None after read attempt.")
        create_exception_log_entry(
            db=db_session, exc=RuntimeError("Dataset is None after read attempt in file-based task"),
            processing_stage=ExceptionProcessingStage.INGESTION, failed_filepath=str(original_filepath),
            original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
            celery_task_id=task_id, initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, commit_on_success=True
        )
        return False, False, "error_file_no_dataset", "Dataset not loaded", [], {}, None, instance_uid, source_identifier

    processed_ds_rules, applied_rules, unique_dest_dicts = process_instance_against_rules(
        original_ds=original_ds, active_rulesets=crud.ruleset.get_active_ordered(db_session),
        source_identifier=source_identifier, db_session=db_session,
        association_info=association_info, ai_portal=ai_portal
    )

    modifications_made = (processed_ds_rules is not original_ds and processed_ds_rules is not None)
    rules_matched_triggered = bool(applied_rules or unique_dest_dicts)
    dataset_to_send = processed_ds_rules if processed_ds_rules else original_ds # Ensure dataset_to_send is not None

    if not rules_matched_triggered:
        log.info(f"No matching rules triggered actions for {source_identifier}.")
        return rules_matched_triggered, modifications_made, "success_no_matching_rules", \
               f"No matching rules.", applied_rules, {}, dataset_to_send, instance_uid, source_identifier

    final_dest_statuses: Dict[str, Dict[str, Any]] = {}
    all_dest_ok = True

    if not unique_dest_dicts:
         log.info("Rules matched, but no destinations configured.")
         return rules_matched_triggered, modifications_made, "success_no_destinations", \
               "Rules matched, no destinations.", applied_rules, {}, dataset_to_send, instance_uid, source_identifier

    log.info(f"Processing {len(unique_dest_dicts)} destinations.")
    instance_uid_for_filename = _safe_get_dicom_value(dataset_to_send, "SOPInstanceUID", instance_uid) # type: ignore

    for i, dest_info in enumerate(unique_dest_dicts):
        dest_id = cast(Optional[int], dest_info.get("id"))
        dest_name = cast(str, dest_info.get("name", f"UnknownDest_ID_{dest_id}"))
        dest_log: StdlibBoundLogger = log.bind(dest_idx=i + 1, dest_id_loop=dest_id, dest_name_loop=dest_name)

        if dest_id is None: # Should be caught by destination_handler, but defensive
            final_dest_statuses[f"MalformedDest_{i+1}"] = {"status": "error", "message": "Missing ID"}
            all_dest_ok = False; continue

        db_storage_model: Optional[DBStorageBackendConfigModel] = crud.crud_storage_backend_config.get(db_session, id=dest_id)
        if not db_storage_model:
            dest_log.warning("Destination DB config not found.")
            final_dest_statuses[dest_name] = {"status": "error", "message": "DB config not found"}
            create_exception_log_entry(
                db=db_session, exc=ValueError(f"Dest config ID {dest_id} ({dest_name}) not found."),
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=dataset_to_send,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                target_destination_db_model=None, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue
        if not db_storage_model.is_enabled:
            dest_log.info("Destination disabled.")
            final_dest_statuses[dest_name] = {"status": "skipped_disabled", "message": "Disabled"}
            continue

        # NEW: RabbitMQ publishing logic
        if db_storage_model.backend_type == "cstore":
            try:
                import pika
                import json
                from app.db.models.storage_backend_config import CStoreBackendConfig
                from app.crud.crud_sender_config import crud_sender_config

                # Cast to CStoreBackendConfig to access cstore-specific fields
                cstore_config = cast(CStoreBackendConfig, db_storage_model)

                connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.RABBITMQ_HOST))
                channel = connection.channel()

                # Determine sender type - first check sender_identifier, then fall back to sender_type
                sender_type = None
                if cstore_config.sender_identifier:
                    # Look up the sender config
                    sender_config_obj = crud_sender_config.get_by_name(db_session, name=cstore_config.sender_identifier)
                    if sender_config_obj and sender_config_obj.is_enabled:
                        sender_type = sender_config_obj.sender_type
                    else:
                        log.warning(f"Sender identifier '{cstore_config.sender_identifier}' not found or disabled")
                        sender_type = cstore_config.sender_type or 'pynetdicom'
                else:
                    sender_type = cstore_config.sender_type or 'pynetdicom'

                queue_name = ""
                if sender_type == "dcm4che":
                    queue_name = "cstore_dcm4che_jobs"
                else:
                    queue_name = "cstore_pynetdicom_jobs"
                
                channel.queue_declare(queue=queue_name, durable=True)

                # We need to save the processed file to a shared volume
                # so the sender container can access it.
                processed_dir = Path('/dicom_data/processed')
                processed_dir.mkdir(parents=True, exist_ok=True)
                processed_filepath = processed_dir / f"{task_id}_{instance_uid_for_filename}.dcm"
                dataset_to_send.save_as(str(processed_filepath), write_like_original=False)

                # The job payload now contains all necessary info.
                # The sender container will not need to call the API.
                job = {
                    "file_path": str(processed_filepath),
                    "destination_config": build_storage_backend_config_dict(db_storage_model, task_id)
                }

                channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    body=json.dumps(job, default=str), # Use default=str for datetimes
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    ))
                
                connection.close()
                final_dest_statuses[dest_name] = {"status": "queued", "queue": queue_name}
                dest_log.info(f"Queued job for {dest_name} to {queue_name}")
                continue # Move to the next destination

            except Exception as e:
                dest_log.error(f"Failed to queue job for {dest_name}", error_msg=str(e), exc_info=True)
                final_dest_statuses[dest_name] = {"status": "error", "message": f"Failed to queue job: {e}"}
                all_dest_ok = False
                # Optionally create an exception log for the failure to queue
                continue


        actual_storage_config = build_storage_backend_config_dict(db_storage_model, task_id)
        if not actual_storage_config:
            dest_log.warning("Failed to build destination storage config.")
            final_dest_statuses[dest_name] = {"status": "error", "message": "Failed to build storage config"}
            create_exception_log_entry(
                db=db_session, exc=RuntimeError(f"Failed to build dest config '{dest_name}' (ID: {dest_id})."),
                processing_stage=ExceptionProcessingStage.DESTINATION_SEND, dataset=dataset_to_send,
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )
            all_dest_ok = False; continue
        
        # Ensure dataset_to_send is not None before proceeding (already handled by previous check if original_ds was None)
        if dataset_to_send is None: # This should ideally never be hit if logic above is sound
            dest_log.critical("dataset_to_send is None unexpectedly before store attempt.")
            # Log critical error and skip this destination
            create_exception_log_entry(
                db=db_session, exc=RuntimeError(f"Critical: dataset_to_send is None for dest '{dest_name}'"),
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
                 raise TypeError(f"Async store method for {dest_name} from sync executor.")

            store_result = storage_backend.store(
                dataset_to_send, original_filepath, filename_ctx, source_identifier
            )
            status_key = "duplicate" if store_result == "duplicate" else "success"
            final_dest_statuses[dest_name] = {"status": status_key, "result": _serialize_result(store_result)}
            dest_log.info(f"Store to {dest_name} reported: {status_key}")
        except StorageBackendError as sbe:
            dest_log.warning(f"StorageBackendError for {dest_name}", error_msg=str(sbe))
            final_dest_statuses[dest_name] = {"status": "error", "message": str(sbe)}
            all_dest_ok = False

            is_sbe_retryable = any(isinstance(sbe, retryable_exc_type) for retryable_exc_type in EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS)
            # Add more refined sbe retryable logic here if needed (e.g. based on sbe.status_code)

            staged_filepath_for_retry: Optional[str] = None
            if is_sbe_retryable and dataset_to_send:
                try:
                    sop_uid_for_stage = _safe_get_dicom_value(dataset_to_send, "SOPInstanceUID", f"NO_SOP_UID_{uuid.uuid4().hex[:8]}")
                    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')
                    staged_filename = f"retry_{task_id}_{sop_uid_for_stage}_{dest_id}_{timestamp_str}.dcm"
                    settings.DICOM_RETRY_STAGING_PATH.mkdir(parents=True, exist_ok=True)
                    staged_file_full_path = settings.DICOM_RETRY_STAGING_PATH / staged_filename
                    dataset_to_send.save_as(str(staged_file_full_path), write_like_original=False)
                    staged_filepath_for_retry = str(staged_file_full_path)
                    dest_log.info("Saved processed dataset to staging for retry.", staged_path=staged_filepath_for_retry)
                except Exception as stage_save_err:
                    dest_log.error("Failed to save PROCESSED dataset to staging for retry!", error=str(stage_save_err), exc_info=True)
                    is_sbe_retryable = False # Cannot retry if staging failed
                    staged_filepath_for_retry = None # Ensure it's None
            
            create_exception_log_entry(
                db=db_session, exc=sbe, processing_stage=ExceptionProcessingStage.DESTINATION_SEND,
                dataset=dataset_to_send, failed_filepath=staged_filepath_for_retry, # Use staged path
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                calling_ae_title=association_info.get("calling_ae_title") if association_info else None,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.RETRY_PENDING if is_sbe_retryable else ExceptionStatus.MANUAL_REVIEW_REQUIRED,
                retryable=is_sbe_retryable, commit_on_success=True
            )
        except Exception as e: # Catch other unexpected errors during store
            dest_log.error(f"Unexpected error for {dest_name}", error_msg=str(e), exc_info=True)
            final_dest_statuses[dest_name] = {"status": "error", "message": f"Unexpected: {e}"}
            all_dest_ok = False
            create_exception_log_entry(
                db=db_session, exc=e, processing_stage=ExceptionProcessingStage.DESTINATION_SEND,
                dataset=dataset_to_send, failed_filepath=None, # No staged file for unexpected errors usually
                original_source_type=source_type_enum_for_exc_log, original_source_identifier=source_identifier,
                calling_ae_title=association_info.get("calling_ae_title") if association_info else None,
                target_destination_db_model=db_storage_model, celery_task_id=task_id,
                initial_status=ExceptionStatus.MANUAL_REVIEW_REQUIRED, retryable=False, commit_on_success=True
            )

    current_status_code = "success_all_destinations" if all_dest_ok else "partial_failure_destinations"
    current_msg = f"File task processing complete. All destinations OK: {all_dest_ok}."
    return (rules_matched_triggered, modifications_made, current_status_code, current_msg,
           applied_rules, final_dest_statuses, dataset_to_send, instance_uid, source_identifier) # Return dataset_to_send