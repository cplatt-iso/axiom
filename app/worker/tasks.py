# app/worker/tasks.py
import asyncio
import json
import os
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union
from copy import deepcopy

import structlog

from celery import shared_task
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.uid import generate_uid

from app.db.session import SessionLocal
from app import crud
from app.db import models
from app.db.models import ProcessedStudySourceType
from app.db.models.storage_backend_config import StorageBackendConfig as DBStorageBackendConfigModel

from app.services.storage_backends import get_storage_backend, StorageBackendError
from app.services import dicomweb_client

from app.core.config import settings

from app.worker.processing_logic import process_dicom_instance
from app.services.storage_backends.google_healthcare import GoogleHealthcareDicomStoreStorage

from app.db.models.storage_backend_config import (
    StorageBackendConfig as DBStorageBackendConfigModel, # Keep base if needed elsewhere, but subclasses are key now
    FileSystemBackendConfig,
    GcsBackendConfig,
    CStoreBackendConfig,
    GoogleHealthcareBackendConfig,
    StowRsBackendConfig
)


logger = structlog.get_logger(__name__)


def move_to_error_dir(filepath: Path, task_id: str):
    if not isinstance(filepath, Path):
         filepath = Path(filepath)

    log = logger.bind(task_id=task_id, original_filepath=str(filepath))

    if not filepath.is_file():
         log.warning("Source path is not a file or does not exist. Cannot move to error dir.")
         return
    try:
        error_base_dir_str = getattr(settings, 'DICOM_ERROR_PATH', None)
        if error_base_dir_str:
             error_dir = Path(error_base_dir_str)
        else:
             storage_path = getattr(settings, 'DICOM_STORAGE_PATH', '/dicom_data/incoming')
             error_dir = Path(storage_path).parent / "errors"
             log.warning("DICOM_ERROR_PATH not set in config, using fallback", fallback_error_dir=str(error_dir))

        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        log.info("Moved file to error directory", error_path=str(error_path))
    except Exception as move_err:
        logger.critical(
            "CRITICAL - Could not move file to error dir",
            task_id=task_id,
            original_filepath=str(filepath),
            target_error_dir=str(error_dir) if 'error_dir' in locals() else 'Unknown',
            error=str(move_err),
            exc_info=True
        )


RETRYABLE_EXCEPTIONS = (
    ConnectionRefusedError,
    StorageBackendError,
    TimeoutError,
    dicomweb_client.DicomWebClientError
)


@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(
    self,
    dicom_filepath_str: str,
    source_type: str,
    source_db_id_or_instance_id: Union[int, str],
    association_info: Optional[Dict[str, str]] = None
):
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    log_source_identifier = f"{source_type}_{source_db_id_or_instance_id}"

    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        original_filepath=str(original_filepath),
        source_type=source_type,
        source_id_or_instance=source_db_id_or_instance_id,
        association_info=association_info or {}
    )

    log.info(f"Received request from source: {log_source_identifier}")

    if not original_filepath.is_file():
        log.error("File not found or is not a file. Cannot process.")
        return {
            "status": "error",
            "message": "File not found",
            "filepath": dicom_filepath_str,
            "source": log_source_identifier,
            "source_type": source_type,
            "source_id": source_db_id_or_instance_id
            }

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    instance_uid_str = "Unknown"
    source_identifier_for_matching = log_source_identifier
    applied_rules_info: List[str] = []

    try:
        db = SessionLocal()

        try:
            source_type_enum = ProcessedStudySourceType(source_type)
            if source_type_enum == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(source_db_id_or_instance_id, str):
                listener_config = crud.crud_dimse_listener_config.get_by_instance_id(db, instance_id=source_db_id_or_instance_id)
                if listener_config:
                    source_identifier_for_matching = listener_config.name
                else:
                    log.warning("Could not find listener config for instance ID.", default_identifier=log_source_identifier)
            elif source_type_enum == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
                 qr_config = crud.crud_dimse_qr_source.get(db, id=source_db_id_or_instance_id)
                 if qr_config:
                      source_identifier_for_matching = qr_config.name
                 else:
                     log.warning("Could not find DIMSE QR config for source ID.", default_identifier=log_source_identifier)
        except ValueError:
             log.error("Invalid source_type received.", received_source_type=source_type)
        except Exception as e:
             log.error("Error looking up source config name.", error=str(e), exc_info=False)

        log = log.bind(source_identifier_for_matching=source_identifier_for_matching)

        log.debug("Reading DICOM file...")
        try:
            original_ds = pydicom.dcmread(str(original_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            log = log.bind(instance_uid=instance_uid_str)
        except InvalidDicomError as e:
            log.error("Invalid DICOM file format", error=str(e))
            move_to_error_dir(original_filepath, task_id)
            return {
                "status": "error",
                "message": f"Invalid DICOM file format: {e}",
                "filepath": dicom_filepath_str,
                "source": log_source_identifier,
                "instance_uid": instance_uid_str,
                "source_type": source_type,
                "source_id": source_db_id_or_instance_id
                }
        except Exception as read_exc:
            log.error("Error reading DICOM file", error=str(read_exc), exc_info=True)
            raise read_exc

        log.debug("Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             log.info("No active rulesets found. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    log.info("Deleting file as no rulesets were active (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    log.warning("Failed to delete unmatched file", error=str(e))
             else:
                 log.info("Leaving file as no rulesets were active (DELETE_UNMATCHED_FILES=false).")
             db.close()
             return {
                 "status": final_status,
                 "message": final_message,
                 "filepath": dicom_filepath_str,
                 "source": log_source_identifier,
                 "instance_uid": instance_uid_str,
                 "source_type": source_type,
                 "source_id": source_db_id_or_instance_id
                 }

        log.debug("Calling core processing logic...")
        try:
            modified_ds, applied_rules_info, unique_destination_identifier_dicts = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier_for_matching,
                association_info=association_info
            )
        except Exception as proc_exc:
             log.error("Error during core processing logic", error=str(proc_exc), exc_info=True)
             move_to_error_dir(original_filepath, task_id)
             db.close()
             return {
                 "status": "error",
                 "message": f"Error during rule processing: {proc_exc}",
                 "filepath": dicom_filepath_str,
                 "source": log_source_identifier,
                 "instance_uid": instance_uid_str,
                 "source_type": source_type,
                 "source_id": source_db_id_or_instance_id
                 }

        log = log.bind(applied_rules_count=len(applied_rules_info), applied_rules=applied_rules_info)

        if not applied_rules_info:
             log.info("No applicable rules matched this source. No actions taken.")
             final_status = "success"
             final_message = f"No matching rules found for source '{source_identifier_for_matching}'"
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    log.info("Deleting file as no rules matched this source (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    log.warning("Failed to delete file with no matching rules", error=str(e))
             else:
                 log.info("Leaving file as no rules matched this source (DELETE_UNMATCHED_FILES=false).")
             db.close()
             return {
                 "status": final_status,
                 "message": final_message,
                 "filepath": dicom_filepath_str,
                 "source": log_source_identifier,
                 "instance_uid": instance_uid_str,
                 "source_type": source_type,
                 "source_id": source_db_id_or_instance_id
                 }

        log = log.bind(unique_destination_count=len(unique_destination_identifier_dicts))

        if not unique_destination_identifier_dicts:
            log.info("Rules matched, but no destinations configured in matched rules.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            delete_original = modified_ds is not None and settings.DELETE_ON_NO_DESTINATION
            if delete_original:
                try:
                    original_filepath.unlink(missing_ok=True)
                except OSError as e:
                    log.warning("Failed to delete original file after processing with no destinations", error=str(e))
            db.close()
            return {
                "status": final_status,
                "message": final_message,
                "applied_rules": applied_rules_info,
                "filepath": dicom_filepath_str,
                "source": log_source_identifier,
                "instance_uid": instance_uid_str,
                "source_type": source_type,
                "source_id": source_db_id_or_instance_id
                }

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        log.info("Processing unique destinations...")
        all_destinations_succeeded = True
        for i, dest_identifier_dict in enumerate(unique_destination_identifier_dicts):
            dest_id_from_dict = dest_identifier_dict.get("id")
            dest_name_from_dict = dest_identifier_dict.get("name", "UnknownName")
            
            dest_log = log.bind(
                destination_index=i+1,
                destination_id_from_dict=dest_id_from_dict,
                destination_name_from_dict=dest_name_from_dict
            )

            if dest_id_from_dict is None:
                dest_log.error("Destination identifier dict is missing 'id'. Skipping.")
                all_destinations_succeeded = False
                success_status[f"Dest_{i+1}_Malformed"] = {"status": "error", "message": "Missing ID in destination identifier"}
                continue

            db_storage_backend_model: Optional[DBStorageBackendConfigModel] = crud.crud_storage_backend_config.get(db, id=dest_id_from_dict)

            if not db_storage_backend_model:
                dest_log.error("Could not find storage backend configuration in DB.", target_db_id=dest_id_from_dict)
                all_destinations_succeeded = False
                success_status[f"Dest_{i+1}_{dest_name_from_dict}"] = {"status": "error", "message": f"DB config not found for ID {dest_id_from_dict}"}
                continue
            
            if not db_storage_backend_model.is_enabled:
                dest_log.info("Storage backend configuration is disabled in DB. Skipping.", target_db_id=dest_id_from_dict, name=db_storage_backend_model.name)
                success_status[f"Dest_{i+1}_{db_storage_backend_model.name}"] = {"status": "skipped_disabled", "message": f"Destination {db_storage_backend_model.name} is disabled."}
                continue

# BEGIN FUCKED UP CODE
            # This is the replacement for your existing try...except block
            # that was failing with "AttributeError: ... object has no attribute 'config'"
            actual_storage_config_dict = {}
            try:
                # 1. Resolve backend_type to a string
                backend_type_value_from_db_model = db_storage_backend_model.backend_type
                resolved_backend_type_for_config: Optional[str] = None

                if isinstance(backend_type_value_from_db_model, str):
                    resolved_backend_type_for_config = backend_type_value_from_db_model
                elif hasattr(backend_type_value_from_db_model, 'value') and isinstance(getattr(backend_type_value_from_db_model, 'value', None), str):
                    resolved_backend_type_for_config = backend_type_value_from_db_model.value
                else:
                    # Using dest_log if available, otherwise fall back to log or a generic logger
                    # Assuming dest_log is defined in the loop iterating destinations
                    current_logger = dest_log if 'dest_log' in locals() else log
                    current_logger.error(
                        "DB model 'backend_type' is invalid or not a string/enum.value.",
                        raw_backend_type=backend_type_value_from_db_model,
                        type_of_raw=type(backend_type_value_from_db_model).__name__,
                        destination_name=db_storage_backend_model.name, # Added for context
                        destination_id=db_storage_backend_model.id      # Added for context
                    )
                    all_destinations_succeeded = False # Ensure this flag is set if used in your loop
                    success_status[db_storage_backend_model.name] = {"status": "error", "message": "Invalid backend_type format from DB."}
                    continue

                if resolved_backend_type_for_config is None:
                    current_logger = dest_log if 'dest_log' in locals() else log
                    current_logger.error(
                        "Failed to resolve backend_type for config dict construction.",
                        destination_name=db_storage_backend_model.name,
                        destination_id=db_storage_backend_model.id
                        )
                    all_destinations_succeeded = False
                    success_status[db_storage_backend_model.name] = {"status": "error", "message": "Internal error resolving backend_type."}
                    continue

                # Initialize with common fields expected by get_storage_backend's config dict
                actual_storage_config_dict['type'] = resolved_backend_type_for_config
                # The 'name' from the DB model might be useful for some storage backend initializations
                actual_storage_config_dict['name'] = db_storage_backend_model.name

                # 2. Populate type-specific fields by DIRECTLY accessing attributes
                #    of db_storage_backend_model, which is an instance of the correct subclass.

                if resolved_backend_type_for_config == "filesystem":
                    if isinstance(db_storage_backend_model, FileSystemBackendConfig):
                        actual_storage_config_dict['path'] = db_storage_backend_model.path
                    else:
                        (dest_log if 'dest_log' in locals() else log).error("Type mismatch: Expected FileSystemBackendConfig.", model_actual_type=type(db_storage_backend_model).__name__, dest_name=db_storage_backend_model.name)
                        all_destinations_succeeded = False; success_status[db_storage_backend_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue

                elif resolved_backend_type_for_config == "gcs":
                    if isinstance(db_storage_backend_model, GcsBackendConfig):
                        actual_storage_config_dict['bucket'] = db_storage_backend_model.bucket
                        actual_storage_config_dict['prefix'] = db_storage_backend_model.prefix
                    else:
                        (dest_log if 'dest_log' in locals() else log).error("Type mismatch: Expected GcsBackendConfig.", model_actual_type=type(db_storage_backend_model).__name__, dest_name=db_storage_backend_model.name)
                        all_destinations_succeeded = False; success_status[db_storage_backend_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue

                elif resolved_backend_type_for_config == "cstore":
                    if isinstance(db_storage_backend_model, CStoreBackendConfig):
                        actual_storage_config_dict['remote_ae_title'] = db_storage_backend_model.remote_ae_title
                        actual_storage_config_dict['remote_host'] = db_storage_backend_model.remote_host
                        actual_storage_config_dict['remote_port'] = db_storage_backend_model.remote_port
                        actual_storage_config_dict['local_ae_title'] = db_storage_backend_model.local_ae_title
                        actual_storage_config_dict['tls_enabled'] = db_storage_backend_model.tls_enabled
                        actual_storage_config_dict['tls_ca_cert_secret_name'] = db_storage_backend_model.tls_ca_cert_secret_name
                        actual_storage_config_dict['tls_client_cert_secret_name'] = db_storage_backend_model.tls_client_cert_secret_name
                        actual_storage_config_dict['tls_client_key_secret_name'] = db_storage_backend_model.tls_client_key_secret_name
                    else:
                        (dest_log if 'dest_log' in locals() else log).error("Type mismatch: Expected CStoreBackendConfig.", model_actual_type=type(db_storage_backend_model).__name__, dest_name=db_storage_backend_model.name)
                        all_destinations_succeeded = False; success_status[db_storage_backend_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue

                elif resolved_backend_type_for_config == "google_healthcare":
                    if isinstance(db_storage_backend_model, GoogleHealthcareBackendConfig):
                        actual_storage_config_dict['gcp_project_id'] = db_storage_backend_model.gcp_project_id
                        actual_storage_config_dict['gcp_location'] = db_storage_backend_model.gcp_location
                        actual_storage_config_dict['gcp_dataset_id'] = db_storage_backend_model.gcp_dataset_id
                        actual_storage_config_dict['gcp_dicom_store_id'] = db_storage_backend_model.gcp_dicom_store_id
                    else:
                        (dest_log if 'dest_log' in locals() else log).error("Type mismatch: Expected GoogleHealthcareBackendConfig.", model_actual_type=type(db_storage_backend_model).__name__, dest_name=db_storage_backend_model.name)
                        all_destinations_succeeded = False; success_status[db_storage_backend_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue

                elif resolved_backend_type_for_config == "stow_rs":
                     if isinstance(db_storage_backend_model, StowRsBackendConfig):
                        actual_storage_config_dict['base_url'] = db_storage_backend_model.base_url
                     else:
                        (dest_log if 'dest_log' in locals() else log).error("Type mismatch: Expected StowRsBackendConfig.", model_actual_type=type(db_storage_backend_model).__name__, dest_name=db_storage_backend_model.name)
                        all_destinations_succeeded = False; success_status[db_storage_backend_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue
                else:
                    (dest_log if 'dest_log' in locals() else log).error("Unknown resolved_backend_type_for_config for building specific config.", type_val=resolved_backend_type_for_config, dest_name=db_storage_backend_model.name)
                    all_destinations_succeeded = False
                    success_status[db_storage_backend_model.name] = {"status": "error", "message": f"Unknown type '{resolved_backend_type_for_config}' for config construction."}
                    continue

            except AttributeError as attr_err:
                # This catch is if a specific attribute (e.g., .bucket) is missing from an expected model type.
                (dest_log if 'dest_log' in locals() else log).error(
                    "Attribute error building specific config dict from DB model. A field expected for the backend type was missing on the model instance.",
                    error=str(attr_err), model_type=type(db_storage_backend_model).__name__,
                    destination_name=db_storage_backend_model.name, exc_info=True
                )
                all_destinations_succeeded = False
                success_status[db_storage_backend_model.name] = {"status": "error", "message": f"Attribute error constructing config: {attr_err}"}
                continue
            except Exception as config_build_err: # Catch any other unexpected error during this block
                (dest_log if 'dest_log' in locals() else log).error(
                    "Generic error building specific config dict from DB model.",
                    error=str(config_build_err), destination_name=db_storage_backend_model.name, exc_info=True
                )
                all_destinations_succeeded = False
                success_status[db_storage_backend_model.name] = {"status": "error", "message": f"Error building config: {config_build_err}"}
                continue
# END FUCKED UP CODE

            dest_log = dest_log.bind(actual_destination_type=actual_storage_config_dict.get('type','?'))
            dest_log.debug(f"Attempting destination type: {actual_storage_config_dict.get('type')}")

            try:
                storage_backend = get_storage_backend(actual_storage_config_dict)
                filename_context = f"{instance_uid_str}.dcm"
                store_result: Any = None

                if actual_storage_config_dict.get('type') == 'google_healthcare':
                    if asyncio.iscoroutinefunction(storage_backend.store):
                        try:
                            store_result = asyncio.run(storage_backend.store(
                                dataset_to_send,
                                original_filepath=original_filepath,
                                filename_context=filename_context,
                                source_identifier=source_identifier_for_matching
                            ))
                        except RuntimeError as e:
                            dest_log.error("Failed to run async GHC store via asyncio.run", error=str(e), exc_info=True)
                            raise StorageBackendError(f"RuntimeError running async GHC store: {e}") from e
                    else:
                        store_result = storage_backend.store(
                           dataset_to_send,
                           original_filepath=original_filepath,
                           filename_context=filename_context,
                           source_identifier=source_identifier_for_matching
                        )
                else:
                    store_result = storage_backend.store(
                       dataset_to_send,
                       original_filepath=original_filepath,
                       filename_context=filename_context,
                       source_identifier=source_identifier_for_matching
                    )

                if store_result == "duplicate":
                    success_status[db_storage_backend_model.name] = {"status": "duplicate", "result": store_result}
                else:
                    success_status[db_storage_backend_model.name] = {"status": "success", "result": store_result}

            except StorageBackendError as e:
                dest_log.error("Failed to store to destination", error=str(e), exc_info=False)
                all_destinations_succeeded = False
                success_status[db_storage_backend_model.name] = {"status": "error", "message": str(e)}
                if any(isinstance(e, exc_type) for exc_type in RETRYABLE_EXCEPTIONS): 
                    raise
            except Exception as e:
                 dest_log.error("Unexpected error during storage to destination", error=str(e), exc_info=True)
                 all_destinations_succeeded = False
                 success_status[db_storage_backend_model.name] = {"status": "error", "message": f"Unexpected error: {e}"}

        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed and stored successfully to {len(unique_destination_identifier_dicts)} destination(s)."
            if settings.DELETE_ON_SUCCESS:
                 try: 
                     original_filepath.unlink(missing_ok=True)
                 except OSError as e: 
                     log.warning("Failed to delete original file", error=str(e))
            
            local_db = None
            try:
                local_db = SessionLocal()
                processed_incremented = False
                if source_type == ProcessedStudySourceType.DIMSE_LISTENER.value and isinstance(source_db_id_or_instance_id, str):
                    processed_incremented = crud.crud_dimse_listener_state.increment_processed_count(db=local_db, listener_id=source_db_id_or_instance_id, count=1)
                elif source_type == ProcessedStudySourceType.DIMSE_QR.value and isinstance(source_db_id_or_instance_id, int):
                     processed_incremented = crud.crud_dimse_qr_source.increment_processed_count(db=local_db, source_id=source_db_id_or_instance_id, count=1)
                
                if processed_incremented: 
                    local_db.commit()
                else: 
                    log.warning("Failed to increment processed count for source.")
            except Exception as e:
                log.error("DB Error incrementing processed count for source", error=str(e), exc_info=True)
                if local_db and local_db.is_active: 
                    local_db.rollback()
            finally:
                if local_db: 
                    local_db.close()
        else:
            failed_count = sum(1 for status_info in success_status.values() if status_info['status'] == 'error')
            log.warning("Some destinations failed", failed_count=failed_count)
            if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
                 move_to_error_dir(original_filepath, task_id)
            final_status = "partial_failure"
            final_message = f"Processing complete, but {failed_count} destination(s) failed."

        db.close()
        serializable_success_status = {}
        for k, v in success_status.items():
            result_val = v.get("result")
            if isinstance(result_val, (dict, str, int, float, list, bool, type(None))):
                 serializable_success_status[k] = v
            elif isinstance(result_val, Path):
                 serializable_success_status[k] = {"status": v["status"], "result": str(result_val)}
            else:
                 serializable_success_status[k] = {"status": v["status"], "result": f"<{type(result_val).__name__}>"}

        return {
            "status": final_status,
            "message": final_message,
            "applied_rules": applied_rules_info,
            "destinations": serializable_success_status,
            "source": log_source_identifier,
            "instance_uid": instance_uid_str,
            "source_type": source_type,
            "source_id": source_db_id_or_instance_id
        }
    except Exception as exc:
        log.error("Unhandled exception during task execution", error=str(exc), exc_info=True)
        if original_filepath and original_filepath.exists():
            move_to_error_dir(original_filepath, task_id)
        final_status = "error"
        final_message = f"Fatal error during processing: {exc!r}"
        if db and db.is_active:
             try: 
                 db.rollback()
             except Exception as rb_e: 
                 logger.error("Error rolling back DB session.", task_id=task_id, rollback_error=str(rb_e))
             finally: 
                 db.close()

        if any(isinstance(exc, exc_type) for exc_type in RETRYABLE_EXCEPTIONS):
             raise
        else:
            return {
                "status": final_status,
                "message": final_message,
                "filepath": dicom_filepath_str,
                "source": log_source_identifier,
                "instance_uid": instance_uid_str,
                "source_type": source_type,
                "source_id": source_db_id_or_instance_id
                }
    finally:
        if db and db.is_active:
            db.close()
        log.info("Finished task processing.", final_status=final_status, final_message=final_message)


@shared_task(bind=True, name="process_dicomweb_metadata_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicomweb_metadata_task(self, source_id: int, study_uid: str, series_uid: str, instance_uid: str):
    task_id = self.request.id
    log = logger.bind(
        task_id=task_id,
        task_name=self.name,
        source_id=source_id,
        study_uid=study_uid,
        series_uid=series_uid,
        instance_uid=instance_uid,
        source_type=ProcessedStudySourceType.DICOMWEB_POLLER.value
    )

    db: Optional[Session] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    original_ds: Optional[pydicom.Dataset] = None
    source_identifier_for_matching: str = f"DICOMweb_Source_{source_id}"
    applied_rules_info: List[str] = []

    try:
        db = SessionLocal()
        source_config = crud.crud_dicomweb_source.get(db, id=source_id)
        if not source_config:
            return {
                "status": "error",
                "message": f"Configuration for source ID {source_id} not found.",
                "instance_uid": instance_uid,
                "source_id": source_id
                }

        source_identifier_for_matching = source_config.name
        log = log.bind(source_name=source_identifier_for_matching)
        
        try:
            client = dicomweb_client
            dicom_metadata = client.retrieve_instance_metadata(
                config=source_config, study_uid=study_uid, series_uid=series_uid, instance_uid=instance_uid
            )
            if dicom_metadata is None:
                 db.close()
                 return {
                     "status": "success",
                     "message": "Instance metadata not found (likely deleted).",
                     "source": source_identifier_for_matching,
                     "instance_uid": instance_uid,
                     "source_id": source_id
                     }
        except Exception as fetch_exc:
             raise fetch_exc

        try:
            original_ds = pydicom.dataset.Dataset.from_json(dicom_metadata)
            file_meta = pydicom.dataset.FileMetaDataset()
            file_meta.FileMetaInformationVersion = b'\x00\x01'
            sop_class_uid_data = original_ds.get("SOPClassUID")
            sop_class_uid_val = getattr(sop_class_uid_data, 'value', ['1.2.840.10008.5.1.4.1.1.2'])[0]
            file_meta.MediaStorageSOPClassUID = sop_class_uid_val
            file_meta.MediaStorageSOPInstanceUID = instance_uid
            file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian
            file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
            file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
            original_ds.file_meta = file_meta
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to parse retrieved metadata: {e}",
                "source": source_identifier_for_matching,
                "instance_uid": instance_uid,
                "source_id": source_id
                }

        if original_ds is None:
             return {
                 "status": "error",
                 "message": "Dataset creation failed unexpectedly.",
                 "source": source_identifier_for_matching,
                 "instance_uid": instance_uid,
                 "source_id": source_id
                 }

        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             db.close()
             return {
                 "status": "success",
                 "message": "No active rulesets found",
                 "source": source_identifier_for_matching,
                 "instance_uid": instance_uid,
                 "source_id": source_id
                 }

        try:
            modified_ds, applied_rules_info, unique_destination_identifier_dicts = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier_for_matching
            )
        except Exception as proc_exc:
             db.close()
             return {
                 "status": "error",
                 "message": f"Error during rule processing: {proc_exc}",
                 "source": source_identifier_for_matching,
                 "instance_uid": instance_uid,
                 "source_id": source_id
                 }

        log = log.bind(applied_rules_count=len(applied_rules_info), applied_rules=applied_rules_info)

        if not applied_rules_info:
             db.close()
             return {
                 "status": "success",
                 "message": f"No matching rules found for source '{source_identifier_for_matching}'",
                 "source": source_identifier_for_matching,
                 "instance_uid": instance_uid,
                 "source_id": source_id
                 }

        log = log.bind(unique_destination_count=len(unique_destination_identifier_dicts))

        if not unique_destination_identifier_dicts:
            db.close()
            return {
                "status": "success",
                "message": "Rules matched, modifications applied (if any), but no destinations configured",
                "applied_rules": applied_rules_info,
                "source": source_identifier_for_matching,
                "instance_uid": instance_uid,
                "source_id": source_id
                }

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        all_destinations_succeeded = True
        for i, dest_identifier_dict in enumerate(unique_destination_identifier_dicts):
            dest_id_from_dict = dest_identifier_dict.get("id")
            dest_name_from_dict = dest_identifier_dict.get("name", "UnknownName")
            
            dest_log = log.bind(
                destination_index=i+1,
                destination_id_from_dict=dest_id_from_dict,
                destination_name_from_dict=dest_name_from_dict
            )

            if dest_id_from_dict is None:
                dest_log.error("Destination identifier dict is missing 'id'. Skipping.")
                all_destinations_succeeded = False
                success_status[f"Dest_{i+1}_Malformed"] = {"status": "error", "message": "Missing ID"}
                continue

            db_storage_backend_model: Optional[DBStorageBackendConfigModel] = crud.crud_storage_backend_config.get(db, id=dest_id_from_dict)

            if not db_storage_backend_model:
                dest_log.error("Could not find storage backend config in DB.", target_db_id=dest_id_from_dict)
                all_destinations_succeeded = False
                success_status[f"Dest_{i+1}_{dest_name_from_dict}"] = {"status": "error", "message": f"DB config not found for ID {dest_id_from_dict}"}
                continue

            if not db_storage_backend_model.is_enabled:
                dest_log.info("Storage backend config is disabled in DB. Skipping.", target_db_id=dest_id_from_dict, name=db_storage_backend_model.name)
                success_status[f"Dest_{i+1}_{db_storage_backend_model.name}"] = {"status": "skipped_disabled", "message": f"Destination {db_storage_backend_model.name} is disabled."}
                continue

            actual_storage_config_dict = {}

            try:
                # 1. Resolve backend_type to a string
                backend_type_value_from_db_model = db_storage_backend_model.backend_type
                resolved_type_str: Optional[str] = None

                if isinstance(backend_type_value_from_db_model, str):
                    resolved_type_str = backend_type_value_from_db_model
                elif hasattr(backend_type_value_from_db_model, 'value') and isinstance(getattr(backend_type_value_from_db_model, 'value', None), str):
                    resolved_type_str = backend_type_value_from_db_model.value
                else:
                    dest_log.error("DB model 'backend_type' is invalid or not a string/enum.value.",
                                   raw_backend_type=backend_type_value_from_db_model,
                                   type_of_raw=type(backend_type_value_from_db_model).__name__)
                    all_destinations_succeeded = False
                    success_status[db_storage_backend_model.name] = {"status": "error", "message": "Invalid backend_type format from DB."}
                    continue 

                if resolved_type_str is None: 
                    dest_log.error("Failed to resolve backend_type for config dict construction.")
                    all_destinations_succeeded = False
                    success_status[db_storage_backend_model.name] = {"status": "error", "message": "Internal error resolving backend_type."}
                    continue
                
                actual_storage_config_dict['type'] = resolved_type_str
                actual_storage_config_dict['name'] = db_storage_backend_model.name # Common field, might be used by backends

                # 2. Populate type-specific fields by DIRECTLY accessing attributes
                #    of db_storage_backend_model, which is an instance of the correct subclass.
                if resolved_type_str == "filesystem":
                    if isinstance(db_storage_backend_model, FileSystemBackendConfig):
                        actual_storage_config_dict['path'] = db_storage_backend_model.path
                    else: # Should not happen if polymorphic loading works and types match
                        dest_log.error("Type mismatch: expected FileSystemBackendConfig.", model_type=type(db_storage_backend_model).__name__)
                        all_destinations_succeeded = False; success_status[db_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue
                
                elif resolved_type_str == "gcs":
                    if isinstance(db_storage_backend_model, GcsBackendConfig):
                        actual_storage_config_dict['bucket'] = db_storage_backend_model.bucket
                        actual_storage_config_dict['prefix'] = db_storage_backend_model.prefix
                    else:
                        dest_log.error("Type mismatch: expected GcsBackendConfig.", model_type=type(db_storage_backend_model).__name__)
                        all_destinations_succeeded = False; success_status[db_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue

                elif resolved_type_str == "cstore":
                    if isinstance(db_storage_backend_model, CStoreBackendConfig):
                        actual_storage_config_dict['remote_ae_title'] = db_storage_backend_model.remote_ae_title
                        actual_storage_config_dict['remote_host'] = db_storage_backend_model.remote_host
                        actual_storage_config_dict['remote_port'] = db_storage_backend_model.remote_port
                        actual_storage_config_dict['local_ae_title'] = db_storage_backend_model.local_ae_title
                        actual_storage_config_dict['tls_enabled'] = db_storage_backend_model.tls_enabled
                        actual_storage_config_dict['tls_ca_cert_secret_name'] = db_storage_backend_model.tls_ca_cert_secret_name
                        actual_storage_config_dict['tls_client_cert_secret_name'] = db_storage_backend_model.tls_client_cert_secret_name
                        actual_storage_config_dict['tls_client_key_secret_name'] = db_storage_backend_model.tls_client_key_secret_name
                    else:
                        dest_log.error("Type mismatch: expected CStoreBackendConfig.", model_type=type(db_storage_backend_model).__name__)
                        all_destinations_succeeded = False; success_status[db_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue

                elif resolved_type_str == "google_healthcare":
                    if isinstance(db_storage_backend_model, GoogleHealthcareBackendConfig):
                        actual_storage_config_dict['gcp_project_id'] = db_storage_backend_model.gcp_project_id
                        actual_storage_config_dict['gcp_location'] = db_storage_backend_model.gcp_location
                        actual_storage_config_dict['gcp_dataset_id'] = db_storage_backend_model.gcp_dataset_id
                        actual_storage_config_dict['gcp_dicom_store_id'] = db_storage_backend_model.gcp_dicom_store_id
                    else:
                        dest_log.error("Type mismatch: expected GoogleHealthcareBackendConfig.", model_type=type(db_storage_backend_model).__name__)
                        all_destinations_succeeded = False; success_status[db_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue
                        
                elif resolved_type_str == "stow_rs":
                     if isinstance(db_storage_backend_model, StowRsBackendConfig):
                        actual_storage_config_dict['base_url'] = db_storage_backend_model.base_url
                     else:
                        dest_log.error("Type mismatch: expected StowRsBackendConfig.", model_type=type(db_storage_backend_model).__name__)
                        all_destinations_succeeded = False; success_status[db_model.name] = {"status": "error", "message": "Internal model type mismatch."}; continue
                else:
                    dest_log.error("Unknown resolved_type_str for building specific config.", type_val=resolved_type_str)
                    all_destinations_succeeded = False
                    success_status[db_storage_backend_model.name] = {"status": "error", "message": f"Unknown type '{resolved_type_str}' for config construction."}
                    continue

            except AttributeError as attr_err: 
                dest_log.error("Attribute error building specific config dict from DB model. This means a field expected for the backend type was missing on the model instance.", error=str(attr_err), exc_info=True)
                all_destinations_succeeded = False
                success_status[db_storage_backend_model.name] = {"status": "error", "message": f"Attribute error: {attr_err}"}
                continue

            except Exception as config_build_err:
                dest_log.error("Generic error building specific config dict from DB model.", error=str(config_build_err), exc_info=True)
                all_destinations_succeeded = False
                success_status[db_storage_backend_model.name] = {"status": "error", "message": f"Error building config: {config_build_err}"}
                continue

            dest_log = dest_log.bind(actual_destination_type=actual_storage_config_dict.get('type','?'))
            dest_log.debug(f"Attempting destination type: {actual_storage_config_dict.get('type')}")
            
            try:
                storage_backend = get_storage_backend(actual_storage_config_dict)
                store_result: Any = None
                if actual_storage_config_dict.get('type') == 'google_healthcare':
                    if asyncio.iscoroutinefunction(storage_backend.store):
                        try: 
                            store_result = asyncio.run(storage_backend.store(dataset_to_send,None,f"{instance_uid}.dcm",source_identifier_for_matching))
                        except RuntimeError as e: 
                            raise StorageBackendError(f"Async GHC store error: {e}") from e
                    else: 
                        store_result = storage_backend.store(dataset_to_send,None,f"{instance_uid}.dcm",source_identifier_for_matching)
                else: 
                    store_result = storage_backend.store(dataset_to_send,None,f"{instance_uid}.dcm",source_identifier_for_matching)
                
                success_status[db_storage_backend_model.name] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                all_destinations_succeeded = False
                success_status[db_storage_backend_model.name] = {"status": "error", "message": str(e)}
                if any(isinstance(e, exc_type) for exc_type in RETRYABLE_EXCEPTIONS): 
                    raise
            except Exception as e:
                 all_destinations_succeeded = False
                 success_status[db_storage_backend_model.name] = {"status": "error", "message": f"Unexpected error: {e}"}

        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed and dispatched to {len(unique_destination_identifier_dicts)} destination(s)."
            local_db = None
            try:
                local_db = SessionLocal()
                if crud.dicomweb_state.increment_processed_count(db=local_db, source_name=source_identifier_for_matching, count=1):
                    local_db.commit()
                else: 
                    log.warning("Failed to increment processed count for DICOMweb source.")
            except Exception as e:
                if local_db and local_db.is_active: 
                    local_db.rollback()
            finally:
                if local_db: 
                    local_db.close()
        else:
            failed_count = sum(1 for si in success_status.values() if si['status'] == 'error')
            final_status = "partial_failure"
            final_message = f"Processing complete, but {failed_count} destination(s) failed."

        db.close()
        serializable_success_status = {k: ({"status": v["status"], "result": str(v["result"])} if isinstance(v.get("result"), Path) else {"status": v["status"], "result": f"<{type(v.get('result')).__name__}>"} if not isinstance(v.get("result"), (dict, str, int, float, list, bool, type(None))) else v) for k, v in success_status.items()}
        return {
            "status": final_status, "message": final_message, "applied_rules": applied_rules_info,
            "destinations": serializable_success_status, "source": source_identifier_for_matching,
            "instance_uid": instance_uid, "source_id": source_id
            }
    except Exception as exc:
        if db and db.is_active:
            try: 
                db.rollback()
            except Exception as rb_e: 
                logger.error("DB rollback error.", task_id=task_id, rb_error=str(rb_e))
            finally: 
                db.close()
        if any(isinstance(exc, exc_type) for exc_type in RETRYABLE_EXCEPTIONS): 
            raise
        else:
             return {"status": "error", "message": f"Fatal error: {exc!r}", "source_id": source_id, "instance_uid": instance_uid}
    finally:
        if db and db.is_active: 
            db.close()
        log.info("Finished task.", final_status=final_status)

@shared_task(bind=True, name="process_google_healthcare_metadata_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
async def process_google_healthcare_metadata_task(self, source_id: int, study_uid: str):
    task_id = self.request.id
    log = logger.bind(task_id=task_id, task_name=self.name, source_id=source_id, study_uid=study_uid, source_type=ProcessedStudySourceType.GOOGLE_HEALTHCARE.value)

    db: Optional[Session] = None
    final_status = "unknown"; final_message = "Task did not complete."
    success_status = {}; original_ds: Optional[pydicom.Dataset] = None
    source_identifier_for_matching: str = f"GoogleHealthcare_{source_id}"
    instance_uid_str: str = "Unknown"; applied_rules_info: List[str] = []

    try:
        db = SessionLocal()
        source_config_model = crud.google_healthcare_source.get(db, id=source_id)
        if not source_config_model:
            return {"status": "error", "message": f"Config for source ID {source_id} not found.", "study_uid": study_uid, "source_id": source_id}

        source_identifier_for_matching = source_config_model.name
        log = log.bind(source_name=source_identifier_for_matching)

        ghc_backend: Optional[GoogleHealthcareDicomStoreStorage] = None
        dicom_metadata: Optional[Dict[str, Any]] = None
        try:
            backend_config_dict = {
                "type": "google_healthcare", "name": f"Processor_{source_config_model.name}",
                "gcp_project_id": source_config_model.gcp_project_id, "gcp_location": source_config_model.gcp_location,
                "gcp_dataset_id": source_config_model.gcp_dataset_id, "gcp_dicom_store_id": source_config_model.gcp_dicom_store_id,
            }
            ghc_backend = GoogleHealthcareDicomStoreStorage(config=backend_config_dict)
            await ghc_backend.initialize_client()
            series_list = await ghc_backend.search_series(study_instance_uid=study_uid, limit=1)
            if not series_list:
                db.close()
                return {"status": "success", "message": "No series found.", "source": source_identifier_for_matching, "study_uid": study_uid, "source_id": source_id}
            
            first_series_uid = series_list[0].get("0020000E", {}).get("Value", [None])[0]
            if not first_series_uid:
                db.close()
                return {"status": "error", "message": "Failed to get SeriesUID.", "source": source_identifier_for_matching, "study_uid": study_uid, "source_id": source_id}

            log = log.bind(series_uid=first_series_uid)
            dicom_metadata_list = await ghc_backend.search_instances(study_instance_uid=study_uid, series_instance_uid=first_series_uid, limit=1)
            if not dicom_metadata_list:
                 db.close()
                 return {"status": "success", "message": "Instance metadata not found.", "source": source_identifier_for_matching, "study_uid": study_uid, "series_uid":first_series_uid, "source_id": source_id}
            
            dicom_metadata = dicom_metadata_list[0]
            instance_uid_str = dicom_metadata.get("00080018", {}).get("Value", ["Unknown"])[0]
            log = log.bind(instance_uid=instance_uid_str)
        except Exception as fetch_exc: 
            raise fetch_exc

        try:
            original_ds = pydicom.dataset.Dataset.from_json(dicom_metadata)
            fm = pydicom.dataset.FileMetaDataset()
            fm.FileMetaInformationVersion=b'\x00\x01'
            fm.MediaStorageSOPClassUID=getattr(original_ds.get("SOPClassUID"),'value',['1.2.840.10008.5.1.4.1.1.2'])[0]
            fm.MediaStorageSOPInstanceUID=instance_uid_str
            fm.TransferSyntaxUID=pydicom.uid.ImplicitVRLittleEndian
            fm.ImplementationClassUID=pydicom.uid.PYDICOM_IMPLEMENTATION_UID
            fm.ImplementationVersionName=f"pydicom {pydicom.__version__}"
            original_ds.file_meta = fm
        except Exception as e: 
            db.close()
            return {"status":"error","message":f"Failed to parse metadata: {e}","source":source_identifier_for_matching,"study_uid":study_uid,"instance_uid":instance_uid_str,"source_id":source_id}
        
        if original_ds is None: 
            db.close()
            return {"status":"error","message":"Dataset creation failed.","source":source_identifier_for_matching,"study_uid":study_uid,"instance_uid":instance_uid_str,"source_id":source_id}

        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets: 
            db.close()
            return {"status":"success","message":"No active rulesets.","source":source_identifier_for_matching,"study_uid":study_uid,"instance_uid":instance_uid_str,"source_id":source_id}

        try: 
            modified_ds, applied_rules_info, unique_destination_identifier_dicts = process_dicom_instance(original_ds,active_rulesets,source_identifier_for_matching)
        except Exception as proc_exc: 
            db.close()
            return {"status":"error","message":f"Rule processing error: {proc_exc}","source":source_identifier_for_matching,"study_uid":study_uid,"instance_uid":instance_uid_str,"source_id":source_id}

        log = log.bind(applied_rules_count=len(applied_rules_info), applied_rules=applied_rules_info)
        if not applied_rules_info: 
            db.close()
            return {"status":"success","message":f"No matching rules for '{source_identifier_for_matching}'.","source":source_identifier_for_matching,"study_uid":study_uid,"instance_uid":instance_uid_str,"source_id":source_id}
        
        log = log.bind(unique_destination_count=len(unique_destination_identifier_dicts))
        if not unique_destination_identifier_dicts: 
            db.close()
            return {"status":"success","message":"Rules matched, no destinations.","applied_rules":applied_rules_info,"source":source_identifier_for_matching,"study_uid":study_uid,"instance_uid":instance_uid_str,"source_id":source_id}

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        all_destinations_succeeded = True
        
        destination_tasks = []
        destination_full_configs_for_tasks = []
        for i, dest_identifier_dict in enumerate(unique_destination_identifier_dicts):
            dest_id = dest_identifier_dict.get("id")
            dest_name = dest_identifier_dict.get("name", "Unknown")
            tdl = log.bind(dest_idx=i+1, dest_id=dest_id, dest_name=dest_name) # temp dest logger
            
            if dest_id is None: 
                tdl.error("Dest ID missing.")
                all_destinations_succeeded = False
                success_status[f"Dest_{i+1}_Malformed"]={"status":"error","message":"Missing ID"}
                continue
            
            db_model = crud.crud_storage_backend_config.get(db,id=dest_id)
            if not db_model: 
                tdl.error("DB config not found.")
                all_destinations_succeeded = False
                success_status[f"Dest_{i+1}_{dest_name}"]={"status":"error","message":f"DB config ID {dest_id} not found"}
                continue
            if not db_model.is_enabled: 
                tdl.info("Dest disabled in DB.")
                success_status[f"Dest_{i+1}_{db_model.name}"]={"status":"skipped_disabled","message":"Disabled."}
                continue

            cfg_dict = {}
            try:
                raw_type = db_model.backend_type
                resolved_type:Optional[str]=None
                if isinstance(raw_type,str): 
                    resolved_type=raw_type
                elif hasattr(raw_type,'value') and isinstance(getattr(raw_type,'value',None),str): 
                    resolved_type=raw_type.value
                else: 
                    tdl.error("Invalid DB backend_type.", raw=raw_type)
                    all_destinations_succeeded=False
                    success_status[db_model.name]={"status":"error","message":"Invalid DB type."}
                    continue
                
                if resolved_type is None: 
                    tdl.error("Failed resolve backend_type.")
                    all_destinations_succeeded=False
                    success_status[db_model.name]={"status":"error","message":"Resolve type error."}
                    continue
                
                cfg_dict['type']=resolved_type
                if isinstance(db_model.config,dict): 
                    cfg_dict.update(db_model.config)
                    cfg_dict['name']=db_model.name # Name needed for GHC backend init maybe?
                else: 
                    tdl.error("DB config field not dict.",type=type(db_model.config).__name__)
                    all_destinations_succeeded=False
                    success_status[db_model.name]={"status":"error","message":"DB config not dict."}
                    continue
                
                destination_full_configs_for_tasks.append(cfg_dict)
            except Exception as cb_err: 
                tdl.error("Failed build storage config.", error=str(cb_err),exc_info=True)
                all_destinations_succeeded=False
                success_status[db_model.name]={"status":"error","message":f"Build error: {cb_err}"}
                continue

        for full_cfg in destination_full_configs_for_tasks:
            be = get_storage_backend(full_cfg)
            fname_ctx = f"{instance_uid_str or study_uid}.dcm"
            destination_tasks.append(be.store(dataset_to_send,None,fname_ctx,source_identifier_for_matching))
        
        task_results = []
        if destination_tasks: 
            task_results = await asyncio.gather(*destination_tasks, return_exceptions=True)
        elif not all_destinations_succeeded: 
            log.warning("No dest tasks due to prior errors.")
        else: 
            log.info("No active/valid dest tasks for GHC metadata.")

        for i, res_or_exc in enumerate(task_results):
            cur_cfg = destination_full_configs_for_tasks[i]
            name_log = cur_cfg.get("name",f"Dest_{i+1}")
            dl = log.bind(dest_idx=i+1, dest_name=name_log, dest_type=cur_cfg.get('type')) # dest logger
            
            if isinstance(res_or_exc, Exception):
                 err=res_or_exc
                 dl.error("Failed to store",error=str(err),exc_info=isinstance(err,StorageBackendError))
                 all_destinations_succeeded=False
                 success_status[name_log]={"status":"error","message":str(err)}
                 if any(isinstance(err,et) for et in RETRYABLE_EXCEPTIONS): 
                     raise err
            else:
                 res=res_or_exc
                 if res=="duplicate": 
                     success_status[name_log]={"status":"duplicate","result":res}
                 else: 
                     success_status[name_log]={"status":"success","result":res}
        
        if all_destinations_succeeded and not unique_destination_identifier_dicts and not destination_tasks and not success_status:
            final_status="success"
            final_message="Processed GHC metadata. All dests skipped."
        if all_destinations_succeeded and destination_tasks:
            final_status="success"
            final_message=f"Processed GHC metadata and dispatched to {len(destination_tasks)} dests."
        elif not all_destinations_succeeded:
            fc = sum(1 for s in success_status.values() if s['status']=='error')
            final_status="partial_failure"
            final_message=f"Processing GHC metadata, {fc} dest(s) failed."
        
        db.close()
        serializable_status = {k: ({"status":v["status"],"result":str(v["result"])} if isinstance(v.get("result"),Path) else {"status":v["status"],"result":f"<{type(v.get('result')).__name__}>"} if not isinstance(v.get("result"),(dict,str,int,float,list,bool,type(None))) else v) for k,v in success_status.items()}
        return {"status":final_status,"message":final_message,"applied_rules":applied_rules_info,"destinations":serializable_status,"source":source_identifier_for_matching,"study_uid":study_uid,"instance_uid":instance_uid_str,"source_id":source_id}
    except Exception as exc:
        if db and db.is_active:
            try: 
                db.rollback()
            except Exception as rb_e: 
                logger.error("DB rollback error.", task_id=task_id, rb_error=str(rb_e))
            finally: 
                db.close()
        if any(isinstance(exc,et) for et in RETRYABLE_EXCEPTIONS): 
            raise
        else: 
            return {"status":"error","message":f"Fatal error: {exc!r}","source_id":source_id,"study_uid":study_uid,"instance_uid":instance_uid_str}
    finally:
        if db and db.is_active: 
            db.close()
        log.info("Finished GHC task.", final_status=final_status)

@shared_task(bind=True, name="process_stow_instance_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_stow_instance_task(self, temp_filepath_str: str, source_ip: Optional[str] = None):
    task_id = self.request.id
    temp_filepath = Path(temp_filepath_str)
    source_identifier_for_matching = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}"
    log = logger.bind(task_id=task_id, task_name=self.name, temp_filepath=temp_filepath_str, source_ip=source_ip, source_identifier=source_identifier_for_matching, source_type=ProcessedStudySourceType.STOW_RS.value)

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    instance_uid_str = "Unknown"
    applied_rules_info: List[str] = []

    try:
        db = SessionLocal()
        try:
            original_ds = pydicom.dcmread(str(temp_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            log = log.bind(instance_uid=instance_uid_str)
        except InvalidDicomError as e: 
            return {"status":"error","message":f"Invalid DICOM: {e}","filepath":temp_filepath_str,"source":source_identifier_for_matching,"instance_uid":instance_uid_str}
        except Exception as read_exc: 
            raise read_exc

        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             final_status="success"
             final_message="No active rulesets"
             db.close()
             if temp_filepath.exists(): 
                 try: 
                     os.remove(temp_filepath) 
                 except OSError as e: 
                     log.error("Failed to remove temp (no rules).",e=str(e))
             return {"status":final_status,"message":final_message,"source":source_identifier_for_matching,"instance_uid":instance_uid_str}
        
        try: 
            modified_ds, applied_rules_info, unique_destination_identifier_dicts = process_dicom_instance(original_ds,active_rulesets,source_identifier_for_matching)
        except Exception as proc_exc:
             db.close()
             if temp_filepath.exists(): 
                 try: 
                     os.remove(temp_filepath) 
                 except OSError as e: 
                     log.error("Failed to remove temp (proc error).",e=str(e))
             return {"status":"error","message":f"Rule processing error: {proc_exc}","source":source_identifier_for_matching,"instance_uid":instance_uid_str}

        log = log.bind(applied_rules_count=len(applied_rules_info), applied_rules=applied_rules_info)
        if not applied_rules_info:
             final_status="success"
             final_message="No matching rules."
             db.close()
             if temp_filepath.exists(): 
                 try: 
                     os.remove(temp_filepath) 
                 except OSError as e: 
                     log.error("Failed to remove temp (no match).",e=str(e))
             return {"status":final_status,"message":final_message,"source":source_identifier_for_matching,"instance_uid":instance_uid_str}

        log = log.bind(unique_destination_count=len(unique_destination_identifier_dicts))
        if not unique_destination_identifier_dicts:
            final_status="success"
            final_message="Rules matched, no destinations."
            db.close()
            if temp_filepath.exists(): 
                try: 
                    os.remove(temp_filepath) 
                except OSError as e: 
                    log.error("Failed to remove temp (no dest).",e=str(e))
            return {"status":final_status,"message":final_message,"applied_rules":applied_rules_info,"source":source_identifier_for_matching,"instance_uid":instance_uid_str}

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        all_destinations_succeeded = True
        for i, dest_identifier_dict in enumerate(unique_destination_identifier_dicts):
            dest_id = dest_identifier_dict.get("id")
            dest_name = dest_identifier_dict.get("name", "Unknown")
            dl = log.bind(dest_idx=i+1, dest_id=dest_id, dest_name=dest_name) # dest logger
            
            if dest_id is None: 
                dl.error("Dest ID missing.")
                all_destinations_succeeded = False
                success_status[f"D_{i+1}_Malformed"] = {"status":"error","message":"Missing ID"}
                continue
            
            db_model = crud.crud_storage_backend_config.get(db,id=dest_id)
            if not db_model: 
                dl.error("DB config not found.")
                all_destinations_succeeded = False
                success_status[f"D_{i+1}_{dest_name}"] = {"status":"error","message":f"DB config ID {dest_id} not found"}
                continue
            if not db_model.is_enabled: 
                dl.info("Dest disabled.")
                success_status[f"D_{i+1}_{db_model.name}"] = {"status":"skipped_disabled","message":"Disabled."}
                continue

            cfg_dict = {}
            try:
                raw_type = db_model.backend_type
                resolved_type:Optional[str]=None
                if isinstance(raw_type,str): 
                    resolved_type = raw_type
                elif hasattr(raw_type,'value') and isinstance(getattr(raw_type,'value',None),str): 
                    resolved_type = raw_type.value
                else: 
                    dl.error("Invalid DB backend_type.", raw=raw_type)
                    all_destinations_succeeded = False
                    success_status[db_model.name] = {"status":"error","message":"Invalid DB type."}
                    continue
                
                if resolved_type is None: 
                    dl.error("Failed resolve backend_type.")
                    all_destinations_succeeded = False
                    success_status[db_model.name] = {"status":"error","message":"Resolve type error."}
                    continue
                
                cfg_dict['type'] = resolved_type
                if isinstance(db_model.config,dict): 
                    cfg_dict.update(db_model.config)
                    # Add name if GHC? Other backends might need it too?
                    if resolved_type == "google_healthcare":
                         cfg_dict['name'] = db_model.name
                else: 
                    dl.error("DB config field not dict.",type=type(db_model.config).__name__)
                    all_destinations_succeeded = False
                    success_status[db_model.name] = {"status":"error","message":"DB config not dict."}
                    continue
            except Exception as cb_err: 
                dl.error("Failed build storage cfg.", error=str(cb_err),exc_info=True)
                all_destinations_succeeded = False
                success_status[db_model.name] = {"status":"error","message":f"Build error: {cb_err}"}
                continue
            
            dl = dl.bind(actual_destination_type=cfg_dict.get('type','?'))
            dl.debug(f"Attempting destination type: {cfg_dict.get('type')}")

            try:
                be = get_storage_backend(cfg_dict)
                fname_ctx = f"{instance_uid_str}.dcm"
                store_res: Any = None
                
                if cfg_dict.get('type') == 'google_healthcare':
                    if asyncio.iscoroutinefunction(be.store):
                        try: 
                            store_res = asyncio.run(be.store(dataset_to_send,temp_filepath,fname_ctx,source_identifier_for_matching))
                        except RuntimeError as e: 
                            raise StorageBackendError(f"Async GHC store error: {e}") from e
                    else: 
                        store_res = be.store(dataset_to_send,temp_filepath,fname_ctx,source_identifier_for_matching)
                else: 
                    store_res = be.store(dataset_to_send,temp_filepath,fname_ctx,source_identifier_for_matching)
                
                if store_res == "duplicate": 
                    success_status[db_model.name] = {"status":"duplicate","result":store_res}
                else: 
                    success_status[db_model.name] = {"status":"success","result":store_res}
            except StorageBackendError as e:
                dl.error("Failed to store STOW instance", error=str(e),exc_info=False)
                all_destinations_succeeded = False
                success_status[db_model.name] = {"status":"error","message":str(e)}
                if any(isinstance(e,et) for et in RETRYABLE_EXCEPTIONS): 
                    raise
            except Exception as e:
                 dl.error("Unexpected error storing STOW instance",error=str(e),exc_info=True)
                 all_destinations_succeeded = False
                 success_status[db_model.name] = {"status":"error","message":f"Unexpected: {e}"}

        if all_destinations_succeeded: 
            final_status="success"
            final_message=f"Processed STOW instance to {len(unique_destination_identifier_dicts)} dests."
        else: 
            fc=sum(1 for s in success_status.values() if s['status']=='error')
            final_status="partial_failure"
            final_message=f"Processing STOW, {fc} dests failed."
        
        db.close()
        serializable_status = {
            k: (
                {"status": v["status"], "result": str(v["result"])}
                if isinstance(v.get("result"), Path)
                else {
                    "status": v["status"],
                    "result": f"<{type(v.get('result')).__name__}>"
                }
                if not isinstance(
                    v.get("result"),
                    (dict, str, int, float, list, bool, type(None))
                )
                else v
            )
            for k, v in success_status.items()
        }

        return {
            "status": final_status,
            "message": final_message,
            "applied_rules": applied_rules_info,
            "destinations": serializable_success_status, # Use the clearly built dict
            "source": source_identifier_for_matching,
            "instance_uid": instance_uid_str
            # Any other relevant fields specific to the task (e.g., source_id, study_uid)
        }
    except Exception as exc:
        if db and db.is_active:
            try: 
                db.rollback()
            except Exception as rb_e: 
                logger.error("DB rollback error.", task_id=task_id, rb_error=str(rb_e))
            finally: 
                db.close()
        if any(isinstance(exc,et) for et in RETRYABLE_EXCEPTIONS): 
            raise
        else: 
            return {"status":"error","message":f"Fatal error: {exc!r}","source":source_identifier_for_matching,"instance_uid":instance_uid_str}
    finally:
        if temp_filepath.exists():
            try: 
                os.remove(temp_filepath)
            except OSError as e: 
                logger.critical("FAILED TO REMOVE temp STOW file",temp_filepath=str(temp_filepath),error=str(e),task_id=task_id)
        if db and db.is_active: 
            db.close()
        log.info("Finished STOW task.", final_status=final_status)
