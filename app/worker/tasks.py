# app/worker/tasks.py
import logging
import json
import os
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Union
from copy import deepcopy

from celery import shared_task
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.uid import generate_uid

from app.db.session import SessionLocal
from app import crud
from app.db import models
from app.db.models import ProcessedStudySourceType

from app.services.storage_backends import get_storage_backend, StorageBackendError
from app.services import dicomweb_client

from app.core.config import settings

from app.worker.processing_logic import process_dicom_instance

logger = logging.getLogger(__name__)


def move_to_error_dir(filepath: Path, task_id: str):
    if not isinstance(filepath, Path):
         filepath = Path(filepath)

    if not filepath.is_file():
         logger.warning(f"Task {task_id}: Source path {filepath} is not a file or does not exist. Cannot move to error dir.")
         return
    try:
        error_base_dir_str = getattr(settings, 'DICOM_ERROR_PATH', None)
        if error_base_dir_str:
             error_dir = Path(error_base_dir_str)
        else:
             storage_path = getattr(settings, 'DICOM_STORAGE_PATH', '/dicom_data/incoming')
             error_dir = Path(storage_path).parent / "errors"
             logger.warning(f"Task {task_id}: DICOM_ERROR_PATH not set in config, using fallback: {error_dir}")

        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        logger.info(f"Task {task_id}: Moved file {filepath.name} to {error_path}")
    except Exception as move_err:
        logger.critical(f"Task {task_id}: CRITICAL - Could not move file {filepath.name} to error dir '{error_dir}': {move_err}", exc_info=True)


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
    association_info: Optional[Dict[str, str]] = None # Add association_info
):
    """
    Celery task to process a DICOM file originally from filesystem/listener:
    reads file, fetches rules, calls processing logic, handles storage destinations, and cleans up.
    """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    log_source_identifier = f"{source_type}_{source_db_id_or_instance_id}" # For logging/fallback

    logger.info(f"Task {task_id}: Received request for file: {original_filepath} from source: {log_source_identifier} (Type: {source_type}, ID/Instance: {source_db_id_or_instance_id}), Assoc: {association_info}")

    if not original_filepath.is_file():
        logger.error(f"Task {task_id}: File not found or is not a file: {original_filepath}. Cannot process.")
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str, "source": log_source_identifier, "source_type": source_type, "source_id": source_db_id_or_instance_id}

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    instance_uid_str = "Unknown"
    source_identifier_for_matching = log_source_identifier # Default to fallback

    try:

        db = SessionLocal()

        # --- Determine the correct Source Identifier for Rule Matching ---
        try:
            source_type_enum = ProcessedStudySourceType(source_type) # Convert string to enum
            if source_type_enum == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(source_db_id_or_instance_id, str):
                listener_config = crud.crud_dimse_listener_config.get_by_instance_id(db, instance_id=source_db_id_or_instance_id)
                if listener_config:
                    source_identifier_for_matching = listener_config.name
                    logger.debug(f"Task {task_id}: Matched DIMSE Listener config name '{source_identifier_for_matching}' for instance ID '{source_db_id_or_instance_id}'.")
                else:
                    logger.warning(f"Task {task_id}: Could not find listener config for instance ID '{source_db_id_or_instance_id}'. Using default source identifier '{log_source_identifier}' for matching.")
            elif source_type_enum == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
                 qr_config = crud.crud_dimse_qr_source.get(db, id=source_db_id_or_instance_id)
                 if qr_config:
                      source_identifier_for_matching = qr_config.name
                      logger.debug(f"Task {task_id}: Matched DIMSE QR config name '{source_identifier_for_matching}' for source ID '{source_db_id_or_instance_id}'.")
                 else:
                     logger.warning(f"Task {task_id}: Could not find DIMSE QR config for source ID '{source_db_id_or_instance_id}'. Using default source identifier '{log_source_identifier}'.")
            # Add other source types (STOW_RS, Filesystem Watcher?) here if they need specific name lookups
            else:
                 logger.debug(f"Task {task_id}: Using default source identifier '{log_source_identifier}' for matching (Type: {source_type}, ID type: {type(source_db_id_or_instance_id)}).")
        except ValueError:
             logger.error(f"Task {task_id}: Invalid source_type received: '{source_type}'. Using fallback identifier.")
        except Exception as e:
             logger.error(f"Task {task_id}: Error looking up source config name: {e}. Using fallback identifier.")


        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            original_ds = pydicom.dcmread(str(original_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            logger.debug(f"Task {task_id}: Successfully read file. SOP Instance UID: {instance_uid_str}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath} (UID: {instance_uid_str}): {e}", exc_info=False)
            move_to_error_dir(original_filepath, task_id)
            return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
        except Exception as read_exc:
            logger.error(f"Task {task_id}: Error reading DICOM file {original_filepath} (UID: {instance_uid_str}): {read_exc}", exc_info=True)
            raise read_exc


        logger.debug(f"Task {task_id}: Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for instance {instance_uid_str} from {log_source_identifier}. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting file {original_filepath} as no rulesets were active (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete unmatched file {original_filepath}: {e}")
             else:
                 logger.info(f"Task {task_id}: Leaving file {original_filepath} as no rulesets were active (DELETE_UNMATCHED_FILES=false).")
             db.close()
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}


        logger.debug(f"Task {task_id}: Calling core processing logic using matching identifier '{source_identifier_for_matching}'...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier_for_matching, # Use the resolved identifier
                association_info=association_info # Pass association info through
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for {original_filepath.name} (UID: {instance_uid_str}): {proc_exc}", exc_info=True)
             move_to_error_dir(original_filepath, task_id)
             db.close()
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}


        if not applied_rules_info:
             logger.info(f"Task {task_id}: No applicable rules matched for source identifier '{source_identifier_for_matching}' on file {original_filepath.name}. No actions taken.")
             final_status = "success"
             final_message = f"No matching rules found for source '{source_identifier_for_matching}'"
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting file {original_filepath} as no rules matched this source (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete file {original_filepath} with no matching rules: {e}")
             else:
                 logger.info(f"Task {task_id}: Leaving file {original_filepath} as no rules matched this source (DELETE_UNMATCHED_FILES=false).")
             db.close()
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}


        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured in matched rules for {original_filepath.name}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            delete_original = modified_ds is not None and settings.DELETE_ON_NO_DESTINATION
            if delete_original:
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting original file {original_filepath} as rules matched (and modified?), but had no destinations (DELETE_ON_NO_DESTINATION=true).")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath} after processing with no destinations: {e}")
            else:
                logger.info(f"Task {task_id}: Leaving original file {original_filepath} as rules matched but had no destinations (DELETE_ON_NO_DESTINATION=false or no modification).")
            db.close()
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        logger.info(f"Task {task_id}: Processing {len(unique_destination_configs)} unique destinations for {original_filepath.name}...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                filename_context = f"{instance_uid_str}.dcm"

                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=original_filepath,
                    filename_context=filename_context,
                    source_identifier=source_identifier_for_matching # Pass matched identifier
                 )

                logger.info(f"Task {task_id}: Destination {dest_id} completed. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store to destination {dest_id}: {e}", exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                if isinstance(e, RETRYABLE_EXCEPTIONS): raise
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error during storage to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}


        if all_destinations_succeeded:
            logger.info(f"Task {task_id}: All {len(unique_destination_configs)} destinations succeeded.")
            if settings.DELETE_ON_SUCCESS:
                 logger.info(f"Task {task_id}: Deleting original file: {original_filepath} (DELETE_ON_SUCCESS=true)")
                 try: original_filepath.unlink(missing_ok=True)
                 except OSError as e: logger.warning(f"Task {task_id}: Failed to delete original file {original_filepath}: {e}")
            else:
                 logger.info(f"Task {task_id}: Leaving original file {original_filepath} as DELETE_ON_SUCCESS is false.")
            final_status = "success"
            final_message = f"Processed and stored successfully to {len(unique_destination_configs)} destination(s)."

            processed_incremented = False
            try:

                local_db = SessionLocal()
                if source_type == ProcessedStudySourceType.DIMSE_LISTENER.value and isinstance(source_db_id_or_instance_id, str):
                    logger.debug(f"Task {task_id}: Incrementing processed count for DIMSE Listener {source_db_id_or_instance_id}")
                    processed_incremented = crud.crud_dimse_listener_state.increment_processed_count(db=local_db, listener_id=source_db_id_or_instance_id, count=1)
                elif source_type == ProcessedStudySourceType.DIMSE_QR.value and isinstance(source_db_id_or_instance_id, int):
                     logger.debug(f"Task {task_id}: Incrementing processed count for DIMSE QR source ID {source_db_id_or_instance_id}")
                     processed_incremented = crud.crud_dimse_qr_source.increment_processed_count(db=local_db, source_id=source_db_id_or_instance_id, count=1)

                else:
                    logger.warning(f"Task {task_id}: Unknown source type '{source_type}' or mismatched ID type '{type(source_db_id_or_instance_id)}' for processed count increment.")

                if processed_incremented:
                    logger.info(f"Task {task_id}: Successfully incremented processed count for source {log_source_identifier}.")
                else:
                    logger.warning(f"Task {task_id}: Failed to increment processed count for source {log_source_identifier}.")
                local_db.close()
            except Exception as e:
                logger.error(f"Task {task_id}: DB Error incrementing processed count for source {log_source_identifier}: {e}")
                if 'local_db' in locals() and local_db.is_active: local_db.rollback(); local_db.close()


        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed. Original file NOT deleted: {original_filepath}")
            if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
                 move_to_error_dir(original_filepath, task_id)
            final_status = "partial_failure"
            final_message = f"Processing complete, but {failed_count} destination(s) failed."

        db.close()
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}

    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception processing {original_filepath.name}: {exc!r}", exc_info=True)
        if original_filepath.exists(): move_to_error_dir(original_filepath, task_id)
        final_status = "error"
        final_message = f"Fatal error during processing: {exc!r}"
        if db and db.is_active:
             try: db.rollback()
             except: pass
             finally: db.close()
        if isinstance(exc, RETRYABLE_EXCEPTIONS): raise
        else:
            return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": log_source_identifier, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
    finally:
        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (may indicate early exit).")
            db.close()
        logger.info(f"Task {task_id}: Finished processing {original_filepath.name}. Final Status: {final_status}.")



@shared_task(bind=True, name="process_dicomweb_metadata_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicomweb_metadata_task(self, source_id: int, study_uid: str, series_uid: str, instance_uid: str):
    task_id = self.request.id
    logger.info(f"Task {task_id}: Received request to process DICOMweb instance UID {instance_uid} from source ID {source_id}.")

    db: Optional[Session] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    original_ds: Optional[pydicom.Dataset] = None
    source_identifier_for_matching: str = f"DICOMweb_Source_{source_id}" # Default/fallback

    try:
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Fetching configuration for source ID {source_id}...")
        source_config = crud.dicomweb_source.get(db, id=source_id)
        if not source_config:
            logger.error(f"Task {task_id}: Could not find configuration for source ID {source_id}. Cannot fetch metadata.")
            return {"status": "error", "message": f"Configuration for source ID {source_id} not found.", "instance_uid": instance_uid, "source_id": source_id}
        # Use the actual configured name for rule matching
        source_identifier_for_matching = source_config.name

        logger.info(f"Task {task_id}: Fetching metadata for instance {instance_uid} from source '{source_identifier_for_matching}'...")
        try:
            client = dicomweb_client
            dicom_metadata = client.retrieve_instance_metadata(
                config=source_config,
                study_uid=study_uid,
                series_uid=series_uid,
                instance_uid=instance_uid
            )
            if dicom_metadata is None:
                 logger.warning(f"Task {task_id}: Metadata not found (or 404) for instance {instance_uid} at source '{source_identifier_for_matching}'. Skipping.")
                 db.close()
                 return {"status": "success", "message": "Instance metadata not found (likely deleted).", "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}
            logger.debug(f"Task {task_id}: Successfully retrieved metadata for {instance_uid}.")
        except Exception as fetch_exc:
             logger.error(f"Task {task_id}: Error fetching metadata for instance {instance_uid} from '{source_identifier_for_matching}': {fetch_exc}", exc_info=isinstance(fetch_exc, dicomweb_client.DicomWebClientError))
             raise fetch_exc


        logger.debug(f"Task {task_id}: Converting JSON metadata to pydicom Dataset...")
        try:
            original_ds = pydicom.dataset.Dataset.from_json(dicom_metadata)
            file_meta = pydicom.dataset.FileMetaDataset()
            file_meta.FileMetaInformationVersion = b'\x00\x01'
            sop_class_uid_val = getattr(original_ds.get("SOPClassUID", None), 'value', '1.2.840.10008.5.1.4.1.1.2')
            file_meta.MediaStorageSOPClassUID = sop_class_uid_val
            file_meta.MediaStorageSOPInstanceUID = instance_uid
            file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian
            file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
            file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
            original_ds.file_meta = file_meta
        except Exception as e:
            logger.error(f"Task {task_id}: Failed to convert retrieved DICOMweb JSON metadata (UID: {instance_uid}) to Dataset: {e}", exc_info=True)
            return {"status": "error", "message": f"Failed to parse retrieved metadata: {e}", "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

        if original_ds is None:
             logger.error(f"Task {task_id}: Dataset object is None after conversion block, cannot proceed.")
             return {"status": "error", "message": "Dataset creation failed unexpectedly.", "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}


        logger.debug(f"Task {task_id}: Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found. No action needed for instance {instance_uid} from {source_identifier_for_matching}.")
             final_status = "success"
             final_message = "No active rulesets found"
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}


        logger.debug(f"Task {task_id}: Calling core processing logic using matching identifier '{source_identifier_for_matching}'...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier_for_matching # Use the config name
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for instance {instance_uid}: {proc_exc}", exc_info=True)
             db.close()
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}


        if not applied_rules_info:
             logger.info(f"Task {task_id}: No applicable rules matched for source '{source_identifier_for_matching}' on instance {instance_uid}. No actions taken.")
             final_status = "success"
             final_message = f"No matching rules found for source '{source_identifier_for_matching}'"
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}


        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured for instance {instance_uid}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            db.close()
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        logger.info(f"Task {task_id}: Processing {len(unique_destination_configs)} unique destinations for instance {instance_uid}...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                filename_context = f"{instance_uid}.dcm"

                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=None,
                    filename_context=filename_context,
                    source_identifier=source_identifier_for_matching # Pass matched identifier
                )

                logger.info(f"Task {task_id}: Destination {dest_id} completed for instance {instance_uid}. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store instance {instance_uid} to destination {dest_id}: {e}", exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                if isinstance(e, RETRYABLE_EXCEPTIONS): raise
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error storing instance {instance_uid} to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}


        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed instance {instance_uid} and dispatched successfully to {len(unique_destination_configs)} destination(s)."

            processed_incremented = False
            try:
                logger.debug(f"Task {task_id}: Incrementing processed count for DICOMweb source '{source_identifier_for_matching}' (ID: {source_id})")
                local_db = SessionLocal()

                processed_incremented = crud.dicomweb_state.increment_processed_count(db=local_db, source_name=source_identifier_for_matching, count=1)
                local_db.commit()
                local_db.close()
            except Exception as e:
                logger.error(f"Task {task_id}: DB Error incrementing processed count for DICOMweb source {source_identifier_for_matching}: {e}")
                if 'local_db' in locals() and local_db.is_active: local_db.rollback(); local_db.close()

            if processed_incremented:
                logger.info(f"Task {task_id}: Successfully incremented processed count for source {source_identifier_for_matching}.")
            else:
                logger.warning(f"Task {task_id}: Failed to increment processed count for source {source_identifier_for_matching}.")


        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed for instance {instance_uid}.")
            final_status = "partial_failure"
            final_message = f"Processing complete for instance {instance_uid}, but {failed_count} destination(s) failed dispatch."

        db.close()
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": source_identifier_for_matching, "instance_uid": instance_uid, "source_id": source_id}

    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception processing DICOMweb instance {instance_uid} from source ID {source_id}: {exc!r}", exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except: pass
            finally: db.close()
        if isinstance(exc, RETRYABLE_EXCEPTIONS): raise
        else:
             return {"status": "error", "message": f"Fatal error during processing: {exc!r}", "source_id": source_id, "instance_uid": instance_uid}
    finally:
        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (may indicate early exit).")
            db.close()
        logger.info(f"Task {task_id}: Finished processing DICOMweb instance {instance_uid}. Final Status: {final_status}.")



@shared_task(bind=True, name="process_stow_instance_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_stow_instance_task(self, temp_filepath_str: str, source_ip: Optional[str] = None):
    task_id = self.request.id
    temp_filepath = Path(temp_filepath_str)
    source_identifier_for_matching = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}" # Use IP for matching STOW

    logger.info(f"Task {task_id}: Received STOW-RS request for temp file: {temp_filepath} from source: {source_identifier_for_matching}")

    if not temp_filepath.is_file():
        logger.error(f"Task {task_id}: Temporary file not found: {temp_filepath}. Cannot process.")
        return {"status": "error", "message": "Temporary file not found", "filepath": temp_filepath_str, "source": source_identifier_for_matching}

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    instance_uid_str = "Unknown"

    try:

        db = SessionLocal()

        logger.debug(f"Task {task_id}: Reading DICOM file from temporary path {temp_filepath}...")
        try:
            original_ds = pydicom.dcmread(str(temp_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            logger.debug(f"Task {task_id}: Successfully read temp file. SOP Instance UID: {instance_uid_str}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file received via STOW-RS: {temp_filepath} (UID: {instance_uid_str}): {e}", exc_info=False)
            return {"status": "error", "message": f"Invalid DICOM format: {e}", "filepath": temp_filepath_str, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}
        except Exception as read_exc:
            logger.error(f"Task {task_id}: Error reading DICOM temp file {temp_filepath} (UID: {instance_uid_str}): {read_exc}", exc_info=True)
            raise read_exc


        logger.debug(f"Task {task_id}: Querying active rulesets...")
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for instance {instance_uid_str} from {source_identifier_for_matching}. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}


        logger.debug(f"Task {task_id}: Calling core processing logic using matching identifier '{source_identifier_for_matching}'...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier_for_matching # Use the constructed identifier
                # No association_info for STOW-RS currently
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for STOW instance {instance_uid_str}: {proc_exc}", exc_info=True)
             db.close()
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "source": source_identifier_for_matching, "instance_uid": instance_uid_str}


        if not applied_rules_info:
             logger.info(f"Task {task_id}: No applicable rules matched for source '{source_identifier_for_matching}' on instance {instance_uid_str}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             db.close()
             return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}


        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured for STOW instance {instance_uid_str}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            db.close()
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}

        dataset_to_send = modified_ds if modified_ds is not None else original_ds
        logger.info(f"Task {task_id}: Processing {len(unique_destination_configs)} unique destinations for STOW instance {instance_uid_str}...")
        all_destinations_succeeded = True
        for i, dest_config in enumerate(unique_destination_configs):
            dest_type = dest_config.get('type','?')
            dest_id_parts = [f"Dest_{i+1}", dest_type]
            if dest_type == 'cstore': dest_id_parts.append(dest_config.get('ae_title','?'))
            elif dest_type == 'filesystem': dest_id_parts.append(dest_config.get('path','?'))
            dest_id = "_".join(part for part in dest_id_parts if part != '?')

            logger.debug(f"Task {task_id}: Attempting destination {dest_id}: {dest_config}")
            try:
                storage_backend = get_storage_backend(dest_config)
                filename_context = f"{instance_uid_str}.dcm"

                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=temp_filepath,
                    filename_context=filename_context,
                    source_identifier=source_identifier_for_matching # Pass matched identifier
                )

                logger.info(f"Task {task_id}: Destination {dest_id} completed for instance {instance_uid_str}. Result: {store_result}")
                success_status[dest_id] = {"status": "success", "result": store_result}
            except StorageBackendError as e:
                logger.error(f"Task {task_id}: Failed to store STOW instance {instance_uid_str} to destination {dest_id}: {e}", exc_info=False)
                all_destinations_succeeded = False
                success_status[dest_id] = {"status": "error", "message": str(e)}
                if isinstance(e, RETRYABLE_EXCEPTIONS): raise
            except Exception as e:
                 logger.error(f"Task {task_id}: Unexpected error storing STOW instance {instance_uid_str} to {dest_id}: {e}", exc_info=True)
                 all_destinations_succeeded = False
                 success_status[dest_id] = {"status": "error", "message": f"Unexpected error: {e}"}


        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed STOW instance {instance_uid_str} and stored successfully to {len(unique_destination_configs)} destination(s)."


            logger.info(f"Task {task_id}: STOW instance {instance_uid_str} processed successfully. Metrics increment TBD.")


        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed for STOW instance {instance_uid_str}.")
            final_status = "partial_failure"
            final_message = f"Processing complete for STOW instance {instance_uid_str}, but {failed_count} destination(s) failed."

        db.close()
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}

    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception processing STOW instance from {source_identifier_for_matching} (UID: {instance_uid_str}, File: {temp_filepath}): {exc!r}", exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except: pass
            finally: db.close()
        if isinstance(exc, RETRYABLE_EXCEPTIONS): raise
        else:
            final_status = "error"
            final_message = f"Fatal error during processing: {exc!r}"
            return {"status": final_status, "message": final_message, "source": source_identifier_for_matching, "instance_uid": instance_uid_str}

    finally:

        if temp_filepath and temp_filepath.exists():
            try:
                os.remove(temp_filepath)
                logger.info(f"Task {task_id}: Removed temporary STOW-RS file: {temp_filepath}")
            except OSError as e:
                logger.critical(f"Task {task_id}: FAILED TO REMOVE temporary STOW-RS file {temp_filepath}: {e}")

        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (may indicate early exit).")
            db.close()

        logger.info(f"Task {task_id}: Finished processing STOW instance {instance_uid_str} from {source_identifier_for_matching}. Final Status: {final_status}.")
