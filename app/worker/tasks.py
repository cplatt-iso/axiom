# app/worker/tasks.py
import logging
import json
import os
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timezone
# --- ADDED: Union for type hint ---
from typing import Optional, List, Dict, Any, Union
# --- END ADDED ---
from copy import deepcopy

from celery import shared_task
from sqlalchemy.orm import Session
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.uid import generate_uid

# Database access
from app.db.session import SessionLocal
# --- Use top-level crud ---
from app import crud
# --- End Use top-level ---
from app.db import models
from app.db.models import ProcessedStudySourceType # Import Enum

# Storage backend & Client
from app.services.storage_backends import get_storage_backend, StorageBackendError
from app.services import dicomweb_client

# Configuration
from app.core.config import settings

# Import the core processing logic
from app.worker.processing_logic import process_dicom_instance as core_process_dicom_instance

# Get a logger for this module
logger = logging.getLogger(__name__)

# --- Helper Function ---
def move_to_error_dir(filepath: Path, task_id: str):
    """Moves a file to the designated error directory, adding a timestamp and task ID."""
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
             # Fallback: relative to incoming storage path's parent
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

# --- Constants ---
# Define exceptions that should trigger automatic retries by Celery
RETRYABLE_EXCEPTIONS = (
    ConnectionRefusedError,
    StorageBackendError,
    TimeoutError,
    dicomweb_client.DicomWebClientError # Add client errors for retries
)

# --- DICOM File Processing Task (Filesystem Watcher/Listener) ---

@shared_task(bind=True, name="process_dicom_file_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
# --- SIGNATURE MODIFIED ---
def process_dicom_file_task(
    self,
    dicom_filepath_str: str,
    source_type: str, # e.g., 'DIMSE_LISTENER' or ProcessedStudySourceType.DIMSE_LISTENER.value
    source_db_id_or_instance_id: Union[int, str] # Listener uses string instance_id
):
# --- END SIGNATURE MODIFIED ---
    """
    Celery task to process a DICOM file originally from filesystem/listener:
    reads file, fetches rules, calls processing logic, handles storage destinations, and cleans up.
    """
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    # --- Construct effective_source based on input ---
    effective_source = f"{source_type}_{source_db_id_or_instance_id}"
    # --- END Construct effective_source ---

    logger.info(f"Task {task_id}: Received request for file: {original_filepath} from source: {effective_source} (Type: {source_type}, ID: {source_db_id_or_instance_id})")

    if not original_filepath.is_file():
        logger.error(f"Task {task_id}: File not found or is not a file: {original_filepath}. Cannot process.")
        # --- Pass source info in return dict ---
        return {"status": "error", "message": "File not found", "filepath": dicom_filepath_str, "source": effective_source, "source_type": source_type, "source_id": source_db_id_or_instance_id}
        # --- END Pass source info ---

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    instance_uid_str = "Unknown"

    try:
        # 1. Read DICOM File
        logger.debug(f"Task {task_id}: Reading DICOM file...")
        try:
            original_ds = pydicom.dcmread(str(original_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            logger.debug(f"Task {task_id}: Successfully read file. SOP Instance UID: {instance_uid_str}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file {original_filepath} (UID: {instance_uid_str}): {e}", exc_info=False)
            move_to_error_dir(original_filepath, task_id)
            # --- Pass source info in return dict ---
            return {"status": "error", "message": f"Invalid DICOM file format: {e}", "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
            # --- END Pass source info ---
        except Exception as read_exc:
            logger.error(f"Task {task_id}: Error reading DICOM file {original_filepath} (UID: {instance_uid_str}): {read_exc}", exc_info=True)
            raise read_exc # Let Celery handle retry

        # 2. Get Rules from Database
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        # --- Use correct crud object ---
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        # --- END Use correct crud object ---
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for instance {instance_uid_str} from {effective_source}. No action needed.")
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
             # --- Pass source info in return dict ---
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
             # --- END Pass source info ---

        # 3. Call Processing Logic
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{effective_source}'...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = core_process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=effective_source # Use effective_source here
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for {original_filepath.name} (UID: {instance_uid_str}): {proc_exc}", exc_info=True)
             move_to_error_dir(original_filepath, task_id)
             db.close()
             # --- Pass source info in return dict ---
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
             # --- END Pass source info ---

        # 4. Destination Processing
        if not applied_rules_info:
             logger.info(f"Task {task_id}: No applicable rules matched for source '{effective_source}' on file {original_filepath.name}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             if settings.DELETE_UNMATCHED_FILES:
                try:
                    original_filepath.unlink(missing_ok=True)
                    logger.info(f"Task {task_id}: Deleting file {original_filepath} as no rules matched this source (DELETE_UNMATCHED_FILES=true).")
                except OSError as e:
                    logger.warning(f"Task {task_id}: Failed to delete file {original_filepath} with no matching rules: {e}")
             else:
                 logger.info(f"Task {task_id}: Leaving file {original_filepath} as no rules matched this source (DELETE_UNMATCHED_FILES=false).")
             db.close()
             # --- Pass source info in return dict ---
             return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
             # --- END Pass source info ---

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
            # --- Pass source info in return dict ---
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
            # --- END Pass source info ---

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
                # --- Pass effective_source to store method ---
                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=original_filepath,
                    filename_context=filename_context,
                    source_identifier=effective_source
                 )
                # --- END Pass effective_source ---
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
                 # Optionally: raise e # Re-raise to trigger retry if desired

        # 5. Cleanup & Increment Processed Count
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

            # --- INCREMENT PROCESSED COUNT ON SUCCESS ---
            processed_incremented = False
            try:
                # Use a new session as the main one might be closed soon
                local_db = SessionLocal()
                if source_type == ProcessedStudySourceType.DIMSE_LISTENER.value and isinstance(source_db_id_or_instance_id, str):
                    logger.debug(f"Task {task_id}: Incrementing processed count for DIMSE Listener {source_db_id_or_instance_id}")
                    processed_incremented = crud.crud_dimse_listener_state.increment_processed_count(db=local_db, listener_id=source_db_id_or_instance_id, count=1)
                elif source_type == ProcessedStudySourceType.DIMSE_QR.value and isinstance(source_db_id_or_instance_id, int):
                     logger.debug(f"Task {task_id}: Incrementing processed count for DIMSE QR source ID {source_db_id_or_instance_id}")
                     processed_incremented = crud.crud_dimse_qr_source.increment_processed_count(db=local_db, source_id=source_db_id_or_instance_id, count=1)
                     # CRUD commits internally here
                # Add elif for STOW_RS or other types later if needed
                else:
                    logger.warning(f"Task {task_id}: Unknown source type '{source_type}' or mismatched ID type '{type(source_db_id_or_instance_id)}' for processed count increment.")

                if processed_incremented:
                    logger.info(f"Task {task_id}: Successfully incremented processed count for source {effective_source}.")
                else:
                    logger.warning(f"Task {task_id}: Failed to increment processed count for source {effective_source}.")
                local_db.close() # Close the temporary session
            except Exception as e:
                logger.error(f"Task {task_id}: DB Error incrementing processed count for source {effective_source}: {e}")
                if 'local_db' in locals() and local_db.is_active: local_db.rollback(); local_db.close()
            # --- END INCREMENT ---

        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed. Original file NOT deleted: {original_filepath}")
            if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
                 move_to_error_dir(original_filepath, task_id)
            final_status = "partial_failure"
            final_message = f"Processing complete, but {failed_count} destination(s) failed."

        db.close()
        # --- Pass source info in return dict ---
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": effective_source, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
        # --- END Pass source info ---

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
            # --- Pass source info in return dict ---
            return {"status": final_status, "message": final_message, "filepath": dicom_filepath_str, "source": effective_source, "instance_uid": instance_uid_str, "source_type": source_type, "source_id": source_db_id_or_instance_id}
            # --- END Pass source info ---
    finally:
        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (may indicate early exit).")
            db.close()
        logger.info(f"Task {task_id}: Finished processing {original_filepath.name}. Final Status: {final_status}.")


# --- DICOMweb Metadata Processing Task ---
@shared_task(bind=True, name="process_dicomweb_metadata_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, # Includes DicomWebClientError now
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
# --- SIGNATURE UNCHANGED (already takes source_id) ---
def process_dicomweb_metadata_task(self, source_id: int, study_uid: str, series_uid: str, instance_uid: str):
    """
    Processes a DICOM instance identified by UIDs from a DICOMweb source.
    Fetches metadata via WADO-RS, converts to Dataset, applies rules, sends to destinations.
    """
# --- END SIGNATURE UNCHANGED ---
    task_id = self.request.id
    logger.info(f"Task {task_id}: Received request to process DICOMweb instance UID {instance_uid} from source ID {source_id}.")

    db: Optional[Session] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    original_ds: Optional[pydicom.Dataset] = None
    source_identifier: str = f"DICOMweb_Source_{source_id}" # Default identifier

    try:
        # 1. Get Source Configuration from DB
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Fetching configuration for source ID {source_id}...")
        # --- Use correct crud object ---
        source_config = crud.dicomweb_source.get(db, id=source_id)
        # --- END Use correct crud object ---
        if not source_config:
            logger.error(f"Task {task_id}: Could not find configuration for source ID {source_id}. Cannot fetch metadata.")
            # --- Pass source info in return dict ---
            return {"status": "error", "message": f"Configuration for source ID {source_id} not found.", "instance_uid": instance_uid, "source_id": source_id}
            # --- END Pass source info ---
        source_identifier = source_config.source_name # Use the actual name

        # 2. Fetch Instance Metadata via WADO-RS
        logger.info(f"Task {task_id}: Fetching metadata for instance {instance_uid} from source '{source_identifier}'...")
        try:
            client = dicomweb_client
            dicom_metadata = client.retrieve_instance_metadata(
                config=source_config,
                study_uid=study_uid,
                series_uid=series_uid,
                instance_uid=instance_uid
            )
            if dicom_metadata is None:
                 logger.warning(f"Task {task_id}: Metadata not found (or 404) for instance {instance_uid} at source '{source_identifier}'. Skipping.")
                 db.close()
                 # --- Pass source info in return dict ---
                 return {"status": "success", "message": "Instance metadata not found (likely deleted).", "source": source_identifier, "instance_uid": instance_uid, "source_id": source_id}
                 # --- END Pass source info ---
            logger.debug(f"Task {task_id}: Successfully retrieved metadata for {instance_uid}.")
        except Exception as fetch_exc:
             logger.error(f"Task {task_id}: Error fetching metadata for instance {instance_uid} from '{source_identifier}': {fetch_exc}", exc_info=isinstance(fetch_exc, dicomweb_client.DicomWebClientError))
             raise fetch_exc # Let Celery handle retry based on autoretry_for


        # 3. Convert Metadata JSON to pydicom Dataset
        logger.debug(f"Task {task_id}: Converting JSON metadata to pydicom Dataset...")
        try:
            original_ds = pydicom.dataset.Dataset.from_json(dicom_metadata)
            # Create essential file_meta
            file_meta = pydicom.dataset.FileMetaDataset()
            file_meta.FileMetaInformationVersion = b'\x00\x01'
            sop_class_uid_val = getattr(original_ds.get("SOPClassUID", None), 'value', '1.2.840.10008.5.1.4.1.1.2')
            file_meta.MediaStorageSOPClassUID = sop_class_uid_val
            file_meta.MediaStorageSOPInstanceUID = instance_uid # Use known UID
            file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian # Default assumption
            file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
            file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
            original_ds.file_meta = file_meta
        except Exception as e:
            logger.error(f"Task {task_id}: Failed to convert retrieved DICOMweb JSON metadata (UID: {instance_uid}) to Dataset: {e}", exc_info=True)
            # --- Pass source info in return dict ---
            return {"status": "error", "message": f"Failed to parse retrieved metadata: {e}", "source": source_identifier, "instance_uid": instance_uid, "source_id": source_id}
            # --- END Pass source info ---

        if original_ds is None:
             logger.error(f"Task {task_id}: Dataset object is None after conversion block, cannot proceed.")
             # --- Pass source info in return dict ---
             return {"status": "error", "message": "Dataset creation failed unexpectedly.", "source": source_identifier, "instance_uid": instance_uid, "source_id": source_id}
             # --- END Pass source info ---


        # 4. Get Rules from Database
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        # --- Use correct crud object ---
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        # --- END Use correct crud object ---
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found. No action needed for instance {instance_uid} from {source_identifier}.")
             final_status = "success"
             final_message = "No active rulesets found"
             db.close()
             # --- Pass source info in return dict ---
             return {"status": final_status, "message": final_message, "source": source_identifier, "instance_uid": instance_uid, "source_id": source_id}
             # --- END Pass source info ---

        # 5. Call Core Processing Logic
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{source_identifier}'...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = core_process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=source_identifier
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for instance {instance_uid}: {proc_exc}", exc_info=True)
             db.close()
             # --- Pass source info in return dict ---
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "source": source_identifier, "instance_uid": instance_uid, "source_id": source_id}
             # --- END Pass source info ---

        # 6. Destination Processing
        if not applied_rules_info:
             logger.info(f"Task {task_id}: No applicable rules matched for source '{source_identifier}' on instance {instance_uid}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             db.close()
             # --- Pass source info in return dict ---
             return {"status": final_status, "message": final_message, "source": source_identifier, "instance_uid": instance_uid, "source_id": source_id}
             # --- END Pass source info ---

        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured for instance {instance_uid}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            db.close()
            # --- Pass source info in return dict ---
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "source": source_identifier, "instance_uid": instance_uid, "source_id": source_id}
            # --- END Pass source info ---

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
                # --- Pass effective_source to store method ---
                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=None,
                    filename_context=filename_context,
                    source_identifier=source_identifier # Use identifier from config
                )
                # --- END Pass effective_source ---
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
                 # Optionally: raise e

        # 7. Final Status Determination & Increment Processed Count
        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed instance {instance_uid} and dispatched successfully to {len(unique_destination_configs)} destination(s)."

            # --- INCREMENT PROCESSED COUNT ON SUCCESS ---
            processed_incremented = False
            try:
                logger.debug(f"Task {task_id}: Incrementing processed count for DICOMweb source '{source_identifier}' (ID: {source_id})")
                local_db = SessionLocal()
                # Use the CRUD object instance from the correct module
                processed_incremented = crud.dicomweb_state.increment_processed_count(db=local_db, source_name=source_identifier, count=1)
                local_db.commit() # Commit the processed count increment
                local_db.close()
            except Exception as e:
                logger.error(f"Task {task_id}: DB Error incrementing processed count for DICOMweb source {source_identifier}: {e}")
                if 'local_db' in locals() and local_db.is_active: local_db.rollback(); local_db.close()

            if processed_incremented:
                logger.info(f"Task {task_id}: Successfully incremented processed count for source {source_identifier}.")
            else:
                logger.warning(f"Task {task_id}: Failed to increment processed count for source {source_identifier}.")
            # --- END INCREMENT ---

        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed for instance {instance_uid}.")
            final_status = "partial_failure"
            final_message = f"Processing complete for instance {instance_uid}, but {failed_count} destination(s) failed dispatch."

        db.close()
        # --- Pass source info in return dict ---
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": source_identifier, "instance_uid": instance_uid, "source_id": source_id}
        # --- END Pass source info ---

    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception processing DICOMweb instance {instance_uid} from source ID {source_id}: {exc!r}", exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except: pass
            finally: db.close()
        if isinstance(exc, RETRYABLE_EXCEPTIONS): raise
        else:
            # --- Pass source info in return dict ---
             return {"status": "error", "message": f"Fatal error during processing: {exc!r}", "source_id": source_id, "instance_uid": instance_uid}
            # --- END Pass source info ---
    finally:
        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (may indicate early exit).")
            db.close()
        logger.info(f"Task {task_id}: Finished processing DICOMweb instance {instance_uid}. Final Status: {final_status}.")


# --- STOW-RS Instance Processing Task ---
@shared_task(bind=True, name="process_stow_instance_task",
             acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES,
             default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS,
             retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
# --- SIGNATURE UNCHANGED ---
def process_stow_instance_task(self, temp_filepath_str: str, source_ip: Optional[str] = None):
    # --- END SIGNATURE UNCHANGED ---
    """
    Celery task to process a single DICOM instance file received via STOW-RS.
    The file provided is temporary and MUST be cleaned up by this task.
    """
    task_id = self.request.id
    temp_filepath = Path(temp_filepath_str)
    effective_source = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}"

    logger.info(f"Task {task_id}: Received STOW-RS request for temp file: {temp_filepath} from source: {effective_source}")

    if not temp_filepath.is_file():
        logger.error(f"Task {task_id}: Temporary file not found: {temp_filepath}. Cannot process.")
        # --- Pass source info in return dict ---
        return {"status": "error", "message": "Temporary file not found", "filepath": temp_filepath_str, "source": effective_source}
        # --- END Pass source info ---

    db: Optional[Session] = None
    original_ds: Optional[pydicom.Dataset] = None
    final_status = "unknown"
    final_message = "Task did not complete."
    success_status = {}
    instance_uid_str = "Unknown"

    try:
        # 1. Read DICOM File
        logger.debug(f"Task {task_id}: Reading DICOM file from temporary path {temp_filepath}...")
        try:
            original_ds = pydicom.dcmread(str(temp_filepath), force=True)
            instance_uid_str = getattr(original_ds, 'SOPInstanceUID', 'Unknown')
            logger.debug(f"Task {task_id}: Successfully read temp file. SOP Instance UID: {instance_uid_str}")
        except InvalidDicomError as e:
            logger.error(f"Task {task_id}: Invalid DICOM file received via STOW-RS: {temp_filepath} (UID: {instance_uid_str}): {e}", exc_info=False)
            # --- Pass source info in return dict ---
            return {"status": "error", "message": f"Invalid DICOM format: {e}", "filepath": temp_filepath_str, "source": effective_source, "instance_uid": instance_uid_str}
            # --- END Pass source info ---
        except Exception as read_exc:
            logger.error(f"Task {task_id}: Error reading DICOM temp file {temp_filepath} (UID: {instance_uid_str}): {read_exc}", exc_info=True)
            raise read_exc

        # 2. Get Rules from Database
        logger.debug(f"Task {task_id}: Opening database session...")
        db = SessionLocal()
        logger.debug(f"Task {task_id}: Querying active rulesets...")
        # --- Use correct crud object ---
        active_rulesets: List[models.RuleSet] = crud.ruleset.get_active_ordered(db)
        # --- END Use correct crud object ---
        if not active_rulesets:
             logger.info(f"Task {task_id}: No active rulesets found for instance {instance_uid_str} from {effective_source}. No action needed.")
             final_status = "success"
             final_message = "No active rulesets found"
             db.close()
             # --- Pass source info in return dict ---
             return {"status": final_status, "message": final_message, "source": effective_source, "instance_uid": instance_uid_str}
             # --- END Pass source info ---

        # 3. Call Processing Logic
        logger.debug(f"Task {task_id}: Calling core processing logic for source '{effective_source}'...")
        try:
            modified_ds, applied_rules_info, unique_destination_configs = core_process_dicom_instance(
                original_ds=original_ds,
                active_rulesets=active_rulesets,
                source_identifier=effective_source
            )
        except Exception as proc_exc:
             logger.error(f"Task {task_id}: Error during core processing logic for STOW instance {instance_uid_str}: {proc_exc}", exc_info=True)
             db.close()
             # --- Pass source info in return dict ---
             return {"status": "error", "message": f"Error during rule processing: {proc_exc}", "source": effective_source, "instance_uid": instance_uid_str}
             # --- END Pass source info ---

        # 4. Destination Processing
        if not applied_rules_info:
             logger.info(f"Task {task_id}: No applicable rules matched for source '{effective_source}' on instance {instance_uid_str}. No actions taken.")
             final_status = "success"
             final_message = "No matching rules found for this source"
             db.close()
             # --- Pass source info in return dict ---
             return {"status": final_status, "message": final_message, "source": effective_source, "instance_uid": instance_uid_str}
             # --- END Pass source info ---

        if not unique_destination_configs:
            logger.info(f"Task {task_id}: Rules matched, but no destinations configured for STOW instance {instance_uid_str}.")
            final_status = "success"
            final_message = "Rules matched, modifications applied (if any), but no destinations configured"
            db.close()
            # --- Pass source info in return dict ---
            return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "source": effective_source, "instance_uid": instance_uid_str}
            # --- END Pass source info ---

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
                # --- Pass effective_source to store method ---
                store_result = storage_backend.store(
                    dataset_to_send,
                    original_filepath=temp_filepath, # Pass temp path for STOW
                    filename_context=filename_context,
                    source_identifier=effective_source
                )
                # --- END Pass effective_source ---
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

        # 5. Final Status Determination & Processed Count
        if all_destinations_succeeded:
            final_status = "success"
            final_message = f"Processed STOW instance {instance_uid_str} and stored successfully to {len(unique_destination_configs)} destination(s)."

            # --- INCREMENT PROCESSED COUNT ON SUCCESS (STOW - Placeholder) ---
            # Decide how to track STOW metrics. For now, just log.
            logger.info(f"Task {task_id}: STOW instance {instance_uid_str} processed successfully. Metrics increment TBD.")
            # processed_incremented = False # Placeholder
            # if processed_incremented: logger.info(...)
            # else: logger.warning(...)
            # --- END INCREMENT ---

        else:
            failed_count = sum(1 for status in success_status.values() if status['status'] == 'error')
            logger.warning(f"Task {task_id}: {failed_count}/{len(unique_destination_configs)} destinations failed for STOW instance {instance_uid_str}.")
            final_status = "partial_failure"
            final_message = f"Processing complete for STOW instance {instance_uid_str}, but {failed_count} destination(s) failed."

        db.close()
        # --- Pass source info in return dict ---
        return {"status": final_status, "message": final_message, "applied_rules": applied_rules_info, "destinations": success_status, "source": effective_source, "instance_uid": instance_uid_str}
        # --- END Pass source info ---

    except Exception as exc:
        logger.error(f"Task {task_id}: Unhandled exception processing STOW instance from {effective_source} (UID: {instance_uid_str}, File: {temp_filepath}): {exc!r}", exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except: pass
            finally: db.close()
        if isinstance(exc, RETRYABLE_EXCEPTIONS): raise
        else:
            final_status = "error"
            final_message = f"Fatal error during processing: {exc!r}"
            # --- Pass source info in return dict ---
            return {"status": final_status, "message": final_message, "source": effective_source, "instance_uid": instance_uid_str}
            # --- END Pass source info ---

    finally:
        # CRITICAL: Clean up the temporary file for STOW-RS task
        if temp_filepath and temp_filepath.exists():
            try:
                os.remove(temp_filepath)
                logger.info(f"Task {task_id}: Removed temporary STOW-RS file: {temp_filepath}")
            except OSError as e:
                logger.critical(f"Task {task_id}: FAILED TO REMOVE temporary STOW-RS file {temp_filepath}: {e}")

        if db and db.is_active:
            logger.warning(f"Task {task_id}: Closing database session in finally block (may indicate early exit).")
            db.close()

        logger.info(f"Task {task_id}: Finished processing STOW instance {instance_uid_str} from {effective_source}. Final Status: {final_status}.")
