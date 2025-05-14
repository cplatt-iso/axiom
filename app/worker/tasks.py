# app/worker/tasks.py
from datetime import datetime, timezone
import logging
import shutil
import sys
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any, Union

import structlog
from celery import shared_task
from sqlalchemy.orm import Session
import pydicom # Needed for type hinting potentially
from pydicom.errors import InvalidDicomError # Needed by executors, maybe tasks

from app import crud
from app.db.session import SessionLocal
from app.db.models import ProcessedStudySourceType
from app.core.config import settings

from app.worker.task_executors import (
    execute_file_based_task,
    execute_dicomweb_task,
    execute_ghc_task,
    execute_stow_task
)
from app.services import ai_assist_service # Still needed for initial check

logger = structlog.get_logger(__name__)

RETRYABLE_EXCEPTIONS = (
    ConnectionRefusedError,
    TimeoutError,
    # StorageBackendError, # Let executors raise specific retryable errors if needed
    # dicomweb_client.DicomWebClientError # Let executors raise specific retryable errors if needed
)

# --- Helper Functions (_move_to_error_dir, _handle_final_file_disposition) ---
# Assume these are defined here as previously shown, or imported if moved elsewhere.

def _move_to_error_dir(filepath: Path, task_id: str, context_log: Any):
    if not isinstance(filepath, Path):
         filepath = Path(filepath)
    if not filepath.is_file():
         context_log.warning("Source path for error move is not a file or does not exist.", target_filepath=str(filepath))
         return
    try:
        error_base_dir_str = getattr(settings, 'DICOM_ERROR_PATH', None)
        if error_base_dir_str: error_dir = Path(error_base_dir_str)
        else:
             storage_path_str = getattr(settings, 'DICOM_STORAGE_PATH', '/dicom_data/incoming')
             error_dir = Path(storage_path_str).parent / "errors"
             context_log.warning("DICOM_ERROR_PATH not set, using fallback error dir.", fallback_error_dir=str(error_dir))
        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        context_log.info("Moved file to error directory.", error_path=str(error_path))
    except Exception as move_err:
        fallback_logger = logging.getLogger(__name__) # Standard logger as last resort
        current_logger = getattr(context_log, 'critical', fallback_logger.critical)
        current_logger("CRITICAL - Could not move file to error dir", task_id=task_id, original_filepath=str(filepath),
                       target_error_dir=str(error_dir) if 'error_dir' in locals() else 'UnknownErrorDir',
                       error=str(move_err), exc_info=True)

def _handle_final_file_disposition(
    original_filepath: Path, was_successful: bool, rules_matched_and_triggered_actions: bool,
    modifications_made: bool, any_destination_failures: bool, context_log: Any, task_id: str
):
    if not original_filepath or not original_filepath.exists(): return
    if was_successful and not any_destination_failures:
        if settings.DELETE_ON_SUCCESS:
            try: original_filepath.unlink(missing_ok=True); context_log.info("Deleted original file on success.")
            except OSError as e: context_log.warning("Failed to delete original file on success.", error=str(e))
        else: context_log.info("Kept original file on success.")
    elif not rules_matched_and_triggered_actions:
        if settings.DELETE_UNMATCHED_FILES:
            try: original_filepath.unlink(missing_ok=True); context_log.info("Deleted original file as no rules matched/triggered.")
            except OSError as e: context_log.warning("Failed to delete unmatched file.", error=str(e))
        else: context_log.info("Kept original file as no rules matched/triggered.")
    elif any_destination_failures:
        if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
            context_log.warning("Partial destination failure. Moving original file to error directory.")
            _move_to_error_dir(original_filepath, task_id, context_log)
        elif settings.DELETE_ON_PARTIAL_FAILURE_IF_MODIFIED and modifications_made:
             try: original_filepath.unlink(missing_ok=True); context_log.info("Deleted modified original file on partial failure.")
             except OSError as e: context_log.warning("Failed to delete modified file on partial failure.", error=str(e))
        else: context_log.info("Kept original file despite partial destination failure.")
    elif rules_matched_and_triggered_actions and not any_destination_failures and modifications_made and settings.DELETE_ON_NO_DESTINATION:
        context_log.info("Rules matched, mods made, no destinations. Deleting original (DELETE_ON_NO_DESTINATION=true).")
        try: original_filepath.unlink(missing_ok=True)
        except OSError as e: context_log.warning("Failed to delete file with no destinations after modification.", error=str(e))
    else: context_log.info("Kept original file based on disposition settings.")

# --- Celery Tasks ---

@shared_task(bind=True, name="process_dicom_file_task", acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES, default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_dicom_file_task(
    self, dicom_filepath_str: str, source_type: str,
    source_db_id_or_instance_id: Union[int, str], association_info: Optional[Dict[str, str]] = None
):
    task_id = self.request.id
    original_filepath = Path(dicom_filepath_str)
    log_source_display_name = f"{source_type}_{source_db_id_or_instance_id}"
    log = logger.bind(task_id=task_id, task_name=self.name, original_filepath=str(original_filepath),
                      source_type=source_type, source_id_or_instance=str(source_db_id_or_instance_id),
                      log_source_display_name=log_source_display_name)
    log.info("Task started: process_dicom_file_task")

    if not original_filepath.is_file():
        log.error("File not found. Task cannot proceed.")
        return {"status": "error_file_not_found", "message": "File not found", "filepath": dicom_filepath_str,
                "instance_uid": "Unknown", "source_display_name": log_source_display_name,
                "applied_rules": [], "destinations_processed": {}}

    db: Optional[Session] = None
    final_status_code = "task_init_failed"
    final_message = "Task initialization or pre-execution failed."
    applied_rules_info_res, dest_statuses_res = [], {}
    instance_uid_res = "UnknownFromFileTaskInit"
    rules_matched_res, modifications_made_res = False, False
    processed_ds_res = None 

    try:
        db = SessionLocal()
        
        # No AI portal management needed here with sync wrapper approach

        rules_matched_res, modifications_made_res, final_status_code, final_message, \
        applied_rules_info_res, dest_statuses_res, processed_ds_res, \
        instance_uid_res, _ = execute_file_based_task(
            log, db, dicom_filepath_str, source_type,
            source_db_id_or_instance_id, task_id, association_info,
            ai_portal=None # Pass None explicitly
        )

        if final_status_code.startswith("success"):
            try:
                source_type_enum_for_inc = ProcessedStudySourceType(source_type)
                should_commit = False
                if source_type_enum_for_inc == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(source_db_id_or_instance_id, str):
                    if crud.crud_dimse_listener_state.increment_processed_count(db=db, listener_id=source_db_id_or_instance_id, count=1):
                        should_commit = True
                elif source_type_enum_for_inc == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
                     if crud.crud_dimse_qr_source.increment_processed_count(db=db, source_id=source_db_id_or_instance_id, count=1):
                        should_commit = True
                if should_commit:
                    db.commit()
                    log.info("Successfully incremented and committed processed count.", source_type=source_type)
            except Exception as db_inc_err:
                log.error("DB Error during processed count increment or commit.", error=str(db_inc_err), exc_info=True)
                if db and db.is_active: log.warning("Rolling back DB session."); db.rollback()

        if db and db.is_active: db.close(); db = None
        return {"status": final_status_code, "message": final_message, "applied_rules": applied_rules_info_res,
                "destinations_processed": dest_statuses_res, "source_display_name": log_source_display_name,
                "instance_uid": instance_uid_res, "filepath": dicom_filepath_str}

    except Exception as exc:
        log.error("Unhandled exception in task process_dicom_file_task.", error=str(exc), exc_info=True)
        if db and db.is_active:
            try: db.rollback()
            except Exception: pass # Ignore rollback error
            finally: db.close(); db = None
        if any(isinstance(exc, retry_exc_type) for retry_exc_type in RETRYABLE_EXCEPTIONS):
            log.info("Caught retryable exception at task level. Retrying.", exc_type=type(exc).__name__)
            raise
        final_status_code = "fatal_task_error_unhandled"
        final_message = f"Fatal unhandled error in task: {exc!r}"
        if original_filepath.exists(): _move_to_error_dir(original_filepath, task_id, log)
        return {"status": final_status_code, "message": final_message, "filepath": dicom_filepath_str,
                "source_display_name": log_source_display_name, "instance_uid": instance_uid_res,
                "applied_rules": applied_rules_info_res, "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active: log.warning("DB session still active in finally block, closing."); db.close()
        if original_filepath.exists():
            any_dest_failed_final = any(d.get("status") == "error" for d in dest_statuses_res.values())
            status_for_disposition = final_status_code if 'final_status_code' in locals() else 'unknown_error_state'
            _handle_final_file_disposition(
                 original_filepath=original_filepath, was_successful=(status_for_disposition.startswith("success") and not any_dest_failed_final),
                 rules_matched_and_triggered_actions=rules_matched_res, modifications_made=modifications_made_res,
                 any_destination_failures=any_dest_failed_final, context_log=log, task_id=task_id)
        log.info("Task finished: process_dicom_file_task.", final_task_status=final_status_code)


@shared_task(bind=True, name="process_dicomweb_metadata_task", acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES, default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
async def process_dicomweb_metadata_task(self, source_id: int, study_uid: str, series_uid: str, instance_uid: str):
    task_id = self.request.id
    log = logger.bind(task_id=task_id, task_name=self.name, source_id=source_id, instance_uid=instance_uid,
                      source_type=ProcessedStudySourceType.DICOMWEB_POLLER.value)
    log.info("Task started: process_dicomweb_metadata_task")
    db: Optional[Session] = None
    final_status_code = "task_init_failed"
    final_message = "DICOMweb task initialization failed."
    applied_rules_res, dest_statuses_res = [], {}
    source_name_res = f"DICOMweb_src_{source_id}"

    try:
        db = SessionLocal()
        # No AI portal management needed with sync wrapper

        _, _, final_status_code, final_message, applied_rules_res, \
        dest_statuses_res, _, source_name_res_from_exec = await execute_dicomweb_task(
            log, db, source_id, study_uid, series_uid, instance_uid, task_id,
            ai_portal=None # Pass None
        )
        if source_name_res_from_exec: source_name_res = source_name_res_from_exec

        await asyncio.to_thread(db.close); db = None
        return {"status": final_status_code, "message": final_message, "applied_rules": applied_rules_res,
                "destinations_processed": dest_statuses_res, "source_name": source_name_res, "instance_uid": instance_uid}
    except Exception as exc:
        log.error("Unhandled exception in task process_dicomweb_metadata_task.", error=str(exc), exc_info=True)
        if db and db.is_active:
            try: await asyncio.to_thread(db.rollback)
            except Exception: pass
            finally: await asyncio.to_thread(db.close); db = None
        if any(isinstance(exc, retry_exc_type) for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled"
        final_message = f"Fatal error: {exc!r}"
        return {"status": final_status_code, "message": final_message, "instance_uid": instance_uid, "source_id": source_id,
                "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active: await asyncio.to_thread(db.close)
        current_final_status = final_status_code if 'final_status_code' in locals() else 'task_ended_in_finally_early_error_dw'
        log.info("Task finished: process_dicomweb_metadata_task.", final_task_status=current_final_status)


@shared_task(bind=True, name="process_stow_instance_task", acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES, default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
def process_stow_instance_task(self, temp_filepath_str: str, source_ip: Optional[str] = None):
    task_id = self.request.id
    temp_filepath = Path(temp_filepath_str)
    source_identifier_log_display = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}"
    log = logger.bind(task_id=task_id, task_name=self.name, temp_filepath_stow=str(temp_filepath),
                      source_ip_stow=source_ip, log_source_display_name=source_identifier_log_display,
                      source_type=ProcessedStudySourceType.STOW_RS.value)
    log.info("Task started: process_stow_instance_task")

    if not temp_filepath.is_file():
        log.error("Temporary STOW file not found. Task cannot proceed.", stow_temp_file=str(temp_filepath))
        return {"status": "error_temp_file_not_found", "message": "Temporary STOW file not found",
                "filepath": temp_filepath_str, "instance_uid": "Unknown",
                "source_display_name": source_identifier_log_display}

    db: Optional[Session] = None
    final_status_code = "task_init_failed_stow"
    final_message = "STOW task initialization failed."
    applied_rules_res, dest_statuses_res = [], {}
    instance_uid_res = "UnknownFromStowTask"

    try:
        db = SessionLocal()
        # No AI portal needed

        _rules_matched, _mods_made, final_status_code, final_message, \
        applied_rules_res, dest_statuses_res, _processed_ds, \
        instance_uid_res, _source_id_match = execute_stow_task(
            log, db, temp_filepath_str, source_ip, task_id,
            ai_portal=None
        )

        db.close(); db = None
        return {"status": final_status_code, "message": final_message, "applied_rules": applied_rules_res,
                "destinations_processed": dest_statuses_res, "source_display_name": source_identifier_log_display,
                "instance_uid": instance_uid_res, "original_stow_filepath": temp_filepath_str}

    except Exception as exc:
        log.error("Unhandled exception in task process_stow_instance_task.", error=str(exc), exc_info=True)
        if db and db.is_active:
             try: db.rollback()
             except Exception: pass
             finally: db.close(); db = None
        if any(isinstance(exc, retry_exc_type) for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled_stow"
        final_message = f"Fatal unhandled error in STOW task: {exc!r}"
        return {"status": final_status_code, "message": final_message, "filepath": temp_filepath_str,
                "source_display_name": source_identifier_log_display, "instance_uid": instance_uid_res,
                "applied_rules": applied_rules_res, "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active: db.close()
        if temp_filepath.exists():
            try: temp_filepath.unlink(missing_ok=True)
            except OSError as e: log.critical("FAILED TO DELETE temporary STOW file in finally.", stow_temp_file_final_error=str(temp_filepath), error_detail=str(e))
        current_final_status = final_status_code if 'final_status_code' in locals() else 'task_ended_in_finally_early_error_stow'
        log.info("Task finished: process_stow_instance_task.", final_task_status=current_final_status)


@shared_task(bind=True, name="process_google_healthcare_metadata_task", acks_late=True,
             max_retries=settings.CELERY_TASK_MAX_RETRIES, default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
             autoretry_for=RETRYABLE_EXCEPTIONS, retry_backoff=True, retry_backoff_max=600, retry_jitter=True)
async def process_google_healthcare_metadata_task(self, source_id: int, study_uid: str):
    task_id = self.request.id
    log = logger.bind(task_id=task_id, task_name=self.name, ghc_source_id=source_id, ghc_study_uid=study_uid,
                      source_type=ProcessedStudySourceType.GOOGLE_HEALTHCARE.value)
    log.info("Task started: process_google_healthcare_metadata_task")
    db: Optional[Session] = None
    final_status_code = "task_init_failed_ghc"
    final_message = "GHC task initialization failed."
    applied_rules_res, dest_statuses_res = [], {}
    instance_uid_res = "UnknownFromGHCTask"
    source_name_res = f"GHC_src_{source_id}"

    try:
        db = SessionLocal()
        # No AI portal needed

        _rules_matched, _mods_made, final_status_code, final_message, applied_rules_res, \
        dest_statuses_res, _processed_ds, instance_uid_res, \
        source_name_res_from_exec = await execute_ghc_task(
            log, db, source_id, study_uid, task_id,
            ai_portal=None
        )
        if source_name_res_from_exec: source_name_res = source_name_res_from_exec

        await asyncio.to_thread(db.close); db = None
        return {"status": final_status_code, "message": final_message, "applied_rules": applied_rules_res,
                "destinations_processed": dest_statuses_res, "source_name": source_name_res,
                "study_uid": study_uid, "instance_uid": instance_uid_res}
    except Exception as exc:
        log.error("Unhandled exception in task process_google_healthcare_metadata_task.", error=str(exc), exc_info=True)
        if db and db.is_active:
            try: await asyncio.to_thread(db.rollback)
            except Exception: pass
            finally: await asyncio.to_thread(db.close); db = None
        if any(isinstance(exc, retry_exc_type) for retry_exc_type in RETRYABLE_EXCEPTIONS):
            raise
        final_status_code = "fatal_task_error_unhandled_ghc"
        final_message = f"Fatal unhandled error in GHC task: {exc!r}"
        return {"status": final_status_code, "message": final_message, "study_uid": study_uid, "source_id": source_id,
                "instance_uid": instance_uid_res, "applied_rules": applied_rules_res, "destinations_processed": dest_statuses_res}
    finally:
        if db and db.is_active: await asyncio.to_thread(db.close)
        current_final_status = final_status_code if 'final_status_code' in locals() else 'task_ended_in_finally_early_error_ghc'
        log.info("Task finished: process_google_healthcare_metadata_task.", final_task_status=current_final_status)
