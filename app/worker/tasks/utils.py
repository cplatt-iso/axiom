# app/worker/tasks/utils.py
from datetime import datetime, timezone
import logging
import shutil
from pathlib import Path
from typing import Optional, List, Any

import pydicom

from app.core.config import settings
from app.services.dustbin_service import dustbin_service


def _move_to_error_dir(filepath: Path, task_id: str, context_log: Any):
    if not isinstance(filepath, Path):
        filepath = Path(filepath)
    if not filepath.is_file():
        context_log.warning(
            "Source path for error move is not a file or does not exist.",
            target_filepath=str(filepath))
        return
    error_dir = None
    try:
        error_base_dir_str = getattr(settings, 'DICOM_ERROR_PATH', None)
        if error_base_dir_str:
            error_dir = Path(error_base_dir_str)
        else:
            storage_path_str = getattr(
                settings, 'DICOM_STORAGE_PATH', '/dicom_data/incoming')
            error_dir = Path(storage_path_str).parent / "errors"
            context_log.warning(
                "DICOM_ERROR_PATH not set, using fallback error dir.",
                fallback_error_dir=str(error_dir))
        error_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')
        error_filename = f"{timestamp}_task_{task_id}_{filepath.name}"
        error_path = error_dir / error_filename
        shutil.move(str(filepath), str(error_path))
        context_log.info(
            "Moved file to error directory.",
            error_path=str(error_path))
    except Exception as move_err:
        fallback_logger = logging.getLogger(
            __name__)  # Standard logger as last resort
        current_logger = getattr(
            context_log,
            'critical',
            fallback_logger.critical)
        current_logger(
            f"CRITICAL - Could not move file to error dir. Task ID: {task_id}, Original Filepath: {str(filepath)}, Target Error Dir: {str(error_dir) if 'error_dir' in locals() else 'UnknownErrorDir'}, Error: {str(move_err)}", exc_info=True)


def _handle_final_file_disposition_with_dustbin(
        original_filepath: Path,
        was_successful: bool,
        rules_matched_and_triggered_actions: bool,
        modifications_made: bool,
        any_destination_failures: bool,
        context_log: Any,
        task_id: str,
        study_instance_uid: Optional[str] = None,
        sop_instance_uid: Optional[str] = None,
        destinations_confirmed: Optional[List[str]] = None):
    """
    Medical-grade file disposition that uses dustbin service instead of immediate deletion.
    NEVER deletes files immediately - always moves to dustbin with verification.
    """
    if not original_filepath or not original_filepath.exists():
        return
    
    # Extract DICOM UIDs if not provided
    if not study_instance_uid or not sop_instance_uid:
        try:
            ds = pydicom.dcmread(str(original_filepath))
            study_instance_uid = study_instance_uid or ds.get('StudyInstanceUID', 'UNKNOWN_STUDY_UID')
            sop_instance_uid = sop_instance_uid or ds.get('SOPInstanceUID', 'UNKNOWN_SOP_UID')
        except Exception as e:
            context_log.warning("Could not read DICOM UIDs for dustbin", error=str(e))
            study_instance_uid = study_instance_uid or f"UNKNOWN_STUDY_{task_id}"
            sop_instance_uid = sop_instance_uid or f"UNKNOWN_SOP_{task_id}"
    
    destinations_confirmed = destinations_confirmed or []
    
    if was_successful and not any_destination_failures:
        # File was processed successfully - move to dustbin with full confirmation
        success = dustbin_service.move_to_dustbin(
            source_file_path=str(original_filepath),
            study_instance_uid=study_instance_uid,
            sop_instance_uid=sop_instance_uid,
            task_id=task_id,
            destinations_confirmed=destinations_confirmed,
            reason="processing_complete_all_destinations_successful"
        )
        if success:
            context_log.info("MEDICAL SAFETY: File moved to dustbin after successful processing with all destinations confirmed")
        else:
            context_log.error("CRITICAL: Failed to move successfully processed file to dustbin - FILE KEPT FOR SAFETY")
            
    elif not rules_matched_and_triggered_actions:
        if settings.DELETE_UNMATCHED_FILES:
            # Even unmatched files go to dustbin for safety
            success = dustbin_service.move_to_dustbin(
                source_file_path=str(original_filepath),
                study_instance_uid=study_instance_uid,
                sop_instance_uid=sop_instance_uid,
                task_id=task_id,
                destinations_confirmed=[],
                reason="no_rules_matched_or_triggered"
            )
            if success:
                context_log.info("MEDICAL SAFETY: Unmatched file moved to dustbin instead of immediate deletion")
            else:
                context_log.error("CRITICAL: Failed to move unmatched file to dustbin - FILE KEPT FOR SAFETY")
        else:
            context_log.info("Kept original file as no rules matched/triggered (per configuration)")
            
    elif any_destination_failures:
        if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
            context_log.warning("Partial destination failure. Moving original file to error directory.")
            _move_to_error_dir(original_filepath, task_id, context_log)
        elif settings.DELETE_ON_PARTIAL_FAILURE_IF_MODIFIED and modifications_made:
            # Even partial failures with modifications go to dustbin for safety
            success = dustbin_service.move_to_dustbin(
                source_file_path=str(original_filepath),
                study_instance_uid=study_instance_uid,
                sop_instance_uid=sop_instance_uid,
                task_id=task_id,
                destinations_confirmed=destinations_confirmed,
                reason="partial_failure_but_modified"
            )
            if success:
                context_log.info("MEDICAL SAFETY: Modified file with partial failures moved to dustbin instead of immediate deletion")
            else:
                context_log.error("CRITICAL: Failed to move partially failed modified file to dustbin - FILE KEPT FOR SAFETY")
        else:
            context_log.info("Kept original file despite partial destination failure (per configuration)")
            
    elif rules_matched_and_triggered_actions and not any_destination_failures and modifications_made and settings.DELETE_ON_NO_DESTINATION:
        # Even files with no destinations go to dustbin for safety
        success = dustbin_service.move_to_dustbin(
            source_file_path=str(original_filepath),
            study_instance_uid=study_instance_uid,
            sop_instance_uid=sop_instance_uid,
            task_id=task_id,
            destinations_confirmed=[],
            reason="rules_matched_modifications_made_no_destinations"
        )
        if success:
            context_log.info("MEDICAL SAFETY: File with rules/modifications but no destinations moved to dustbin")
        else:
            context_log.error("CRITICAL: Failed to move no-destination file to dustbin - FILE KEPT FOR SAFETY")
    
    else:
        context_log.info("File disposition: No action taken, file kept for safety")


def _handle_final_file_disposition(
        original_filepath: Path,
        was_successful: bool,
        rules_matched_and_triggered_actions: bool,
        modifications_made: bool,
        any_destination_failures: bool,
        context_log: Any,
        task_id: str):
    """
    DEPRECATED: Legacy file disposition function.
    This function has been replaced with _handle_final_file_disposition_with_dustbin
    for medical-grade safety. Keeping for reference but should not be used.
    """
    context_log.warning("DEPRECATED: Using legacy file disposition - should use dustbin service instead")
    
    if not original_filepath or not original_filepath.exists():
        return
    if was_successful and not any_destination_failures:
        if settings.DELETE_ON_SUCCESS:
            try:
                original_filepath.unlink(missing_ok=True)
                context_log.info("Deleted original file on success.")
            except OSError as e:
                context_log.warning(
                    "Failed to delete original file on success.",
                    error=str(e))
        else:
            context_log.info("Kept original file on success.")
    elif not rules_matched_and_triggered_actions:
        if settings.DELETE_UNMATCHED_FILES:
            try:
                original_filepath.unlink(missing_ok=True)
                context_log.info(
                    "Deleted original file as no rules matched/triggered.")
            except OSError as e:
                context_log.warning(
                    "Failed to delete unmatched file.", error=str(e))
        else:
            context_log.info(
                "Kept original file as no rules matched/triggered.")
    elif any_destination_failures:
        if settings.MOVE_TO_ERROR_ON_PARTIAL_FAILURE:
            context_log.warning(
                "Partial destination failure. Moving original file to error directory.")
            _move_to_error_dir(original_filepath, task_id, context_log)
        elif settings.DELETE_ON_PARTIAL_FAILURE_IF_MODIFIED and modifications_made:
            try:
                original_filepath.unlink(missing_ok=True)
                context_log.info(
                    "Deleted modified original file on partial failure.")
            except OSError as e:
                context_log.warning(
                    "Failed to delete modified file on partial failure.",
                    error=str(e))
        else:
            context_log.info(
                "Kept original file despite partial destination failure.")
    elif rules_matched_and_triggered_actions and not any_destination_failures and modifications_made and settings.DELETE_ON_NO_DESTINATION:
        context_log.info(
            "Rules matched, mods made, no destinations. Deleting original (DELETE_ON_NO_DESTINATION=true).")
        try:
            original_filepath.unlink(missing_ok=True)
        except OSError as e:
            context_log.warning(
                "Failed to delete file with no destinations after modification.",
                error=str(e))
    else:
        context_log.info("Kept original file based on disposition settings.")
