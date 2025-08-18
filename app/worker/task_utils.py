# app/worker/task_utils.py

from pathlib import Path
import traceback
import uuid
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
# import json # Not strictly needed if specific_config is not used this way

import pydicom
from pydicom.multival import MultiValue
from pydicom.dataelem import DataElement
import pydicom.uid  # Import the pydicom.uid module
from pydicom.valuerep import PersonName
from sqlalchemy.orm import Session
import structlog
from pydantic import TypeAdapter

from app import crud
# Specific model imports
from app.db.models.storage_backend_config import StorageBackendConfig as DBStorageBackendConfigModel
from app.db.models.dicom_exception_log import DicomExceptionLog
# Correct import for StorageBackendType enum
# <--- CORRECTED IMPORT
from app.services.storage_backends.base_backend import StorageBackendError, StorageBackendType as EnumStorageBackendType

# Pydantic schemas for validation and serialization
from app.schemas.dicom_exception_log import DicomExceptionLogCreate
# Import the *discriminated union* for reading storage backend configs
# <--- IMPORT THE UNION
from app.schemas.storage_backend_config import StorageBackendConfigRead
from app.schemas.enums import (
    ProcessedStudySourceType,
    ExceptionProcessingStage,
    ExceptionStatus
)
from app.core.config import settings

EXECUTOR_LEVEL_RETRYABLE_STORAGE_ERRORS = (
    # Or more specific subtypes if you define them, e.g. NetworkStorageError
    StorageBackendError,
    # DicomWebClientError, # If it can be raised by a storage backend or
    # similar context
    ConnectionRefusedError,  # These can also be caught directly by executors
    TimeoutError,
)

logger = structlog.get_logger(__name__)
logger_task_utils = structlog.get_logger("app.worker.task_utils")


def _safe_get_dicom_value(ds: Optional[pydicom.Dataset],
                          tag_keyword: str,
                          default: Optional[str] = None) -> Optional[str]:
    if not ds:
        return default

    retrieved_item = None
    vr = "N/A"  # Default VR if we can't get it

    try:
        retrieved_item = ds.get(tag_keyword)

        if retrieved_item is None:
            return default

        # Determine the actual value and VR
        if isinstance(retrieved_item, DataElement):
            val = retrieved_item.value
            vr = retrieved_item.VR
        else:
            # If ds.get() returns the bare value (e.g., in some test mocks or
            # older pydicom versions for some cases)
            val = retrieved_item
            # We can't easily get the VR if it's not a DataElement, so vr remains "N/A"
            # or we could try to infer it, but that's complex.

        if val is None:  # Tag exists but has no value, or bare value is None
            return default

        # Process val (the actual value)
        if isinstance(val, MultiValue):
            if not val:  # Empty MultiValue list
                return default
            return "\\".join(map(str, val))
        elif isinstance(val, (PersonName, pydicom.uid.UID)):
            return str(val)
        elif isinstance(val, bytes):
            try:
                return val.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    return val.decode('latin-1')
                except UnicodeDecodeError:
                    logger_task_utils.warning(
                        "Failed to decode bytes for tag",
                        tag=tag_keyword,
                        vr_log=vr,
                        value_repr=repr(val))
                    return f"<bytes_undecodable_{len(val)}>"
        elif isinstance(val, list):
            if not val:
                return default
            return "\\".join(map(str, val))

        return str(val)

    except Exception as e:
        # Log with the VR if we managed to get it
        logger_task_utils.warning(
            "Error accessing/converting DICOM tag in _safe_get_dicom_value",
            tag=tag_keyword,
            vr_log=vr,
            error_str=str(e),
            # exc_info=True # Consider adding for debug if errors persist
        )
        return default


def build_storage_backend_config_dict(
    db_storage_model: DBStorageBackendConfigModel,
    task_id: Optional[str] = "N/A"
) -> Optional[Dict[str, Any]]:
    log = logger.bind(
        storage_backend_id=db_storage_model.id,
        storage_backend_name=db_storage_model.name,
        task_id=task_id)
    log.debug(
        f"Attempting to build config dict for backend type from DB: {db_storage_model.backend_type}")

    try:
        adapter = TypeAdapter(StorageBackendConfigRead)
        pydantic_read_model = adapter.validate_python(
            db_storage_model, from_attributes=True)
        log.debug("Validated DB model to Pydantic model.", pydantic_model_type=type(
            pydantic_read_model).__name__, pydantic_model_attrs=dir(pydantic_read_model))

        backend_type_value: Optional[str] = None
        if hasattr(pydantic_read_model, 'backend_type'):
            log.debug(
                "Pydantic model has 'backend_type' attribute.",
                backend_type_attr_type=type(
                    pydantic_read_model.backend_type),
                backend_type_attr_value=str(
                    pydantic_read_model.backend_type))
            if isinstance(
                    pydantic_read_model.backend_type,
                    EnumStorageBackendType):
                backend_type_value = pydantic_read_model.backend_type.value
                log.debug(
                    "Derived backend_type_value from Enum.",
                    derived_value=backend_type_value)
            else:
                # Fallback if it's already a string or some other type that can
                # be stringified
                backend_type_value = str(pydantic_read_model.backend_type)
                log.debug(
                    "Derived backend_type_value via str() fallback.",
                    derived_value=backend_type_value)
        else:
            log.warning(
                "Pydantic model does NOT have 'backend_type' attribute.")
            # Attempt to infer from db_storage_model.backend_type as a last
            # resort if schema is inconsistent
            if db_storage_model.backend_type and isinstance(
                    db_storage_model.backend_type, EnumStorageBackendType):
                backend_type_value = db_storage_model.backend_type.value
                log.info(
                    "Falling back to db_storage_model.backend_type for backend_type_value.",
                    derived_value=backend_type_value)
            elif db_storage_model.backend_type:  # if it's already a string in DB model
                backend_type_value = str(db_storage_model.backend_type)
                log.info(
                    "Falling back to str(db_storage_model.backend_type) for backend_type_value.",
                    derived_value=backend_type_value)

        if not backend_type_value:
            log.error(
                "Critical: Could not determine backend_type value from Pydantic model or DB fallback. 'backend_type' attribute missing or invalid.",
                pydantic_model_type=type(pydantic_read_model).__name__,
                pydantic_model_dict=pydantic_read_model.model_dump() if pydantic_read_model else "N/A")
            return None  # If backend_type_value is not found, this function returns None

        config_dict = pydantic_read_model.model_dump(exclude_none=True)
        log.debug("Initial model_dump before adding 'type'",
                  initial_dict_keys=list(config_dict.keys()))

        # Ensure the 'type' key required by get_storage_backend is present in
        # the config_dict.
        config_dict['type'] = backend_type_value

        log.debug(
            "Successfully built storage backend config dict.",
            final_config_keys=list(
                config_dict.keys()),
            type_value_set=config_dict.get('type'))
        return config_dict
    except Exception as e:
        log.error(
            "Failed to build storage backend config dict from DB model.",
            error=str(e),
            backend_type_from_db=db_storage_model.backend_type,
            exc_info=True
        )
        return None


def _serialize_result(result_val: Any) -> Any:
    """Safely serializes common result types for Celery task return."""
    if isinstance(result_val, (dict, str, int, float, list, bool, type(None))):
        return result_val
    elif isinstance(result_val, Path):
        return str(result_val)
    # ... (other specific type handling if needed) ...
    else:
        try:
            return str(result_val)
        except Exception:
            return f"<{type(result_val).__name__} Instance (Unserializable)>"


def create_exception_log_entry(
    db: Session,
    *,
    exc: Exception,
    processing_stage: ExceptionProcessingStage,
    dataset: Optional[pydicom.Dataset] = None,
    study_instance_uid_str: Optional[str] = None,
    series_instance_uid_str: Optional[str] = None,
    sop_instance_uid_str: Optional[str] = None,
    failed_filepath: Optional[str] = None,
    original_source_type: Optional[ProcessedStudySourceType] = None,
    original_source_identifier: Optional[str] = None,
    calling_ae_title: Optional[str] = None,
    target_destination_db_model: Optional[DBStorageBackendConfigModel] = None,
    celery_task_id: Optional[str] = None,
    custom_error_message: Optional[str] = None,
    initial_status: ExceptionStatus = ExceptionStatus.NEW,
    retryable: bool = False,
    retry_delay_seconds: Optional[int] = None,
    commit_on_success: bool = False
) -> Optional[DicomExceptionLog]:
    # ... (this function's body remains the same as the last fully corrected version,
    # as its Pylance issues were related to type hints that should now be resolved
    # by the more specific DicomExceptionLog and DBStorageBackendConfigModel
    # imports)
    log = logger.bind(
        celery_task_id=celery_task_id,
        processing_stage_log=processing_stage.value
    )
    if not db:
        log.critical(
            "CRITICAL: No DB session provided to create_exception_log_entry")
        return None

    error_message = custom_error_message if custom_error_message else str(exc)
    capped_error_message = error_message[:2000]
    if exc.__traceback__:  # NEW - Check if traceback exists
        error_details_str = "".join(
            traceback.format_exception(
                type(exc), exc, exc.__traceback__))
    else:  # If no traceback (e.g. exception created but not raised) provide basic info
        error_details_str = f"{type(exc).__name__}: {exc}"
    capped_error_details = error_details_str[:
                                             65530] if error_details_str else None

    final_study_uid = study_instance_uid_str if study_instance_uid_str is not None else _safe_get_dicom_value(
        dataset, "StudyInstanceUID")
    final_series_uid = series_instance_uid_str if series_instance_uid_str is not None else _safe_get_dicom_value(
        dataset, "SeriesInstanceUID")
    final_sop_uid = sop_instance_uid_str if sop_instance_uid_str is not None else _safe_get_dicom_value(
        dataset, "SOPInstanceUID")

    log_create_data = DicomExceptionLogCreate(
        study_instance_uid=final_study_uid,  # Use the resolved UID
        series_instance_uid=final_series_uid,  # Use the resolved UID
        sop_instance_uid=final_sop_uid,  # Use the resolved UID
        patient_name=_safe_get_dicom_value(dataset, "PatientName"),
        patient_id=_safe_get_dicom_value(dataset, "PatientID"),
        accession_number=_safe_get_dicom_value(dataset, "AccessionNumber"),
        modality=_safe_get_dicom_value(dataset, "Modality"),
        processing_stage=processing_stage,
        error_message=capped_error_message,
        error_details=capped_error_details,
        failed_filepath=failed_filepath,
        original_source_type=original_source_type,
        original_source_identifier=original_source_identifier,
        calling_ae_title=calling_ae_title,
        target_destination_id=target_destination_db_model.id if target_destination_db_model else None,
        target_destination_name=target_destination_db_model.name if target_destination_db_model else None,
        status=initial_status,
        retry_count=0,
        next_retry_attempt_at=None,
        last_retry_attempt_at=None,
        resolved_at=None,
        resolved_by_user_id=None,
        resolution_notes=None,
        celery_task_id=celery_task_id,
    )

    if retryable and initial_status == ExceptionStatus.RETRY_PENDING:
        delay = retry_delay_seconds if retry_delay_seconds is not None else settings.CELERY_TASK_RETRY_DELAY
        log_create_data.next_retry_attempt_at = datetime.now(
            timezone.utc) + timedelta(seconds=delay)
        log.info("Exception marked as retryable, next attempt scheduled.",
                 next_attempt_at=log_create_data.next_retry_attempt_at)

    try:
        db_exception_log = crud.dicom_exception_log.create(
            db=db, obj_in=log_create_data)
        log.info(
            "DicomExceptionLog entry created in DB session.",
            exception_log_id_temp=db_exception_log.id,
            exception_uuid_temp=db_exception_log.exception_uuid
        )
        if commit_on_success:
            try:
                db.commit()
                log.info(
                    "DicomExceptionLog entry committed.",
                    exception_log_id=db_exception_log.id)
                db.refresh(db_exception_log)
            except Exception as db_commit_err:
                log.critical(
                    "Failed to commit DicomExceptionLog to DB after creation!",
                    error=str(db_commit_err),
                    exc_info=True)
                if db.is_active:
                    db.rollback()
                return None
        return db_exception_log
    except Exception as db_exc:
        log.critical(
            "CRITICAL DB ERROR: Failed to create DicomExceptionLog entry in DB session.",
            error=str(db_exc),
            original_exception_message=error_message,
            log_data_preview=log_create_data.model_dump(
                exclude_none=True,
                exclude_defaults=True),
            exc_info=True)
        if db.is_active:
            try:
                db.rollback()
            except Exception:
                pass
        return None
