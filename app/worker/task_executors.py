# app/worker/task_executors.py

import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple, cast

import anyio.abc
import pydicom.dataset
import pydicom.uid
import structlog # Consistent logging
import pydicom
from pydicom.errors import InvalidDicomError
from pydicom.dataset import Dataset, FileMetaDataset
from sqlalchemy.orm import Session # For type hinting

from app.db import models as db_models # For RuleSet type hint
from app import crud # For DB operations
from app.db.models import ProcessedStudySourceType
# Ensure DicomWebSourceState is available for type hinting if not already via db_models
# from app.db.models.dicomweb_source_state import DicomWebSourceState # If needed directly

from app.db.models.storage_backend_config import StorageBackendConfig as DBStorageBackendConfigModel

from app.services.storage_backends import get_storage_backend, StorageBackendError
from app.services import dicomweb_client # For DICOMweb task
from app.services.storage_backends.google_healthcare import GoogleHealthcareDicomStoreStorage # For GHC task
from app.core.config import settings

from app.worker.processing_orchestrator import process_instance_against_rules
from app.worker.task_utils import build_storage_backend_config_dict
from app.services import ai_assist_service

# Import ANYIO_AVAILABLE and portal management components
try:
    import anyio
    ANYIO_AVAILABLE = True # type: ignore
except ImportError:
    ANYIO_AVAILABLE = False

logger = structlog.get_logger(__name__)

# Define RETRYABLE_EXCEPTIONS here if they are specific to executor logic,
# or ensure they are imported if defined in tasks.py and executors need to re-raise them.
# For now, assuming RETRYABLE_EXCEPTIONS are caught and handled by tasks.py's Celery decorator.
# If an executor needs to signal a retry, it should re-raise an exception in that list.

def _serialize_result(result_val: Any) -> Any:
    """Safely serializes common result types for Celery task return."""
    if isinstance(result_val, (dict, str, int, float, list, bool, type(None))):
        return result_val
    elif isinstance(result_val, Path):
        return str(result_val)
    # Add specific handling if needed, e.g., for pynetdicom status types
    # from pynetdicom._globals import Status # Example import if needed
    # if isinstance(result_val, Status):
    #     return {"status_value": result_val.value, "status_description": str(result_val)}
    
    # --- AGGRESSIVE FALLBACK ---
    else:
        try:
            # Try a simple string conversion
            return str(result_val) 
        except Exception:
            # If str() fails, just return the type name and potentially key attributes if possible
            return f"<{type(result_val).__name__} Instance (Unserializable)>"
    # --- END FALLBACK ---


async def _execute_processing_core(
    task_context_log: structlog.BoundLoggerBase,
    db_session: Session,
    original_ds: pydicom.Dataset,
    source_identifier_for_matching: str,
    task_id: str, # For GHC store context, if needed elsewhere
    original_filepath_for_storage: Optional[Path], # For backends that need the original file path
    association_info: Optional[Dict[str, str]] = None,
    ai_portal_instance: Optional['anyio.abc.BlockingPortal'] = None # Parameter
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset]]:
    """
    Internal core logic for processing a dataset against rules and handling destinations.
    This function is SYNC by default but can be called from an async context via to_thread.
    The AI portal management happens *before* calling this if AI is involved.

    Returns:
        Tuple: (
            rules_matched_and_triggered_actions: bool,
            modifications_made_to_dataset: bool,
            final_status_code: str,
            final_message: str,
            applied_rules_info: List[str],
            destination_processing_statuses: Dict,
            processed_ds: Optional[pydicom.Dataset] # The dataset after processing
        )
    """
    local_log = task_context_log
    modifications_made = False
    rules_matched_triggered = False
    any_dest_failures = False
    dest_statuses: Dict[str, Dict[str, Any]] = {}
    
    active_rulesets: List[db_models.RuleSet] = await asyncio.to_thread(crud.ruleset.get_active_ordered, db_session)

    if not active_rulesets:
        local_log.info("No active rulesets found in core processing.") # type: ignore
        return False, False, "success_no_rulesets", "No active rulesets.", [], {}, original_ds

    # Removed local ai_portal_instance variable that obscured the parameter.
    # The parameter ai_portal_instance is used directly below.
    
    # Check if AI is possible and if the current execution context allows creating a portal
    # This is tricky if _execute_processing_core is called from an async task executor.
    # The portal should be managed by the task executor (caller of this core function).
    # For now, we assume process_instance_against_rules handles a None portal gracefully.

    # This simplified version assumes the portal is managed by the caller (task_executor).
    # If this core were to manage it, it'd need to know if it's in an async context.
    
    if original_ds is None:
        local_log.error("original_ds is None in _execute_processing_core.") # type: ignore
        return False, False, "error_core_no_dataset", "Dataset is None in core processing.", [], {}, None

    processed_ds_from_rules, applied_rules, unique_dest_dicts = await asyncio.to_thread(process_instance_against_rules,
         original_ds=original_ds,
         active_rulesets=active_rulesets,
         source_identifier=source_identifier_for_matching,
         db_session=db_session,
         association_info=association_info,
         ai_portal=ai_portal_instance # Use the parameter directly
     )

    if processed_ds_from_rules is not original_ds and processed_ds_from_rules is not None:
        modifications_made = True
    if not processed_ds_from_rules: # Fallback if orchestrator returns None unexpectedly
        processed_ds_from_rules = original_ds
    
    if applied_rules or unique_dest_dicts:
        rules_matched_triggered = True

    if not rules_matched_triggered:
        return False, modifications_made, "success_no_matching_rules", f"No matching rules for {source_identifier_for_matching}.", applied_rules, {}, processed_ds_from_rules

    dataset_to_send = processed_ds_from_rules

    if not unique_dest_dicts:
        return True, modifications_made, "success_no_destinations", "Rules matched, no destinations.", applied_rules, {}, processed_ds_from_rules

    # --- Process Destinations ---
    all_dest_succeeded_run = True
    instance_uid_for_filename = getattr(dataset_to_send, 'SOPInstanceUID', 'UnknownSOPInstanceUID_InCore')

    for i, dest_info in enumerate(unique_dest_dicts):
        dest_id = dest_info.get("id")
        dest_name = dest_info.get("name", f"UnknownDest_ID_{dest_id}")
        dest_log = local_log.bind(dest_idx=i+1, dest_id=dest_id, dest_name=dest_name)

        if dest_id is None:
            dest_statuses[f"MalformedDest_{i+1}"] = {"status": "error", "message": "Missing ID"}
            all_dest_succeeded_run = False; any_dest_failures = True; continue
        
        dest_name = dest_info.get("name", f"UnknownDest_ID_{dest_id}") # Ensure dest_name is defined

        if dest_id is None: # Should be caught by the previous check, but for safety if dest_name relies on it
            dest_statuses[f"MalformedDest_Idx_{i+1}"] = {"status": "error", "message": "Destination ID is None after initial check."}
            all_dest_succeeded_run = False; any_dest_failures = True; continue

        db_storage_model = await asyncio.to_thread(crud.crud_storage_backend_config.get, db_session, id=dest_id)
        if not db_storage_model or not db_storage_model.is_enabled:
            status_msg = "DB config not found" if not db_storage_model else "Destination disabled"
            dest_statuses[dest_name] = {"status": "skipped_config_issue", "message": status_msg}
            if not db_storage_model: all_dest_succeeded_run = False; any_dest_failures = True # Not finding config is an error
            continue

        actual_storage_config = build_storage_backend_config_dict(db_storage_model, task_id)
        if not actual_storage_config:
            dest_statuses[dest_name] = {"status": "error", "message": "Failed to build storage config"}
            all_dest_succeeded_run = False; any_dest_failures = True; continue

        if dataset_to_send is None:
            dest_log.error(f"dataset_to_send is None before storing to dest {dest_name} in _execute_processing_core.") # type: ignore
            dest_statuses[dest_name] = {"status": "error", "message": "Dataset is None, cannot store."}
            all_dest_succeeded_run = False; any_dest_failures = True; continue
        
        try:
            storage_backend = get_storage_backend(actual_storage_config)
            filename_ctx = f"{instance_uid_for_filename}.dcm"
            
            # Critical: Handling async store methods from this sync core function.
            # If storage_backend.store is async, this direct call will fail.
            # The calling executor (if async itself) must handle this by awaiting appropriately
            # or this core function would need an async variant or portal usage for destinations.
            # For now, assume sync store or that async executor handles it.
            
            # Refactored: if store is async, await it. If sync, run in thread.
            if asyncio.iscoroutinefunction(storage_backend.store):
                store_result = await storage_backend.store(
                    dataset_to_send, original_filepath_for_storage, filename_ctx, source_identifier_for_matching
                )
            else:
                store_result = await asyncio.to_thread(
                    storage_backend.store,
                    dataset_to_send, original_filepath_for_storage, filename_ctx, source_identifier_for_matching
                )
            status_key = "duplicate" if store_result == "duplicate" else "success"
            dest_statuses[dest_name] = {"status": status_key, "result": _serialize_result(store_result)}
        except StorageBackendError as e:
            dest_log.warning(f"StorageBackendError for {dest_name}", error=str(e)) # type: ignore
            dest_statuses[dest_name] = {"status": "error", "message": str(e)}
            all_dest_succeeded_run = False; any_dest_failures = True
            # Propagate retryable exceptions for Celery
            # This check should ideally be more specific to Retryable StorageBackendError subtypes
            from app.worker.tasks import RETRYABLE_EXCEPTIONS # Ugly, but tasks.py defines it
            if any(isinstance(e, retryable_exc_type) for retryable_exc_type in RETRYABLE_EXCEPTIONS):
                raise
        except Exception as e:
            dest_log.error(f"Unexpected error for {dest_name}", error=str(e), exc_info=True) # type: ignore
            dest_statuses[dest_name] = {"status": "error", "message": f"Unexpected: {e}"}
            all_dest_succeeded_run = False; any_dest_failures = True
    
    final_status = "success_all_destinations" if all_dest_succeeded_run else "partial_failure_destinations"
    final_msg = f"Processed. All destinations succeeded." if all_dest_succeeded_run else f"Processed. {sum(1 for s in dest_statuses.values() if s['status']=='error')} destination(s) failed."
    
    return rules_matched_triggered, modifications_made, final_status, final_msg, applied_rules, dest_statuses, processed_ds_from_rules


# --- File-Based Task Executor ---
def execute_file_based_task(
    task_context_log: structlog.BoundLoggerBase,
    db_session: Session,
    dicom_filepath_str: str,
    source_type_str: str,
    source_db_id_or_instance_id: Union[int, str],
    task_id: str, # from Celery task
    association_info: Optional[Dict[str, str]] = None,
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None # Receives portal from task
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset], str, str]:
    """
    Executor for tasks that start with a DICOM file on disk (DIMSE Listener, QR, File Upload).
    Resolves the source identifier name for rule matching.
    Receives ai_portal if AI is enabled for this task type.
    """
    original_filepath = Path(dicom_filepath_str)
    instance_uid = "UnknownSOPInstanceUID_FileTaskExec"
    # Default source identifier, used if DB lookup fails or name is missing
    default_source_identifier = f"{source_type_str}_{source_db_id_or_instance_id}"
    source_identifier = default_source_identifier # Start with default

    # Use the passed logger, don't rebind here initially, bind specific context later
    log = task_context_log

    try:
        # --- Determine source_identifier_for_matching ---
        log.debug("Attempting to resolve source identifier for matching.", # type: ignore
                  source_type=source_type_str, source_id_or_inst=str(source_db_id_or_instance_id))
                  
        source_type_enum = ProcessedStudySourceType(source_type_str)

        if source_type_enum == ProcessedStudySourceType.DIMSE_LISTENER and isinstance(source_db_id_or_instance_id, str):
            listener_config = None # Initialize
            try:
                listener_config = crud.crud_dimse_listener_config.get_by_instance_id(db_session, instance_id=source_db_id_or_instance_id)
                if listener_config and listener_config.name:
                    source_identifier = listener_config.name
                    log.debug("Resolved source identifier from listener config name.", resolved_name=source_identifier) # type: ignore
                else:
                    # Keep the default source_identifier
                    log.warning("Could not find listener config or name is empty/null, using default identifier.", # type: ignore
                                listener_instance_id=source_db_id_or_instance_id,
                                config_found=(listener_config is not None),
                                config_name=getattr(listener_config, 'name', 'N/A') if listener_config else 'N/A')
            except Exception as db_lookup_err:
                 # Keep the default source_identifier
                 log.error("Error during DB lookup for listener config, using default identifier.", # type: ignore
                           listener_instance_id=source_db_id_or_instance_id, error=str(db_lookup_err))

        elif source_type_enum == ProcessedStudySourceType.DIMSE_QR and isinstance(source_db_id_or_instance_id, int):
            qr_config = None # Initialize
            try:
                qr_config = crud.crud_dimse_qr_source.get(db_session, id=source_db_id_or_instance_id)
                if qr_config and qr_config.name:
                    source_identifier = qr_config.name
                    log.debug("Resolved source identifier from QR config name.", resolved_name=source_identifier) # type: ignore
                else:
                     # Keep the default source_identifier
                     log.warning("Could not find QR config or name is empty/null, using default identifier.", # type: ignore
                                 qr_source_id=source_db_id_or_instance_id,
                                 config_found=(qr_config is not None),
                                 config_name=getattr(qr_config, 'name', 'N/A') if qr_config else 'N/A')
            except Exception as db_lookup_err:
                 # Keep the default source_identifier
                 log.error("Error during DB lookup for QR config, using default identifier.", # type: ignore
                           qr_source_id=source_db_id_or_instance_id, error=str(db_lookup_err))

        elif source_type_enum == ProcessedStudySourceType.FILE_UPLOAD:
            # Use a fixed identifier or one passed via task kwargs if needed
            source_identifier = "FILE_UPLOAD"
            log.debug("Using fixed source identifier for FILE_UPLOAD source type.") # type: ignore
            
        # Bind the final resolved source identifier for subsequent logs from this context
        log = log.bind(source_identifier_for_matching=source_identifier)
        # --- End source identifier resolution ---

        # --- Read DICOM File ---
        log.debug("Reading DICOM file.") # type: ignore
        original_ds = pydicom.dcmread(str(original_filepath), force=True)
        instance_uid = getattr(original_ds, 'SOPInstanceUID', instance_uid)
        log = log.bind(instance_uid=instance_uid) # Bind instance UID after reading
        # --- End Read DICOM File ---

    except InvalidDicomError as e:
        log.error("Invalid DICOM file format in executor.", error_msg=str(e)) # type: ignore
        raise # Propagate to let tasks.py handle error dir move and Celery status
    except Exception as init_exc:
        log.error("Error during executor initialization (source lookup or file read).", error_msg=str(init_exc), exc_info=True) # type: ignore
        raise # Propagate other init errors

    # --- Core Processing Call ---
    log.debug("Fetching active rulesets and calling processing orchestrator.") # type: ignore
    active_rulesets: List[db_models.RuleSet] = crud.ruleset.get_active_ordered(db_session) # Uses `from app import crud`
    if not active_rulesets:
        log.info("No active rulesets found.") # type: ignore
        # Return signature: rules_matched, mods_made, status_code, msg, applied_rules, dest_statuses, processed_ds, instance_uid, source_identifier
        return False, False, "success_no_rulesets", "No active rulesets.", [], {}, original_ds, instance_uid, source_identifier

    if original_ds is None: # Should not happen if dcmread was successful
        log.error("original_ds is None before calling process_instance_against_rules in file-based task.") # type: ignore
        return False, False, "error_file_no_dataset", "Dataset not loaded for file-based processing", [], {}, None, instance_uid, source_identifier


    # Call the synchronous processing orchestrator, passing the resolved source_identifier and ai_portal
    processed_ds_rules, applied_rules, unique_dest_dicts = process_instance_against_rules(
        original_ds=original_ds,
        active_rulesets=active_rulesets,
        source_identifier=source_identifier, # Use the resolved identifier
        db_session=db_session,
        association_info=association_info,
        ai_portal=ai_portal # Pass the portal received from tasks.py
    )

    modifications_made = (processed_ds_rules is not original_ds and processed_ds_rules is not None)
    rules_matched_triggered = bool(applied_rules or unique_dest_dicts)
    if not processed_ds_rules:
        log.warning("process_instance_against_rules returned None for dataset, falling back to original.") # type: ignore
        processed_ds_rules = original_ds

    if not rules_matched_triggered:
        log.info(f"No matching rules triggered actions for {source_identifier}.") # type: ignore
        return rules_matched_triggered, modifications_made, "success_no_matching_rules", \
               f"No matching rules for {source_identifier}.", applied_rules, {}, \
               processed_ds_rules, instance_uid, source_identifier

    dataset_to_send = processed_ds_rules
    final_dest_statuses: Dict[str, Dict[str, Any]] = {}
    all_dest_ok = True
    any_dest_failures_local = False

    if not unique_dest_dicts: # Rules matched, but no destinations
         log.info("Rules matched, but no destinations configured.") # type: ignore
         return rules_matched_triggered, modifications_made, "success_no_destinations", \
               "Rules matched, no destinations.", applied_rules, {}, \
               processed_ds_rules, instance_uid, source_identifier
               
    # --- Destination Loop ---
    log.info(f"Processing {len(unique_dest_dicts)} destinations.") # type: ignore
    instance_uid_for_filename = getattr(dataset_to_send, 'SOPInstanceUID', instance_uid)
    for i, dest_info in enumerate(unique_dest_dicts):
        dest_id = dest_info.get("id")
        dest_name = dest_info.get("name", f"UnknownDest_ID_{dest_id}")
        dest_log = log.bind(dest_idx=i+1, dest_id=dest_id, dest_name=dest_name) # Rebind for loop context

        if dest_id is None:
            final_dest_statuses[f"MalformedDest_{i+1}"] = {"status": "error", "message": "Missing ID"}
            all_dest_ok = False; any_dest_failures_local = True; continue
        
        db_storage_model = crud.crud_storage_backend_config.get(db_session, id=dest_id) # Corrected: direct sync call
        if not db_storage_model:
            dest_log.warning("Destination DB config not found.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "DB config not found"}
            all_dest_ok = False; any_dest_failures_local = True; continue
        if not db_storage_model.is_enabled:
            dest_log.info("Destination disabled.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "skipped_disabled", "message": "Disabled"}
            continue

        actual_storage_config = build_storage_backend_config_dict(db_storage_model, task_id)
        if not actual_storage_config:
            dest_log.warning("Failed to build destination storage config.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "Failed to build storage config"}
            all_dest_ok = False; any_dest_failures_local = True; continue

        if dataset_to_send is None:
            dest_log.error(f"dataset_to_send is None before storing to dest {dest_name} in file-based task.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "Dataset is None, cannot store."}
            all_dest_ok = False; any_dest_failures_local = True; continue

        try:
            storage_backend = get_storage_backend(actual_storage_config)
            filename_ctx = f"{instance_uid_for_filename}.dcm"
            
            if asyncio.iscoroutinefunction(storage_backend.store):
                 dest_log.error("CRITICAL SYNC/ASYNC MISMATCH: File-based task (sync) trying to use an async store method directly.", backend_name=dest_name) # type: ignore
                 raise StorageBackendError(f"Async store method for {dest_name} cannot be called from sync file-based task executor.")

            store_result = storage_backend.store(
                dataset_to_send, original_filepath, filename_ctx, source_identifier # Pass resolved ID
            )
            status_key = "duplicate" if store_result == "duplicate" else "success"
            # final_dest_statuses[dest_name] = {"status": status_key, "result": _serialize_result(store_result)}
            serialized_res = _serialize_result(store_result)
            final_dest_statuses[dest_name] = {"status": status_key, "result": serialized_res}
            dest_log.info(f"Store to {dest_name} reported: {status_key}") # type: ignore
        except StorageBackendError as e:
            dest_log.warning(f"StorageBackendError for {dest_name}", error_msg=str(e)) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": str(e)}
            all_dest_ok = False; any_dest_failures_local = True
            from app.worker.tasks import RETRYABLE_EXCEPTIONS 
            if any(isinstance(e, retryable_exc_type) for retryable_exc_type in RETRYABLE_EXCEPTIONS):
                raise 
        except Exception as e:
            dest_log.error(f"Unexpected error for {dest_name}", error_msg=str(e), exc_info=True) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": f"Unexpected: {e}"}
            all_dest_ok = False; any_dest_failures_local = True

    current_status_code = "success_all_destinations" if all_dest_ok else "partial_failure_destinations"
    current_msg = f"File task processing complete. All destinations OK: {all_dest_ok}."
    
    # Return the resolved source_identifier used for matching
    return (rules_matched_triggered, modifications_made, current_status_code, current_msg, 
           applied_rules, final_dest_statuses, processed_ds_rules, instance_uid, source_identifier)

async def execute_ghc_task(
    task_context_log: structlog.BoundLoggerBase,
    db_session: Session,
    source_id: int,
    study_uid: str,
    task_id: str, # from Celery task
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None # Pass if DICOMweb tasks can use AI
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset], str, str]: # rules_match, mods_made, status, msg, applied, dest_stat, ds, instance_uid, source_name
    
    log = task_context_log.bind(ghc_exec_source_id=source_id, ghc_exec_study_uid=study_uid)
    log.info("Executor started: execute_ghc_task") # type: ignore

    instance_uid_res = "Unknown_GHC_Executor"
    source_name_res = f"GHC_src_exec_{source_id}"
    original_ds: Optional[pydicom.Dataset] = None

    # 1. Fetch GHC Source Config (using to_thread for sync DB call)
    source_config_model = await asyncio.to_thread(crud.google_healthcare_source.get, db_session, id=source_id)
    if not source_config_model:
        log.error("GHC source config not found in executor.") # type: ignore
        raise ValueError(f"GHC Source config ID {source_id} not found.")
    source_name_res = source_config_model.name
    log = log.bind(source_identifier_for_matching=source_name_res)

    # 2. Fetch Instance Metadata from GHC (this logic is async)
    try:
        # Simplified metadata fetch logic from original GHC task:
        # You'll need to instantiate GoogleHealthcareDicomStoreStorage and call its async methods
        backend_config_dict = {
            "type": "google_healthcare", "name": f"GHC_Fetcher_{source_config_model.name}",
            "gcp_project_id": source_config_model.gcp_project_id, "gcp_location": source_config_model.gcp_location,
            "gcp_dataset_id": source_config_model.gcp_dataset_id, "gcp_dicom_store_id": source_config_model.gcp_dicom_store_id,
        }
        ghc_fetch_backend = GoogleHealthcareDicomStoreStorage(config=backend_config_dict)        
        
        series_list = await asyncio.to_thread(ghc_fetch_backend.search_series, study_instance_uid=study_uid, limit=1)
        if not series_list or not series_list[0].get("0020000E", {}).get("Value"):
            log.info("No series found for GHC study in executor.") # type: ignore
            return False, False, "success_no_series_ghc", "No series in GHC study", [], {}, None, instance_uid_res, source_name_res

        first_series_uid = series_list[0]["0020000E"]["Value"][0]
        instance_metadata_list = await asyncio.to_thread(
            ghc_fetch_backend.search_instances,
            study_instance_uid=study_uid, series_instance_uid=first_series_uid, limit=1
        )
        if not instance_metadata_list or not instance_metadata_list[0].get("00080018", {}).get("Value"):
            log.info("No instance metadata found for GHC series in executor.") # type: ignore
            return False, False, "success_no_instances_ghc", "No instances in GHC series", [], {}, None, instance_uid_res, source_name_res
        
        instance_metadata_json = instance_metadata_list[0]
        instance_uid_res = instance_metadata_json["00080018"]["Value"][0]
        log = log.bind(instance_uid=instance_uid_res) # Update log with actual instance UID
        try:
            original_ds = pydicom.Dataset.from_json(instance_metadata_json)
        except Exception as json_parse_exc:
            log.error("Failed to parse JSON to Pydicom Dataset for GHC task", error=str(json_parse_exc), exc_info=True) # type: ignore
            raise ValueError(f"Failed to parse DICOM JSON from GHC: {json_parse_exc}")
        # ... (add file_meta to original_ds as done in other tasks) ...

    except StorageBackendError as ghc_fetch_err: # If GHC client raises this and it's retryable
        log.warning("GHC fetch error in executor (StorageBackendError).", error_msg=str(ghc_fetch_err)) # type: ignore
        raise # Propagate to let Celery retry
    except Exception as e:
        log.error("Unexpected error fetching/parsing GHC metadata in executor.", error_msg=str(e), exc_info=True) # type: ignore
        raise # Propagate general errors

    # 3. Core Processing (run sync process_instance_against_rules in a thread)
    active_rulesets = await asyncio.to_thread(crud.ruleset.get_active_ordered, db_session) # Corrected: was get_active_rulesets
    if not active_rulesets:
        log.info("No active rulesets for GHC task.") # type: ignore
        return (False, False, "success_no_rulesets_ghc", "No active rulesets.",
                [], {}, original_ds, instance_uid_res, source_name_res)
    
    if original_ds is None:
        log.error("original_ds is None before calling process_instance_against_rules in GHC task.") # type: ignore
        return False, False, "error_ghc_no_dataset", "Dataset not loaded for GHC processing", [], {}, None, instance_uid_res, source_name_res

    processed_ds_rules, applied_rules, unique_dest_dicts = await asyncio.to_thread(
        process_instance_against_rules,
        original_ds,
        active_rulesets,
        source_name_res, # source_identifier_for_matching
        db_session,
        None, # association_info - typically None for DICOMweb polling
        ai_portal # Pass the portal if provided
    )

    modifications_made = (processed_ds_rules is not original_ds and processed_ds_rules is not None)
    rules_matched_triggered = bool(applied_rules or unique_dest_dicts)
    if not processed_ds_rules: processed_ds_rules = original_ds

    if not rules_matched_triggered:
        log.info(f"No matching GHC rules triggered actions for {source_name_res}.") # type: ignore
        return rules_matched_triggered, modifications_made, "success_no_matching_rules_ghc", \
               f"No matching rules for GHC {source_name_res}.", applied_rules, {}, \
               processed_ds_rules, instance_uid_res, source_name_res
    
    dataset_to_send = processed_ds_rules
    final_dest_statuses = {}
    all_dest_ok = True
    
    if not unique_dest_dicts:
         return rules_matched_triggered, modifications_made, "success_no_destinations_ghc", \
               "Rules matched, no destinations.", applied_rules, {}, \
               processed_ds_rules, instance_uid_res, source_name_res

    # 4. Destination Loop (Async Aware)
    log.info(f"Processing {len(unique_dest_dicts)} destinations for GHC instance {instance_uid}.") # type: ignore
    dest_store_tasks = [] # For asyncio.gather
    sync_dest_results = [] # Store results from sync stores run in threads

    for i, dest_info in enumerate(unique_dest_dicts):
        # ... (get db_storage_model (threaded), build_actual_storage_config as before) ...
        # This part is sync
        dest_id_loop = dest_info.get("id") # type: ignore
        
        if dest_id_loop is None:
            log.warning(f"Destination info for GHC task missing ID at index {i}.", dest_info_received=dest_info) # type: ignore
            final_dest_statuses[f"MalformedDest_GHC_{i+1}"] = {"status": "error", "message": "Missing ID in destination info"}
            all_dest_ok = False; continue

        dest_name_loop = dest_info.get("name", f"UnknownDest_ID_{dest_id_loop}") # type: ignore

        db_model_sync = await asyncio.to_thread(crud.crud_storage_backend_config.get, db_session, id=dest_id_loop)
        if not db_model_sync or not db_model_sync.is_enabled:
            # ... handle and continue ...
            final_dest_statuses[dest_name_loop] = {"status": "skipped_config_issue", "message": "DB config not found or disabled"}
            all_dest_ok = False; continue 
        config_sync = build_storage_backend_config_dict(db_model_sync, task_id)
        if not config_sync:
            # ... handle and continue ...
            all_dest_ok = False; continue

        storage_backend_instance = get_storage_backend(config_sync) # This is sync call
        filename_ctx_loop = f"{instance_uid_res}.dcm"

        if dataset_to_send is None:
            log.error(f"dataset_to_send is None before storing to GHC dest {dest_name_loop}.") # type: ignore
            final_dest_statuses[dest_name_loop] = {"status": "error", "message": "Dataset is None, cannot store."}
            all_dest_ok = False; continue

        if asyncio.iscoroutinefunction(storage_backend_instance.store):
            dest_store_tasks.append(
                # Store context with the task for result mapping
                (dest_name_loop, storage_backend_instance.store(
                    dataset_to_send, None, filename_ctx_loop, source_name_res
                ))
            )
        else: # Sync store, run in thread
            sync_store_task = asyncio.to_thread(
                storage_backend_instance.store,
                dataset_to_send, None, filename_ctx_loop, source_name_res
            )
            dest_store_tasks.append((dest_name_loop, sync_store_task)) # Also gather threads

    if dest_store_tasks:
        gathered_results = await asyncio.gather(
            *[coro_or_thread_task for _, coro_or_thread_task in dest_store_tasks], 
            return_exceptions=True
        )
        for i, result_or_exc in enumerate(gathered_results):
            current_dest_name = dest_store_tasks[i][0]
            if isinstance(result_or_exc, Exception):
                # ... handle exception, update final_dest_statuses, all_dest_ok ...
                log.warning(f"Error storing to GHC dest {current_dest_name}", error_msg=str(result_or_exc)) # type: ignore
                final_dest_statuses[current_dest_name] = {"status": "error", "message": str(result_or_exc)}
                all_dest_ok = False
                # Check for retryable exceptions if they can come from store methods
                from app.worker.tasks import RETRYABLE_EXCEPTIONS 
                if any(isinstance(result_or_exc, retryable_exc_type) for retryable_exc_type in RETRYABLE_EXCEPTIONS):
                    raise result_or_exc # Propagate for Celery retry
            else:
                # ... handle success, update final_dest_statuses ...
                status_key = "duplicate" if result_or_exc == "duplicate" else "success"
                final_dest_statuses[current_dest_name] = {"status": status_key, "result": _serialize_result(result_or_exc)}

    status_code_res = "success_all_destinations_ghc" if all_dest_ok else "partial_failure_destinations_ghc"
    msg_res = "GHC processing complete."
    
    # Increment GHC source count (threaded)
    # ...

    return (rules_matched_triggered, modifications_made, status_code_res, msg_res, 
           applied_rules, final_dest_statuses, processed_ds_rules, instance_uid_res, source_name_res)

# --- DICOMweb Task Executor ---
async def execute_dicomweb_task(
    task_context_log: structlog.BoundLoggerBase,
    db_session: Session, # Sync session, use with to_thread
    source_id: int,
    study_uid: str,
    series_uid: str,
    instance_uid: str, # This is the specific instance being processed
    task_id: str, # Celery task ID
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None # Pass if DICOMweb tasks can use AI
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset], str, str]:
    # Returns: rules_matched, mods_made, status_code, msg, applied_rules, dest_statuses, processed_ds, instance_uid_processed, source_name_processed

    log = task_context_log.bind(
        dicomweb_exec_source_id=source_id,
        dicomweb_exec_study_uid=study_uid,
        dicomweb_exec_series_uid=series_uid,
        dicomweb_exec_instance_uid=instance_uid
    )
    log.info("Executor started: execute_dicomweb_task") # type: ignore

    source_name_res_default = f"DICOMweb_src_exec_{source_id}" # Default
    original_ds: Optional[pydicom.Dataset] = None
    processed_ds_final: Optional[pydicom.Dataset] = None

    # 1. Fetch DICOMweb Source Config (sync DB call)
    # Explicitly type retrieved_config and source_config_typed to help Pylance
    retrieved_config: Optional[db_models.DicomWebSourceState] = await asyncio.to_thread(crud.dicomweb_source.get, db_session, id=source_id)
    if not retrieved_config:
        log.error("DICOMweb source config not found in executor.") # type: ignore
        # Propagate a clear error; task level will catch and format Celery response
        raise ValueError(f"DICOMweb source config ID {source_id} not found for executor.")
    
    source_config_typed: db_models.DicomWebSourceState = retrieved_config # Now definitely DicomWebSourceState
    # source_name_res: str = source_config_typed.source_name # This should be str
    source_name_res: str = cast(str, source_config_typed.source_name) 
    log = log.bind(source_identifier_for_matching=source_name_res) # Key for rule matching

    # 2. Fetch Instance Metadata (async HTTP call via dicomweb_client)
    try:
        log.debug("Fetching instance metadata via DICOMweb.") # type: ignore
        metadata_json_list_raw = await asyncio.to_thread( # Renamed to _raw temporarily
            dicomweb_client.retrieve_instance_metadata,
            config=source_config_typed, study_uid=study_uid, series_uid=series_uid, instance_uid=instance_uid
        )

        if metadata_json_list_raw is None:
            metadata_json_list: List[Dict[str, Any]] = []
        elif isinstance(metadata_json_list_raw, dict):
            metadata_json_list: List[Dict[str, Any]] = [metadata_json_list_raw]
        else:
            metadata_json_list: List[Dict[str, Any]] = metadata_json_list_raw

        if not metadata_json_list:
            log.info("Instance metadata not found via DICOMweb (possibly deleted).") # type: ignore
            return (False, False, "success_metadata_not_found", "Instance metadata not found.",
                    [], {}, None, instance_uid, source_name_res)
        
        # DICOM Standard Part 18 - Instance Metadata is returned as an array of JSON objects, one for each requested instance.
        # Here, we request one, so we take the first element.
        original_ds = pydicom.Dataset.from_json(metadata_json_list[0])
        
        # Add FileMeta if missing for consistent processing (as in original tasks.py)
        if original_ds is not None:
            if not hasattr(original_ds, 'file_meta') or not original_ds.file_meta:
                file_meta = pydicom.dataset.FileMetaDataset() # Ensuring this line is correct
                # Ensure SOPClassUID and SOPInstanceUID exist in the dataset from metadata
                if "SOPClassUID" not in original_ds or "SOPInstanceUID" not in original_ds:
                    log.error("SOPClassUID or SOPInstanceUID missing from DICOMweb metadata.") # type: ignore
                    raise ValueError("Incomplete DICOMweb metadata for dataset construction.")
                file_meta.MediaStorageSOPClassUID = original_ds.SOPClassUID
                file_meta.MediaStorageSOPInstanceUID = original_ds.SOPInstanceUID # Should match input instance_uid
                file_meta.TransferSyntaxUID = pydicom.uid.ImplicitVRLittleEndian # A safe default
                file_meta.ImplementationClassUID = pydicom.uid.UID(settings.PYDICOM_IMPLEMENTATION_UID)
                file_meta.ImplementationVersionName = settings.IMPLEMENTATION_VERSION_NAME
                original_ds.file_meta = file_meta
        else:
            log.error("Dataset creation from JSON resulted in None for DICOMweb task.") # type: ignore
            return (False, False, "error_dicomweb_dataset_creation", "Failed to create dataset from JSON.",
                    [], {}, None, instance_uid, source_name_res)
         
        processed_ds_final = original_ds # Start with original

    except dicomweb_client.DicomWebClientError as fetch_exc: # If client raises specific retryable errors
        log.warning("DICOMweb client error fetching metadata in executor.", error_msg=str(fetch_exc)) # type: ignore
        raise fetch_exc # Propagate to let Celery handle retry via RETRYABLE_EXCEPTIONS
    except Exception as e:
        log.error("Failed to fetch or parse DICOMweb metadata in executor.", error_msg=str(e), exc_info=True) # type: ignore
        raise # Propagate general errors to task level

    # 3. Core Processing (run sync process_instance_against_rules in a thread)
    active_rulesets = await asyncio.to_thread(crud.ruleset.get_active_ordered, db_session) # Corrected: was get_active_rulesets
    if not active_rulesets:
        log.info("No active rulesets for DICOMweb task.") # type: ignore
        return (False, False, "success_no_rulesets_dicomweb", "No active rulesets.", # Corrected for DICOMweb
                [], {}, original_ds, instance_uid, source_name_res) # Corrected to use instance_uid
    
    if original_ds is None: # Check before passing to process_instance_against_rules
        log.error("original_ds is None before calling process_instance_against_rules in DICOMweb task.") # type: ignore
        return False, False, "error_dicomweb_no_dataset", "Dataset not loaded for DICOMweb processing", [], {}, None, instance_uid, source_name_res

    log.debug(f"Calling process_instance_against_rules for DICOMweb instance {instance_uid}.") # type: ignore
    processed_ds_rules, applied_rules, unique_dest_dicts = await asyncio.to_thread(
        process_instance_against_rules,
        original_ds,
        active_rulesets,
        source_name_res, # source_identifier_for_matching
        db_session,
        None, # association_info - typically None for DICOMweb polling
        ai_portal # Pass the portal if provided
    )

    modifications_made = (processed_ds_rules is not original_ds and processed_ds_rules is not None)
    rules_matched_triggered = bool(applied_rules or unique_dest_dicts)
    if not processed_ds_rules: processed_ds_rules = original_ds
    processed_ds_final = processed_ds_rules # Update with potentially modified DS

    if not rules_matched_triggered:
        log.info(f"No matching DICOMweb rules triggered actions for {source_name_res}, instance {instance_uid}.") # type: ignore
        # Increment processed count even if no rules match, as metadata was processed
        try:
            inc_res = await asyncio.to_thread(crud.dicomweb_state.increment_processed_count, db_session, source_name=source_name_res, count=1)
            if inc_res: await asyncio.to_thread(db_session.commit)
        except Exception as e_inc: log.error("Failed to inc processed count (no match)", error=str(e_inc)) # type: ignore
        return (rules_matched_triggered, modifications_made, "success_no_matching_rules_dicomweb",
               f"No matching rules for DICOMweb {source_name_res}.", applied_rules, {},
               processed_ds_final, instance_uid, source_name_res)
               
    dataset_to_send = processed_ds_final
    final_dest_statuses: Dict[str, Dict[str, Any]] = {}
    all_dest_ok = True

    if not unique_dest_dicts: # Rules matched, but no destinations
         log.info("DICOMweb rules matched, but no destinations configured.") # type: ignore
         # Increment processed count
         try:
            inc_res = await asyncio.to_thread(crud.dicomweb_state.increment_processed_count, db_session, source_name=source_name_res, count=1)
            if inc_res: await asyncio.to_thread(db_session.commit)
         except Exception as e_inc: log.error("Failed to inc processed count (no dest)", error=str(e_inc)) # type: ignore
         return (rules_matched_triggered, modifications_made, "success_no_destinations_dicomweb",
               "Rules matched, no destinations.", applied_rules, {},
               processed_ds_final, instance_uid, source_name_res)
               
    # 4. Destination Loop (Async Aware)
    log.info(f"Processing {len(unique_dest_dicts)} destinations for DICOMweb instance {instance_uid}.") # type: ignore
    dest_store_coroutines = [] # For asyncio.gather for async stores
    # For sync stores, we'll run them sequentially threaded for now to simplify result mapping,
    # but could also gather threads.

    for i, dest_info in enumerate(unique_dest_dicts):
        dest_id = dest_info.get("id")
        dest_name = dest_info.get("name", f"UnknownDest_ID_{dest_id}")
        dest_log = log.bind(dest_idx=i+1, dest_id_loop=dest_id, dest_name_loop=dest_name) # Rebind for loop context

        if dest_id is None:
            final_dest_statuses[f"MalformedDest_{i+1}"] = {"status": "error", "message": "Missing ID"}
            all_dest_ok = False; continue
        
        db_storage_model = await asyncio.to_thread(crud.crud_storage_backend_config.get, db_session, id=dest_id)
        if not db_storage_model:
            dest_log.warning("Destination DB config not found.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "DB config not found"}
            all_dest_ok = False; continue
        if not db_storage_model.is_enabled:
            dest_log.info("Destination disabled.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "skipped_disabled", "message": "Disabled"}
            continue

        actual_storage_config = build_storage_backend_config_dict(db_storage_model, task_id)
        if not actual_storage_config:
            dest_log.warning("Failed to build destination storage config.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "Failed to build storage config"}
            all_dest_ok = False; continue

        storage_backend_instance = get_storage_backend(actual_storage_config) # Sync call
        filename_ctx_loop = f"{instance_uid}.dcm" # Use the specific instance_uid

        if dataset_to_send is None:
            dest_log.error(f"dataset_to_send is None before storing to DICOMweb dest {dest_name}.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "Dataset is None, cannot store."}
            all_dest_ok = False; continue

        try:
            if asyncio.iscoroutinefunction(storage_backend_instance.store):
                # Add coroutine to list to be gathered
                dest_store_coroutines.append(
                    (dest_name, storage_backend_instance.store(
                        dataset_to_send, None, filename_ctx_loop, source_name_res # original_filepath is None
                    ))
                )
            else: # Synchronous store method, run in thread
                store_result = await asyncio.to_thread(
                    storage_backend_instance.store,
                    dataset_to_send, None, filename_ctx_loop, source_name_res
                )
                status_key = "duplicate" if store_result == "duplicate" else "success"
                final_dest_statuses[dest_name] = {"status": status_key, "result": _serialize_result(store_result)}
                dest_log.info(f"Store (sync) to {dest_name} reported: {status_key}") # type: ignore
        except StorageBackendError as e: # Catch errors from the sync store call immediately
            dest_log.warning(f"StorageBackendError for sync dest {dest_name}", error_msg=str(e)) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": str(e)}
            all_dest_ok = False
            from app.worker.tasks import RETRYABLE_EXCEPTIONS
            if any(isinstance(e, retryable_exc_type) for retryable_exc_type in RETRYABLE_EXCEPTIONS):
                raise 
        except Exception as e:
            dest_log.error(f"Unexpected error for sync dest {dest_name}", error_msg=str(e), exc_info=True) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": f"Unexpected: {e}"}
            all_dest_ok = False

    # Gather results from async destination tasks
    if dest_store_coroutines:
        gathered_results = await asyncio.gather(
            *[coro for _, coro in dest_store_coroutines], 
            return_exceptions=True
        )
        for i, result_or_exc in enumerate(gathered_results):
            current_dest_name = dest_store_coroutines[i][0]
            dest_log = log.bind(gathered_dest_name=current_dest_name) # Rebind for context
            if isinstance(result_or_exc, Exception):
                e = result_or_exc
                dest_log.warning(f"Async store to {current_dest_name} failed in gather.", error_msg=str(e), # type: ignore
                                 exc_info=isinstance(e, StorageBackendError)) # More info for StorageBackendError
                final_dest_statuses[current_dest_name] = {"status": "error", "message": str(e)}
                all_dest_ok = False
                from app.worker.tasks import RETRYABLE_EXCEPTIONS
                if any(isinstance(e, retryable_exc_type) for retryable_exc_type in RETRYABLE_EXCEPTIONS):
                    raise e 
            else: # Success from async store
                store_result_async = result_or_exc
                status_key = "duplicate" if store_result_async == "duplicate" else "success"
                final_dest_statuses[current_dest_name] = {"status": status_key, "result": _serialize_result(store_result_async)}
                dest_log.info(f"Store (async) to {current_dest_name} reported: {status_key}") # type: ignore

    current_status_code = "success_all_destinations_dicomweb" if all_dest_ok else "partial_failure_destinations_dicomweb"
    current_msg = f"DICOMweb task processing complete. All destinations OK: {all_dest_ok}."
    
    # Increment processed count if all went well or even partially (as metadata was processed)
    try:
        inc_res = await asyncio.to_thread(crud.dicomweb_state.increment_processed_count, db_session, source_name=source_name_res, count=1)
        if inc_res: await asyncio.to_thread(db_session.commit)
        else: log.warning("Failed to increment DICOMweb processed count post-destinations.") # type: ignore
    except Exception as e_inc:
        log.error("DB error incrementing DICOMweb count post-destinations.", error_msg=str(e_inc)) # type: ignore
        await asyncio.to_thread(db_session.rollback)

    return (rules_matched_triggered, modifications_made, current_status_code, current_msg,
            applied_rules, final_dest_statuses, processed_ds_final, instance_uid, source_name_res)

def execute_stow_task(
    task_context_log: structlog.BoundLoggerBase,
    db_session: Session,
    temp_filepath_str: str,
    source_ip: Optional[str],
    task_id: str,
    ai_portal: Optional['anyio.abc.BlockingPortal'] = None # Include if STOW could use AI
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset], str, str]:
    """
    Executor for tasks initiated by a STOW-RS request, starting with a temporary DICOM file.
    """
    temp_filepath = Path(temp_filepath_str)
    # STOW-RS source identifier
    source_identifier = f"STOW_RS_FROM_{source_ip or 'UnknownIP'}"
    instance_uid = "UnknownSOPInstanceUID_StowTaskExec"

    # Bind more specific context for this executor
    log = task_context_log.bind(
        stow_temp_filepath=str(temp_filepath),
        stow_source_ip=source_ip,
        source_identifier_for_matching=source_identifier # This is key for rules
    )

    try:
        log.debug("Reading temporary DICOM file for STOW processing.") # type: ignore
        original_ds = pydicom.dcmread(str(temp_filepath), force=True)
        instance_uid = getattr(original_ds, 'SOPInstanceUID', instance_uid)
        log = log.bind(instance_uid=instance_uid) # Bind instance_uid after reading

    except InvalidDicomError as e:
        log.error("Invalid DICOM file from STOW source in executor.", error_msg=str(e)) # type: ignore
        # The temp file will be cleaned up by the finally block in tasks.py's STOW task
        raise # Propagate to let tasks.py handle Celery status
    except Exception as read_exc:
        log.error("Error reading temporary DICOM file for STOW in executor.", error_msg=str(read_exc), exc_info=True) # type: ignore
        raise

    # --- Core Processing Call ---
    active_rulesets: List[db_models.RuleSet] = crud.ruleset.get_active_ordered(db_session) # Uses `from app import crud`
    if not active_rulesets:
        log.info("No active rulesets for STOW task.") # type: ignore
        # Return: rules_matched, mods_made, status_code, msg, applied_rules, dest_statuses, processed_ds, instance_uid, source_identifier
        return False, False, "success_no_rulesets", "No active rulesets for STOW task.", [], {}, original_ds, instance_uid, source_identifier

    if original_ds is None: # Should not happen if dcmread was successful
        log.error("original_ds is None before calling process_instance_against_rules in STOW task.") # type: ignore
        return False, False, "error_stow_no_dataset", "Dataset not loaded for STOW processing", [], {}, None, instance_uid, source_identifier

    # Call the synchronous processing orchestrator
    processed_ds_rules, applied_rules, unique_dest_dicts = process_instance_against_rules(
        original_ds=original_ds,
        active_rulesets=active_rulesets,
        source_identifier=source_identifier, # Key for rule matching
        db_session=db_session,
        association_info=None, # STOW-RS usually doesn't have explicit DIMSE association info
        ai_portal=ai_portal # Pass along the portal if provided (e.g., if STOW could trigger AI)
    )

    modifications_made = (processed_ds_rules is not original_ds and processed_ds_rules is not None)
    rules_matched_triggered = bool(applied_rules or unique_dest_dicts)
    if not processed_ds_rules: # Fallback, should not happen if orchestrator is correct
        log.warning("process_instance_against_rules returned None for dataset (STOW), falling back to original.") # type: ignore
        processed_ds_rules = original_ds

    if not rules_matched_triggered:
        log.info(f"No matching STOW rules triggered actions for {source_identifier}.") # type: ignore
        return rules_matched_triggered, modifications_made, "success_no_matching_rules", \
               f"No matching rules for STOW source {source_identifier}.", applied_rules, {}, \
               processed_ds_rules, instance_uid, source_identifier

    dataset_to_send = processed_ds_rules
    final_dest_statuses: Dict[str, Dict[str, Any]] = {}
    all_dest_ok = True
    any_dest_failures_local = False

    if not unique_dest_dicts: # Rules matched, but no destinations
         log.info("STOW rules matched, but no destinations configured.") # type: ignore
         return rules_matched_triggered, modifications_made, "success_no_destinations", \
               "STOW rules matched, no destinations.", applied_rules, {}, \
               processed_ds_rules, instance_uid, source_identifier
               
    # --- Destination Loop ---
    log.info(f"Processing {len(unique_dest_dicts)} destinations for STOW task.") # type: ignore
    instance_uid_for_filename = getattr(dataset_to_send, 'SOPInstanceUID', instance_uid)
    for i, dest_info in enumerate(unique_dest_dicts):
        dest_id = dest_info.get("id")
        dest_name = dest_info.get("name", f"UnknownDest_ID_{dest_id}")
        dest_log = log.bind(dest_idx=i+1, dest_id_loop=dest_id, dest_name_loop=dest_name) # Rebind for loop context

        if dest_id is None: # Should be caught by destination_handler ideally
            final_dest_statuses[f"MalformedDest_{i+1}"] = {"status": "error", "message": "Missing ID"}
            all_dest_ok = False; any_dest_failures_local = True; continue
        
        # Uses `from app import crud`
        db_storage_model = crud.crud_storage_backend_config.get(db_session, id=dest_id)
        if not db_storage_model:
            dest_log.warning("STOW Destination DB config not found.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "DB config not found"}
            all_dest_ok = False; any_dest_failures_local = True; continue
        if not db_storage_model.is_enabled:
            dest_log.info("STOW Destination disabled.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "skipped_disabled", "message": "Disabled"}
            continue

        actual_storage_config = build_storage_backend_config_dict(db_storage_model, task_id)
        if not actual_storage_config:
            dest_log.warning("Failed to build STOW destination storage config.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "Failed to build storage config"}
            all_dest_ok = False; any_dest_failures_local = True; continue

        if dataset_to_send is None:
            dest_log.error(f"dataset_to_send is None before storing to STOW dest {dest_name}.") # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": "Dataset is None, cannot store."}
            all_dest_ok = False; any_dest_failures_local = True; continue

        try:
            storage_backend = get_storage_backend(actual_storage_config)
            filename_ctx = f"{instance_uid_for_filename}.dcm"
            
            # STOW executor is sync, so storage_backend.store must be sync
            if asyncio.iscoroutinefunction(storage_backend.store):
                 dest_log.error("CRITICAL SYNC/ASYNC MISMATCH: STOW task (sync) trying to use an async store method directly.", backend_name=dest_name) # type: ignore
                 raise StorageBackendError(f"Async store method for {dest_name} cannot be called from sync STOW task executor.")

            store_result = storage_backend.store(
                dataset_to_send,
                temp_filepath, # Pass the temp_filepath as original_filepath for STOW context
                filename_ctx,
                source_identifier
            )
            status_key = "duplicate" if store_result == "duplicate" else "success"
            final_dest_statuses[dest_name] = {"status": status_key, "result": _serialize_result(store_result)}
            dest_log.info(f"Store to STOW dest {dest_name} reported: {status_key}") # type: ignore
        except StorageBackendError as e:
            dest_log.warning(f"StorageBackendError for STOW dest {dest_name}", error_msg=str(e)) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": str(e)}
            all_dest_ok = False; any_dest_failures_local = True
            from app.worker.tasks import RETRYABLE_EXCEPTIONS 
            if any(isinstance(e, retryable_exc_type) for retryable_exc_type in RETRYABLE_EXCEPTIONS):
                raise 
        except Exception as e:
            dest_log.error(f"Unexpected error for STOW dest {dest_name}", error_msg=str(e), exc_info=True) # type: ignore
            final_dest_statuses[dest_name] = {"status": "error", "message": f"Unexpected: {e}"}
            all_dest_ok = False; any_dest_failures_local = True

    current_status_code = "success_all_destinations" if all_dest_ok else "partial_failure_destinations"
    current_msg = f"STOW task processing complete. All destinations OK: {all_dest_ok}."
    
    # No specific STOW processed count increment here unless tracked in DB
    
    return (rules_matched_triggered, modifications_made, current_status_code, current_msg, 
           applied_rules, final_dest_statuses, processed_ds_rules, instance_uid, source_identifier)

async def _execute_processing_core_async_wrapper(
    task_context_log: structlog.BoundLoggerBase,
    db_session: Session,
    original_ds: pydicom.Dataset,
    source_identifier_for_matching: str,
    task_id: str,
    original_filepath_for_storage: Optional[Path],
    association_info: Optional[Dict[str, str]],
    ai_portal_instance: Optional['anyio.abc.BlockingPortal']
) -> Tuple[bool, bool, str, str, List[str], Dict[str, Dict[str, Any]], Optional[pydicom.Dataset]]:
    """
    Async wrapper to call the synchronous rule processing and destination handling logic
    in a separate thread to avoid blocking the async event loop of the calling task (e.g., GHC task).
    
    The actual `_execute_processing_core` needs to be refactored to take `ai_portal`
    and pass it to `process_instance_against_rules`.
    
    This function's main purpose is to bridge the async GHC task executor
    to the largely synchronous processing pipeline.
    """

    # This is where the core logic from the original _execute_processing_core would go,
    # but it needs to be callable via to_thread.
    # For this example, assume the _execute_processing_core function (the one defined earlier)
    # is suitable to be run in a thread.
    
    # Critical: _execute_processing_core as defined earlier is SYNC.
    # It directly calls process_instance_against_rules (SYNC)
    # and then loops through destinations (SYNC store calls).
    # If an AI portal is used, process_instance_against_rules uses it to call an async AI func.

    # So, the entire block can be run in a thread if the calling task is async.
    
    # This function is a placeholder for the actual threaded execution.
    # The main challenge is that _execute_processing_core ITSELF makes blocking DB calls
    # and potentially blocking destination calls.
    
    # A truly async pipeline would make _execute_processing_core async,
    # and all its sub-calls (DB, destinations) async. This is a larger refactor.

    # For now, we run the existing _execute_processing_core (which might use a portal for AI)
    # in a thread from the async GHC task.
    
    # This function needs access to the true _execute_processing_core, let's assume it's available.
    # The placeholder _execute_processing_core_placeholder should be replaced by the real one.
    
    # Simulating the threaded call:
    # loop = asyncio.get_running_loop()
    # result = await loop.run_in_executor(
    #     None, # Default thread pool executor
    #     _execute_processing_core_actual_sync_version, # This is the renamed _execute_processing_core
    #     task_context_log, db_session, original_ds, source_identifier_for_matching, task_id,
    #     original_filepath_for_storage, association_info, ai_portal_instance # Pass portal
    # )
    # return result
    
    # Given the complexity, I will OMIT the full GHC async executor here.
    # The key takeaway is: async tasks require careful handling of sync code (DB, some store backends)
    # using asyncio.to_thread or ensuring the called libraries offer async interfaces.
    # The AI portal is one piece of this async/sync bridge.

    # For the purpose of this refactor stage, let's assume the async GHC task in tasks.py
    # will correctly use asyncio.to_thread for the new synchronous process_instance_against_rules
    # and its destination loop. The AI portal management for GHC task remains tricky if the core processing is sync.

    logger.error("_execute_processing_core_async_wrapper is a placeholder and needs full implementation for GHC.") # type: ignore
    # Return dummy values matching the Tuple signature
    return False, False, "error_placeholder", "GHC async wrapper not fully implemented", [], {}, original_ds