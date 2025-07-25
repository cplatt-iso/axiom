# app/services/network/dimse/handlers.py

import os
import uuid
import re
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import date, datetime, time

# Use structlog for logging
import structlog # type: ignore

from pydicom import dcmread
from pydicom.errors import InvalidDicomError
from pydicom.dataset import Dataset

from app.core.config import settings
from app.worker.tasks import process_dicom_file_task
from app.worker.celery_app import app as current_celery_app
from app import crud, schemas
from app.db.session import SessionLocal
from app.schemas.enums import ProcessedStudySourceType, OrderStatus, MppsStatus
from app.crud import crud_mpps


from app.crud.crud_imaging_order import imaging_order
from app.db.models.imaging_order import ImagingOrder



# MPPS SOP Class UID (N-CREATE/SET)
MPPS_SOP_CLASS_UID = "1.2.840.10008.3.1.2.3.3"

DMWL_SOP_CLASS_UID = "1.2.840.10008.5.1.4.31"

# Get logger instance (should inherit config from server.py)
logger = structlog.get_logger("dicom_listener.handlers")
INCOMING_DIR = Path(settings.DICOM_STORAGE_PATH)

_current_listener_name: Optional[str] = None
_current_listener_instance_id: Optional[str] = None

def set_current_listener_context(name: str, instance_id: str):
    """Sets the name and instance ID of the currently running listener configuration."""
    global _current_listener_name, _current_listener_instance_id
    _current_listener_name = name
    _current_listener_instance_id = instance_id
    # Bind context for subsequent handler logs if needed, or just log directly
    log = logger.bind(listener_name=name, listener_instance_id=instance_id)
    log.info("Handler context set for listener")

# --- C-ECHO Handler ---
def handle_echo(event):
    """Handle a C-ECHO request event with structured logging."""
    log = logger.bind(
        assoc_id=event.assoc.native_id,
        calling_ae=event.assoc.requestor.ae_title,
        called_ae=event.assoc.acceptor.ae_title,
        source_ip=event.assoc.requestor.address,
        event_type="C-ECHO"
    )
    log.info("Received C-ECHO request")
    return 0x0000 # Success

# --- C-STORE Handler ---
def handle_store(event):
    """Handle a C-STORE request event with structured logging."""
    filepath: Optional[Path] = None
    assoc_id = event.assoc.native_id
    calling_ae = event.assoc.requestor.ae_title
    called_ae = event.assoc.acceptor.ae_title
    source_ip = event.assoc.requestor.address

    source_identifier = _current_listener_name or f"dimse_listener_{settings.LISTENER_HOST}"
    listener_instance_id = _current_listener_instance_id

    # Bind context for this specific C-STORE operation
    log = logger.bind(
        assoc_id=assoc_id,
        calling_ae=calling_ae,
        called_ae=called_ae,
        source_ip=source_ip,
        listener_name=source_identifier, # Use the resolved name
        listener_instance_id=listener_instance_id,
        event_type="C-STORE"
    )

    if not listener_instance_id:
         log.warning("Handler could not determine specific listener instance ID. Metrics will not be incremented.")

    association_info: Dict[str, Any] = {
        "calling_ae_title": calling_ae,
        "called_ae_title": called_ae,
        "source_ip": source_ip,
    }

    try:
        ds = event.dataset
        ds.file_meta = event.file_meta
        sop_instance_uid = ds.get("SOPInstanceUID", None)
        log = log.bind(instance_uid=sop_instance_uid or "Unknown") # Add UID to context

        if sop_instance_uid:
            # Sanitize SOP Instance UID for use in filename
            filename_sop = re.sub(r'[^\w.-]', '_', str(sop_instance_uid)) # Ensure it's a string
            filename_sop = filename_sop[:100] # Limit length
            filename = f"{filename_sop}.dcm"
        else:
             # Generate a unique filename if SOPInstanceUID is missing
             unique_id = uuid.uuid4()
             filename = f"no_sopuid_{unique_id}.dcm"
             log.warning("Received instance missing SOPInstanceUID. Using generated filename.", generated_filename=filename)

        filepath = INCOMING_DIR / filename
        log = log.bind(target_filepath=str(filepath)) # Add target path to context
        log.info("Received C-STORE request") # Simple message, details in context

        log.debug("Saving received object")
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        ds.save_as(filepath, write_like_original=False) # Consider performance implications
        log.info("Successfully saved DICOM file")

        # Increment Received Count
        if listener_instance_id:
            db_session = None
            increment_succeeded = False
            try:
                db_session = SessionLocal()
                increment_succeeded = crud.crud_dimse_listener_state.increment_received_count(
                    db=db_session, listener_id=listener_instance_id, count=1
                )
                db_session.commit() # Commit the increment here
                if not increment_succeeded:
                     log.warning("Received count increment did not affect any rows (state record might be missing initially?).")
                else:
                     log.debug("Incremented received count successfully.")
            except Exception as db_err:
                 log.error("Error during received count increment", database_error=str(db_err), exc_info=True)
                 if db_session and db_session.is_active: db_session.rollback()
            finally:
                 if db_session: db_session.close()
        else:
             log.warning("Cannot increment received count: Listener instance ID is unknown.")

        log.debug("Attempting to dispatch processing task...")
        try:
            source_type_str = ProcessedStudySourceType.DIMSE_LISTENER.value
            if not listener_instance_id:
                 log.error("Cannot dispatch task - Listener instance ID is unknown. File will not be processed.")
                 return 0xA700 # Out of Resources

            log.debug("Dispatching task", task_name='process_dicom_file_task', source_type=source_type_str)
            current_celery_app.send_task(
                 'process_dicom_file_task',
                 args=[str(filepath)],
                 kwargs={
                      'source_type': source_type_str,
                      'source_db_id_or_instance_id': listener_instance_id,
                      'association_info': association_info
                      }
            )
            log.info("Task dispatched successfully")

        except Exception as celery_err:
            log.error("Failed to dispatch Celery task", error=str(celery_err), exc_info=True)
            if filepath and filepath.exists():
                 try: filepath.unlink(); log.info("Cleaned up file after failed Celery dispatch")
                 except OSError as unlink_err: log.error("Could not delete file after failed Celery dispatch", error=str(unlink_err))
            return 0xA700 # Out of Resources - Cannot Queue

        return 0x0000 # Success

    except InvalidDicomError as e:
        log.error("Invalid DICOM data received", error=str(e), exc_info=False)
        return 0xC000 # Cannot understand
    except Exception as e:
        log.error("Unexpected error handling C-STORE request", error=str(e), exc_info=True)
        if filepath and filepath.exists():
            try: filepath.unlink(); log.info("Cleaned up potentially partial file after error")
            except OSError as unlink_err: log.error("Could not delete file after error", error=str(unlink_err))
        return 0xA900 # Processing Failure

def _parse_dicom_date_range(date_str: str) -> tuple[Optional[date], Optional[date]]:
    """Parses a DICOM date string (YYYYMMDD or YYYYMMDD-YYYYMMDD) into start and end dates."""
    if not date_str:
        return None, None
    
    parts = date_str.split('-')
    try:
        start_dt = datetime.strptime(parts[0].strip(), '%Y%m%d').date()
        if len(parts) > 1 and parts[1].strip():
            end_dt = datetime.strptime(parts[1].strip(), '%Y%m%d').date()
            return start_dt, end_dt
        return start_dt, start_dt  # If only one date, the range is that single day
    except (ValueError, IndexError):
        logger.warn("DMWL_INVALID_DATE_FORMAT", provided_date=date_str)
        return None, None

def _imaging_order_to_dmwl_dataset(order: ImagingOrder) -> Dataset:
    """Converts our ImagingOrder model into a pydicom Dataset for a DMWL response."""
    ds = Dataset()

    # Patient Level
    ds.PatientName = order.patient_name
    ds.PatientID = order.patient_id
    if order.patient_dob:
        # Use string conversion to ensure compatibility with date or string types
        ds.PatientBirthDate = str(order.patient_dob).replace('-', '')
    ds.PatientSex = order.patient_sex

    # Study Level
    ds.AccessionNumber = order.accession_number
    ds.StudyInstanceUID = order.study_instance_uid or '' # Must not be None
    ds.RequestingPhysician = order.requesting_physician
    ds.ReferringPhysicianName = order.referring_physician
    
    # Scheduled Procedure Step Sequence (the most important part)
    sps_item = Dataset()
    sps_item.ScheduledStationAETitle = order.scheduled_station_ae_title
    sps_item.ScheduledStationName = order.scheduled_station_name
    if order.scheduled_procedure_step_start_datetime:
        sps_item.ScheduledProcedureStepStartDate = order.scheduled_procedure_step_start_datetime.strftime('%Y%m%d')
        sps_item.ScheduledProcedureStepStartTime = order.scheduled_procedure_step_start_datetime.strftime('%H%M%S')
    sps_item.Modality = order.modality
    sps_item.ScheduledPerformingPhysicianName = '' # Placeholder
    sps_item.ScheduledProcedureStepDescription = order.requested_procedure_description
    sps_item.ScheduledProcedureStepStatus = order.order_status.value if hasattr(order.order_status, 'value') else str(order.order_status)
    
    # The sequence itself must be a list of datasets
    ds.ScheduledProcedureStepSequence = [sps_item]
    
    # Required but often empty
    ds.RequestedProcedureID = ''
    ds.RequestedProcedureDescription = order.requested_procedure_description # Can be at top level too
    
    # Add the Specific Character Set
    ds.SpecificCharacterSet = "ISO_IR 100"

    return ds

# --- C-FIND Handler (The new star of the show) ---
def handle_c_find(event):
    """Handle a C-FIND request event."""
    log = logger.bind(
        assoc_id=event.assoc.native_id,
        calling_ae=event.assoc.requestor.ae_title,
        called_ae=event.assoc.acceptor.ae_title,
        source_ip=event.assoc.requestor.address,
        event_type="C-FIND"
    )
    log.info("C_FIND_REQUEST_RECEIVED")

    # CORRECTED: The SOP Class UID is in the `abstract_syntax` attribute of the context.
    if event.context.abstract_syntax != DMWL_SOP_CLASS_UID:
        log.warn("C_FIND_UNSUPPORTED_SOP_CLASS", sop_class=event.context.abstract_syntax)
        # Yield success with no data for unsupported types
        yield 0x0000, None
        return

    log.info("C_FIND_DMWL_QUERY_DETECTED")
    query_dataset = event.identifier
    
    # Extract query parameters from the ScheduledProcedureStepSequence
    # This is how a DMWL query is structured.
    sps_query = query_dataset.ScheduledProcedureStepSequence[0]
    
    modality = sps_query.get("Modality", None)
    ae_title = sps_query.get("ScheduledStationAETitle", None)
    
    # Extract and parse the date range
    date_str = sps_query.get("ScheduledProcedureStepStartDate", "")
    start_date, end_date = _parse_dicom_date_range(date_str)
    
    # Extract top-level patient identifiers
    patient_name = query_dataset.get("PatientName", None)
    patient_id = query_dataset.get("PatientID", None)
    status_str = sps_query.get("ScheduledProcedureStepStatus", None)

    log.info(
        "C_FIND_DMWL_QUERY_PARAMS",
        modality=modality,
        ae_title=ae_title,
        date_range=date_str,
        patient_name=patient_name,
        patient_id=patient_id,
        status=status_str,
    )
    
    db = None
    try:
        db = SessionLocal()
        # Use our glorious CRUD function
        worklist_items = imaging_order.get_worklist(
            db,
            modality=modality,
            scheduled_station_ae_title=ae_title,
            patient_name=patient_name,
            patient_id=patient_id,
            start_date=start_date,
            end_date=end_date,
            status=status_str,
        )

        log.info("C_FIND_DMWL_QUERY_FOUND_RESULTS", count=len(worklist_items))

        # Yield each matching item as a Pending response
        for item in worklist_items:
            response_ds = _imaging_order_to_dmwl_dataset(item)
            yield (0xFF00, response_ds) # 0xFF00 = Pending

        log.info("C_FIND_DMWL_QUERY_COMPLETED")
        # After sending all matches, send a final Success response
        yield (0x0000, None)

    except Exception as e:
        log.error("C_FIND_DMWL_HANDLER_ERROR", error=str(e), exc_info=True)
        # Yield a failure status
        yield (0xA900, None) # Processing Failure
    finally:
        if db:
            db.close()
            log.debug("C_FIND_DB_SESSION_CLOSED")

def handle_n_create(event):
    """Handle an N-CREATE request event for MPPS."""
    log = logger.bind(
        assoc_id=event.assoc.native_id,
        calling_ae=event.assoc.requestor.ae_title,
        called_ae=event.assoc.acceptor.ae_title,
        source_ip=event.assoc.requestor.address,
        event_type="N-CREATE"
    )
    log.info("N_CREATE_REQUEST_RECEIVED")

    # Prepare a status dataset. We'll modify it on failure.
    status_ds = Dataset()
    status_ds.Status = 0x0000 # Assume success until proven otherwise

    if event.request.AffectedSOPClassUID != MPPS_SOP_CLASS_UID:
        log.warning("N_CREATE_WRONG_SOP_CLASS", sop_class_uid=event.request.AffectedSOPClassUID)
        status_ds.Status = 0x0110  # Processing failure
        status_ds.ErrorComment = f"Unsupported SOP Class UID: {event.request.AffectedSOPClassUID}"
        return status_ds, None

    req_ds = event.request.AttributeList
    if not isinstance(req_ds, Dataset):
        try:
            # If it's not a dataset, it might be a stream. Parse it.
            # force=True is critical for datasets sent via DIMSE without file meta info
            req_ds = dcmread(req_ds, force=True)
        except InvalidDicomError as e:
            log.error("N_CREATE_DCMREAD_FAILED", error=str(e), exc_info=True)
            status_ds.Status = 0xC002 # Not a DICOM dataset
            status_ds.ErrorComment = "Failed to parse AttributeList as DICOM."
            return status_ds, None
        except Exception as e:
            log.error("N_CREATE_UNEXPECTED_PARSE_ERROR", error=str(e), exc_info=True)
            status_ds.Status = 0xA900 # Processing failure
            status_ds.ErrorComment = "Unexpected error parsing AttributeList."
            return status_ds, None
        
    sop_instance_uid = req_ds.SOPInstanceUID

    with SessionLocal() as db:
        existing_mpps = crud.mpps.get_by_sop_instance_uid(db, sop_instance_uid=sop_instance_uid)
        if existing_mpps:
            log.warning("N_CREATE_MPPS_EXISTS", sop_instance_uid=sop_instance_uid)
            status_ds.Status = 0x0107  # Duplicate SOP Instance
            status_ds.ErrorComment = f"MPPS SOP Instance UID {sop_instance_uid} already exists."
            return status_ds, None

        try:
            mpps_status_str = req_ds.PerformedProcedureStepStatus
            mpps_status = MppsStatus(mpps_status_str.upper())
        except (AttributeError, ValueError):
            log.error("N_CREATE_INVALID_STATUS", status=req_ds.get("PerformedProcedureStepStatus"))
            status_ds.Status = 0x0106  # Invalid attribute value
            status_ds.ErrorComment = f"Invalid or missing PerformedProcedureStepStatus: {req_ds.get('PerformedProcedureStepStatus')}"
            return status_ds, None

        # --- Your existing logic for linking to an order ---
        imaging_order_id = None
        accession_number = req_ds.ScheduledStepAttributesSequence[0].get("AccessionNumber") if 'ScheduledStepAttributesSequence' in req_ds and req_ds.ScheduledStepAttributesSequence else None
        if accession_number:
            db_order = crud.imaging_order.get_by_accession_number(db, accession_number=accession_number)
            if db_order:
                imaging_order_id = db_order.id
                log.info("N_CREATE_LINKED_TO_ORDER", order_id=imaging_order_id)
                if mpps_status == MppsStatus.IN_PROGRESS:
                    crud.imaging_order.update_status(db, order_id=db_order.id, new_status=OrderStatus.IN_PROGRESS)
                    log.info("N_CREATE_ORDER_STATUS_UPDATED", order_id=db_order.id, new_status=OrderStatus.IN_PROGRESS.value)
            else:
                 log.warning("N_CREATE_ORDER_NOT_FOUND", accession_number=accession_number)
        # --- End order logic ---

        mpps_in = schemas.MppsCreate(
            sop_instance_uid=sop_instance_uid,
            status=mpps_status,
            modality=req_ds.get("Modality"),
            performed_station_ae_title=req_ds.get("PerformedStationAETitle"),
            # Be defensive with date parsing if needed
            performed_procedure_step_start_datetime=req_ds.get("PerformedProcedureStepStartDate"),
            raw_mpps_message=req_ds.to_json_dict(),
            imaging_order_id=imaging_order_id,
        )
        crud.mpps.create(db, obj_in=mpps_in)

    log.info("N_CREATE_SUCCESS", sop_instance_uid=sop_instance_uid)
    # --- DICOM STANDARD COMPLIANCE FIX ---
    # The response to N-CREATE can include the attributes of the created object.
    # It must return a tuple: (status_dataset, affected_sop_instance_dataset).
    return status_ds, req_ds

# --- N-SET Handler ---
def handle_n_set(event):
    """Handle an N-SET request event for MPPS."""
    log = logger.bind(
        assoc_id=event.assoc.native_id,
        calling_ae=event.assoc.requestor.ae_title,
        called_ae=event.assoc.acceptor.ae_title,
        source_ip=event.assoc.requestor.address,
        event_type="N-SET"
    )
    log.info("N_SET_REQUEST_RECEIVED")
    
    # Prepare a status dataset. We'll modify it on failure.
    status_ds = Dataset()
    status_ds.Status = 0x0000 # Assume success

    if event.request.RequestedSOPClassUID != MPPS_SOP_CLASS_UID:
        log.warning("N_SET_WRONG_SOP_CLASS", sop_class_uid=event.request.RequestedSOPClassUID)
        status_ds.Status = 0x0110  # Processing failure
        status_ds.ErrorComment = f"Unsupported SOP Class UID: {event.request.RequestedSOPClassUID}"
        return status_ds, None

    req_ds = event.request.ModificationList
    if not isinstance(req_ds, Dataset):
        try:
            # If it's not a dataset, it might be a stream. Parse it.
            # force=True is critical for datasets sent via DIMSE without file meta info
            req_ds = dcmread(req_ds, force=True)
        except InvalidDicomError as e:
            log.error("N_SET_DCMREAD_FAILED", error=str(e), exc_info=True)
            status_ds.Status = 0xC002 # Not a DICOM dataset
            status_ds.ErrorComment = "Failed to parse ModificationList as DICOM."
            return status_ds, None
        except Exception as e:
            log.error("N_SET_UNEXPECTED_PARSE_ERROR", error=str(e), exc_info=True)
            status_ds.Status = 0xA900 # Processing failure
            status_ds.ErrorComment = "Unexpected error parsing ModificationList."
            return status_ds, None
        
    sop_instance_uid = event.request.RequestedSOPInstanceUID

    with SessionLocal() as db:
        db_mpps = crud.mpps.get_by_sop_instance_uid(db, sop_instance_uid=sop_instance_uid)
        if not db_mpps:
            log.warning("N_SET_MPPS_NOT_FOUND", sop_instance_uid=sop_instance_uid)
            status_ds.Status = 0x0112  # No such SOP Instance
            status_ds.ErrorComment = f"MPPS SOP Instance UID {sop_instance_uid} not found."
            return status_ds, None

        try:
            mpps_status_str = req_ds.PerformedProcedureStepStatus
            mpps_status = MppsStatus(mpps_status_str.upper())
        except (AttributeError, ValueError):
            log.error("N_SET_INVALID_STATUS", status=req_ds.get("PerformedProcedureStepStatus"))
            status_ds.Status = 0x0106  # Invalid attribute value
            status_ds.ErrorComment = f"Invalid or missing PerformedProcedureStepStatus: {req_ds.get('PerformedProcedureStepStatus')}"
            return status_ds, None

        # --- Your existing logic for updating order status ---
        if db_mpps.imaging_order_id:
            new_order_status = None
            if mpps_status == MppsStatus.COMPLETED: new_order_status = OrderStatus.COMPLETED
            elif mpps_status == MppsStatus.DISCONTINUED: new_order_status = OrderStatus.CANCELED
            if new_order_status:
                crud.imaging_order.update_status(db, order_id=db_mpps.imaging_order_id, new_status=new_order_status)
                log.info("N_SET_ORDER_STATUS_UPDATED", order_id=db_mpps.imaging_order_id, new_status=new_order_status.value)
        # --- End order logic ---

        mpps_update = schemas.MppsUpdate(
            status=mpps_status,
            performed_procedure_step_end_datetime=req_ds.get("PerformedProcedureStepEndDate"),
            raw_mpps_message=req_ds.to_json_dict(),
        )
        crud.mpps.update(db, db_obj=db_mpps, obj_in=mpps_update)

    log.info("N_SET_SUCCESS", sop_instance_uid=sop_instance_uid)
    return status_ds, None # Return the success status dataset