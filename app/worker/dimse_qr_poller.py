# app/worker/dimse_qr_poller.py
import logging
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, Optional, Generator, Tuple, List
import re
import ssl
import os
import tempfile

# Pynetdicom imports
from pynetdicom import AE, debug_logger, Association
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,
    StudyRootQueryRetrieveInformationModelFind,
)
from pydicom.dataset import Dataset
from pydicom.tag import Tag

# Application imports
from sqlalchemy.orm import Session
from celery import shared_task
from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models
from app.db.models import ProcessedStudySourceType
from app import crud
from .dimse_qr_retriever import trigger_dimse_cmove_task

# Secret Manager Imports
try:
    from app.core.gcp_utils import (
        fetch_secret_content,
        SecretManagerError,
        SecretNotFoundError,
        PermissionDeniedError
    )
    SECRET_MANAGER_ENABLED = True
except ImportError:
    logging.getLogger(__name__).warning("GCP Utils / Secret Manager not available. TLS secret fetching will fail.")
    SECRET_MANAGER_ENABLED = False
    class SecretManagerError(Exception): pass
    class SecretNotFoundError(SecretManagerError): pass
    class PermissionDeniedError(SecretManagerError): pass
    def fetch_secret_content(*args, **kwargs): raise NotImplementedError("Secret Manager fetch not available")


# Configure logging
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
    # Assuming structlog is configured elsewhere for the Celery worker process
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    # Add a fallback warning if structlog isn't available
    logger.warning("structlog not found, using standard logging. Bind method will not work.")

# debug_logger() # Uncomment for verbose pynetdicom logs

class DimseQrPollingError(Exception):
    """Custom exception for DIMSE Q/R polling errors."""
    pass

# Constants
QR_LEVEL_SOP_CLASSES = {
    "PATIENT": PatientRootQueryRetrieveInformationModelFind,
    "STUDY": StudyRootQueryRetrieveInformationModelFind,
}

# Helper function _resolve_dynamic_date_filter
def _resolve_dynamic_date_filter(value: Any) -> Optional[str]:
    """
    Resolves special date strings into DICOM DA or range format.
    Returns None if the value is invalid or cannot be resolved.
    """
    if not isinstance(value, str):
        return None

    val_upper = value.strip().upper()
    today = date.today()

    if val_upper == "TODAY":
        return today.strftime('%Y%m%d')
    if val_upper == "YESTERDAY":
        yesterday = today - timedelta(days=1)
        return yesterday.strftime('%Y%m%d')

    match_days_ago = re.match(r"^-(\d+)D$", val_upper)
    if match_days_ago:
        try:
            days = int(match_days_ago.group(1))
            if days >= 0:
                start_date = today - timedelta(days=days)
                return f"{start_date.strftime('%Y%m%d')}-"
        except (ValueError, OverflowError):
            pass # Fall through if conversion fails
        return None # Invalid number of days or format

    if re.match(r"^\d{8}$", val_upper): return val_upper
    if re.match(r"^\d{8}-$", val_upper): return val_upper
    if re.match(r"^\d{8}-\d{8}$", val_upper): return val_upper

    logger.warning(f"Unrecognized dynamic date format: {value}")
    return None


# Main Celery Beat Task
@shared_task(name="poll_all_dimse_qr_sources")
def poll_all_dimse_qr_sources() -> Dict[str, Any]:
    """
    Celery Beat task to poll all enabled AND active DIMSE Q/R sources using C-FIND.
    """
    logger.info("Starting DIMSE Q/R polling cycle...")
    processed_sources = 0
    failed_sources = 0
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        all_sources = crud.crud_dimse_qr_source.get_multi(db, limit=settings.DIMSE_QR_POLLER_MAX_SOURCES)

        sources_to_poll = [
            s for s in all_sources if s.is_enabled and s.is_active
        ]
        logger.info(f"Found {len(sources_to_poll)} enabled and active DIMSE Q/R sources to poll.")

        for source_config in sources_to_poll:
            # Check again just before processing
            if not source_config.is_enabled or not source_config.is_active:
                 logger.debug(f"Skipping DIMSE Q/R source '{source_config.name}' (ID: {source_config.id}) as it became disabled/inactive.")
                 continue

            try:
                 # Call the polling function for the source
                 _poll_single_dimse_source(db, source_config)
                 processed_sources += 1
            except Exception as e:
                logger.error(f"Polling failed for DIMSE Q/R source '{source_config.name}' (ID: {source_config.id}): {e}", exc_info=True)
                failed_sources += 1
                try:
                    crud.crud_dimse_qr_source.update_query_status(
                         db=db,
                         source_id=source_config.id,
                         last_error_time=datetime.now(timezone.utc),
                         last_error_message=str(e)[:1024] # Truncate
                    )
                    db.commit() # Commit status update immediately on error
                except Exception as db_err:
                    logger.error(f"Failed to update error status for {source_config.name}: {db_err}", exc_info=True)
                    db.rollback() # Rollback if status update fails

        # No final commit here as successful updates and error updates are committed individually
    except Exception as e:
        logger.error(f"Critical error during DIMSE Q/R polling cycle: {e}", exc_info=True);
        if db: db.rollback()
    finally:
        if db:
            db.close()
            logger.debug("Database session closed.")

    logger.info("DIMSE Q/R polling cycle finished.")
    return {"status": "cycle_complete", "processed": processed_sources, "failed": failed_sources}


# Helper to Poll a Single Source
def _poll_single_dimse_source(db: Session, config: models.DimseQueryRetrieveSource):
    """Polls a single DIMSE Q/R source using C-FIND, handling TLS."""
    log = logger.bind(
        source_name=config.name,
        source_id=config.id,
        remote_ae=config.remote_ae_title,
        remote_host=config.remote_host,
        remote_port=config.remote_port,
        tls_enabled=config.tls_enabled
    )
    log.info("Polling DIMSE Q/R source")

    # --- AE Setup ---
    ae = AE(ae_title=config.local_ae_title)
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind) # Assuming Study Root for polling
    # Add Verification context if needed/desired
    # ae.add_requested_context(Verification)

    # --- Build C-FIND Identifier ---
    find_sop_class = QR_LEVEL_SOP_CLASSES.get(config.query_level.upper());
    if not find_sop_class: raise ValueError(f"Unsupported query level: {config.query_level}")
    identifier = Dataset(); identifier.QueryRetrieveLevel = config.query_level.upper(); filter_keys_used = set()
    if isinstance(config.query_filters, dict):
        for key, value in config.query_filters.items():
            resolved_value = value;
            if key.upper() == 'STUDYDATE': resolved_value = _resolve_dynamic_date_filter(value)
            if resolved_value is None and key.upper() == 'STUDYDATE': log.warning(f"Skipping StudyDate filter: {value}"); continue
            try: tag = Tag(key); keyword = tag.keyword or key
            except Exception: keyword = key
            try: setattr(identifier, keyword, resolved_value); filter_keys_used.add(keyword); log.debug(f" Filter: {keyword}={resolved_value}")
            except Exception as e: log.warning(f"Error setting filter {keyword}={resolved_value}: {e}")
    default_return_keys = [];
    if config.query_level.upper() == "STUDY": default_return_keys = ["PatientID", "PatientName", "StudyInstanceUID", "StudyDate", "StudyTime", "AccessionNumber", "ModalitiesInStudy"]
    for key in default_return_keys:
        if key not in filter_keys_used:
            try: setattr(identifier, key, '')
            except Exception as e: log.warning(f"Could not set return key {key}: {e}")
    log.debug(f"Final C-FIND Identifier:\n{identifier}")

    # --- Association / Query Variables ---
    assoc: Optional[Association] = None
    studies_queued_this_run = 0
    found_study_uids_this_run = set()
    source_id_for_task = config.id
    source_id_str = str(config.id)
    # TLS specific variables
    ssl_context: Optional[ssl.SSLContext] = None
    temp_tls_files: List[str] = []
    ca_cert_file, client_cert_file, client_key_file = None, None, None

    try:
        # --- TLS Setup ---
        if config.tls_enabled:
            log.info("TLS enabled for C-FIND. Preparing SSLContext...")
            if not SECRET_MANAGER_ENABLED: raise DimseQrPollingError("Secret Manager unavailable for TLS.")
            if not config.tls_ca_cert_secret_name: raise DimseQrPollingError("TLS CA cert secret name missing.")

            try:
                # Fetch CA Cert
                log.debug("Fetching CA cert for TLS", secret_name=config.tls_ca_cert_secret_name)
                ca_bytes = fetch_secret_content(config.tls_ca_cert_secret_name)
                tf_ca = tempfile.NamedTemporaryFile(delete=False, suffix="-ca.pem", prefix="qr_poller_scu_")
                tf_ca.write(ca_bytes); tf_ca.close(); ca_cert_file = tf_ca.name; temp_tls_files.append(ca_cert_file)
                log.debug("CA cert written to temporary file", path=ca_cert_file)

                # Fetch Client Cert/Key for mTLS if configured
                if config.tls_client_cert_secret_name and config.tls_client_key_secret_name:
                    log.debug("Fetching client cert/key for mTLS...")
                    cert_bytes = fetch_secret_content(config.tls_client_cert_secret_name)
                    key_bytes = fetch_secret_content(config.tls_client_key_secret_name)
                    tf_cert = tempfile.NamedTemporaryFile(delete=False, suffix="-cert.pem", prefix="qr_poller_scu_")
                    tf_cert.write(cert_bytes); tf_cert.close(); client_cert_file = tf_cert.name; temp_tls_files.append(client_cert_file)
                    tf_key = tempfile.NamedTemporaryFile(delete=False, suffix="-key.pem", prefix="qr_poller_scu_")
                    tf_key.write(key_bytes); tf_key.close(); client_key_file = tf_key.name; temp_tls_files.append(client_key_file)
                    try: os.chmod(client_key_file, 0o600)
                    except OSError as chmod_err: log.warning(f"Could not set permissions on temp key file {client_key_file}: {chmod_err}")
                    log.debug("Client cert/key written to temporary files", cert_path=client_cert_file, key_path=client_key_file)
                elif config.tls_client_cert_secret_name or config.tls_client_key_secret_name:
                     raise DimseQrPollingError("Both client cert/key secrets required for mTLS")

                # Create and Configure ssl.SSLContext
                log.debug("Creating ssl.SSLContext for SCU...")
                # Use create_default_context for better defaults (TLSv1.2+)
                ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_file)
                # Explicitly disable hostname checking if needed (common for DIMSE)
                ssl_context.check_hostname = False
                # CERT_REQUIRED ensures we verify the server using the CA
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                # Optional: Set specific TLS versions or cipher suites if required
                # ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
                # ssl_context.set_ciphers(...)

                if client_cert_file and client_key_file:
                    log.debug("Loading client cert chain into SSLContext for mTLS...")
                    ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
                log.info("SSLContext configured successfully.")

            except (SecretManagerError, SecretNotFoundError, PermissionDeniedError, IOError, OSError, ssl.SSLError) as e:
                 log.error(f"Failed to prepare TLS configuration: {e}", exc_info=True)
                 raise DimseQrPollingError(f"TLS setup failed: {e}") from e
        # --- End TLS Setup ---


        # --- Associate ---
        tls_args = (ssl_context, None) if config.tls_enabled and ssl_context else None
        tls_status_log = f"{'(TLS enabled)' if config.tls_enabled else '(TLS disabled)'}"
        log.info(f"Requesting association {tls_status_log}...")
        assoc = ae.associate( config.remote_host, config.remote_port, ae_title=config.remote_ae_title, tls_args=tls_args )
        # --- End Associate ---

        if assoc.is_established:
            log.info(f"Association established with {config.remote_ae_title} for C-FIND {tls_status_log}.")
            responses: Generator[Tuple[Dataset, Optional[Dataset]], Any, None] = assoc.send_c_find(identifier, find_sop_class)
            response_count = 0; success_or_pending_received = False
            for status, result_identifier in responses:
                if status and hasattr(status, 'Status'):
                     status_int = int(status.Status)
                     if status.Status == 0x0000: # Success
                         success_or_pending_received = True; log.info(f"C-FIND success. {response_count} results."); break
                     elif status.Status in (0xFF00, 0xFF01): # Pending
                         success_or_pending_received = True
                         if result_identifier:
                             response_count += 1; study_uid = result_identifier.get("StudyInstanceUID", None)
                             if study_uid:
                                 found_study_uids_this_run.add(study_uid)
                                 pat_id=result_identifier.get("PatientID", "N/A"); study_date=result_identifier.get("StudyDate", "N/A"); log.info(f" -> Found Study: {study_uid}, PID={pat_id}, Date={study_date} [{response_count}]")
                                 already_processed = crud.crud_processed_study_log.check_exists(
                                     db=db,
                                     source_type=ProcessedStudySourceType.DIMSE_QR,
                                     source_id=source_id_str,
                                     study_instance_uid=study_uid
                                 )
                                 if already_processed: log.debug(f"Study {study_uid} already logged. Skipping.")
                                 elif config.move_destination_ae_title:
                                     try:
                                         log.debug(f"Queueing C-MOVE: {study_uid} -> {config.move_destination_ae_title}")
                                         trigger_dimse_cmove_task.delay(source_id=source_id_for_task, study_instance_uid=study_uid)
                                         log_created = crud.crud_processed_study_log.create_log_entry(
                                             db=db,
                                             source_type=ProcessedStudySourceType.DIMSE_QR,
                                             source_id=source_id_str,
                                             study_instance_uid=study_uid,
                                             commit=False # Commit happens outside loop
                                         )
                                         if log_created:
                                             studies_queued_this_run += 1
                                             log.info(f"Logged {study_uid} for queueing.")
                                         else: log.error(f"Failed log {study_uid} after queueing.")
                                     except Exception as queue_err: log.error(f"Failed queue C-MOVE {study_uid}. Error: {queue_err}", exc_info=True)
                                 else: log.warning(f"Study {study_uid} found but no move destination configured.")
                             else: log.warning(f"Pending Result [{response_count}] missing StudyUID.")
                         else: log.warning(f"Pending status 0x{status_int:04X} no identifier.")
                         continue
                     elif status.Status == 0xFE00: raise DimseQrPollingError(f"C-FIND Cancelled by remote (0xFE00)")
                     else: error_comment = status.get('ErrorComment', 'No comment'); raise DimseQrPollingError(f"C-FIND Failed (0x{status_int:04X} - {error_comment})")
                else: raise DimseQrPollingError("Invalid/Missing Status in C-FIND response")
            if not success_or_pending_received: raise DimseQrPollingError("No valid C-FIND status (Success/Pending) received")

            # Increment Counts AFTER successful loop
            if len(found_study_uids_this_run) > 0:
                 try:
                     if crud.crud_dimse_qr_source.increment_found_study_count(db=db, source_id=source_id_for_task, count=len(found_study_uids_this_run)):
                          log.info(f"Incremented found study count by {len(found_study_uids_this_run)}")
                     else:
                          log.warning("Failed to increment found study count (rows affected 0)")
                 except Exception as inc_err:
                       log.error(f"DB Error incrementing found study count: {inc_err}")

            if studies_queued_this_run > 0:
                 try:
                     if crud.crud_dimse_qr_source.increment_move_queued_count(db=db, source_id=source_id_for_task, count=studies_queued_this_run):
                          log.info(f"Incremented move queued count by {studies_queued_this_run}")
                     else:
                           log.warning("Failed to increment move queued count (rows affected 0)")
                 except Exception as inc_err:
                       log.error(f"DB Error incrementing move queued count: {inc_err}")

            # Update last successful query timestamp
            crud.crud_dimse_qr_source.update_query_status(db=db, source_id=config.id, last_successful_query=datetime.now(timezone.utc))

            # Commit successful increments and timestamp together
            db.commit()
            log.info(f"Committed DB updates for successful poll.")

            assoc.release(); log.info(f"Association released. Found {len(found_study_uids_this_run)} unique studies, queued {studies_queued_this_run} new moves.")
        else:
            # Log specific rejection reason if available from assoc object
            reject_reason = "Unknown"
            if assoc and assoc.is_rejected:
                # Accessing assoc.rejected_contexts requires pynetdicom >= 1.5 (approx)
                # Safer approach is result_source and result_reason if available
                reason_code = getattr(assoc, 'result_reason', 'N/A')
                reason_source = getattr(assoc, 'result_source', 'N/A')
                reject_reason = f"Reason: {reason_source} ({reason_code})"
            raise DimseQrPollingError(f"Association failed with {config.remote_ae_title}. {reject_reason}")

    except ssl.SSLError as e: # Catch SSL errors specifically during association
        log.error(f"TLS Handshake/SSL Error during C-FIND: {e}", exc_info=True)
        if assoc and not assoc.is_released and not assoc.is_aborted: assoc.abort()
        raise DimseQrPollingError(f"TLS Handshake Error: {e}") from e # Re-raise as polling error
    except Exception as e:
        log.error(f"Error during C-FIND poll: {e}", exc_info=True)
        if assoc and assoc.is_established: assoc.abort(); log.warning(f"Aborted association with {config.remote_ae_title} due to error.")
        db.rollback() # Rollback any pending processed log entries from this failed run
        raise e # Re-raise to be caught by main task loop and update error status
    finally:
        # Clean up temporary TLS files
        for file_path in temp_tls_files:
            try:
                if os.path.exists(file_path): os.remove(file_path); log.debug(f"Cleaned temp TLS file: {file_path}")
            except OSError as rm_err: log.warning(f"Failed cleanup {file_path}: {rm_err}")
        # Ensure association is closed if something went wrong after establishment but before release/abort
        if assoc and not assoc.is_released and not assoc.is_aborted:
             log.warning("Association found open in finally block, aborting.")
             try: assoc.abort()
             except Exception: pass
