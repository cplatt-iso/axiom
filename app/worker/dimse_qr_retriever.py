# app/worker/dimse_qr_retriever.py
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
import ssl
import os
import tempfile

# Pynetdicom imports
from pynetdicom import AE, debug_logger, Association
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove
from pydicom.dataset import Dataset

# Application imports
from sqlalchemy.orm import Session
from celery import shared_task, Task
from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models
# --- CORRECTED IMPORT USAGE ---
from app.crud import crud_dimse_qr_source
# --- END CORRECTION ---

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

# Configure logging using structlog if available, otherwise standard logging
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logger.warning("structlog not found, using standard logging.")

# Optional: Enable pynetdicom logging
# debug_logger()

class DimseMoveError(Exception):
    """Custom exception for C-MOVE errors."""
    pass

@shared_task(
    bind=True,
    name="trigger_dimse_cmove_task",
    acks_late=True,
    max_retries=settings.CELERY_TASK_MAX_RETRIES,
    default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
    autoretry_for=(ConnectionRefusedError, TimeoutError, OSError, DimseMoveError, ssl.SSLError),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True
)
def trigger_dimse_cmove_task(self: Task, source_id: int, study_instance_uid: str):
    """
    Initiates a C-MOVE request for a specific study from a configured DIMSE Q/R source.
    Handles TLS connections based on source configuration.
    """
    task_id = self.request.id
    log = logger.bind(task_id=task_id, source_id=source_id, study_uid=study_instance_uid) if hasattr(logger, 'bind') else logger
    log.info("Received request to trigger C-MOVE")

    db: Optional[Session] = None
    config: Optional[models.DimseQueryRetrieveSource] = None
    assoc: Optional[Association] = None
    ssl_context: Optional[ssl.SSLContext] = None
    temp_tls_files: List[str] = []
    ca_cert_file, client_cert_file, client_key_file = None, None, None

    try:
        db = SessionLocal()
        # --- CORRECTED CRUD CALL ---
        config = crud_dimse_qr_source.get(db, id=source_id)
        # --- END CORRECTION ---
        if not config:
            log.error("Cannot initiate C-MOVE. Configuration not found.")
            return {"status": "error", "message": f"Configuration for source ID {source_id} not found."}

        if hasattr(log, 'bind'):
            log = log.bind(source_name=config.name, remote_ae=config.remote_ae_title, tls_enabled=config.tls_enabled)

        if not config.is_enabled:
             log.warning("Source is disabled. Skipping C-MOVE.")
             return {"status": "skipped", "message": "Source is disabled."}

        if not config.move_destination_ae_title:
            log.error("Cannot initiate C-MOVE. 'move_destination_ae_title' is not configured.")
            return {"status": "error", "message": "C-MOVE destination AE Title not configured for this source."}

        log.info(f"Initiating C-MOVE -> {config.move_destination_ae_title}")

        # 1. Initialize AE
        ae = AE(ae_title=config.local_ae_title)
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)

        # --- TLS Setup ---
        if config.tls_enabled:
            log.info("TLS enabled for C-MOVE. Preparing SSLContext...")
            # (TLS Setup logic remains the same as previous version)
            if not SECRET_MANAGER_ENABLED: raise DimseMoveError("Secret Manager unavailable for TLS.")
            if not config.tls_ca_cert_secret_name: raise DimseMoveError("TLS CA cert secret name missing for SCU verification.")
            try:
                # Fetch CA Cert
                log.debug("Fetching CA cert for TLS", secret_name=config.tls_ca_cert_secret_name)
                ca_bytes = fetch_secret_content(config.tls_ca_cert_secret_name)
                tf_ca = tempfile.NamedTemporaryFile(delete=False, suffix="-ca.pem", prefix="cmove_scu_")
                tf_ca.write(ca_bytes); tf_ca.close(); ca_cert_file = tf_ca.name; temp_tls_files.append(ca_cert_file)
                log.debug("CA cert written to temporary file", path=ca_cert_file)
                # Fetch Client Cert/Key for mTLS if configured
                if config.tls_client_cert_secret_name and config.tls_client_key_secret_name:
                    log.debug("Fetching client cert/key for mTLS...")
                    cert_bytes = fetch_secret_content(config.tls_client_cert_secret_name)
                    key_bytes = fetch_secret_content(config.tls_client_key_secret_name)
                    tf_cert = tempfile.NamedTemporaryFile(delete=False, suffix="-cert.pem", prefix="cmove_scu_")
                    tf_cert.write(cert_bytes); tf_cert.close(); client_cert_file = tf_cert.name; temp_tls_files.append(client_cert_file)
                    tf_key = tempfile.NamedTemporaryFile(delete=False, suffix="-key.pem", prefix="cmove_scu_")
                    tf_key.write(key_bytes); tf_key.close(); client_key_file = tf_key.name; temp_tls_files.append(client_key_file)
                    try: os.chmod(client_key_file, 0o600)
                    except OSError as chmod_err: log.warning(f"Could not set permissions on temp key file", key_path=client_key_file, error=str(chmod_err))
                    log.debug("Client cert/key written to temporary files", cert_path=client_cert_file, key_path=client_key_file)
                elif config.tls_client_cert_secret_name or config.tls_client_key_secret_name:
                     raise DimseMoveError("Both client cert/key secrets required for mTLS")
                # Create and Configure ssl.SSLContext
                log.debug("Creating ssl.SSLContext for SCU...")
                ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_file)
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                if client_cert_file and client_key_file:
                    log.debug("Loading client cert chain into SSLContext for mTLS...")
                    ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
                log.info("SSLContext configured successfully.")
            except (SecretManagerError, SecretNotFoundError, PermissionDeniedError, IOError, OSError, ssl.SSLError) as e:
                 log.error(f"Failed to prepare TLS configuration for C-MOVE", error=str(e), exc_info=True)
                 raise DimseMoveError(f"TLS setup failed: {e}") from e
        # --- End TLS Setup ---

        # 2. Associate
        tls_args = (ssl_context, None) if config.tls_enabled and ssl_context else None
        tls_status_log = f"{'(TLS enabled)' if config.tls_enabled else '(TLS disabled)'}"
        log.info(f"Requesting association {tls_status_log}...")
        assoc = ae.associate(
            config.remote_host,
            config.remote_port,
            ae_title=config.remote_ae_title,
            tls_args=tls_args
        )

        if assoc.is_established:
            log.info(f"Association established {tls_status_log}.")

            # 3. Build Identifier
            identifier = Dataset()
            identifier.QueryRetrieveLevel = "STUDY"
            identifier.StudyInstanceUID = study_instance_uid

            # 4. Send C-MOVE Request
            log.debug("Sending C-MOVE request...")
            responses = assoc.send_c_move(
                identifier,
                config.move_destination_ae_title,
                StudyRootQueryRetrieveInformationModelMove
            )

            # 5. Process Responses
            final_move_status = -1
            for status, response_identifier in responses:
                if status and hasattr(status, 'Status'):
                    status_int = int(status.Status)
                    final_move_status = status_int
                    log.debug(f"Received C-MOVE response status 0x{status_int:04X}")
                    remaining = status.get("NumberOfRemainingSuboperations", "N/A")
                    completed = status.get("NumberOfCompletedSuboperations", "N/A")
                    failed = status.get("NumberOfFailedSuboperations", "N/A")
                    warning = status.get("NumberOfWarningSuboperations", "N/A")
                    log.info(f"  -> Status: 0x{status_int:04X}, Rem: {remaining}, Comp: {completed}, Fail: {failed}, Warn: {warning}")

                    if status.Status == 0x0000: # Final Success
                        log.info("C-MOVE completed successfully.")
                        # --- CORRECTED CRUD CALL ---
                        crud_dimse_qr_source.update_move_status(db=db, source_id=config.id, last_successful_move=datetime.now(timezone.utc))
                        # --- END CORRECTION ---
                        db.commit()
                        break
                    elif status.Status in (0xFF00, 0xFF01): continue
                    elif status.Status == 0xFE00:
                         log.warning("C-MOVE cancelled by remote AE.")
                         raise DimseMoveError(f"C-MOVE Cancelled by Remote AE (Status: 0xFE00)")
                    else:
                         error_comment = status.get('ErrorComment', 'No comment')
                         log.error("C-MOVE failed", status=f"0x{status_int:04X}", comment=error_comment)
                         raise DimseMoveError(f"C-MOVE Failed (Status: 0x{status_int:04X} - {error_comment})")
                else:
                     log.error("Received invalid or missing status in C-MOVE response.")
                     raise DimseMoveError("Invalid/Missing Status in C-MOVE response")

            if final_move_status != 0x0000:
                 log.error("C-MOVE finished without final success status", last_status=f"0x{final_move_status:04X}")
                 raise DimseMoveError(f"C-MOVE finished without final success status (Last: 0x{final_move_status:04X})")

            assoc.release()
            log.info("Association released.")
            return {"status": "success", "message": f"C-MOVE successful for Study {study_instance_uid}"}

        else:
            reject_reason = "Unknown"
            if assoc and assoc.is_rejected:
                reason_code = getattr(assoc, 'result_reason', 'N/A')
                reason_source = getattr(assoc, 'result_source', 'N/A')
                reject_reason = f"Reason: {reason_source} ({reason_code})"
            log.error(f"Association rejected/aborted by remote AE for C-MOVE. {reject_reason}")
            raise DimseMoveError(f"Association failed with {config.remote_ae_title}. {reject_reason}")


    except (DimseMoveError, ssl.SSLError) as move_exc:
        log.error(f"Error during C-MOVE operation: {move_exc}", exc_info=True)
        if assoc and assoc.is_established: assoc.abort()
        try:
            # --- CORRECTED CRUD CALL ---
            crud_dimse_qr_source.update_move_status(
                 db=db,
                 source_id=config.id,
                 last_error_time=datetime.now(timezone.utc),
                 last_error_message=f"C-MOVE Error: {str(move_exc)[:500]}"
            )
            # --- END CORRECTION ---
            db.commit()
        except Exception as db_err:
             log.error("Failed to update error status in DB during C-MOVE failure handling", db_error=str(db_err))
             if db: db.rollback()
        raise move_exc

    except (ConnectionRefusedError, TimeoutError, OSError) as net_err:
        log.error(f"Network error during C-MOVE operation: {net_err}", exc_info=True)
        if assoc and assoc.is_established: assoc.abort()
        try:
            # --- CORRECTED CRUD CALL ---
            crud_dimse_qr_source.update_move_status(
                 db=db, source_id=config.id,
                 last_error_time=datetime.now(timezone.utc),
                 last_error_message=f"C-MOVE Network Error: {str(net_err)[:500]}"
            )
            # --- END CORRECTION ---
            db.commit()
        except Exception as db_err:
            log.error("Failed to update error status in DB during network error handling", db_error=str(db_err))
            if db: db.rollback()
        raise net_err

    except Exception as e:
        log.critical(f"CRITICAL - Unhandled exception: {e}", exc_info=True)
        if db and config:
            try:
                # --- CORRECTED CRUD CALL ---
                crud_dimse_qr_source.update_move_status(
                    db=db, source_id=config.id,
                    last_error_time=datetime.now(timezone.utc),
                    last_error_message=f"Unhandled Task Error: {str(e)[:500]}"
                )
                # --- END CORRECTION ---
                db.commit()
            except Exception as db_err:
                 log.error("Failed to update error status in DB during critical failure handling", db_error=str(db_err))
                 if db: db.rollback()
        elif db:
             db.rollback()
        return {"status": "error", "message": f"Critical task error: {e}"}
    finally:
        # Clean up temporary TLS files
        for file_path in temp_tls_files:
            try:
                if os.path.exists(file_path): os.remove(file_path); log.debug(f"Cleaned temp TLS file: {file_path}")
            except OSError as rm_err: log.warning(f"Failed cleanup {file_path}: {rm_err}")
        # Ensure association is closed
        if assoc and not assoc.is_released and not assoc.is_aborted:
             log.warning("Association found open in finally block, aborting.")
             try: assoc.abort()
             except Exception: pass
        # Ensure DB session is closed
        if db:
            db.close()
