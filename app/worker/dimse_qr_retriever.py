# app/worker/dimse_qr_retriever.py
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

# Pynetdicom imports
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove # type: ignore[attr-defined]

# Application imports
from sqlalchemy.orm import Session
from celery import shared_task, Task
from app.core.config import settings
from app.db.session import SessionLocal
from app.db import models
# --- CORRECT IMPORT ---
from app.crud import crud_dimse_qr_source
# --- END CORRECT IMPORT ---
# Import the SCU service
from app.services.network.dimse.scu_service import (
    move_study,
    DimseScuError,
    AssociationError,
    DimseCommandError,
    TlsConfigError
)


# Configure logging
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = structlog.get_logger(__name__)
    logger.warning("structlog not found, using standard logging.")


# Celery Task for C-MOVE Initiation (Refactored)
@shared_task(
    bind=True,
    name="trigger_dimse_cmove_task",
    acks_late=True,
    max_retries=settings.CELERY_TASK_MAX_RETRIES,
    default_retry_delay=settings.CELERY_TASK_RETRY_DELAY,
    # Retry on specific SCU service errors + network errors
    autoretry_for=(
        ConnectionRefusedError, TimeoutError, OSError, # Network issues
        AssociationError, DimseCommandError, TlsConfigError # Service errors (includes SSLError via AssociationError)
    ),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True
)
def trigger_dimse_cmove_task(self: Task, source_id: int, study_instance_uid: str):
    """
    Initiates a C-MOVE request via scu_service.move_study.
    """
    task_id = self.request.id
    log = (logger.bind) # type: ignore[attr-defined])(task_id=task_id, source_id=source_id, study_uid=study_instance_uid) if hasattr(logger, 'bind') else logger
    log.info("Received request to trigger C-MOVE")

    db: Optional[Session] = None
    config: Optional[models.DimseQueryRetrieveSource] = None

    try:
        db = SessionLocal()
        # --- CORRECTED CALL ---
        config = crud_dimse_qr_source.get(db, id=source_id)
        # --- END CORRECTED CALL ---
        if not config:
            log.error("Cannot initiate C-MOVE. Configuration not found.")
            return {"status": "error", "message": f"Configuration for source ID {source_id} not found."}

        if hasattr(log, 'bind'):
            log = (log.bind) # type: ignore[attr-defined])(source_name=config.name, remote_ae=config.remote_ae_title, tls_enabled=config.tls_enabled)

        if not config.is_enabled:
             log.warning("Source is disabled. Skipping C-MOVE.")
             return {"status": "skipped", "message": "Source is disabled."}

        if not config.move_destination_ae_title:
            log.error("Cannot initiate C-MOVE. 'move_destination_ae_title' is not configured.")
            return {"status": "error", "message": "C-MOVE destination AE Title not configured for this source."}

        log.info(f"Initiating C-MOVE via scu_service -> {config.move_destination_ae_title}")

        scu_config_dict = {
            "remote_host": config.remote_host,
            "remote_port": config.remote_port,
            "remote_ae_title": config.remote_ae_title,
            "local_ae_title": config.local_ae_title,
            "tls_enabled": config.tls_enabled,
            "tls_ca_cert_secret_name": config.tls_ca_cert_secret_name,
            "tls_client_cert_secret_name": config.tls_client_cert_secret_name,
            "tls_client_key_secret_name": config.tls_client_key_secret_name,
        }

        move_result = move_study(
            config=scu_config_dict,
            study_instance_uid=study_instance_uid,
            move_destination_ae=config.move_destination_ae_title,
            move_sop_class=StudyRootQueryRetrieveInformationModelMove
        )

        log.info("C-MOVE command successful via service", result=move_result) # type: ignore[call-arg]

        try:
            # --- CORRECTED CALL ---
            crud_dimse_qr_source.update_move_status(
                db=db,
                source_id=config.id,
                last_successful_move=datetime.now(timezone.utc)
            )
            # --- END CORRECTED CALL ---
            db.commit()
            log.info("Updated last_successful_move timestamp.")
        except Exception as db_err:
            log.error("Failed to update last_successful_move timestamp", db_error=str(db_err)) # type: ignore[call-arg]
            db.rollback()

        return {"status": "success", "message": f"C-MOVE successful for Study {study_instance_uid}", "details": move_result}

    except (TlsConfigError, AssociationError, DimseCommandError, ConnectionRefusedError, TimeoutError, OSError) as move_exc:
        log.error(f"Error during C-MOVE operation, will retry if possible: {move_exc}", exc_info=True)
        try:
            if db: # ADDED check for db
                # --- CORRECTED CALL ---
                crud_dimse_qr_source.update_move_status(
                     db=db, # type: ignore[arg-type] # db is now confirmed not None
                     source_id=config.id if config else source_id,
                     last_error_time=datetime.now(timezone.utc),
                     last_error_message=f"C-MOVE Error: {str(move_exc)[:500]}"
                )
                # --- END CORRECTED CALL ---
                db.commit()
        except Exception as db_err:
             log.error("Failed to update error status in DB during C-MOVE failure handling", db_error=str(db_err)) # type: ignore[call-arg]
             if db: db.rollback()
        raise move_exc

    except Exception as e:
        log.critical(f"CRITICAL - Unhandled exception: {e}", exc_info=True)
        if db and config:
            try:
                # --- CORRECTED CALL ---
                crud_dimse_qr_source.update_move_status(
                    db=db, source_id=config.id,
                    last_error_time=datetime.now(timezone.utc),
                    last_error_message=f"Unhandled Task Error: {str(e)[:500]}"
                )
                # --- END CORRECTED CALL ---
                db.commit()
            except Exception as db_err:
                 log.error("Failed to update error status in DB during critical failure handling", db_error=str(db_err)) # type: ignore[call-arg]
                 if db: db.rollback()
        elif db: # If config is None but db exists
             db.rollback()
        return {"status": "error", "message": f"Critical task error: {e}"}
    finally:
        if db:
            db.close()
