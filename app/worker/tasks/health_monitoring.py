# app/worker/tasks/health_monitoring.py
import asyncio
from datetime import datetime, timezone
from typing import Optional

import structlog
from celery import shared_task
from sqlalchemy.orm import Session

from app import crud
from app.db.session import SessionLocal
from app.core.config import settings

# Logger setup
logger = structlog.get_logger(__name__)


@shared_task(bind=True, acks_late=True, reject_on_worker_lost=True)
def health_monitoring_task(self, force_check: bool = False):
    """
    Periodic task to check the health status of all configured scraper sources.

    This task runs periodically (e.g., every 5-15 minutes) to test connections
    to all DICOMWeb, DIMSE Q/R, and Google Healthcare sources and updates
    their health status in the database.

    Args:
        force_check: If True, check all sources regardless of last check time.
                    If False, only check sources that haven't been checked recently.
    """
    log = structlog.get_logger()
    log.info("Starting health monitoring task", force_check=force_check)

    db = None
    try:
        db = SessionLocal()

        # Import the connection test service
        from app.services.connection_test_service import ConnectionTestService
        from app.schemas.enums import HealthStatus
        from app.db import models

        results = {
            "dicomweb_sources": {"checked": 0, "ok": 0, "down": 0, "error": 0},
            "dimse_qr_sources": {"checked": 0, "ok": 0, "down": 0, "error": 0},
            "google_healthcare_sources": {"checked": 0, "ok": 0, "down": 0, "error": 0},
        }

        # Check DICOMWeb sources
        dicomweb_sources = crud.dicomweb_state.get_all(db, limit=1000)
        for source in dicomweb_sources:
            if not force_check and source.last_health_check:
                # Skip if checked within the last 10 minutes
                time_since_check = datetime.now(
                    timezone.utc) - source.last_health_check
                if time_since_check.total_seconds() < 600:  # 10 minutes
                    continue

            try:
                # Use asyncio.run to run the async function
                health_status, error_message = asyncio.run(
                    ConnectionTestService.test_dicomweb_connection(source)
                )

                # Update health status
                asyncio.run(ConnectionTestService.update_source_health_status(
                    db_session=db,
                    source_type="dicomweb",
                    source_id=source.id,
                    health_status=health_status,
                    error_message=error_message
                ))

                results["dicomweb_sources"]["checked"] += 1
                if health_status == HealthStatus.OK:
                    results["dicomweb_sources"]["ok"] += 1
                elif health_status == HealthStatus.DOWN:
                    results["dicomweb_sources"]["down"] += 1
                else:
                    results["dicomweb_sources"]["error"] += 1

                log.info(
                    f"Checked DICOMWeb source {source.id} ({source.source_name}): {health_status.value}")

            except Exception as e:
                log.error(
                    f"Error checking DICOMWeb source {source.id}: {str(e)}")
                results["dicomweb_sources"]["error"] += 1

        # Check DIMSE Q/R sources
        dimse_qr_sources = crud.crud_dimse_qr_source.get_multi(db, limit=1000)
        for source in dimse_qr_sources:
            if not force_check and source.last_health_check:
                time_since_check = datetime.now(
                    timezone.utc) - source.last_health_check
                if time_since_check.total_seconds() < 600:  # 10 minutes
                    continue

            try:
                health_status, error_message = asyncio.run(
                    ConnectionTestService.test_dimse_qr_connection(source)
                )

                asyncio.run(ConnectionTestService.update_source_health_status(
                    db_session=db,
                    source_type="dimse_qr",
                    source_id=source.id,
                    health_status=health_status,
                    error_message=error_message
                ))

                results["dimse_qr_sources"]["checked"] += 1
                if health_status == HealthStatus.OK:
                    results["dimse_qr_sources"]["ok"] += 1
                elif health_status == HealthStatus.DOWN:
                    results["dimse_qr_sources"]["down"] += 1
                else:
                    results["dimse_qr_sources"]["error"] += 1

                log.info(
                    f"Checked DIMSE Q/R source {source.id} ({source.remote_ae_title}@{source.remote_host}): {health_status.value}")

            except Exception as e:
                log.error(
                    f"Error checking DIMSE Q/R source {source.id}: {str(e)}")
                results["dimse_qr_sources"]["error"] += 1

        # Check Google Healthcare sources
        google_healthcare_sources = crud.google_healthcare_source.get_multi(
            db, limit=1000)
        for source in google_healthcare_sources:
            if not force_check and source.last_health_check:
                time_since_check = datetime.now(
                    timezone.utc) - source.last_health_check
                if time_since_check.total_seconds() < 600:  # 10 minutes
                    continue

            try:
                health_status, error_message = asyncio.run(
                    ConnectionTestService.test_google_healthcare_connection(source))

                asyncio.run(ConnectionTestService.update_source_health_status(
                    db_session=db,
                    source_type="google_healthcare",
                    source_id=source.id,
                    health_status=health_status,
                    error_message=error_message
                ))

                results["google_healthcare_sources"]["checked"] += 1
                if health_status == HealthStatus.OK:
                    results["google_healthcare_sources"]["ok"] += 1
                elif health_status == HealthStatus.DOWN:
                    results["google_healthcare_sources"]["down"] += 1
                else:
                    results["google_healthcare_sources"]["error"] += 1

                log.info(
                    f"Checked Google Healthcare source {source.id} (project: {source.gcp_project_id}): {health_status.value}")

            except Exception as e:
                log.error(
                    f"Error checking Google Healthcare source {source.id}: {str(e)}")
                results["google_healthcare_sources"]["error"] += 1

        total_checked = sum(source_type["checked"]
                            for source_type in results.values())
        total_ok = sum(source_type["ok"] for source_type in results.values())
        total_down = sum(source_type["down"]
                         for source_type in results.values())
        total_error = sum(source_type["error"]
                          for source_type in results.values())

        log.info("Health monitoring task completed",
                 total_checked=total_checked,
                 total_ok=total_ok,
                 total_down=total_down,
                 total_error=total_error,
                 results=results)

        return {
            "status": "success",
            "total_checked": total_checked,
            "total_ok": total_ok,
            "total_down": total_down,
            "total_error": total_error,
            "details": results
        }

    except Exception as e:
        log.error(
            "Error during health monitoring task",
            error=str(e),
            exc_info=True)
        if db and db.is_active:
            db.rollback()
        raise
    finally:
        if db and db.is_active:
            db.close()
