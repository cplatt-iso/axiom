# app/api/api_v1/endpoints/dashboard.py

import logging
import structlog
import time
import socket
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List # Import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from celery import Celery # Import Celery class for inspection

# Application imports
from app.api import deps
from app.core.config import settings
from app.schemas import HealthCheckResponse, ComponentStatus # Response schema
from app.db import models # Import models for type hints
from app.crud import crud_dimse_listener_state # Import CRUD for listener state

# Configure Logger
logger = structlog.get_logger(__name__)

# Constants
LISTENER_HEARTBEAT_TIMEOUT_SECONDS = 90 # Consider listener stale after 90 seconds (3 heartbeats)

# Router instance
router = APIRouter()

@router.get(
    "/status",
    summary="Get Detailed System Status for Dashboard",
    tags=["Dashboard"],
    response_model=HealthCheckResponse
)
async def get_detailed_system_status(
    db: Session = Depends(deps.get_db) # Keep DB dependency
    # Removed Redis dependency as it's no longer used here
):
    """
    Provides a detailed status overview of key system components for the dashboard.
    Checks Database, Message Broker, Celery Workers, and DICOM Listener status from DB.
    """
    component_statuses: Dict[str, ComponentStatus] = {
        "database": ComponentStatus(status="unknown", details=None),
        "message_broker": ComponentStatus(status="unknown", details=None),
        "api_service": ComponentStatus(status="ok", details="Responding"), # API is working if this runs
        "dicom_listener": ComponentStatus(status="unknown", details=None),
        "celery_workers": ComponentStatus(status="unknown", details=None),
    }

    # 1. Database Check
    try:
        # Execute a simple query to check DB connectivity
        db.execute(text("SELECT 1"))
        component_statuses["database"] = ComponentStatus(status="ok", details="Connected")
        logger.debug("Dashboard: Database check successful.")
    except Exception as e:
        component_statuses["database"] = ComponentStatus(status="error", details="Connection failed")
        logger.error(f"Dashboard: Database check failed: {e}", exc_info=settings.DEBUG)

    # 2. Message Broker Check (Simple port check)
    broker_reachable = False
    try:
        # Attempt to open a socket connection to the broker host/port
        with socket.create_connection((settings.RABBITMQ_HOST, settings.RABBITMQ_PORT), timeout=1):
             component_statuses["message_broker"] = ComponentStatus(status="ok", details="Broker port reachable")
             broker_reachable = True
             logger.debug("Dashboard: Message broker port check successful.")
    except ConnectionRefusedError:
         component_statuses["message_broker"] = ComponentStatus(status="error", details="Broker port connection refused")
         logger.warning("Dashboard: Message broker port connection refused.")
    except socket.timeout:
         component_statuses["message_broker"] = ComponentStatus(status="error", details="Broker port check timed out")
         logger.warning("Dashboard: Message broker port check timed out.")
    except Exception as e:
         component_statuses["message_broker"] = ComponentStatus(status="error", details="Broker port check failed")
         logger.error(f"Dashboard: Message broker port check failed: {e}", exc_info=settings.DEBUG)

    # 3. DICOM Listener Check (Database Status)
    try:
        # Fetch listener states from the database
        listener_states: List[models.DimseListenerState] = crud_dimse_listener_state.get_all_listener_states(db)

        if not listener_states:
            component_statuses["dicom_listener"] = ComponentStatus(status="unknown", details="No listener status found in database.")
            logger.warning("Dashboard: No DICOM listener status records found in DB.")
        else:
            # Check the status of the found listeners
            now_utc = datetime.now(timezone.utc)
            all_ok = True
            stale_listeners = []
            error_listeners = []

            for state in listener_states:
                is_stale = (now_utc - state.last_heartbeat) > timedelta(seconds=LISTENER_HEARTBEAT_TIMEOUT_SECONDS)
                if state.status != 'running':
                    all_ok = False
                    error_listeners.append(f"{state.listener_id} (status: {state.status})")
                elif is_stale:
                    all_ok = False
                    stale_listeners.append(f"{state.listener_id} (last beat: {state.last_heartbeat.isoformat()})")

            if all_ok:
                component_statuses["dicom_listener"] = ComponentStatus(status="ok", details=f"{len(listener_states)} listener(s) running and reporting.")
            elif error_listeners:
                component_statuses["dicom_listener"] = ComponentStatus(status="error", details=f"Error state reported for: {', '.join(error_listeners)}")
            elif stale_listeners:
                component_statuses["dicom_listener"] = ComponentStatus(status="degraded", details=f"Stale heartbeat for: {', '.join(stale_listeners)}")
            else:
                 # This case shouldn't happen if logic is correct, but handle defensively
                 component_statuses["dicom_listener"] = ComponentStatus(status="unknown", details="Listener status check inconclusive.")

        logger.debug(f"Dashboard: DICOM Listener DB check completed. Status: {component_statuses['dicom_listener'].status}")

    except Exception as e:
        component_statuses["dicom_listener"] = ComponentStatus(status="error", details="Failed to query listener status from database.")
        logger.error(f"Dashboard: Error checking listener status from DB: {e}", exc_info=settings.DEBUG)


    # 4. Celery Worker Check
    if broker_reachable:
        celery_inspect_timeout = 1.5 # Timeout for celery inspect command
        try:
            # Create a temporary Celery app instance to inspect the broker
            temp_celery_app = Celery(broker=settings.CELERY_BROKER_URL, backend='rpc://') # Use RPC backend for inspect results
            inspector = temp_celery_app.control.inspect(timeout=celery_inspect_timeout)
            # Ping active workers
            active_workers = inspector.ping() # Returns dict {worker_name: {'ok': 'pong'}} or None

            if active_workers: # Check if the dictionary is not None and not empty
                worker_count = len(active_workers)
                component_statuses["celery_workers"] = ComponentStatus(status="ok", details=f"{worker_count} worker(s) responded.")
                logger.debug(f"Dashboard: Celery inspect ping successful ({worker_count} workers).")
            else:
                # Ping returning None or empty dict means no workers responded within timeout
                component_statuses["celery_workers"] = ComponentStatus(status="error", details=f"No workers responded to ping within {celery_inspect_timeout}s.")
                logger.warning(f"Dashboard: No Celery workers responded to ping (timeout: {celery_inspect_timeout}s).")

        except Exception as e:
            component_statuses["celery_workers"] = ComponentStatus(status="error", details="Worker inspection failed.")
            logger.error(f"Dashboard: Celery worker inspection failed: {e}", exc_info=settings.DEBUG)
    else:
        # If broker isn't reachable, workers cannot be checked
        component_statuses["celery_workers"] = ComponentStatus(status="unknown", details="Broker unreachable, cannot check workers.")
        logger.warning("Dashboard: Broker unreachable, skipping worker check.")

    # Determine overall system status based on components
    overall_status = "ok"
    # Ordered check: error > degraded > unknown > ok
    if any(comp.status == "error" for comp in component_statuses.values()):
        overall_status = "error"
    elif any(comp.status == "degraded" for comp in component_statuses.values()):
        overall_status = "degraded"
    elif any(comp.status == "unknown" for comp in component_statuses.values()):
        overall_status = "unknown"

    # Return the structured response
    logger.debug(f"Dashboard status compiled: overall={overall_status}")
    return HealthCheckResponse(
        status=overall_status,
        components=component_statuses
    )
