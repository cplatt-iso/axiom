# app/api/api_v1/endpoints/dashboard.py

import logging
import time
import socket
import redis # Ensure redis is imported
from typing import Dict, Any # Import Dict and Any for type hints
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.api import deps
from app.core.config import settings
# Import Celery class itself
from celery import Celery
# Import the response schema
from app.schemas import HealthCheckResponse, ComponentStatus # Assuming these are defined in schemas

# Define constants for Redis heartbeat check
LISTENER_HEARTBEAT_KEY = "axiom_flow:listener:heartbeat"
HEARTBEAT_TIMEOUT_SECONDS = 60 # Consider a key stale after N seconds

# Configure Logger
logger = logging.getLogger(__name__)

# --- Redis Dependency ---
def get_redis_client():
    """FastAPI Dependency to get a Redis client connection."""
    client = None
    try:
        logger.debug(f"Connecting to Redis for dashboard status: {settings.REDIS_URL}")
        # Added socket_timeout for operations like GET
        client = redis.Redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=2, # Timeout for establishing connection
            socket_timeout=2 # Timeout for individual commands (like PING, GET)
        )
        client.ping() # Verify connection
        logger.debug("Dashboard: Redis connection ping successful.")
        yield client # Provide the client to the endpoint function
    except redis.exceptions.TimeoutError:
        logger.error("Dashboard: Redis command timed out.")
        yield None
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Dashboard: Redis connection failed: {e}")
        yield None # Indicate failure to connect
    except Exception as e:
        logger.error(f"Dashboard: Error creating Redis client: {e}", exc_info=settings.DEBUG)
        yield None
    finally:
        # Close connection after request finishes
        if client:
            try:
                client.close()
                logger.debug("Dashboard: Redis connection closed.")
            except Exception as e:
                 logger.error(f"Dashboard: Error closing Redis connection: {e}", exc_info=settings.DEBUG)


# Router instance
router = APIRouter()

@router.get(
    "/status",
    summary="Get Detailed System Status for Dashboard",
    tags=["Dashboard"],
    # Use the specific response model from schemas
    response_model=HealthCheckResponse # Assuming HealthCheckResponse has {status: str, components: Dict[str, ComponentStatus]}
)
async def get_detailed_system_status( # Renamed function for clarity
    db: Session = Depends(deps.get_db),
    redis_client: redis.Redis | None = Depends(get_redis_client)
):
    """
    Provides a detailed status overview of key system components for the dashboard.
    """
    # Initialize dictionary to hold individual component statuses
    component_statuses: Dict[str, Dict[str, Any]] = {
        "database": {"status": "unknown", "details": None},
        "message_broker": {"status": "unknown", "details": None},
        "api_service": {"status": "ok", "details": "Responding"}, # API is working
        "dicom_listener": {"status": "unknown", "details": None},
        "celery_workers": {"status": "unknown", "details": None},
    }

    # 1. Database Check
    try:
        db.execute(text("SELECT 1"))
        component_statuses["database"]["status"] = "ok"
        component_statuses["database"]["details"] = "Connected"
        logger.debug("Dashboard: Database check successful.")
    except Exception as e:
        component_statuses["database"]["status"] = "error"
        component_statuses["database"]["details"] = "Connection failed"
        logger.error(f"Dashboard: Database check failed: {e}", exc_info=settings.DEBUG)

    # 2. Message Broker Check (Port Reachability)
    broker_reachable = False
    try:
        with socket.create_connection((settings.RABBITMQ_HOST, settings.RABBITMQ_PORT), timeout=1):
             component_statuses["message_broker"]["status"] = "ok"
             component_statuses["message_broker"]["details"] = "Broker port reachable"
             broker_reachable = True
             logger.debug("Dashboard: Message broker port check successful.")
    except ConnectionRefusedError:
         component_statuses["message_broker"]["status"] = "error"
         component_statuses["message_broker"]["details"] = "Broker port connection refused"
         logger.warning("Dashboard: Message broker port connection refused.")
    except socket.timeout:
         component_statuses["message_broker"]["status"] = "error"
         component_statuses["message_broker"]["details"] = "Broker port check timed out"
         logger.warning("Dashboard: Message broker port check timed out.")
    except Exception as e:
         component_statuses["message_broker"]["status"] = "error"
         component_statuses["message_broker"]["details"] = "Broker port check failed"
         logger.error(f"Dashboard: Message broker port check failed: {e}", exc_info=settings.DEBUG)

    # 3. DICOM Listener Check (Redis Heartbeat)
    if redis_client:
        try:
            last_heartbeat_str = redis_client.get(LISTENER_HEARTBEAT_KEY)
            if last_heartbeat_str:
                try:
                    last_heartbeat_ts = int(last_heartbeat_str)
                    current_time = time.time()
                    time_diff = current_time - last_heartbeat_ts
                    logger.debug(f"Dashboard: Listener heartbeat found. Timestamp={last_heartbeat_ts}, Diff={time_diff:.2f}s")

                    if 0 <= time_diff <= HEARTBEAT_TIMEOUT_SECONDS:
                        component_statuses["dicom_listener"]["status"] = "ok"
                        component_statuses["dicom_listener"]["details"] = f"Last heartbeat {int(time_diff)}s ago."
                    else:
                        component_statuses["dicom_listener"]["status"] = "degraded"
                        component_statuses["dicom_listener"]["details"] = f"Heartbeat stale ({int(time_diff)}s > {HEARTBEAT_TIMEOUT_SECONDS}s timeout)."
                        logger.warning(f"Dashboard: Listener heartbeat is stale (age: {time_diff:.2f}s).")
                except (ValueError, TypeError):
                     component_statuses["dicom_listener"]["status"] = "error"
                     component_statuses["dicom_listener"]["details"] = f"Invalid heartbeat timestamp format in Redis ('{last_heartbeat_str}')."
                     logger.error(f"Dashboard: Invalid listener heartbeat timestamp found in Redis: {last_heartbeat_str}")
            else:
                component_statuses["dicom_listener"]["status"] = "down" # Changed from 'down' to 'error' or 'unknown'? Use 'unknown' maybe.
                component_statuses["dicom_listener"]["status"] = "unknown"
                component_statuses["dicom_listener"]["details"] = "No heartbeat key found in Redis. Listener may be down or not reporting."
                logger.warning("Dashboard: Listener heartbeat key not found in Redis.")
        except redis.exceptions.TimeoutError:
            component_statuses["dicom_listener"]["status"] = "error"
            component_statuses["dicom_listener"]["details"] = "Redis command timed out during heartbeat check."
            logger.error("Dashboard: Redis command timed out checking heartbeat.")
        except redis.exceptions.ConnectionError as redis_err:
             component_statuses["dicom_listener"]["status"] = "error"
             component_statuses["dicom_listener"]["details"] = "Redis connection error during heartbeat check."
             logger.error(f"Dashboard: Redis connection error checking heartbeat: {redis_err}", exc_info=settings.DEBUG)
        except Exception as e:
            component_statuses["dicom_listener"]["status"] = "error"
            component_statuses["dicom_listener"]["details"] = "Listener heartbeat check failed."
            logger.error(f"Dashboard: Error checking listener heartbeat: {e}", exc_info=settings.DEBUG)
    else:
        component_statuses["dicom_listener"]["status"] = "unknown"
        component_statuses["dicom_listener"]["details"] = "Redis client unavailable for heartbeat check."
        logger.warning("Dashboard: Redis client unavailable, cannot check listener heartbeat.")

    # 4. Celery Worker Check
    if broker_reachable:
        celery_inspect_timeout = 1.0 # Increased timeout slightly
        try:
            temp_celery_app = Celery(broker=settings.CELERY_BROKER_URL, backend='rpc://') # Added backend for inspect
            inspector = temp_celery_app.control.inspect(timeout=celery_inspect_timeout)
            active_workers = inspector.ping()

            if active_workers and len(active_workers) > 0:
                worker_count = len(active_workers)
                component_statuses["celery_workers"]["status"] = "ok"
                component_statuses["celery_workers"]["details"] = f"{worker_count} worker(s) responded."
                logger.debug(f"Dashboard: Celery inspect ping successful ({worker_count} workers).")
            else:
                # Ping returning None or empty dict can mean no workers or timeout
                component_statuses["celery_workers"]["status"] = "error" # Consider 'degraded' or 'unknown'? Error seems appropriate if none respond.
                component_statuses["celery_workers"]["details"] = f"No workers responded to ping within {celery_inspect_timeout}s."
                logger.warning(f"Dashboard: No Celery workers responded to ping (timeout: {celery_inspect_timeout}s).")

        except Exception as e:
            component_statuses["celery_workers"]["status"] = "error"
            component_statuses["celery_workers"]["details"] = "Worker inspection failed."
            logger.error(f"Dashboard: Celery worker inspection failed: {e}", exc_info=settings.DEBUG)
    else: # Broker unreachable
        component_statuses["celery_workers"]["status"] = "unknown"
        component_statuses["celery_workers"]["details"] = "Broker unreachable, cannot check workers."
        logger.warning("Dashboard: Broker unreachable, skipping worker check.")

    # --- Determine overall status based on component statuses ---
    overall_status = "ok"
    # Define status hierarchy: error > degraded > unknown > ok
    if any(status_info["status"] == "error" for status_info in component_statuses.values()):
        overall_status = "error"
    elif any(status_info["status"] == "degraded" for status_info in component_statuses.values()):
        overall_status = "degraded"
    elif any(status_info["status"] == "unknown" for status_info in component_statuses.values()):
        overall_status = "unknown" # Or maybe treat unknown as degraded?

    # --- Return the structured response matching HealthCheckResponse schema ---
    logger.debug(f"Dashboard status compiled: overall={overall_status}, components={component_statuses}")
    return {
        "status": overall_status,
        "components": component_statuses # Nest the components under the "components" key
    }
