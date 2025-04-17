# app/api/api_v1/endpoints/dashboard.py

import logging
import time
import socket
import redis # Ensure redis is imported
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.api import deps
from app.core.config import settings
# --- Import Celery class itself ---
from celery import Celery

# Define constants for Redis heartbeat check
LISTENER_HEARTBEAT_KEY = "axiom_flow:listener:heartbeat"
HEARTBEAT_TIMEOUT_SECONDS = 60 # Consider a key stale after N seconds (e.g., 4x interval)

# Configure Logger
logger = logging.getLogger(__name__)

# --- Redis Dependency ---
def get_redis_client():
    """FastAPI Dependency to get a Redis client connection."""
    client = None
    try:
        logger.debug(f"Connecting to Redis for dashboard status: {settings.REDIS_URL}")
        client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True, socket_connect_timeout=2)
        client.ping() # Verify connection
        yield client # Provide the client to the endpoint function
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


# This needs to be *outside* the function definition above
router = APIRouter()

@router.get("/status", summary="Get Overall System Status", tags=["Dashboard"])
async def get_system_status(
    db: Session = Depends(deps.get_db),
    redis_client: redis.Redis | None = Depends(get_redis_client) # Inject Redis client via dependency
    # current_user: models.User = Depends(deps.get_current_active_user), # Optional: Secure endpoint
):
    """
    Provides a status overview of key system components.
    """
    # Initialize status report
    status_report = {
        "database": {"status": "unknown", "details": None},
        "message_broker": {"status": "unknown", "details": None},
        "api_service": {"status": "ok", "details": "Responding"}, # API is working if this endpoint is reached
        "dicom_listener": {"status": "unknown", "details": None},
        "celery_workers": {"status": "unknown", "details": None},
    }

    # 1. Database Check
    try:
        db.execute(text("SELECT 1"))
        status_report["database"]["status"] = "ok"
        status_report["database"]["details"] = "Connected"
        logger.debug("Dashboard: Database check successful.")
    except Exception as e:
        status_report["database"]["status"] = "error"
        status_report["database"]["details"] = f"Connection failed" # Avoid exposing detailed DB errors
        logger.error(f"Dashboard: Database check failed: {e}", exc_info=settings.DEBUG)

    # 2. Message Broker Check (Port Reachability)
    broker_reachable = False
    try:
        # Use settings for host and port
        with socket.create_connection((settings.RABBITMQ_HOST, settings.RABBITMQ_PORT), timeout=1):
             status_report["message_broker"]["status"] = "ok"
             status_report["message_broker"]["details"] = "Broker port reachable"
             broker_reachable = True
             logger.debug("Dashboard: Message broker port check successful.")
    except ConnectionRefusedError:
         status_report["message_broker"]["status"] = "error"
         status_report["message_broker"]["details"] = "Broker port connection refused"
         logger.warning("Dashboard: Message broker port connection refused.")
    except Exception as e:
         status_report["message_broker"]["status"] = "error"
         status_report["message_broker"]["details"] = "Broker port check failed"
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

                    if time_diff >= 0 and time_diff <= HEARTBEAT_TIMEOUT_SECONDS: # Check age is reasonable
                        status_report["dicom_listener"]["status"] = "ok"
                        status_report["dicom_listener"]["details"] = f"Last heartbeat {int(time_diff)}s ago."
                    else:
                        status_report["dicom_listener"]["status"] = "degraded"
                        status_report["dicom_listener"]["details"] = f"Heartbeat stale ({int(time_diff)}s > {HEARTBEAT_TIMEOUT_SECONDS}s timeout)."
                        logger.warning(f"Dashboard: Listener heartbeat is stale (age: {time_diff:.2f}s).")
                except (ValueError, TypeError):
                     status_report["dicom_listener"]["status"] = "error"
                     status_report["dicom_listener"]["details"] = f"Invalid heartbeat timestamp format in Redis ('{last_heartbeat_str}')."
                     logger.error(f"Dashboard: Invalid listener heartbeat timestamp found in Redis: {last_heartbeat_str}")
            else:
                status_report["dicom_listener"]["status"] = "down"
                status_report["dicom_listener"]["details"] = "No heartbeat key found in Redis. Listener might be down or Redis key expired."
                logger.warning("Dashboard: Listener heartbeat key not found in Redis.")
        except redis.exceptions.ConnectionError as redis_err:
             status_report["dicom_listener"]["status"] = "error"
             status_report["dicom_listener"]["details"] = f"Redis connection error during heartbeat check."
             logger.error(f"Dashboard: Redis connection error checking heartbeat: {redis_err}", exc_info=settings.DEBUG)
        except Exception as e:
            status_report["dicom_listener"]["status"] = "error"
            status_report["dicom_listener"]["details"] = f"Listener heartbeat check failed."
            logger.error(f"Dashboard: Error checking listener heartbeat: {e}", exc_info=settings.DEBUG)
    else:
        status_report["dicom_listener"]["status"] = "unknown"
        status_report["dicom_listener"]["details"] = "Redis client unavailable for heartbeat check."
        logger.warning("Dashboard: Redis client unavailable, cannot check listener heartbeat.")

    # 4. Celery Worker Check (using temporary inspect app)
    if broker_reachable:
        try:
            # Create a temporary Celery instance configured ONLY for inspection
            temp_celery_app = Celery(broker=settings.CELERY_BROKER_URL)

            # Use inspect with a timeout
            inspector = temp_celery_app.control.inspect(timeout=0.8)
            active_workers = inspector.ping() # Ping returns a dict like {'celery@worker_name': {'ok': 'pong'}}

            if active_workers and len(active_workers) > 0:
                worker_count = len(active_workers)
                status_report["celery_workers"]["status"] = "ok"
                status_report["celery_workers"]["details"] = f"{worker_count} worker(s) responded to ping."
                logger.debug(f"Dashboard: Celery inspect ping successful ({worker_count} workers).")
            else:
                status_report["celery_workers"]["status"] = "error"
                status_report["celery_workers"]["details"] = "No workers responded to ping within timeout."
                logger.warning("Dashboard: No Celery workers responded to ping.")

        except Exception as e:
            status_report["celery_workers"]["status"] = "error"
            status_report["celery_workers"]["details"] = f"Worker inspection failed."
            logger.error(f"Dashboard: Celery worker inspection failed: {e}", exc_info=settings.DEBUG)
    else: # Broker unreachable
        status_report["celery_workers"]["status"] = "unknown"
        status_report["celery_workers"]["details"] = "Broker unreachable, cannot check workers."
        logger.warning("Dashboard: Broker unreachable, skipping worker check.")
    # --- End Update Celery Worker Check ---

    return status_report
