# app/main.py
import logging
import sys
from typing import Optional # Import Optional for type hinting
import asyncio

import structlog # type: ignore # <-- ADDED: Import structlog
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import aio_pika
from aio_pika.abc import AbstractRobustConnection

# Core Application Imports
from app.core.config import settings # Needs LOG_LEVEL setting
from app.core.logging_config import configure_json_logging
from app.db.session import get_db, engine, SessionLocal, try_connect_db # Import necessary DB components
from app.api import deps # For dependency injection, e.g., getting current user
from app import schemas # Import Pydantic schemas
from app.api.api_v1.api import api_router # Import the main V1 router
from app.db.base import Base # Import Base for table creation
from app.events import rabbitmq_consumer, ORDERS_EXCHANGE_NAME

# --- Global Variables ---
rabbitmq_connection: Optional[AbstractRobustConnection] = None
sse_consumer_task: Optional[asyncio.Task] = None

# --- Configure logging ---
configure_json_logging("api")
logger = structlog.get_logger(__name__)


# --- Database Table Creation Function ---
def create_tables():
    """Ensures all tables defined in models are created in the DB if they don't exist."""
    # --- Import ALL Base-inherited models BEFORE create_all ---
    from app.db import models # noqa F401

    logger.info("Attempting to create database tables (if they don't exist)...") # structlog logger now
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables check/creation complete.")
    except Exception as e:
        logger.error("Error during database table creation", error=str(e), exc_info=True)
        # raise e


# --- Seed Default Roles ---
def seed_default_roles(db: Session):
    """Checks for default roles (Admin, User) and creates them if missing."""
    from app.db import models # Ensure models are available

    logger.info("Checking/Seeding default roles...") # structlog logger now
    default_roles_to_seed = {
        "Admin": "Full administrative privileges",
        "User": "Standard user privileges",
    }
    roles_created_count = 0
    try:
        existing_roles = {role.name for role in db.query(models.Role.name).all()}
        logger.debug("Existing roles found", roles=existing_roles)

        for role_name, role_desc in default_roles_to_seed.items():
            if role_name not in existing_roles:
                logger.info("Creating default role", role_name=role_name)
                role = models.Role(name=role_name, description=role_desc)
                db.add(role)
                roles_created_count += 1

        if roles_created_count > 0:
            db.commit()
            logger.info("Committed new default roles.", count=roles_created_count)
        else:
            logger.info("All default roles already exist.")

    except Exception as e:
        logger.error("Error during role seeding", error=str(e), exc_info=True)
        db.rollback()


# --- Initialize FastAPI App ---
run_environment = getattr(settings, 'ENVIRONMENT', 'development').lower()
root_path_setting = f"/{settings.PROJECT_NAME.lower().replace(' ', '_')}" if run_environment != "development" else ""
app_version = getattr(settings, 'PROJECT_VERSION', '0.1.0')

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    docs_url=f"{settings.API_V1_STR}/docs",
    redoc_url=f"{settings.API_V1_STR}/redoc",
    version=app_version,
    description="Axiom Flow: DICOM Tag Morphing and Routing System",
    debug=settings.DEBUG,
    # root_path=root_path_setting
)


# --- Middleware ---
# Add correlation ID middleware for request tracing
from app.core.middleware.correlation import CorrelationMiddleware
app.add_middleware(CorrelationMiddleware)

if settings.BACKEND_CORS_ORIGINS:
    origins = settings.BACKEND_CORS_ORIGINS
    if origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[str(origin) for origin in origins],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        logger.info("CORS enabled", origins=[str(o) for o in origins])
    else:
         logger.warning("BACKEND_CORS_ORIGINS specified but resulted in empty list. CORS not configured.")
else:
     logger.warning("CORS is not configured (BACKEND_CORS_ORIGINS not set).")


# --- API Routers ---
@app.get("/", tags=["Root"], summary="Root Endpoint")
async def read_root():
    """ Root endpoint providing basic application information. """
    logger.debug("Root endpoint requested") # Example debug log
    return {
        "message": f"Welcome to {settings.PROJECT_NAME}",
        "docs_url": app.docs_url,
        "redoc_url": app.redoc_url,
        "api_prefix": settings.API_V1_STR,
        "project_version": app.version
        }

# Health check endpoint
@app.get("/health", tags=["Health"], status_code=status.HTTP_200_OK, summary="Health Check")
async def health_check(db: Session = Depends(get_db)):
    """ Performs a basic health check of the API, including database connectivity. """
    db_status = "ok"
    db_details = "Connection successful."
    logger.debug("Performing health check...")
    try:
        from sqlalchemy.sql import text
        db.execute(text("SELECT 1"))
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error("Health check failed: Database connection error", error=str(e), exc_info=False)
        db_status = "error"
        db_details = f"Database connection error: {e}"
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=db_details
        )

    health_data = {
        "status": "ok",
        "components": {
             "database": {"status": db_status, "details": db_details},
        }
    }
    logger.info("Health check successful", components=health_data["components"])
    return health_data


# Mount the main API router for V1 endpoints
app.include_router(api_router, prefix=settings.API_V1_STR)

# Mount the log management router
from app.api.log_management import router as log_management_router
app.include_router(log_management_router, prefix=settings.API_V1_STR)


# --- Startup Event Handler ---
@app.on_event("startup")
async def startup_event():
    """Initialize services on application startup."""
    global rabbitmq_connection
    logger.info("Application starting up...")
    
    # Create database tables if they don't exist
    create_tables()
    
    # Initialize RabbitMQ connection
    try:
        rabbitmq_connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        app.state.rabbitmq_connection = rabbitmq_connection
        logger.info("RabbitMQ connection established")
    except Exception as e:
        logger.error("Failed to connect to RabbitMQ", error=str(e))
        # Set to None so dependency knows it's not available
        app.state.rabbitmq_connection = None
    
    # Initialize cache (temporarily disabled - cache_manager missing)
    # from app.cache.cache_manager import cache_manager
    # await cache_manager.initialize()
    # logger.info("Cache manager initialized")
    
    # Start background services (temporarily disabled for testing)
    # from app.services.background.dicom_cleanup_service import dicom_cleanup_service
    # from app.services.background.system_health_monitor import system_health_monitor
    # from app.services.immediate_batch_processor import immediate_batch_processor
    
    # Start cleanup service (temporarily disabled)
    # dicom_cleanup_service.start()
    # logger.info("DICOM cleanup service started")
    
    # Start system health monitor (temporarily disabled)
    # system_health_monitor.start()
    # logger.info("System health monitor started")
    
    # Start immediate batch processor for zero-latency exam batching
    from app.services.immediate_batch_processor import immediate_batch_processor
    immediate_batch_processor.start()
    logger.info("Immediate batch processor started")
    
    # Start exam batch completion service
    from app.services.exam_batch_completion_service import ExamBatchCompletionService
    completion_service = ExamBatchCompletionService()
    app.state.completion_service_task = asyncio.create_task(completion_service.start())
    logger.info("Exam batch completion service started")
    
    # Start exam batch sender service  
    from app.services.exam_batch_sender_service import ExamBatchSenderService
    sender_service = ExamBatchSenderService()
    app.state.sender_service_task = asyncio.create_task(sender_service.start())
    logger.info("Exam batch sender service started")
    
    # Start maintenance service - the digital janitor
    from app.services.maintenance_service import maintenance_service
    app.state.maintenance_service_task = asyncio.create_task(maintenance_service.start())
    logger.info("Maintenance service started - digital janitor is on the job!")
    
    # Start SSE RabbitMQ consumer for real-time order events
    global sse_consumer_task
    if rabbitmq_connection:
        sse_consumer_task = asyncio.create_task(rabbitmq_consumer(rabbitmq_connection))
        app.state.sse_consumer_task = sse_consumer_task
        logger.info("SSE RabbitMQ consumer started for order events")
    else:
        logger.warning("SSE consumer not started - RabbitMQ connection failed")
    
    logger.info("Application startup complete")


# --- Shutdown Event Handler ---
@app.on_event("shutdown")
async def shutdown_event():
    """ Actions to perform on application shutdown. """
    global rabbitmq_connection, sse_consumer_task
    logger.info("Application shutting down...")
    
    # Stop exam batch services 
    try:
        logger.info("Stopping exam batch background services...")
        
        # Stop immediate batch processor
        from app.services.immediate_batch_processor import immediate_batch_processor
        immediate_batch_processor.stop()
        logger.info("Immediate batch processor stopped")
        
        # from app.services.exam_batch_completion_service import stop_completion_service
        # from app.services.exam_batch_sender_service import stop_sender_service
        
        # stop_completion_service()
        # stop_sender_service()
        
        # Cancel the service tasks
        if hasattr(app.state, 'completion_service_task') and not app.state.completion_service_task.done():
            app.state.completion_service_task.cancel()
            try:
                await app.state.completion_service_task
            except asyncio.CancelledError:
                logger.info("Exam batch completion service task cancelled.")
        
        if hasattr(app.state, 'sender_service_task') and not app.state.sender_service_task.done():
            app.state.sender_service_task.cancel()
            try:
                await app.state.sender_service_task
            except asyncio.CancelledError:
                logger.info("Exam batch sender service task cancelled.")
        
        # Stop maintenance service
        if hasattr(app.state, 'maintenance_service_task') and not app.state.maintenance_service_task.done():
            from app.services.maintenance_service import maintenance_service
            await maintenance_service.stop()
            app.state.maintenance_service_task.cancel()
            try:
                await app.state.maintenance_service_task
            except asyncio.CancelledError:
                logger.info("Maintenance service task cancelled - janitor has clocked out.")
        
        # Stop SSE consumer task
        if hasattr(app.state, 'sse_consumer_task') and not app.state.sse_consumer_task.done():
            app.state.sse_consumer_task.cancel()
            try:
                await app.state.sse_consumer_task
            except asyncio.CancelledError:
                logger.info("SSE consumer task cancelled.")
                
        logger.info("Exam batch background services stopped (disabled).")
    except Exception as e:
        logger.error("Error stopping exam batch services during shutdown.", error=str(e), exc_info=True)
    
    # Stop SSE and RabbitMQ
    global rabbitmq_connection, sse_consumer_task
    if sse_consumer_task and not sse_consumer_task.done():
        sse_consumer_task.cancel()
        try:
            await sse_consumer_task
        except asyncio.CancelledError:
            logger.info("SSE consumer task successfully cancelled.")
    if rabbitmq_connection and not rabbitmq_connection.is_closed:
        await rabbitmq_connection.close()
        logger.info("RabbitMQ connection closed.")
    logger.info("Shutdown complete.")


# --- Main execution block (for direct running with uvicorn) ---
# Note: Uvicorn's logger might still output its own non-JSON logs,
# but our application logs should now be JSON via structlog.
if __name__ == "__main__":
    import uvicorn
    dev_port = getattr(settings, 'DEV_SERVER_PORT', 8000)
    # Use the globally configured log_level
    log_level_cli = settings.LOG_LEVEL.lower() if hasattr(settings, 'LOG_LEVEL') else 'info'

    logger.info(f"Starting Uvicorn server directly (for local dev)...", host="0.0.0.0", port=dev_port, log_level=log_level_cli)
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=dev_port,
        reload=settings.DEBUG,
        log_level=log_level_cli,
        # We rely on our handler; don't let uvicorn override stdlib logging completely
        # Use default uvicorn access logs unless silenced above
        # use_colors=False # Might help if colors interfere with JSON
    )
