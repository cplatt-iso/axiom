# app/main.py
import logging
import sys
from typing import Optional # Import Optional for type hinting

import structlog # type: ignore # <-- ADDED: Import structlog
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import asyncio

# Core Application Imports
from app.core.config import settings # Needs LOG_LEVEL setting
from app.db.session import get_db, engine, SessionLocal, try_connect_db # Import necessary DB components
from app.api import deps # For dependency injection, e.g., getting current user
from app import schemas # Import Pydantic schemas
from app.api.api_v1.api import api_router # Import the main V1 router
from app.db.base import Base # Import Base for table creation

# --- Configure logging ---
# Clear existing handlers from the root logger
# to prevent duplicate logs when uvicorn/FastAPI initializes its own handlers
logging.basicConfig(handlers=[logging.NullHandler()]) # <-- CHANGED: Prevent basicConfig output

# Determine log level from settings or default
log_level_str = getattr(settings, 'LOG_LEVEL', "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

def configure_logging():
    """Configures logging using structlog to output JSON."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"), # ISO 8601 timestamp
            structlog.processors.format_exc_info, # Render exception info if present
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        # Processor to render logs as JSON
        processor=structlog.processors.JSONRenderer(),
        # foreign_pre_chain: Process logs from other libraries (optional but good)
        # Order matters: Add level/name, timestamp, then render
        foreign_pre_chain=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
        ],
    )

    # Get the root logger and remove existing handlers
    # This helps prevent duplicate outputs, especially when using --reload
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add a new handler to output logs to stdout using our JSON formatter
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    # Set the overall logging level for the root logger
    root_logger.setLevel(log_level)

    # Optionally silence overly verbose libraries
    # logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    # Get a logger for this module AFTER configuration
    logger = structlog.get_logger(__name__)
    logger.info(
        "Structlog logging configured for FastAPI",
        log_level=log_level_str,
        config_source=f"settings.LOG_LEVEL={settings.LOG_LEVEL}" if hasattr(settings, 'LOG_LEVEL') else "Default=INFO"
    )
    return logger # Return the configured logger instance

# Call configure_logging() immediately to set up logging system-wide
# and get the logger instance for this module
logger = configure_logging()


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


# --- Startup Event Handler ---
@app.on_event("startup")
async def startup_event():
    """ Actions to perform on application startup. """
    global rabbitmq_connection, sse_consumer_task
    logger.info("Application starting up...", api_root_path=app.root_path)

    logger.info("Checking/Creating database tables...")
    create_tables()

    logger.info("Checking/Seeding default roles...")
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        seed_default_roles(db)
    except Exception as e:
        logger.error("Failed to seed roles during startup", error=str(e), exc_info=True)
    finally:
        if db:
            db.close()

    try:
        logger.info("Connecting to RabbitMQ for SSE...")
        rabbitmq_connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        # Pass the connection to the dependency injection system
        app.state.rabbitmq_connection = rabbitmq_connection
        
        # Start the consumer task
        sse_consumer_task = asyncio.create_task(rabbitmq_consumer(rabbitmq_connection))
        logger.info("RabbitMQ connection and SSE consumer started successfully.")
    except Exception as e:
        logger.error("Failed to connect to RabbitMQ or start SSE consumer during startup.", error=str(e), exc_info=True)
        # Depending on requirements, you might want to exit if RabbitMQ is essential
        # For now, we log the error and continue.

    logger.info("Startup complete.")


# --- Shutdown Event Handler ---
@app.on_event("shutdown")
async def shutdown_event():
    """ Actions to perform on application shutdown. """
    global rabbitmq_connection, sse_consumer_task
    logger.info("Application shutting down...")
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
    log_level_cli = log_level_str.lower()

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

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from app.events import rabbitmq_consumer, ORDERS_EXCHANGE_NAME

# --- RabbitMQ Connection and SSE Consumer Task ---
rabbitmq_connection: Optional[AbstractRobustConnection] = None
sse_consumer_task: Optional[asyncio.Task] = None
