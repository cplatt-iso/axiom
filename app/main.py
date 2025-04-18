# app/main.py
import logging
from typing import Optional # Import Optional for type hinting
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

# Core Application Imports
from app.core.config import settings
from app.db.session import get_db, engine, SessionLocal, try_connect_db # Import necessary DB components
from app.api import deps # For dependency injection, e.g., getting current user
from app import schemas # Import Pydantic schemas
from app.api.api_v1.api import api_router # Import the main V1 router

# Configure logging
# Simplified basicConfig for now
logging.basicConfig(level=logging.INFO if not settings.DEBUG else logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Database Table Creation Function ---
def create_tables():
    """Ensures all tables defined in models are created in the DB if they don't exist."""
    # --- Import ALL Base-inherited models BEFORE create_all ---
    # This relies on app/db/models/__init__.py importing all model classes
    # And also relies on app/db/base.py defining the Base class
    from app.db import models # noqa F401 - Ensure package init runs, registers models
    from app.db.base import Base # Import Base

    logger.info("Attempting to create database tables (if they don't exist)...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables check/creation complete.")
    except Exception as e:
        logger.error(f"Error during database table creation: {e}", exc_info=True)
        # Consider raising the error to halt startup if DB is absolutely essential immediately
        # raise e


# --- Seed Default Roles ---
def seed_default_roles(db: Session):
    """Checks for default roles (Admin, User) and creates them if missing."""
    # Need models imported for querying and creating
    from app.db import models # Ensure models are available

    logger.info("Checking/Seeding default roles...")
    default_roles_to_seed = {
        "Admin": "Full administrative privileges",
        "User": "Standard user privileges",
        # Add other default roles here if needed
    }
    roles_created_count = 0
    try:
        existing_roles = {role.name for role in db.query(models.Role.name).all()}
        logger.debug(f"Existing roles found: {existing_roles}")

        for role_name, role_desc in default_roles_to_seed.items():
            if role_name not in existing_roles:
                logger.info(f"Creating default role: '{role_name}'")
                role = models.Role(name=role_name, description=role_desc)
                db.add(role)
                roles_created_count += 1

        if roles_created_count > 0:
            db.commit()
            logger.info(f"Committed {roles_created_count} new default roles.")
        else:
            logger.info("All default roles already exist.")

    except Exception as e:
        logger.error(f"Error during role seeding: {e}", exc_info=True)
        db.rollback() # Rollback on any error during seeding


# --- Initialize FastAPI App ---
# Determine root_path based on environment (useful for proxy setups)
run_environment = getattr(settings, 'ENVIRONMENT', 'development').lower()
root_path_setting = f"/{settings.PROJECT_NAME.lower().replace(' ', '_')}" if run_environment != "development" else ""
# Note: You might adjust root_path based on your proxy (Nginx/Traefik) config

app_version = getattr(settings, 'PROJECT_VERSION', '0.1.0')

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    docs_url=f"{settings.API_V1_STR}/docs",
    redoc_url=f"{settings.API_V1_STR}/redoc",
    version=app_version,
    description="Axiom Flow: DICOM Tag Morphing and Routing System",
    debug=settings.DEBUG,
    # root_path=root_path_setting # Uncomment and adjust if running behind a proxy needing path prefix
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
        logger.info(f"CORS enabled for origins: {[str(o) for o in origins]}")
    else:
         logger.warning("BACKEND_CORS_ORIGINS specified but resulted in empty list. CORS not configured.")
else:
     logger.warning("CORS is not configured (BACKEND_CORS_ORIGINS not set).")


# --- API Routers ---
@app.get("/", tags=["Root"], summary="Root Endpoint")
async def read_root():
    """ Root endpoint providing basic application information. """
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
    try:
        # Simple query to check DB connection
        db.execute("SELECT 1")
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}", exc_info=False)
        db_status = "error"
        db_details = f"Database connection error: {e}"
        # Return 503 immediately if DB is down
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=db_details
        )

    # Add checks for other services (Redis, RabbitMQ) here later if needed
    # rabbitmq_status = "not_checked"
    # redis_status = "not_checked"

    return {
        "status": "ok",
        "components": {
             "database": {"status": db_status, "details": db_details},
             # "message_queue": {"status": rabbitmq_status, "details": None},
             # "cache": {"status": redis_status, "details": None},
        }
    }


# Mount the main API router for V1 endpoints
app.include_router(api_router, prefix=settings.API_V1_STR)


# --- Startup Event Handler ---
@app.on_event("startup")
async def startup_event():
    """ Actions to perform on application startup. """
    logger.info("Application starting up...")
    logger.info(f"API Root Path (if behind proxy): '{app.root_path}'")

    # 1. Create Database Tables (if they don't exist)
    logger.info("Checking/Creating database tables...")
    create_tables()

    # 2. Seed Default Roles (Admin, User)
    logger.info("Checking/Seeding default roles...")
    db: Optional[Session] = None
    try:
        db = SessionLocal() # Get a session for seeding
        seed_default_roles(db)
    except Exception as e:
        logger.error(f"Failed to seed roles during startup: {e}", exc_info=True)
    finally:
        if db:
            db.close() # Ensure session is closed

    # 3. Optional: Perform initial DB connection check
    # logger.info("Performing initial database connection check...")
    # try_connect_db() # Uncomment if you have this function defined

    logger.info("Startup complete.")


# --- Shutdown Event Handler ---
@app.on_event("shutdown")
async def shutdown_event():
    """ Actions to perform on application shutdown. """
    logger.info("Application shutting down...")
    # Add any cleanup tasks here if needed


# --- Main execution block (for direct running with uvicorn) ---
if __name__ == "__main__":
    import uvicorn
    # Attempt to get settings for direct run, with defaults
    dev_port = getattr(settings, 'DEV_SERVER_PORT', 8000)
    log_level = getattr(settings, 'LOG_LEVEL', ("debug" if settings.DEBUG else "info")).lower()

    logger.info(f"Starting Uvicorn server directly on port {dev_port} (for local dev)...")
    uvicorn.run(
        "app.main:app", # Point to the FastAPI app instance
        host="0.0.0.0", # Listen on all interfaces for easier access from host
        port=dev_port,
        reload=settings.DEBUG, # Enable auto-reload if DEBUG is True
        log_level=log_level
        )
