# app/main.py
import logging
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware # Ensure CORS is imported
from sqlalchemy.orm import Session
from app.core.config import settings
from app.db.session import get_db # Import get_db from its actual location
from app.api import deps
from app.db import models
from app import schemas
from app.db.session import engine, try_connect_db # Import engine and try_connect_db
from app.api.api_v1.api import api_router # Import the main V1 router

# Configure logging
# Simplified basicConfig for now
logging.basicConfig(level=logging.INFO if not settings.DEBUG else logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

run_environment = getattr(settings, 'ENVIRONMENT', 'development').lower()
root_path_setting = settings.API_V1_STR if run_environment != "development" else ""

# --- Seed Default Roles --- #
def seed_default_roles(db: Session):
    """Checks for default roles and creates them if missing."""
    logger = logging.getLogger(__name__)
    default_roles = [
        schemas.RoleCreate(name="Admin", description="Full administrative privileges"),
        schemas.RoleCreate(name="User", description="Standard user privileges"),
        # Add other default roles as needed (e.g., "Editor", "Viewer")
    ]
    roles_created_count = 0
    for role_in in default_roles:
        try:
            existing_role = db.query(models.Role).filter(models.Role.name == role_in.name).first()
            if not existing_role:
                logger.info(f"Creating default role: '{role_in.name}'")
                role = models.Role(**role_in.model_dump())
                db.add(role)
                roles_created_count += 1
            # else: logger.debug(f"Default role '{role_in.name}' already exists.")
        except Exception as e:
            logger.error(f"Error creating role '{role_in.name}': {e}", exc_info=True)
            # Decide if this should be fatal

    if roles_created_count > 0:
        try:
            db.commit()
            logger.info(f"Committed {roles_created_count} new default roles.")
        except Exception as e:
            logger.error(f"Error committing default roles: {e}", exc_info=True)
            db.rollback()
    else:
        logger.info("Default roles already exist.")

# --- Optional: Database Table Creation ---
def create_tables():
    from app.db import models # noqa
    from app.db.base import Base # noqa
    logger.info("Attempting to create database tables (if they don't exist)...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables check/creation complete.")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        # raise e # Decide if you want to halt startup on DB error


# Uncomment/manage table creation as needed
# if settings.DEBUG:
#     logger.warning("Development mode: Creating database tables on startup if needed!")
#     create_tables()

# --- Initialize FastAPI App ---
# Use settings for version if available, otherwise default
app_version = getattr(settings, 'PROJECT_VERSION', '0.1.0')

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    docs_url=f"{settings.API_V1_STR}/docs",
    redoc_url=f"{settings.API_V1_STR}/redoc",
    version=app_version,
    description="axiom flow api",
    debug=settings.DEBUG,
    root_path=root_path_setting
)

# --- Middleware ---
if settings.BACKEND_CORS_ORIGINS:
    # Use the validator already present in config.py
    origins = settings.BACKEND_CORS_ORIGINS
    if origins: # Ensure the list isn't empty after potential parsing
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[str(origin) for origin in origins], # Convert AnyHttpUrl etc. to str
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
async def health_check(db: Session = Depends(get_db)): # <-- Use get_db directly
    """ Performs a basic health check of the API, including database connectivity. """
    db_status = "ok"
    try:
        # Simple query to check DB connection
        db.execute("SELECT 1")
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}", exc_info=False)
        db_status = "error"
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error."
        )

    # rabbitmq_status = "not_checked" # Placeholder
    return {
        "status": "ok",
        "database_connection": db_status,
        # "message_queue_connection": rabbitmq_status,
    }


# Mount the main API router for V1 endpoints
app.include_router(api_router, prefix=settings.API_V1_STR)


@app.on_event("startup")
async def startup_event():
    logger.info("Application starting up...")
    logger.info(f"CORS allowed origins: {settings.BACKEND_CORS_ORIGINS}")

    # --- Seed Roles on Startup ---
    # Use a context manager for the session
    logger.info("Attempting to seed default roles...")
    db: Optional[Session] = None
    try:
        db = deps.SessionLocal() # Get a new session
        seed_default_roles(db)
    except Exception as e:
        logger.error(f"Failed to seed roles during startup: {e}", exc_info=True)
    finally:
        if db:
            db.close()
    # --- End Seed Roles ---
    if settings.DEBUG:
        # ... (existing debug startup checks like try_connect_db) ...
        pass



@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutting down...")


# --- Main execution block (for direct running) ---
if __name__ == "__main__":
    import uvicorn
    # Attempt to get settings for direct run, with defaults
    dev_port = getattr(settings, 'DEV_SERVER_PORT', 8000)
    log_level = getattr(settings, 'LOG_LEVEL', ("debug" if settings.DEBUG else "info")).lower()

    logger.info(f"Starting Uvicorn server directly on port {dev_port} (for local dev)...")
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=dev_port,
        reload=settings.DEBUG,
        log_level=log_level
        )
