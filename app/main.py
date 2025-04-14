# app/main.py
import logging
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.session import engine, get_db, try_connect_db # Import engine and get_db
from app.api.api_v1.api import api_router # Import the main V1 router

# Configure logging
# Set basic configuration for the root logger
# Other modules can use logging.getLogger(__name__)
# Consider more sophisticated logging config later (e.g., using dictConfig)
logging.basicConfig(level=logging.INFO if not settings.DEBUG else logging.DEBUG)
logger = logging.getLogger(__name__)


# --- Optional: Database Table Creation ---
# This is generally better handled by migration tools like Alembic
# but can be useful for simple setups or initial development.
# Consider moving this to a separate script or management command.
def create_tables():
    # Import all models here so they are registered with Base.metadata
    from app.db import models # noqa
    from app.db.base import Base # noqa
    logger.info("Attempting to create database tables (if they don't exist)...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables check/creation complete.")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        # Decide if the app should exit if DB init fails
        # raise e

# Uncomment the line below to attempt table creation on startup (run once or use migrations)
# Important: If you modify models, create_all will NOT update existing tables. Use Alembic for migrations.
# logger.warning("Development mode: Creating database tables on startup!")
# create_tables()
# --- End Optional Database Table Creation ---

# --- Initialize FastAPI App ---
# Add description, version etc. if desired
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json", # Use constant from settings
    docs_url="/docs", # Default Swagger UI URL
    redoc_url="/redoc", # Default ReDoc URL
    version="0.1.0", # Example version
    description="DICOM Tag Morphing and Routing Engine",
    debug=settings.DEBUG,
)

# --- Middleware ---
# Set all CORS enabled origins
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"], # Allow all standard methods
        allow_headers=["*"], # Allow all headers
    )
else:
     # If no origins specified, CORS will be effectively disabled
     logger.warning("CORS is not configured. No origins allowed.")


# --- API Routers ---
# Root endpoint for basic info/welcome
@app.get("/", tags=["Root"])
async def read_root():
    """
    Root endpoint providing basic application information.
    """
    return {
        "message": f"Welcome to {settings.PROJECT_NAME}",
        "docs_url": app.docs_url,
        "redoc_url": app.redoc_url,
        "api_version": settings.API_V1_STR,
        "project_version": app.version
        }

# Health check endpoint
@app.get("/health", tags=["Health"], status_code=status.HTTP_200_OK)
async def health_check(db: Session = Depends(get_db)):
    """
    Performs a basic health check of the API.

    Checks database connectivity.
    (Could be expanded to check message queue, etc.)
    """
    db_status = "ok"
    try:
        # Try a simple query to check DB connection
        db.execute("SELECT 1")
        logger.debug("Health check: Database connection successful.")
    except Exception as e:
        logger.error(f"Health check failed: Database connection error: {e}", exc_info=True)
        db_status = "error"
        # Raise 503 Service Unavailable if DB is critical and unreachable
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection error: {e}"
        )

    # TODO: Add check for RabbitMQ connection if needed

    return {
        "status": "ok",
        "database_connection": db_status,
        # "message_queue_connection": rabbitmq_status, # Placeholder
    }


# Mount the main API router for V1 endpoints
app.include_router(api_router, prefix=settings.API_V1_STR)


# --- Optional: Run test connection on startup ---
# Useful for quick feedback during development startup
@app.on_event("startup")
async def startup_event():
    logger.info("Application starting up...")
    logger.info(f"CORS allowed origins: {settings.BACKEND_CORS_ORIGINS}")
    if settings.DEBUG:
        logger.info("Running startup database connection test...")
        if not try_connect_db():
             logger.error("Startup database connection test FAILED.")
        # else: (already logs success)


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutting down...")


# Allow running directly with uvicorn for simple development (though Docker is preferred)
# Example: uvicorn app.main:app --reload
# Not typically needed when running via Docker CMD/command
if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Uvicorn server directly (for local dev without Docker)...")
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8000, # Standard local dev port, distinct from Docker's internal/mapped port
        reload=True, # Enable auto-reload
        log_level="debug" if settings.DEBUG else "info"
        )
