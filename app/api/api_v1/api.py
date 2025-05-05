# app/api/api_v1/api.py

from fastapi import APIRouter

# Import endpoint modules
from app.api.api_v1.endpoints import (
    rules,
    auth,
    api_keys,
    users,
    roles,
    dashboard,
    system,
    dicomweb,
    config_dicomweb,
    config_dimse_listeners,
    config_dimse_qr,
    config_storage_backends,
    config_crosswalk,
    config_schedules,
    ai_assist,
    data_browser, # <-- ADDED IMPORT
    config_google_healthcare_sources,
)

api_router = APIRouter()

# Include routers - using consistent '/resource' pattern (no trailing slash)
api_router.include_router(system.router, prefix="/system", tags=["System"])
api_router.include_router(rules.router, prefix="/rules-engine", tags=["Rules Engine"])
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(api_keys.router, prefix="/apikeys", tags=["API Keys"])
api_router.include_router(users.router, prefix="/users", tags=["Users"])
api_router.include_router(roles.router, prefix="/roles", tags=["Roles"])
api_router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
api_router.include_router(dicomweb.router, prefix="/dicomweb", tags=["DICOMweb"])
api_router.include_router(ai_assist.router, prefix="/ai-assist", tags=["AI Assist"])
api_router.include_router(data_browser.router, prefix="/data-browser", tags=["Data Browser"]) # <-- ADDED ROUTER

# Add route for new system config area if needed (using placeholder endpoint)
# api_router.include_router(system_config_placeholder.router, prefix="/admin/system-config", tags=["System Configuration"])

# Configuration Routers
api_router.include_router(
    config_dicomweb.router,
    prefix="/config/dicomweb-sources",
    tags=["Configuration - DICOMweb Sources"]
)
api_router.include_router(
    config_dimse_listeners.router,
    prefix="/config/dimse-listeners",
    tags=["Configuration - DIMSE Listeners"]
)
api_router.include_router(
    config_dimse_qr.router,
    prefix="/config/dimse-qr-sources",
    tags=["Configuration - DIMSE Q/R Sources"]
)
api_router.include_router(
    config_storage_backends.router,
    prefix="/config/storage-backends",
    tags=["Configuration - Storage Backends"]
)
api_router.include_router(
    config_crosswalk.router,
    prefix="/config/crosswalk",
    tags=["Configuration - Crosswalk"]
)
api_router.include_router(
    config_schedules.router,
    prefix="/config/schedules",
    tags=["Configuration - Schedules"]
)
api_router.include_router(config_google_healthcare_sources.router, prefix="/config/google-healthcare-sources", tags=["config-google-healthcare-sources"])
