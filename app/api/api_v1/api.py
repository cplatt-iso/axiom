# app/api/api_v1/api.py

from fastapi import APIRouter

# Import endpoint modules
from app.api.api_v1.endpoints import (
    rules,
    auth,
    api_keys,
    users,
    roles,
    dashboard, # Assuming dashboard exists
    system,
    dicomweb,
    config_dicomweb, # <-- Import the new DICOMweb config router
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
api_router.include_router(dicomweb.router, prefix="/dicomweb", tags=["DICOMweb"]) # Renamed tag slightly
# --- Add the new DICOMweb config router ---
api_router.include_router(
    config_dicomweb.router,
    prefix="/config/dicomweb-sources", # Define the specific prefix
    tags=["Configuration - DICOMweb Sources"] # Use a descriptive tag
)
# --- End new router inclusion ---
