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
    system # <-- Import the new system router
)

api_router = APIRouter()

# Include routers - using consistent '/resource' pattern (no trailing slash)
# Observation: Some existing prefixes have trailing slashes, some don't.
# Standardizing on no trailing slash for future additions.
api_router.include_router(system.router, prefix="/system", tags=["System"]) # <-- Add the system router
api_router.include_router(rules.router, prefix="/rules-engine", tags=["Rules Engine"])
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(api_keys.router, prefix="/apikeys", tags=["API Keys"]) # Observation: `/apikeys` instead of `/api-keys`? Keep as is.
api_router.include_router(users.router, prefix="/users", tags=["Users"])
api_router.include_router(roles.router, prefix="/roles", tags=["Roles"])
api_router.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])
