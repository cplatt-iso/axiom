# app/api/api_v1/api.py

from fastapi import APIRouter

from app.api.api_v1.endpoints import rules, auth, api_keys
# Import other endpoint routers here later (e.g., auth, status)

api_router = APIRouter()

api_router.include_router(rules.router, prefix="/rules-engine", tags=["Rules Engine"])
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(api_keys.router, prefix="/apikeys", tags=["API Keys"])
