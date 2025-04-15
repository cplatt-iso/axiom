# app/schemas/api_key.py
from pydantic import BaseModel, ConfigDict
from typing import Optional
from datetime import datetime

# --- Base Schema ---
class ApiKeyBase(BaseModel):
    name: str
    expires_at: Optional[datetime] = None

# --- Schema for Creating a Key (Input) ---
class ApiKeyCreate(ApiKeyBase):
   pass # Only name is needed from user, key is generated

# --- Schema for Create Response (Output) ---
# This is special: Includes the full key, ONLY shown once
class ApiKeyCreateResponse(ApiKeyBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    prefix: str
    is_active: bool
    created_at: datetime
    updated_at: datetime
    user_id: int
    # --- The full, unhashed API key ---
    full_key: str # This is generated and added ONLY in the response

# --- Schema for Updating a Key (Input) ---
class ApiKeyUpdate(BaseModel):
    name: Optional[str] = None
    is_active: Optional[bool] = None
    expires_at: Optional[datetime] = None # Allow setting/clearing expiration

# --- Standard API Key Schema (Output for GET requests) ---
# Does NOT include the full key or hash
class ApiKey(ApiKeyBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    prefix: str
    is_active: bool
    last_used_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    user_id: int

# --- Internal Schema (Includes Hash - NOT for API response) ---
class ApiKeyInDB(ApiKey): # Inherits fields from ApiKey
    hashed_key: str
