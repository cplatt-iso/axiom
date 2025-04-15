# app/schemas/token.py (New File)
from pydantic import BaseModel
from typing import Optional
from .user import User # Assuming you have a User schema for response

class GoogleToken(BaseModel):
    token: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: Optional[User] = None # Optionally return user info
