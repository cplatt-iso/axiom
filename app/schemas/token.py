# app/schemas/token.py (New File)
import time
from pydantic import BaseModel, Field
from typing import Optional, Any
from .user import User # Assuming you have a User schema for response

class GoogleToken(BaseModel):
    token: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: Optional[User] = None # Optionally return user info

class TokenPayload(BaseModel):
    """Schema representing the data encoded within a JWT access token."""
    sub: Optional[int] = Field(None, description="Subject (usually User ID)")
    exp: Optional[int] = Field(None, description="Expiration time (Unix timestamp)")
    # Add any other custom claims you might include in the token
    # Example: iss: Optional[str] = Field(None, description="Issuer")
    # Example: aud: Optional[str] = Field(None, description="Audience")

    # You could add validation here, e.g., check if 'sub' is present
    # @validator('sub')
    # def sub_must_exist(cls, v):
    #     if v is None:
    #         raise ValueError("'sub' claim (user ID) must be present in token")
    #     return v

    # Helper to check if token is expired (optional)
    def is_expired(self) -> bool:
        """Checks if the token's 'exp' claim indicates it has expired."""
        if self.exp is None:
             return False # Token without expiration is considered non-expired by this check
        # Compare expiration timestamp with current UTC timestamp
        return datetime.now(timezone.utc).timestamp() > self.exp
