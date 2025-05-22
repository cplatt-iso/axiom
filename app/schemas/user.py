# app/schemas/user.py (New File)

from pydantic import BaseModel, EmailStr, ConfigDict
from typing import Optional, List
from datetime import datetime

# --- Role Schemas (Define here or import if in separate file) ---
# If Role schemas are complex, move them to schemas/role.py or similar

class RoleBase(BaseModel):
    name: str
    description: Optional[str] = None

class RoleCreate(RoleBase):
    pass

class Role(RoleBase):
    model_config = ConfigDict(from_attributes=True) # Enable ORM mode

    id: int
    created_at: datetime
    updated_at: datetime

# --- User Schemas ---

class UserBase(BaseModel):
    """Shared base properties for a user."""
    email: Optional[EmailStr] = None
    is_active: Optional[bool] = True
    is_superuser: bool = False
    full_name: Optional[str] = None
    google_id: Optional[str] = None # Add google_id here
    picture: Optional[str] = None

class UserCreate(UserBase):
    """Properties required to create a new user."""
    # For initial Google signup, email and google_id are key
    # We might set a dummy password in the CRUD layer
    email: EmailStr = "default@example.com"  # type: ignore # Provide a default value as a string
    google_id: str  # type: ignore # Make google_id required for this path
    full_name: Optional[str] = None
    # Password might not be needed here if set automatically in CRUD
    # password: str # If allowing password creation


class UserUpdate(UserBase):
    """Properties allowed when updating a user."""
    # Define which fields can be updated via API
    email: Optional[EmailStr] = None
    full_name: Optional[str] = None
    is_active: Optional[bool] = None
    is_superuser: Optional[bool] = None # type: ignore
    # Generally don't allow updating google_id or password directly here
    # roles: Optional[List[int]] = None # Example: Update roles by ID list


# Properties shared by models stored in DB
class UserInDBBase(UserBase):
    model_config = ConfigDict(from_attributes=True) # Enable ORM mode

    id: int
    # email: EmailStr # Email is required in DB model
    # hashed_password: str # Don't expose hash by default
    created_at: datetime
    updated_at: datetime
    roles: List[Role] = [] # Include roles relationship

# Properties to return to client (main User schema)
class User(UserInDBBase):
    """User representation returned by the API."""
    # Inherits fields from UserInDBBase
    # Add any additional fields needed or modify inherited ones
    pass


# Properties stored in DB (potentially including hashed_password)
# Only use this internally if needed, don't return to client
class UserInDB(UserInDBBase):
    hashed_password: str
