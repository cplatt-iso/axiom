# app/db/models/api_key.py
from typing import Optional
from datetime import datetime

from sqlalchemy import String, DateTime, Integer, ForeignKey, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
# Import User if needed for relationship typing, but ForeignKey is primary link
# from .user import User

class ApiKey(Base):
    """
    API Key Model. Represents an API key associated with a user.
    """
    __tablename__ = 'api_keys'

    # Inherits id, created_at, updated_at from Base

    # Only store the HASH of the key, never the key itself
    hashed_key: Mapped[str] = mapped_column(String(255), index=True, nullable=False, unique=True)
    # Store the first few characters (prefix) for identification purposes
    prefix: Mapped[str] = mapped_column(String(8), index=True, nullable=False, unique=True)
    # User-friendly name for the key
    name: Mapped[str] = mapped_column(String(100), index=True, nullable=False)
    # Link to the user who owns the key
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey('users.id', ondelete="CASCADE"), nullable=False)
    # Optional expiration date
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Optional last used timestamp for auditing
    last_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    # Whether the key is currently active
    is_active: Mapped[bool] = mapped_column(Boolean(), default=True, nullable=False)

    # Relationship back to the User (optional but useful)
    user: Mapped["User"] = relationship(back_populates="api_keys") # type: ignore

    def __repr__(self):
        return f"<ApiKey(id={self.id}, name='{self.name}', prefix='{self.prefix}', user_id={self.user_id})>"
