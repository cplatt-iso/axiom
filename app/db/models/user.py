# app/db/models/user.py
from typing import List, Optional
from datetime import datetime

from sqlalchemy import (
    Column, String, Boolean, DateTime, Table, ForeignKey, Integer, UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.db.base import Base

from .api_key import ApiKey
from .imaging_order import ImagingOrder


# Association Table for User <-> Role (Many-to-Many)
user_role_association = Table(
    'user_role_association',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete="CASCADE"), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id', ondelete="CASCADE"), primary_key=True)
    # Consider adding unique constraint here if not implicitly handled by PK
    # UniqueConstraint('user_id', 'role_id', name='uq_user_role')
)

class Role(Base):
    """
    User Role Model. Defines permissions/access levels.
    Inherits id, created_at, updated_at from Base.
    """
    __tablename__ = 'roles' # type: ignore

    # Implicitly has id, created_at, updated_at from Base

    name: Mapped[str] = mapped_column(String(50), unique=True, index=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationship back to users
    users: Mapped[List["User"]] = relationship(
        secondary=user_role_association,
        back_populates="roles",
        lazy="selectin", # Use selectin loading for roles->users if often needed
    )

    def __repr__(self):
        return f"<Role(id={self.id}, name='{self.name}')>"


class User(Base):
    """
    User Model. Represents a user who can log in and interact with the system.
    Inherits id, created_at, updated_at from Base.
    """
    __tablename__ = 'users' # type: ignore

    # Implicitly has id, created_at, updated_at from Base

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    # Store Google's unique subject ID, make it unique and indexed if used for lookups
    google_id: Mapped[Optional[str]] = mapped_column(String(255), unique=True, index=True, nullable=True)

    # Consolidated name field, often populated from Google login
    full_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    picture: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    # Hashed password for potential future local login strategy
    # Keep non-nullable, use an unusable hash for external auth users
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean(), default=True, nullable=False)
    is_superuser: Mapped[bool] = mapped_column(Boolean(), default=False, nullable=False)

    # Relationship to roles
    roles: Mapped[List["Role"]] = relationship(
        secondary=user_role_association,
        back_populates="users",
        lazy="selectin", # Eagerly load roles when loading a user
        # cascade="all, delete-orphan" # Consider cascade options carefully
    )

    api_keys: Mapped[List["ApiKey"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan" # Delete keys if user is deleted
    )

    # Relationships to other models if needed (e.g., track creator)
    # created_rulesets: Mapped[List["RuleSet"]] = relationship(back_populates="created_by_user")
    imaging_orders: Mapped[List["ImagingOrder"]] = relationship(back_populates="creator")


    # Add table args for potential constraints if needed (e.g., ensure email or google_id is set)
    # __table_args__ = (
    #     CheckConstraint('(email IS NOT NULL) OR (google_id IS NOT NULL)', name='ck_user_identity'),
    # )

    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}', google_id='{self.google_id}')>"
