# app/db/models/user.py
from typing import List, Optional
from datetime import datetime

from sqlalchemy import (
    Column, String, Boolean, DateTime, Table, ForeignKey, Integer
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.db.base import Base

# Association Table for User <-> Role (Many-to-Many)
# Kept as standard Table definition, which is fine alongside declarative models.
user_role_association = Table(
    'user_role_association',
    Base.metadata, # Use Base.metadata for naming conventions etc.
    # Use mapped_column syntax indirectly here for consistency? No, Table syntax is different.
    # Just specify the columns directly for association tables.
    Column('user_id', Integer, ForeignKey('users.id', ondelete="CASCADE"), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id', ondelete="CASCADE"), primary_key=True)
)

class Role(Base):
    """
    User Role Model. Defines permissions/access levels.
    Inherits id, created_at, updated_at from Base.
    """
    __tablename__ = 'roles'

    name: Mapped[str] = mapped_column(String(50), unique=True, index=True)
    description: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Relationship back to users
    # Use Mapped[List["User"]] type hint
    # Specify lazy="selectin" to often avoid N+1 queries when accessing user roles
    users: Mapped[List["User"]] = relationship(
        secondary=user_role_association,
        back_populates="roles",
        lazy="selectin", # Eagerly load associated users via separate SELECT
        # init=False # Typically don't initialize relationships directly
    )

    def __repr__(self):
        return f"<Role(id={self.id}, name='{self.name}')>"


class User(Base):
    """
    User Model. Represents a user who can log in and interact with the system.
    Inherits id, created_at, updated_at from Base.
    """
    __tablename__ = 'users'

    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255)) # Password should never be nullable
    first_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean(), default=True)
    is_superuser: Mapped[bool] = mapped_column(Boolean(), default=False)

    # Timestamps are inherited from Base

    # Relationship to roles
    # Use Mapped[List["Role"]] type hint
    roles: Mapped[List["Role"]] = relationship(
        secondary=user_role_association,
        back_populates="users",
        lazy="selectin" # Eagerly load roles when loading a user
        # init=False
    )

    # Relationships to other models if needed (e.g., track creator)
    # created_rulesets: Mapped[List["RuleSet"]] = relationship(back_populates="created_by_user", init=False)

    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}')>"
