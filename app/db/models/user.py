# app/db/models/user.py
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Table, ForeignKey
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func # For server-side default timestamps

from app.db.base import Base

# Association Table for User <-> Role (Many-to-Many)
user_role_association = Table(
    'user_role_association',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id'), primary_key=True)
)

class Role(Base):
    """
    User Role Model. Defines permissions/access levels.
    """
    __tablename__ = 'roles' # Explicitly naming, overriding Base default if needed

    name = Column(String(50), unique=True, index=True, nullable=False)
    description = Column(String(255), nullable=True)

    users = relationship(
        "User",
        secondary=user_role_association,
        back_populates="roles"
    )

    def __repr__(self):
        return f"<Role(name='{self.name}')>"


class User(Base):
    """
    User Model. Represents a user who can log in and interact with the system.
    """
    __tablename__ = 'users' # Explicitly naming

    email = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)
    is_active = Column(Boolean(), default=True)
    is_superuser = Column(Boolean(), default=False) # Optional superuser flag

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    roles = relationship(
        "Role",
        secondary=user_role_association,
        back_populates="users",
        lazy="selectin" # Eagerly load roles when loading a user
    )

    # Add relationships to other models if needed (e.g., who created a ruleset)
    # created_rulesets = relationship("RuleSet", back_populates="created_by_user")

    def __repr__(self):
        return f"<User(email='{self.email}')>"
