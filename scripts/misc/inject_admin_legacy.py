# inject_admin.py
import logging
import structlog
import os
import sys
from typing import Optional

# -- Setup Project Path (Still useful if importing models/base) --
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
# -- End Setup --

from sqlalchemy import create_engine, select, Column, Integer, String, DateTime, Boolean, ForeignKey, Table # Import core elements
from sqlalchemy.orm import sessionmaker, Session, relationship, declarative_base, joinedload
from sqlalchemy.sql import func
from sqlalchemy.exc import SQLAlchemyError

# --- Database Configuration ---
# Replace with your actual connection string
# Use environment variable if possible for security
DATABASE_URI = os.getenv("ADMIN_SCRIPT_DB_URI", "postgresql+psycopg://axiom_user:tipper@localhost:5432/axiom_db")
# NOTE: Using localhost assumes you map the DB port in docker-compose or run the script where 'localhost' resolves to the DB.
# If running script on host and DB is only in docker, use host IP or mapped port. If NPM is port 5432, it will not resolve.
# Consider changing 'localhost' above if needed, or map port 5432 from db container to host 5432.

# --- Target Configuration ---
ADMIN_ROLE_NAME = "Admin"
USER_ROLE_NAME = "User"
TARGET_USER_ID = 1
# --- End Configuration ---

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# --- Define Models Directly (or minimal Base) ---
# Avoid importing app.db.base to prevent side effects from __init__ etc.
# Define a minimal Base or recreate necessary models here if imports fail.

Base = declarative_base()

# Minimal definitions matching your actual models structure are needed for SQLAlchemy ORM operations

user_role_association = Table(
    'user_role_association', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete="CASCADE"), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id', ondelete="CASCADE"), primary_key=True)
)

class Role(Base):
    __tablename__ = 'roles'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, index=True, nullable=False)
    description = Column(String(255), nullable=True)
    # Minimal relationship needed for query logic if joining back from user
    # users = relationship("User", secondary=user_role_association, back_populates="roles")


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    # Define roles relationship needed for the script
    roles = relationship("Role", secondary=user_role_association, lazy="selectin", back_populates="users") # Use back_populates if Role defines users

# Define back-population on Role if needed for User relationship:
Role.users = relationship("User", secondary=user_role_association, back_populates="roles")


def inject_admin_role():
    """
    Ensures default roles exist and assigns the Admin role to the target user
    using a direct database connection.
    """
    logger.info(f"Starting admin injection process for DB: {DATABASE_URI.split('@')[-1]}") # Don't log password

    engine = None
    SessionLocal = None
    try:
        # Adjust pool settings if needed, pre-ping is good for scripts
        engine = create_engine(DATABASE_URI, pool_pre_ping=True, echo=False) # Set echo=True for SQL debug
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        logger.info("Database engine created.")
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        sys.exit(1)

    session: Optional[Session] = None
    try:
        session = SessionLocal()
        logger.info("Database session opened.")

        # --- 1. Ensure Roles Exist ---
        roles_to_ensure = {
            ADMIN_ROLE_NAME: "Full administrative privileges",
            USER_ROLE_NAME: "Standard user privileges"
        }
        admin_role: Optional[Role] = None
        needs_commit = False

        for role_name, role_desc in roles_to_ensure.items():
            existing_role = session.scalar(select(Role).filter_by(name=role_name))
            if not existing_role:
                logger.info(f"Role '{role_name}' not found. Creating...")
                new_role = Role(name=role_name, description=role_desc)
                session.add(new_role)
                needs_commit = True
                if role_name == ADMIN_ROLE_NAME:
                    admin_role = new_role
            elif role_name == ADMIN_ROLE_NAME:
                admin_role = existing_role

        if needs_commit:
            logger.info("Committing new roles...")
            session.commit()
            logger.info("Roles committed.")
            if admin_role and admin_role not in session: # If newly created, needs re-fetch
                 admin_role = session.scalar(select(Role).filter_by(name=ADMIN_ROLE_NAME))


        if not admin_role:
            admin_role = session.scalar(select(Role).filter_by(name=ADMIN_ROLE_NAME))
            if not admin_role:
                logger.error(f"Failed to find or create the '{ADMIN_ROLE_NAME}' role after commit attempt.")
                sys.exit(1)
            logger.info(f"Found existing '{ADMIN_ROLE_NAME}' role (ID: {admin_role.id}).")

        # --- 2. Find Target User ---
        logger.info(f"Looking for User with ID: {TARGET_USER_ID}")
        target_user = session.query(User).options(joinedload(User.roles)).filter(User.id == TARGET_USER_ID).first()

        if not target_user:
            logger.error(f"User with ID {TARGET_USER_ID} not found in the database.")
            logger.warning("Cannot assign Admin role.")
            sys.exit(1)

        logger.info(f"Found User: {target_user.email} (ID: {target_user.id})")

        # --- 3. Assign Admin Role if Missing ---
        is_admin_already = any(role.id == admin_role.id for role in target_user.roles)

        if is_admin_already:
            logger.info(f"User {TARGET_USER_ID} already has the '{ADMIN_ROLE_NAME}' role.")
        else:
            logger.info(f"Assigning '{ADMIN_ROLE_NAME}' role (ID: {admin_role.id}) to User {TARGET_USER_ID}...")
            # Append the persistent admin_role object to the user's roles
            target_user.roles.append(admin_role)
            session.add(target_user) # Mark user as dirty
            session.commit()
            logger.info("Admin role assigned successfully.")

        logger.info("Admin injection process completed.")

    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {e}", exc_info=True)
        if session:
            session.rollback()
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        if session:
            session.rollback()
        sys.exit(1)
    finally:
        if session:
            session.close()
            logger.info("Database session closed.")
        # Dispose engine if created locally? Optional for short script.
        # if engine:
        #     engine.dispose()


if __name__ == "__main__":
    inject_admin_role()
