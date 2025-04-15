# app/crud/crud_user.py
from sqlalchemy.orm import Session, SessionTransaction # Import SessionTransaction
from sqlalchemy import select
import logging # Import logging

from app.db.models.user import User
from app.schemas.user import UserCreate
from app.core.security import get_password_hash

logger = logging.getLogger(__name__) # Setup logger for this module


def get_user_by_email(db: Session, *, email: str) -> User | None:
    """Gets a user by email."""
    statement = select(User).where(User.email == email)
    return db.execute(statement).scalar_one_or_none()

def get_user_by_google_id(db: Session, *, google_id: str) -> User | None:
    """Gets a user by their Google subject ID."""
    statement = select(User).where(User.google_id == google_id)
    return db.execute(statement).scalar_one_or_none()

def create_user_from_google(db: Session, *, google_info: dict) -> User:
    """Creates a new user from validated Google token info."""
    email = google_info.get("email")
    google_id = google_info.get("sub")
    full_name = google_info.get("name")

    if not email or not google_id:
         raise ValueError("Google token info missing required fields (email, sub).")
    if not google_info.get("email_verified", False):
         raise ValueError("Google email is not verified.")

    logger.info(f"Creating new user for email: {email}, google_id: {google_id[:5]}...") # Log creation

    db_user = User(
        email=email,
        full_name=full_name,
        google_id=google_id,
        is_active=True,
        is_superuser=False,
        hashed_password=get_password_hash("!") # Use an unusable hash
    )
    db.add(db_user)

    try:
        # Commit the session to insert the user and trigger DB defaults
        db.commit()
        # Refresh the instance to load DB-generated values like id, created_at, updated_at
        db.refresh(db_user)
        logger.info(f"Successfully created user ID: {db_user.id}")
        # Log timestamps to verify they are loaded
        logger.debug(f"User timestamps after refresh - Created: {db_user.created_at}, Updated: {db_user.updated_at}")
        return db_user
    except Exception as e:
        logger.error(f"Error during user creation commit/refresh: {e}", exc_info=True)
        db.rollback() # Rollback the transaction on error
        raise # Re-raise the exception


def update_user_google_info(db: Session, *, db_user: User, google_info: dict) -> User:
    """Updates an existing user with info from Google login if needed."""
    updated = False
    if not db_user.google_id and google_info.get("sub"):
        db_user.google_id = google_info.get("sub")
        logger.info(f"Updating google_id for user ID: {db_user.id}")
        updated = True
    if not db_user.full_name and google_info.get("name"):
         db_user.full_name = google_info.get("name")
         logger.info(f"Updating full_name for user ID: {db_user.id}")
         updated = True

    if updated:
        db.add(db_user) # Add the instance again to mark it dirty for update
        try:
            # Commit first to trigger onupdate timestamps
            db.commit()
            # Refresh to load updated values
            db.refresh(db_user)
            logger.info(f"Successfully updated user ID: {db_user.id}")
            logger.debug(f"User timestamps after update refresh - Created: {db_user.created_at}, Updated: {db_user.updated_at}")

        except Exception as e:
            logger.error(f"Error during user update commit/refresh: {e}", exc_info=True)
            db.rollback()
            raise
    return db_user
