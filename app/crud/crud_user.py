# app/crud/crud_user.py
from typing import List, Optional
from sqlalchemy.orm import Session, SessionTransaction, joinedload # Import SessionTransaction
from sqlalchemy import select
import logging # Import logging
import structlog

from app.db.models.user import User, Role
from app.schemas.user import UserCreate, UserUpdate
from app.core.security import get_password_hash

logger = structlog.get_logger(__name__) # Setup logger for this module


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
    picture = google_info.get("picture")

    if not email or not google_id:
         raise ValueError("Google token info missing required fields (email, sub).")
    if not google_info.get("email_verified", False):
         raise ValueError("Google email is not verified.")

    logger.info(f"Creating new user for email: {email}, google_id: {google_id[:5]}...") # Log creation

    db_user = User(
        email=email,
        full_name=full_name,
        google_id=google_id,
        picture=picture,
        is_active=True,
        is_superuser=False,
        hashed_password=get_password_hash("!") # Use an unusable hash
    )

    # --- Assign Default 'User' Role ---
    default_role_name = "User"
    default_role = db.query(Role).filter(Role.name == default_role_name).first()
    if default_role:
        logger.info(f"Assigning default role '{default_role_name}' to new user {email}")
        db_user.roles.append(default_role)
    else:
        # This should ideally not happen if seeding works, but handle it
        logger.error(f"Default role '{default_role_name}' not found in database. Cannot assign to new user.")
        # Decide if this should prevent user creation or just log a warning
        # raise ValueError(f"Default role '{default_role_name}' missing.")
    # --- End Role Assignment ---

    db.add(db_user)

    try:
        # Commit the session to insert the user and trigger DB defaults
        db.commit()
        # Refresh the instance to load DB-generated values like id, created_at, updated_at
        db.refresh(db_user)
        logger.info(f"Successfully created user ID: {db_user.id}")
        # Log timestamps to verify they are loaded
        logger.debug(f"User timestamps after refresh - Created: {db_user.created_at}, Updated: {db_user.updated_at}")
        logger.debug(f"User roles: {[role.name for role in db_user.roles]}")
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
    if not db_user.picture and google_info.get("picture"): # <-- Update picture if missing
         db_user.picture = google_info.get("picture")
         logger.info(f"Updating picture for user ID: {db_user.id}")
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

def update(db: Session, *, db_user: User, user_in: UserUpdate) -> User:
    """Updates a user record."""
    update_data = user_in.model_dump(exclude_unset=True)

    # Explicitly handle potentially sensitive fields if needed
    # e.g., don't allow updating email/google_id easily here unless intended
    if "email" in update_data:
        logger.warning(f"Attempt to update email for user {db_user.id} via generic update ignored.")
        del update_data["email"]
    if "google_id" in update_data:
        logger.warning(f"Attempt to update google_id for user {db_user.id} via generic update ignored.")
        del update_data["google_id"]

    logger.info(f"Updating user {db_user.id} with data: {update_data}")
    for field, value in update_data.items():
        setattr(db_user, field, value)

    db.add(db_user)
    try:
        db.commit()
        db.refresh(db_user)
        db.refresh(db_user, attribute_names=['roles']) # Also refresh roles relationship
        return db_user
    except Exception as e:
        logger.error(f"Error updating user {db_user.id}: {e}", exc_info=True)
        db.rollback()
        raise

def assign_role(db: Session, *, db_user: User, db_role: Role) -> User:
    """Assigns a role to a user if not already assigned."""
    if db_role not in db_user.roles:
        logger.info(f"Assigning role '{db_role.name}' (ID: {db_role.id}) to user {db_user.id}")
        db_user.roles.append(db_role)
        db.add(db_user)
        try:
            db.commit()
            db.refresh(db_user)
            db.refresh(db_user, attribute_names=['roles']) # Refresh roles relationship
        except Exception as e:
            logger.error(f"Error assigning role {db_role.id} to user {db_user.id}: {e}", exc_info=True)
            db.rollback()
            raise
    else:
        logger.debug(f"User {db_user.id} already has role '{db_role.name}' (ID: {db_role.id})")
    return db_user

def remove_role(db: Session, *, db_user: User, db_role: Role) -> User:
    """Removes a role from a user if assigned."""
    if db_role in db_user.roles:
        logger.info(f"Removing role '{db_role.name}' (ID: {db_role.id}) from user {db_user.id}")
        db_user.roles.remove(db_role)
        db.add(db_user)
        try:
            db.commit()
            db.refresh(db_user)
            db.refresh(db_user, attribute_names=['roles']) # Refresh roles relationship
        except Exception as e:
            logger.error(f"Error removing role {db_role.id} from user {db_user.id}: {e}", exc_info=True)
            db.rollback()
            raise
    else:
        logger.warning(f"Role '{db_role.name}' (ID: {db_role.id}) not found on user {db_user.id}, cannot remove.")
    return db_user

def get_multi(db: Session, *, skip: int = 0, limit: int = 100) -> List[User]:
    """Gets multiple users, ordered by ID, eagerly loading roles."""
    statement = (
        select(User)
        .options(joinedload(User.roles)) # Eager load roles
        .order_by(User.id)
        .offset(skip)
        .limit(limit)
    )
    # Using unique() with scalars() helps deduplicate results when using joinedload on a many-to-many relationship
    return list(db.execute(statement).scalars().unique().all())

def get(db: Session, user_id: int) -> Optional[User]:
    """Gets a user by primary key ID, eagerly loading roles."""
    # Use Session.get for PK lookup if only needing the user object itself,
    # or use query().options() if you need eager loading consistently here.
    # Query approach for consistency with get_multi:
    return db.query(User).options(joinedload(User.roles)).filter(User.id == user_id).first()
    # Simpler Session.get approach (roles might lazy load later):
    # return db.get(User, user_id)


# Optional: Add delete user function if needed
# def delete(db: Session, *, user_id: int) -> bool: ...
