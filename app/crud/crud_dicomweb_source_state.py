# app/crud/crud_dicomweb_source_state.py

from typing import List, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import update as sql_update

from app.db.models import DicomWebSourceState # Import the model
# from app.schemas.system import DicomWebSourceStateUpdate # Assuming a schema for updates later, or use dict

# Get a specific source's state
def get_state(db: Session, source_name: str) -> Optional[DicomWebSourceState]:
    """Retrieves the state for a specific DICOMweb source by name."""
    return db.query(DicomWebSourceState).filter(DicomWebSourceState.source_name == source_name).first()

# Get all source states (useful for monitoring)
def get_all_states(db: Session) -> List[DicomWebSourceState]:
    """Retrieves the state for all configured DICOMweb sources."""
    return db.query(DicomWebSourceState).order_by(DicomWebSourceState.source_name).all()

# Create or Update state (Upsert)
def create_or_update_state(db: Session, source_name: str, **kwargs) -> DicomWebSourceState:
    """
    Creates a new state entry or updates an existing one for a source.
    Initializes state based on config if creating.

    Args:
        db: The database session.
        source_name: The unique name of the DICOMweb source.
        **kwargs: Fields to update (e.g., last_processed_timestamp, is_enabled).
                  'is_enabled' should ideally be passed during initial setup/updates
                  based on the application config.
    """
    db_state = get_state(db, source_name)

    if db_state:
        # Update existing state
        update_stmt = (
            sql_update(DicomWebSourceState)
            .where(DicomWebSourceState.source_name == source_name)
            .values(**kwargs)
            # Important: prevent primary key update if passed in kwargs
            .values({key: value for key, value in kwargs.items() if key != 'source_name'})
            .execution_options(synchronize_session="fetch") # Or "evaluate" if appropriate
        )
        db.execute(update_stmt)
        # No commit here, handled by caller (e.g., API request or task context)
        # Refreshing requires re-fetching or careful session management
        # Re-fetch to return the updated object:
        db.expire(db_state) # Expire cached attributes
        db_state = get_state(db, source_name) # Re-fetch

    else:
        # Create new state
        # 'is_enabled' might come from kwargs or default to True if not provided
        if 'is_enabled' not in kwargs:
            kwargs['is_enabled'] = True # Default to enabled if creating without explicit value

        db_state = DicomWebSourceState(source_name=source_name, **kwargs)
        db.add(db_state)
        # No commit here, handled by caller
        db.flush() # Assign generated defaults if any, make object available in session
        db.refresh(db_state) # Ensure we have the potentially new object state

    # Commit should happen outside this function, typically at the end of a task or request
    # db.commit()
    return db_state

# Update specific fields (Example: updating timestamps and errors)
def update_run_state(
    db: Session,
    source_name: str,
    last_processed_timestamp: Optional[datetime] = None,
    last_successful_run: Optional[datetime] = None,
    last_error_run: Optional[datetime] = None,
    last_error_message: Optional[str] = None,
) -> Optional[DicomWebSourceState]:
    """ Atomically updates the run status fields for a source state. """
    update_values = {}
    if last_processed_timestamp is not None:
        update_values["last_processed_timestamp"] = last_processed_timestamp
    if last_successful_run is not None:
        update_values["last_successful_run"] = last_successful_run
        update_values["last_error_message"] = None # Clear error on success
        update_values["last_error_run"] = None # Clear error timestamp on success
    if last_error_run is not None:
        update_values["last_error_run"] = last_error_run
    if last_error_message is not None:
        # Update error message only if error timestamp is also being set or already set
        if last_error_run is not None or db.query(DicomWebSourceState.last_error_run).filter(DicomWebSourceState.source_name == source_name).scalar() is not None:
             update_values["last_error_message"] = last_error_message

    if not update_values:
        return get_state(db, source_name) # Nothing to update

    update_stmt = (
        sql_update(DicomWebSourceState)
        .where(DicomWebSourceState.source_name == source_name)
        .values(**update_values)
        .execution_options(synchronize_session=False) # Use False for potentially faster updates when not refreshing session immediately
    )
    result = db.execute(update_stmt)

    # Commit should happen outside this function
    # db.commit()

    if result.rowcount > 0:
         # Optionally re-fetch and return the updated object
         # db.expire_all() # Or expire specific instance if you have it
         return get_state(db, source_name)
    else:
         # Source name didn't exist
         return None

# (Optional) Delete state - useful if a source is removed from config
def delete_state(db: Session, source_name: str) -> bool:
    """Deletes the state for a specific DICOMweb source."""
    db_state = get_state(db, source_name)
    if db_state:
        db.delete(db_state)
        # Commit should happen outside this function
        # db.commit()
        return True
    return False

# Optional: Define a CRUD object instance (like in crud_rule.py)
class DicomWebSourceStateCRUD:
    def get(self, db: Session, source_name: str) -> Optional[DicomWebSourceState]:
        return get_state(db, source_name)

    def get_all(self, db: Session) -> List[DicomWebSourceState]:
        return get_all_states(db)

    def create_or_update(self, db: Session, source_name: str, **kwargs) -> DicomWebSourceState:
        # Note: commit needs to be handled by the caller
        return create_or_update_state(db, source_name, **kwargs)

    def update_run_status(self, db: Session, source_name: str, **kwargs) -> Optional[DicomWebSourceState]:
         # Note: commit needs to be handled by the caller
         # Pass only relevant kwargs: last_processed_timestamp, last_successful_run, etc.
         return update_run_state(db, source_name, **kwargs)

    def delete(self, db: Session, source_name: str) -> bool:
         # Note: commit needs to be handled by the caller
        return delete_state(db, source_name)

dicomweb_state = DicomWebSourceStateCRUD()
