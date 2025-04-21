# app/crud/crud_dicomweb_source_state.py

from typing import List, Optional
from datetime import datetime
import logging # Import logging
from sqlalchemy.orm import Session
from sqlalchemy import update as sql_update, select

from app.db.models import DicomWebSourceState # Import the model
# from app.schemas.system import DicomWebSourceStateUpdate # Assuming a schema for updates later, or use dict

logger = logging.getLogger(__name__) # Setup logger

# Get a specific source's state
def get_state(db: Session, source_name: str) -> Optional[DicomWebSourceState]:
    """Retrieves the state for a specific DICOMweb source by name."""
    logger.debug(f"Querying state for DICOMweb source: {source_name}")
    statement = select(DicomWebSourceState).where(DicomWebSourceState.source_name == source_name)
    result = db.execute(statement).scalar_one_or_none()
    if result:
        logger.debug(f"Found state for source: {source_name}")
    else:
        logger.debug(f"No state found for source: {source_name}")
    return result


# Get all source states (useful for monitoring)
# --- ADDED skip/limit parameters ---
def get_all_states(db: Session, *, skip: int = 0, limit: int = 100) -> List[DicomWebSourceState]:
    """Retrieves the state for all configured DICOMweb sources."""
    logger.debug(f"Querying all DICOMweb source states (skip={skip}, limit={limit})")
    statement = (
        select(DicomWebSourceState)
        .order_by(DicomWebSourceState.source_name)
        .offset(skip)
        .limit(limit)
    )
    results = list(db.execute(statement).scalars().all()) # Convert iterator to list
    logger.debug(f"Found {len(results)} DICOMweb source states.")
    return results
# --- END ADDED skip/limit ---

# Create or Update state (Upsert)
# Note: commit needs to be handled by the caller
def create_or_update_state(db: Session, source_name: str, **kwargs) -> DicomWebSourceState:
    """
    Creates a new state entry or updates an existing one for a source.
    Initializes state based on config if creating.
    Commit needs to be handled by the caller.

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
        logger.debug(f"Updating existing state for DICOMweb source: {source_name} with data: {kwargs}")
        # Filter out the primary key if passed inadvertently
        update_data = {key: value for key, value in kwargs.items() if key != 'source_name'}
        if not update_data:
             logger.debug(f"No fields to update for source {source_name}, returning existing state.")
             return db_state # Return early if no actual updates

        for field, value in update_data.items():
            setattr(db_state, field, value)
        # No db.add needed for updates when using the ORM object directly
        db.flush() # Flush changes to session before refresh
        db.refresh(db_state)
        logger.debug(f"State updated for source: {source_name}")

    else:
        # Create new state
        logger.info(f"Creating new state for DICOMweb source: {source_name} with data: {kwargs}")
        # 'is_enabled' might come from kwargs or default to True if not provided
        if 'is_enabled' not in kwargs:
            kwargs['is_enabled'] = True # Default to enabled if creating without explicit value

        db_state = DicomWebSourceState(source_name=source_name, **kwargs)
        db.add(db_state)
        db.flush() # Assign generated defaults if any, make object available in session
        db.refresh(db_state) # Ensure we have the potentially new object state with defaults
        logger.info(f"New state created for source: {source_name}")

    # Commit should happen outside this function, typically at the end of a task or request
    # db.commit()
    return db_state

# Update specific fields (Example: updating timestamps and errors)
# Note: commit needs to be handled by the caller
def update_run_state(
    db: Session,
    source_name: str,
    last_processed_timestamp: Optional[datetime] = None,
    last_successful_run: Optional[datetime] = None,
    last_error_run: Optional[datetime] = None,
    last_error_message: Optional[str] = None,
) -> Optional[DicomWebSourceState]:
    """ Atomically updates the run status fields for a source state. Commit handled by caller. """
    update_values = {}
    if last_processed_timestamp is not None:
        update_values["last_processed_timestamp"] = last_processed_timestamp
    if last_successful_run is not None:
        update_values["last_successful_run"] = last_successful_run
        # Optionally clear error fields on success - be careful if errors can happen *after* success timestamp update
        update_values["last_error_message"] = None
        update_values["last_error_run"] = None
    if last_error_run is not None:
        update_values["last_error_run"] = last_error_run
    if last_error_message is not None:
        # Only update error message if error timestamp is also being set
        if last_error_run is not None:
            update_values["last_error_message"] = last_error_message
        else:
            logger.warning(f"Attempted to set last_error_message for {source_name} without setting last_error_run timestamp. Skipping message update.")


    if not update_values:
        logger.debug(f"No run state fields provided to update for source {source_name}.")
        return get_state(db, source_name) # Nothing to update

    logger.debug(f"Updating run state for DICOMweb source: {source_name} with values: {update_values}")
    # Use ORM update for potentially better performance if not needing the object immediately
    update_stmt = (
        sql_update(DicomWebSourceState)
        .where(DicomWebSourceState.source_name == source_name)
        .values(**update_values)
        # synchronize_session=False is generally ok if you don't need the updated object in the current session immediately
        # If you need it, use 'fetch' or re-fetch manually after commit.
        .execution_options(synchronize_session=False)
    )
    result = db.execute(update_stmt)
    # db.flush() # Flush to apply changes before potential commit outside

    # Commit should happen outside this function
    # db.commit()

    if result.rowcount > 0:
         logger.debug(f"Run state updated successfully for {source_name} (rows affected: {result.rowcount}).")
         # Optionally re-fetch and return the updated object - requires commit first or careful session handling
         # return get_state(db, source_name)
         # For now, return None as commit is external and object state in session might be stale
         return None # Or return True? Or fetch after caller commits? Returning None indicates success without refreshed object.
    else:
         logger.warning(f"Run state update failed for {source_name} (source not found?).")
         # Source name didn't exist
         return None

# (Optional) Delete state - useful if a source is removed from config
# Note: commit needs to be handled by the caller
def delete_state(db: Session, source_name: str) -> bool:
    """Deletes the state for a specific DICOMweb source. Commit handled by caller."""
    db_state = get_state(db, source_name)
    if db_state:
        logger.info(f"Deleting state for DICOMweb source: {source_name}")
        db.delete(db_state)
        db.flush() # Ensure delete is flushed before potential commit outside
        # db.commit() # Commit handled by caller
        return True
    logger.warning(f"Could not delete state for DICOMweb source: {source_name} (not found).")
    return False

# Define the CRUD object instance
class DicomWebSourceStateCRUD:
    def get(self, db: Session, source_name: str) -> Optional[DicomWebSourceState]:
        # This method calls the standalone function
        return get_state(db, source_name)

    # --- UPDATED get_all method signature ---
    def get_all(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[DicomWebSourceState]:
        # This method calls the updated standalone function
        return get_all_states(db=db, skip=skip, limit=limit)
    # --- END UPDATE ---

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

# Export the instance (matches how it's likely imported in crud/__init__.py)
dicomweb_state = DicomWebSourceStateCRUD()
