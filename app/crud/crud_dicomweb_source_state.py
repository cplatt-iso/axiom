# app/crud/crud_dicomweb_source_state.py

from typing import List, Optional
from datetime import datetime
import logging
from sqlalchemy.orm import Session
from sqlalchemy import update as sql_update, select

# F is NOT imported

from app.db.models import DicomWebSourceState # Import the model

logger = logging.getLogger(__name__) # Setup logger

# --- Standalone Functions (Original Structure) ---

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

def get_all_states(db: Session, *, skip: int = 0, limit: int = 100) -> List[DicomWebSourceState]:
    """Retrieves the state for all configured DICOMweb sources."""
    logger.debug(f"Querying all DICOMweb source states (skip={skip}, limit={limit})")
    statement = (
        select(DicomWebSourceState)
        .order_by(DicomWebSourceState.source_name)
        .offset(skip)
        .limit(limit)
    )
    results = list(db.execute(statement).scalars().all())
    logger.debug(f"Found {len(results)} DICOMweb source states.")
    return results

def create_or_update_state(db: Session, source_name: str, **kwargs) -> DicomWebSourceState:
    """
    Creates a new state entry or updates an existing one for a source.
    Initializes state based on config if creating.
    Commit needs to be handled by the caller.
    """
    db_state = get_state(db, source_name)

    if db_state:
        # Update existing state
        logger.debug(f"Updating existing state for DICOMweb source: {source_name} with data: {kwargs}")
        update_data = {key: value for key, value in kwargs.items() if key != 'source_name'}
        if not update_data:
             logger.debug(f"No fields to update for source {source_name}, returning existing state.")
             return db_state # Return early if no actual updates

        for field, value in update_data.items():
            setattr(db_state, field, value)
        db.flush() # Flush changes to session before refresh
        db.refresh(db_state)
        logger.debug(f"State updated for source: {source_name}")

    else:
        # Create new state
        logger.info(f"Creating new state for DICOMweb source: {source_name} with data: {kwargs}")
        if 'is_enabled' not in kwargs:
            kwargs['is_enabled'] = True # Default to enabled if creating without explicit value

        db_state = DicomWebSourceState(source_name=source_name, **kwargs)
        db.add(db_state)
        db.flush() # Assign generated defaults if any, make object available in session
        db.refresh(db_state) # Ensure we have the potentially new object state with defaults
        logger.info(f"New state created for source: {source_name}")

    # Commit should happen outside this function
    return db_state

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
        update_values["last_error_message"] = None
        update_values["last_error_run"] = None
    if last_error_run is not None:
        update_values["last_error_run"] = last_error_run
    if last_error_message is not None:
        if last_error_run is not None:
            update_values["last_error_message"] = last_error_message
        else:
            logger.warning(f"Attempted to set last_error_message for {source_name} without setting last_error_run timestamp. Skipping message update.")

    if not update_values:
        logger.debug(f"No run state fields provided to update for source {source_name}.")
        return get_state(db, source_name) # Nothing to update

    logger.debug(f"Updating run state for DICOMweb source: {source_name} with values: {update_values}")
    update_stmt = (
        sql_update(DicomWebSourceState)
        .where(DicomWebSourceState.source_name == source_name)
        .values(**update_values)
        .execution_options(synchronize_session=False)
    )
    result = db.execute(update_stmt)

    if result.rowcount > 0:
         logger.debug(f"Run state updated successfully for {source_name} (rows affected: {result.rowcount}).")
         return None # Indicate success without refreshed object
    else:
         logger.warning(f"Run state update failed for {source_name} (source not found?).")
         return None

def delete_state(db: Session, source_name: str) -> bool:
    """Deletes the state for a specific DICOMweb source. Commit handled by caller."""
    db_state = get_state(db, source_name)
    if db_state:
        logger.info(f"Deleting state for DICOMweb source: {source_name}")
        db.delete(db_state)
        db.flush()
        return True
    logger.warning(f"Could not delete state for DICOMweb source: {source_name} (not found).")
    return False

# --- CRUD Class Wrapper ---
class DicomWebSourceStateCRUD:
    def get(self, db: Session, source_name: str) -> Optional[DicomWebSourceState]:
        return get_state(db, source_name)

    def get_all(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[DicomWebSourceState]:
        return get_all_states(db=db, skip=skip, limit=limit)

    def create_or_update(self, db: Session, source_name: str, **kwargs) -> DicomWebSourceState:
        return create_or_update_state(db, source_name, **kwargs)

    def update_run_status(self, db: Session, source_name: str, **kwargs) -> Optional[DicomWebSourceState]:
        return update_run_state(db, source_name, **kwargs)

    def delete(self, db: Session, source_name: str) -> bool:
        return delete_state(db, source_name)

    # --- CORRECTED Increment Methods (No F Import) ---
    def increment_found_count(self, db: Session, *, source_name: str, count: int = 1) -> bool:
        """Atomically increments the found instance count for a source."""
        if count <= 0: return False
        logger.debug(f"Incrementing found count for source '{source_name}' by {count}")
        stmt = (
            sql_update(DicomWebSourceState)
            .where(DicomWebSourceState.source_name == source_name)
             # FINAL CORRECT SYNTAX: {ColumnObject: ColumnObject + value}
            .values({
                DicomWebSourceState.found_instance_count: DicomWebSourceState.found_instance_count + count
             })
            .execution_options(synchronize_session=False)
        )
        try:
            result = db.execute(stmt)
            if result.rowcount > 0: return True
            else: logger.warning(f"Failed increment found count source '{source_name}'."); return False
        except Exception as e: logger.error(f"DB error incrementing found count source '{source_name}': {e}", exc_info=True); return False

    def increment_queued_count(self, db: Session, *, source_name: str, count: int = 1) -> bool:
        """Atomically increments the queued instance count for a source."""
        if count <= 0: return False
        logger.debug(f"Incrementing queued count for source '{source_name}' by {count}")
        stmt = (
            sql_update(DicomWebSourceState)
            .where(DicomWebSourceState.source_name == source_name)
            # FINAL CORRECT SYNTAX: {ColumnObject: ColumnObject + value}
            .values({
                DicomWebSourceState.queued_instance_count: DicomWebSourceState.queued_instance_count + count
             })
            .execution_options(synchronize_session=False)
        )
        try:
            result = db.execute(stmt)
            if result.rowcount > 0: return True
            else: logger.warning(f"Failed increment queued count source '{source_name}'."); return False
        except Exception as e: logger.error(f"DB error incrementing queued count source '{source_name}': {e}", exc_info=True); return False

    def increment_processed_count(self, db: Session, *, source_name: str, count: int = 1) -> bool:
        """Atomically increments the processed instance count for a source."""
        if count <= 0: return False
        logger.debug(f"Incrementing processed count for source '{source_name}' by {count}")
        stmt = (
            sql_update(DicomWebSourceState)
            .where(DicomWebSourceState.source_name == source_name)
            .values({
                DicomWebSourceState.processed_instance_count: DicomWebSourceState.processed_instance_count + count
             })
            .execution_options(synchronize_session=False)
        )
        try:
            result = db.execute(stmt)
            if result.rowcount > 0: return True
            else: logger.warning(f"Failed increment processed count source '{source_name}'."); return False
        except Exception as e: logger.error(f"DB error incrementing processed count source '{source_name}': {e}", exc_info=True); return False

# Export the instance
dicomweb_state = DicomWebSourceStateCRUD()
