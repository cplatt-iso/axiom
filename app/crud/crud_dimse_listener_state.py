# app/crud/crud_dimse_listener_state.py
import logging
from typing import Optional, List
from datetime import datetime, timezone

from sqlalchemy.orm import Session
# Correct Imports
from sqlalchemy import select, update as sql_update, func
# F is NOT imported

from app.db import models

logger = logging.getLogger(__name__)

class CRUDDimseListenerState:
    """
    CRUD operations for managing the state of DIMSE listener instances
    recorded in the database.
    """

    def get_listener_state(self, db: Session, *, listener_id: str) -> Optional[models.DimseListenerState]:
        """
        Retrieves the current state record for a specific DIMSE listener by its unique ID.
        """
        statement = select(models.DimseListenerState).where(models.DimseListenerState.listener_id == listener_id)
        result = db.execute(statement).scalar_one_or_none()
        return result

    def get_all_listener_states(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[models.DimseListenerState]:
        """
        Retrieves multiple DIMSE listener state records with pagination.
        """
        logger.debug(f"Querying all DIMSE listener states (skip={skip}, limit={limit})")
        statement = (
            select(models.DimseListenerState)
            .order_by(models.DimseListenerState.listener_id) # Order for consistency
            .offset(skip)
            .limit(limit)
        )
        results = list(db.execute(statement).scalars().all())
        logger.debug(f"Found {len(results)} DIMSE listener states.")
        return results

    def update_listener_state(
        self,
        db: Session, *,
        listener_id: str,
        status: str,
        status_message: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        ae_title: Optional[str] = None,
    ) -> models.DimseListenerState:
        """
        Updates the state of a DIMSE listener in the database.
        If the listener ID does not exist, a new record is created.
        Requires the caller to commit the transaction.
        """
        db_obj = self.get_listener_state(db, listener_id=listener_id)

        if db_obj:
            # Update existing record
            logger.debug(f"Updating DIMSE listener state for '{listener_id}': status='{status}'")
            db_obj.status = status
            if status_message is not None:
                db_obj.status_message = status_message
            if host is not None:
                db_obj.host = host
            if port is not None:
                db_obj.port = port
            if ae_title is not None:
                db_obj.ae_title = ae_title
            # last_heartbeat is handled by onupdate or update_listener_heartbeat
            db.add(db_obj) # Mark dirty
            db.flush() # Flush to trigger onupdate (if applicable for this operation)
        else:
            # Create new record if it doesn't exist
            logger.info(f"Creating new DIMSE listener state for '{listener_id}': status='{status}'")
            db_obj = models.DimseListenerState(
                listener_id=listener_id,
                status=status,
                status_message=status_message,
                host=host,
                port=port,
                ae_title=ae_title,
            )
            db.add(db_obj)
            db.flush() # Flush to get the object ready

        db.refresh(db_obj) # Refresh to get DB defaults/updates
        return db_obj

    def update_listener_heartbeat(self, db: Session, *, listener_id: str) -> Optional[models.DimseListenerState]:
        """
        Specifically updates only the 'last_heartbeat' timestamp using direct SQL UPDATE.
        Requires the caller to commit the transaction.
        """
        logger.debug(f"Updating DIMSE listener heartbeat for '{listener_id}' using direct SQL UPDATE.")
        # func import needs to be available
        stmt = (
            sql_update(models.DimseListenerState)
            .where(models.DimseListenerState.listener_id == listener_id)
            .values(last_heartbeat=func.now()) # Use database's NOW() function
            .execution_options(synchronize_session=False)
        )
        result = db.execute(stmt)

        if result.rowcount == 0:
            logger.warning(f"Cannot update heartbeat for non-existent listener_id: '{listener_id}'")
            return None
        else:
            # Re-fetch after commit by caller for guaranteed freshness,
            # or return possibly stale session obj
            db_obj = self.get_listener_state(db, listener_id=listener_id)
            return db_obj

    def increment_received_count(self, db: Session, *, listener_id: str, count: int = 1) -> bool:
        """Atomically increments the received instance count for a listener."""
        if count <= 0: return False
        logger.debug(f"Incrementing received count for listener '{listener_id}' by {count}")
        stmt = (
            sql_update(models.DimseListenerState)
            .where(models.DimseListenerState.listener_id == listener_id)
            # FINAL CORRECT SYNTAX: {ColumnObject: ColumnObject + value}
            .values({
                models.DimseListenerState.received_instance_count: models.DimseListenerState.received_instance_count + count
             })
            .execution_options(synchronize_session=False)
        )
        success = False
        try:
            result = db.execute(stmt)
            db.commit() # Commit immediately
            if result.rowcount > 0: success = True; logger.debug(f"Incremented received count for '{listener_id}'.")
            else: logger.warning(f"Failed increment received count for '{listener_id}' (not found?).")
        except Exception as e:
            logger.error(f"DB error incrementing received count for '{listener_id}': {e}", exc_info=True); db.rollback()
        return success

    def increment_processed_count(self, db: Session, *, listener_id: str, count: int = 1) -> bool:
        """Atomically increments the processed instance count for a listener."""
        if count <= 0: return False
        logger.debug(f"Incrementing processed count for listener '{listener_id}' by {count}")
        stmt = (
            sql_update(models.DimseListenerState)
            .where(models.DimseListenerState.listener_id == listener_id)
            .values({
                models.DimseListenerState.processed_instance_count: models.DimseListenerState.processed_instance_count + count
             })
            .execution_options(synchronize_session=False)
        )
        success = False
        try:
            result = db.execute(stmt)
            db.commit() # Commit immediately for listener counts
            if result.rowcount > 0: success = True; logger.debug(f"Incremented processed count for '{listener_id}'.")
            else: logger.warning(f"Failed increment processed count for '{listener_id}' (not found?).")
        except Exception as e:
            logger.error(f"DB error incrementing processed count for '{listener_id}': {e}", exc_info=True); db.rollback()
        return success

# Create a singleton instance
crud_dimse_listener_state = CRUDDimseListenerState()
