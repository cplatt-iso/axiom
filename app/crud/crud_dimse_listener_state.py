# app/crud/crud_dimse_listener_state.py
import logging
from typing import Optional, List
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import select # Use select for querying

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

        Args:
            db: The database session.
            listener_id: The unique identifier of the listener instance.

        Returns:
            The DimseListenerState model instance if found, otherwise None.
        """
        statement = select(models.DimseListenerState).where(models.DimseListenerState.listener_id == listener_id)
        result = db.execute(statement).scalar_one_or_none()
        return result

    def get_all_listener_states(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[models.DimseListenerState]:
        """
        Retrieves multiple DIMSE listener state records with pagination.

        Args:
            db: The database session.
            skip: Number of records to skip.
            limit: Maximum number of records to return.

        Returns:
            A list of DimseListenerState model instances.
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
        Manages the last_heartbeat timestamp automatically.
        Requires the caller to commit the transaction.

        Args:
            db: The database session.
            listener_id: The unique ID of the listener.
            status: The current status string (e.g., 'running', 'stopped', 'error').
            status_message: An optional message providing more detail.
            host: The hostname/IP the listener is bound to (optional update).
            port: The port the listener is using (optional update).
            ae_title: The AE Title of the listener (optional update).

        Returns:
            The updated or newly created DimseListenerState model instance.
        """
        # Attempt to get the existing record for this listener ID
        db_obj = self.get_listener_state(db, listener_id=listener_id)
        now_utc = datetime.now(timezone.utc) # Still needed for created_at

        if db_obj:
            # Update existing record
            logger.debug(f"Updating DIMSE listener state for '{listener_id}': status='{status}'")
            db_obj.status = status
            # Only update message if provided, otherwise keep existing
            if status_message is not None:
                db_obj.status_message = status_message
            # Update other fields if they are provided
            if host is not None: db_obj.host = host
            if port is not None: db_obj.port = port
            if ae_title is not None: db_obj.ae_title = ae_title
            # --- REMOVE MANUAL HEARTBEAT UPDATE ---
            # db_obj.last_heartbeat = now_utc # REMOVE THIS LINE - rely on onupdate=func.now()
            # --- END REMOVE ---
            db.add(db_obj) # Ensure object is marked dirty
            db.flush() # Flush to potentially trigger onupdate before refresh
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
                # Let server_default handle created_at and initial last_heartbeat
                # last_heartbeat=now_utc, # Let server_default handle initial
                # created_at=now_utc # Let server_default handle
            )
            db.add(db_obj)
            db.flush() # Flush to get the object ready in the session, assign defaults

        # Refresh the object to get the latest state from the DB after flush/commit
        # Note: Commit must happen outside this function.
        db.refresh(db_obj)
        return db_obj

    def update_listener_heartbeat(self, db: Session, *, listener_id: str) -> Optional[models.DimseListenerState]:
        """
        Specifically updates only the 'last_heartbeat' timestamp for a listener
        using a direct SQL UPDATE for reliability with onupdate triggers.
        Useful for periodic updates when the status hasn't changed.
        Requires the caller to commit the transaction.

        Args:
            db: The database session.
            listener_id: The unique ID of the listener.

        Returns:
            The potentially updated DimseListenerState model instance if found (state might be stale until commit), otherwise None.
        """
        logger.debug(f"Updating DIMSE listener heartbeat for '{listener_id}' using direct SQL UPDATE.")
        # --- USE DIRECT SQL UPDATE ---
        from sqlalchemy import update as sql_update, func # Import func for now()

        stmt = (
            sql_update(models.DimseListenerState)
            .where(models.DimseListenerState.listener_id == listener_id)
            .values(last_heartbeat=func.now()) # Use database's NOW() function
            .execution_options(synchronize_session=False) # Don't sync session immediately
        )
        result = db.execute(stmt)

        if result.rowcount == 0:
            logger.warning(f"Cannot update heartbeat for non-existent listener_id: '{listener_id}'")
            return None
        else:
            # Don't flush/refresh here as it might not reflect the DB state until commit
            # Return the existing object from the session, but acknowledge it might be slightly stale
            # Or, we could just return True/False indicating if the UPDATE statement affected rows.
            # Let's return the object from the session as it might be useful, but caller beware of staleness pre-commit.
            # Fetching it again would be safest post-commit by the caller.
            db_obj = self.get_listener_state(db, listener_id=listener_id) # Get possibly stale session object
            return db_obj
        # --- END DIRECT SQL UPDATE ---

# Create a singleton instance of the CRUD class for easy import and use
crud_dimse_listener_state = CRUDDimseListenerState()
