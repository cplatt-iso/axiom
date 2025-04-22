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
        now_utc = datetime.now(timezone.utc)

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
            db_obj.last_heartbeat = now_utc # Always update heartbeat on state change/update
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
                last_heartbeat=now_utc,
                created_at=now_utc # Set creation time for new records
            )
            db.add(db_obj)
            db.flush() # Flush to get the object ready in the session, potentially assign defaults

        # Refresh the object to get the latest state from the DB after flush/commit
        # Note: Commit must happen outside this function.
        db.refresh(db_obj)
        return db_obj

    def update_listener_heartbeat(self, db: Session, *, listener_id: str) -> Optional[models.DimseListenerState]:
        """
        Specifically updates only the 'last_heartbeat' timestamp for a listener.
        Useful for periodic updates when the status hasn't changed.
        Requires the caller to commit the transaction.

        Args:
            db: The database session.
            listener_id: The unique ID of the listener.

        Returns:
            The updated DimseListenerState model instance if found, otherwise None.
        """
        db_obj = self.get_listener_state(db, listener_id=listener_id)
        if db_obj:
            logger.debug(f"Updating DIMSE listener heartbeat for '{listener_id}'")
            db_obj.last_heartbeat = datetime.now(timezone.utc)
            db.flush() # Ensure change is ready to be committed
            db.refresh(db_obj)
            return db_obj
        else:
            logger.warning(f"Cannot update heartbeat for non-existent listener_id: '{listener_id}'")
            return None

# Create a singleton instance of the CRUD class for easy import and use
crud_dimse_listener_state = CRUDDimseListenerState()
