# app/crud/crud_dimse_listener_state.py
import logging
from typing import Optional, Type, List # <-- Add List import
from datetime import datetime, timezone

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.db import models
# Schemas are not strictly needed here unless you adopt CRUDBase with schemas
# from app.schemas.system import DimseListenerStatusUpdate # Example if using schemas

logger = logging.getLogger(__name__)

# Define the CRUD Class wrapping the logic
class CRUDDimseListenerState:

    def get_listener_state(self, db: Session, *, listener_id: str) -> Optional[models.DimseListenerState]:
        """Gets the current state for a specific DIMSE listener ID."""
        return db.query(models.DimseListenerState).filter(models.DimseListenerState.listener_id == listener_id).first()

    # --- ADDED get_all_states METHOD ---
    def get_all_states(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[models.DimseListenerState]:
        """Retrieves all DIMSE listener state records, ordered by listener_id."""
        return (
            db.query(models.DimseListenerState)
            .order_by(models.DimseListenerState.listener_id)
            .offset(skip)
            .limit(limit)
            .all()
        )
    # --- END ADDED get_all_states METHOD ---

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
        Updates the state of a DIMSE listener, creating it if it doesn't exist.
        Commit needs to be handled by the caller.
        """
        # Use self.get_listener_state to call the method within the class
        db_obj = self.get_listener_state(db, listener_id=listener_id)
        now = datetime.now(timezone.utc)

        if db_obj:
            logger.debug(f"Updating DIMSE listener state for '{listener_id}': status='{status}'")
            db_obj.status = status
            db_obj.status_message = status_message
            if host is not None: db_obj.host = host
            if port is not None: db_obj.port = port
            if ae_title is not None: db_obj.ae_title = ae_title
            db_obj.last_heartbeat = now
            # No db.add(db_obj) needed here, SQLAlchemy tracks changes
        else:
            logger.info(f"Creating new DIMSE listener state for '{listener_id}': status='{status}'")
            db_obj = models.DimseListenerState(
                listener_id=listener_id,
                status=status,
                status_message=status_message,
                host=host,
                port=port,
                ae_title=ae_title,
                last_heartbeat=now,
                created_at=now
            )
            db.add(db_obj)
            db.flush() # Flush to assign defaults and get the object ready in session

        # Commit should happen outside this function
        # db.commit()
        db.refresh(db_obj) # Refresh to load any DB-generated values after flush/commit
        return db_obj

    def update_listener_heartbeat(self, db: Session, *, listener_id: str) -> Optional[models.DimseListenerState]:
        """
        Specifically updates only the heartbeat timestamp for a DIMSE listener.
        Commit needs to be handled by the caller.
        """
        db_obj = self.get_listener_state(db, listener_id=listener_id)
        if db_obj:
            logger.debug(f"Updating DIMSE listener heartbeat for '{listener_id}'")
            db_obj.last_heartbeat = datetime.now(timezone.utc)
            db.flush() # Ensure change is flushed before potential commit outside
            # db.commit() # Commit handled by caller
            db.refresh(db_obj)
            return db_obj
        else:
            logger.warning(f"Cannot update heartbeat for non-existent listener_id: '{listener_id}'")
            return None

# --- Create an instance for export ---
# This instance name matches what __init__.py tries to import
crud_dimse_listener_state = CRUDDimseListenerState()
