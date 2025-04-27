# app/crud/crud_schedule.py
import logging
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status

# Import the model and schemas
from app.db import models
from app.schemas import schedule as schemas_schedule # Use alias

logger = logging.getLogger(__name__)

class CRUDSchedule:
    """
    CRUD operations for Schedule configurations.
    Interacts with the Schedule model.
    """

    def get(self, db: Session, id: int) -> Optional[models.Schedule]:
        """Get a schedule by its ID."""
        logger.debug(f"Querying schedule by ID: {id}")
        result = db.get(models.Schedule, id)
        if result:
            logger.debug(f"Found schedule ID: {id}, Name: {result.name}")
        else:
            logger.debug(f"No schedule found with ID: {id}")
        return result

    def get_by_name(self, db: Session, name: str) -> Optional[models.Schedule]:
        """Get a schedule by its unique name."""
        logger.debug(f"Querying schedule by name: {name}")
        statement = select(models.Schedule).where(models.Schedule.name == name)
        result = db.execute(statement).scalar_one_or_none()
        if result:
            logger.debug(f"Found schedule Name: {name}, ID: {result.id}")
        else:
            logger.debug(f"No schedule found with Name: {name}")
        return result

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[models.Schedule]:
        """Retrieve multiple schedules."""
        logger.debug(f"Querying multiple schedules (skip={skip}, limit={limit})")
        statement = (
            select(models.Schedule)
            .order_by(models.Schedule.name) # Order by name
            .offset(skip)
            .limit(limit)
        )
        results = list(db.execute(statement).scalars().all())
        logger.debug(f"Found {len(results)} schedules.")
        return results

    def create(self, db: Session, *, obj_in: schemas_schedule.ScheduleCreate) -> models.Schedule:
        """Create a new schedule."""
        logger.info(f"Attempting to create schedule with name: {obj_in.name}")

        if self.get_by_name(db, name=obj_in.name):
            logger.warning(f"Schedule creation failed: Name '{obj_in.name}' already exists.")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists.")

        try:
            # The TimeRange validation happens in the Pydantic schema
            db_obj = models.Schedule(**obj_in.model_dump())
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully created schedule ID: {db_obj.id}, Name: {db_obj.name}")
            return db_obj
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Database integrity error during schedule creation for '{obj_in.name}': {e}", exc_info=True)
            # Check specific constraint violations if needed
            if "uq_schedules_name" in str(e.orig):
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists (commit conflict).")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error creating schedule '{obj_in.name}'.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error during schedule creation for '{obj_in.name}': {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error creating schedule '{obj_in.name}'.")

    def update(
        self,
        db: Session,
        *,
        db_obj: models.Schedule,
        obj_in: Union[schemas_schedule.ScheduleUpdate, Dict[str, Any]]
    ) -> models.Schedule:
        """Update an existing schedule."""
        logger.info(f"Attempting to update schedule ID: {db_obj.id}, Name: {db_obj.name}")

        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True) # Only update fields provided

        if not update_data:
             logger.warning(f"Update request for schedule ID {db_obj.id} contained no data.")
             return db_obj # Return unchanged object

        # Check for name conflict if the name is being changed
        if "name" in update_data and update_data["name"] != db_obj.name:
            existing_name = self.get_by_name(db, name=update_data["name"])
            if existing_name and existing_name.id != db_obj.id:
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists.")

        logger.debug(f"Updating fields for schedule ID {db_obj.id}: {list(update_data.keys())}")
        for field, value in update_data.items():
            if hasattr(db_obj, field):
                setattr(db_obj, field, value)
            else:
                 logger.warning(f"Attempted to update non-existent field '{field}' on Schedule for ID {db_obj.id}. Skipping.")

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully updated schedule ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
             db.rollback()
             logger.error(f"Database integrity error during schedule update for ID {db_obj.id}: {e}", exc_info=True)
             if "name" in update_data and "uq_schedules_name" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists (commit conflict).")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error updating schedule ID {db_obj.id}.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error during schedule update for ID {db_obj.id}: {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error updating schedule ID {db_obj.id}.")


    def remove(self, db: Session, *, id: int) -> models.Schedule:
        """Delete a schedule by ID."""
        logger.info(f"Attempting to delete schedule ID: {id}")
        db_obj = self.get(db, id=id)
        if not db_obj:
            logger.warning(f"Schedule deletion failed: No schedule found with ID: {id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Schedule with ID {id} not found")

        deleted_name = db_obj.name
        db.delete(db_obj)
        try:
            db.commit()
            logger.info(f"Successfully deleted schedule ID: {id}, Name: {deleted_name}")
            # Return the deleted object (transient state) as confirmation
            return db_obj
        except Exception as e:
            # Handle potential foreign key issues if rules reference this schedule
            # and ondelete="SET NULL" is not working as expected or desired.
            logger.error(f"Database error during schedule deletion for ID {id}: {e}", exc_info=True)
            db.rollback()
            if "foreign key constraint" in str(e).lower():
                 raise HTTPException(
                     status_code=status.HTTP_409_CONFLICT, # Conflict because it's in use
                     detail=f"Cannot delete schedule ID {id} as it is still referenced by one or more rules.",
                 )
            else:
                 raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Database error occurred while deleting schedule ID {id}.",
                 )

# Create a singleton instance of the CRUD class
crud_schedule = CRUDSchedule()
