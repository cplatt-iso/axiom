# app/crud/crud_dimse_listener_config.py
import logging
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status

# Import the model and schemas
from app.db import models
from app.schemas import dimse_listener_config as schemas_dimse

logger = logging.getLogger(__name__)

class CRUDDimseListenerConfig:
    """
    CRUD operations for DIMSE Listener configurations.
    Interacts with the DimseListenerConfig model.
    """

    def get(self, db: Session, id: int) -> Optional[models.DimseListenerConfig]:
        """Get a DIMSE listener configuration by its ID."""
        logger.debug(f"Querying DIMSE listener config by ID: {id}")
        result = db.get(models.DimseListenerConfig, id)
        if result:
            logger.debug(f"Found DIMSE listener config ID: {id}, Name: {result.name}")
        else:
            logger.debug(f"No DIMSE listener config found with ID: {id}")
        return result

    def get_by_name(self, db: Session, name: str) -> Optional[models.DimseListenerConfig]:
        """Get a DIMSE listener configuration by its unique name."""
        logger.debug(f"Querying DIMSE listener config by name: {name}")
        statement = select(models.DimseListenerConfig).where(models.DimseListenerConfig.name == name)
        result = db.execute(statement).scalar_one_or_none()
        if result:
            logger.debug(f"Found DIMSE listener config Name: {name}, ID: {result.id}")
        else:
            logger.debug(f"No DIMSE listener config found with Name: {name}")
        return result

    def get_by_instance_id(self, db: Session, instance_id: str) -> Optional[models.DimseListenerConfig]:
        """Get a DIMSE listener configuration by its unique instance_id."""
        logger.debug(f"Querying DIMSE listener config by instance_id: {instance_id}")
        statement = select(models.DimseListenerConfig).where(models.DimseListenerConfig.instance_id == instance_id)
        result = db.execute(statement).scalar_one_or_none()
        if result:
            logger.debug(f"Found DIMSE listener config for instance_id: {instance_id}, ID: {result.id}")
        else:
            logger.debug(f"No DIMSE listener config found for instance_id: {instance_id}")
        return result

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[models.DimseListenerConfig]:
        """Retrieve multiple DIMSE listener configurations."""
        logger.debug(f"Querying multiple DIMSE listener configs (skip={skip}, limit={limit})")
        statement = (
            select(models.DimseListenerConfig)
            .order_by(models.DimseListenerConfig.name) # Order by name
            .offset(skip)
            .limit(limit)
        )
        results = list(db.execute(statement).scalars().all())
        logger.debug(f"Found {len(results)} DIMSE listener configs.")
        return results

    def create(self, db: Session, *, obj_in: schemas_dimse.DimseListenerConfigCreate) -> models.DimseListenerConfig:
        """Create a new DIMSE listener configuration."""
        logger.info(f"Attempting to create DIMSE listener config with name: {obj_in.name}")

        # Check constraints before attempting insert
        if self.get_by_name(db, name=obj_in.name):
            logger.warning(f"DIMSE listener config creation failed: Name '{obj_in.name}' already exists.")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists.")
        if obj_in.instance_id and self.get_by_instance_id(db, instance_id=obj_in.instance_id):
            logger.warning(f"DIMSE listener config creation failed: Instance ID '{obj_in.instance_id}' is already assigned.")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Instance ID '{obj_in.instance_id}' is already assigned.")
        # Optional: Check AE Title / Port uniqueness if constraint added to model
        # existing_ae_port = db.execute(select(models.DimseListenerConfig).where(models.DimseListenerConfig.ae_title == obj_in.ae_title, models.DimseListenerConfig.port == obj_in.port)).scalar_one_or_none()
        # if existing_ae_port:
        #     raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Combination of AE Title '{obj_in.ae_title}' and Port '{obj_in.port}' already exists.")

        try:
            db_obj = models.DimseListenerConfig(**obj_in.model_dump())
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully created DIMSE listener config ID: {db_obj.id}, Name: {db_obj.name}")
            return db_obj
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Database integrity error during DIMSE listener creation for '{obj_in.name}': {e}", exc_info=True)
            # Check specific constraint violations if needed (more robust than string matching)
            if "uq_dimse_listener_configs_name" in str(e.orig):
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists (commit conflict).")
            if "uq_dimse_listener_configs_instance_id" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Instance ID '{obj_in.instance_id}' is already assigned (commit conflict).")
            # if "uq_aetitle_port" in str(e.orig): # If constraint was added
            #     raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"AE Title/Port combination already exists (commit conflict).")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error creating listener '{obj_in.name}'.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error during DIMSE listener creation for '{obj_in.name}': {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error creating listener '{obj_in.name}'.")

    def update(
        self,
        db: Session,
        *,
        db_obj: models.DimseListenerConfig,
        obj_in: Union[schemas_dimse.DimseListenerConfigUpdate, Dict[str, Any]]
    ) -> models.DimseListenerConfig:
        """Update an existing DIMSE listener configuration."""
        logger.info(f"Attempting to update DIMSE listener config ID: {db_obj.id}, Name: {db_obj.name}")

        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True) # Only update fields provided

        if not update_data:
             logger.warning(f"Update request for DIMSE listener config ID {db_obj.id} contained no data.")
             return db_obj # Return unchanged object

        # Check for potential conflicts before applying updates
        if "name" in update_data and update_data["name"] != db_obj.name:
            existing_name = self.get_by_name(db, name=update_data["name"])
            if existing_name and existing_name.id != db_obj.id:
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists.")
        if "instance_id" in update_data and update_data["instance_id"] != db_obj.instance_id:
             if update_data["instance_id"] is not None: # Check only if setting a new non-null ID
                 existing_instance = self.get_by_instance_id(db, instance_id=update_data["instance_id"])
                 if existing_instance and existing_instance.id != db_obj.id:
                     raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Instance ID '{update_data['instance_id']}' is already assigned.")
        # Optional: Check AE Title / Port uniqueness if constraint added to model and fields are changing

        logger.debug(f"Updating fields for DIMSE listener ID {db_obj.id}: {list(update_data.keys())}")
        for field, value in update_data.items():
            if hasattr(db_obj, field):
                setattr(db_obj, field, value)
            else:
                 logger.warning(f"Attempted to update non-existent field '{field}' on DimseListenerConfig for ID {db_obj.id}. Skipping.")

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully updated DIMSE listener config ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
             db.rollback()
             logger.error(f"Database integrity error during DIMSE listener update for ID {db_obj.id}: {e}", exc_info=True)
             # Handle potential unique constraint violations on commit
             if "name" in update_data and "uq_dimse_listener_configs_name" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists (commit conflict).")
             if "instance_id" in update_data and "uq_dimse_listener_configs_instance_id" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Instance ID '{update_data['instance_id']}' is already assigned (commit conflict).")
             # Add AE Title/Port check if applicable
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error updating listener ID {db_obj.id}.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error during DIMSE listener update for ID {db_obj.id}: {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error updating listener ID {db_obj.id}.")


    def remove(self, db: Session, *, id: int) -> models.DimseListenerConfig:
        """Delete a DIMSE listener configuration by ID."""
        logger.info(f"Attempting to delete DIMSE listener config ID: {id}")
        db_obj = self.get(db, id=id)
        if not db_obj:
            logger.warning(f"DIMSE listener config deletion failed: No config found with ID: {id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"DIMSE listener config with ID {id} not found")

        deleted_name = db_obj.name
        db.delete(db_obj)
        try:
            db.commit()
            logger.info(f"Successfully deleted DIMSE listener config ID: {id}, Name: {deleted_name}")
            # Return the deleted object (transient state) as confirmation
            return db_obj
        except Exception as e:
            logger.error(f"Database error during DIMSE listener deletion for ID {id}: {e}", exc_info=True)
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error occurred while deleting listener config ID {id}.",
            )

# Create a singleton instance of the CRUD class
crud_dimse_listener_config = CRUDDimseListenerConfig()
