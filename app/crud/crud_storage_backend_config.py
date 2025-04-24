# app/crud/crud_storage_backend_config.py
import logging
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status

from app.db import models
from app.schemas import storage_backend_config as schemas_storage # Use alias

logger = logging.getLogger(__name__)

class CRUDStorageBackendConfig:
    """
    CRUD operations for Storage Backend configurations.
    Interacts with the StorageBackendConfig model.
    """

    def get(self, db: Session, id: int) -> Optional[models.StorageBackendConfig]:
        """Get a storage backend config by its ID."""
        logger.debug(f"Querying storage backend config by ID: {id}")
        result = db.get(models.StorageBackendConfig, id)
        if result:
            logger.debug(f"Found storage backend config ID: {id}, Name: {result.name}")
        else:
            logger.debug(f"No storage backend config found with ID: {id}")
        return result

    def get_by_name(self, db: Session, name: str) -> Optional[models.StorageBackendConfig]:
        """Get a storage backend config by its unique name."""
        logger.debug(f"Querying storage backend config by name: {name}")
        statement = select(models.StorageBackendConfig).where(models.StorageBackendConfig.name == name)
        result = db.execute(statement).scalar_one_or_none()
        if result:
            logger.debug(f"Found storage backend config Name: {name}, ID: {result.id}")
        else:
            logger.debug(f"No storage backend config found with Name: {name}")
        return result

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[models.StorageBackendConfig]:
        """Retrieve multiple storage backend configurations."""
        logger.debug(f"Querying multiple storage backend configs (skip={skip}, limit={limit})")
        statement = (
            select(models.StorageBackendConfig)
            .order_by(models.StorageBackendConfig.name) # Order by name
            .offset(skip)
            .limit(limit)
        )
        results = list(db.execute(statement).scalars().all())
        logger.debug(f"Found {len(results)} storage backend configs.")
        return results

    def create(self, db: Session, *, obj_in: schemas_storage.StorageBackendConfigCreate) -> models.StorageBackendConfig:
        """Create a new storage backend configuration."""
        logger.info(f"Attempting to create storage backend config with name: {obj_in.name}")

        if self.get_by_name(db, name=obj_in.name):
            logger.warning(f"Storage backend config creation failed: Name '{obj_in.name}' already exists.")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists.")

        try:
            db_obj = models.StorageBackendConfig(**obj_in.model_dump())
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully created storage backend config ID: {db_obj.id}, Name: {db_obj.name}")
            return db_obj
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Database integrity error during storage backend creation for '{obj_in.name}': {e}", exc_info=True)
            if "uq_storage_backend_configs_name" in str(e.orig):
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists (commit conflict).")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error creating backend '{obj_in.name}'.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error during storage backend creation for '{obj_in.name}': {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error creating backend '{obj_in.name}'.")

    def update(
        self,
        db: Session,
        *,
        db_obj: models.StorageBackendConfig,
        obj_in: Union[schemas_storage.StorageBackendConfigUpdate, Dict[str, Any]]
    ) -> models.StorageBackendConfig:
        """Update an existing storage backend configuration."""
        logger.info(f"Attempting to update storage backend config ID: {db_obj.id}, Name: {db_obj.name}")

        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)

        if not update_data:
             logger.warning(f"Update request for storage backend config ID {db_obj.id} contained no data.")
             return db_obj

        if "name" in update_data and update_data["name"] != db_obj.name:
            existing_name = self.get_by_name(db, name=update_data["name"])
            if existing_name and existing_name.id != db_obj.id:
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists.")

        logger.debug(f"Updating fields for storage backend ID {db_obj.id}: {list(update_data.keys())}")
        for field, value in update_data.items():
            if hasattr(db_obj, field):
                setattr(db_obj, field, value)
            else:
                 logger.warning(f"Attempted to update non-existent field '{field}' on StorageBackendConfig for ID {db_obj.id}. Skipping.")

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully updated storage backend config ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
             db.rollback()
             logger.error(f"Database integrity error during storage backend update for ID {db_obj.id}: {e}", exc_info=True)
             if "name" in update_data and "uq_storage_backend_configs_name" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists (commit conflict).")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error updating backend ID {db_obj.id}.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error during storage backend update for ID {db_obj.id}: {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error updating backend ID {db_obj.id}.")


    def remove(self, db: Session, *, id: int) -> models.StorageBackendConfig:
        """Delete a storage backend configuration by ID."""
        logger.info(f"Attempting to delete storage backend config ID: {id}")
        db_obj = self.get(db, id=id)
        if not db_obj:
            logger.warning(f"Storage backend config deletion failed: No config found with ID: {id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Storage backend config with ID {id} not found")

        deleted_name = db_obj.name
        db.delete(db_obj)
        try:
            db.commit()
            logger.info(f"Successfully deleted storage backend config ID: {id}, Name: {deleted_name}")
            return db_obj
        except Exception as e:
            logger.error(f"Database error during storage backend deletion for ID {id}: {e}", exc_info=True)
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error occurred while deleting backend config ID {id}.",
            )

# Create a singleton instance of the CRUD class
crud_storage_backend_config = CRUDStorageBackendConfig()
