# app/crud/crud_crosswalk.py
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime # Import datetime for type hints

from sqlalchemy.orm import Session, joinedload, selectinload
# --- ADDED func import ---
from sqlalchemy import select, update as sql_update, func
# --- END ADDED ---
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status
from pydantic import SecretStr # Keep SecretStr if used elsewhere potentially

from app.db import models
from app.schemas import crosswalk as schemas_crosswalk

logger = logging.getLogger(__name__)

# --- CRUD for CrosswalkDataSource ---
class CRUDCrosswalkDataSource:

    def get(self, db: Session, id: int) -> Optional[models.CrosswalkDataSource]:
        return db.get(models.CrosswalkDataSource, id)

    def get_by_name(self, db: Session, name: str) -> Optional[models.CrosswalkDataSource]:
        stmt = select(models.CrosswalkDataSource).where(models.CrosswalkDataSource.name == name)
        return db.execute(stmt).scalar_one_or_none()

    def get_multi(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[models.CrosswalkDataSource]:
        stmt = select(models.CrosswalkDataSource).order_by(models.CrosswalkDataSource.name).offset(skip).limit(limit)
        return list(db.execute(stmt).scalars().all())

    def get_enabled_sources(self, db: Session) -> List[models.CrosswalkDataSource]:
        stmt = select(models.CrosswalkDataSource).where(models.CrosswalkDataSource.is_enabled == True).order_by(models.CrosswalkDataSource.name)
        return list(db.execute(stmt).scalars().all())

    def create(self, db: Session, *, obj_in: schemas_crosswalk.CrosswalkDataSourceCreate) -> models.CrosswalkDataSource:
        logger.info(f"Creating Crosswalk Data Source: {obj_in.name}")
        if self.get_by_name(db, name=obj_in.name):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists.")

        try:
            db_obj = models.CrosswalkDataSource(**obj_in.model_dump())
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Created Crosswalk Data Source ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
            db.rollback()
            logger.error(f"DB integrity error creating source '{obj_in.name}': {e}", exc_info=True)
            if "uq_crosswalk_data_sources_name" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' conflict.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error creating source '{obj_in.name}'.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error creating source '{obj_in.name}': {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error creating source '{obj_in.name}'.")

    def update(self, db: Session, *, db_obj: models.CrosswalkDataSource, obj_in: Union[schemas_crosswalk.CrosswalkDataSourceUpdate, Dict[str, Any]]) -> models.CrosswalkDataSource:
        logger.info(f"Updating Crosswalk Data Source ID: {db_obj.id}")
        update_data = obj_in if isinstance(obj_in, dict) else obj_in.model_dump(exclude_unset=True)

        if not update_data:
            logger.warning(f"No update data provided for source ID {db_obj.id}.")
            return db_obj

        if "name" in update_data and update_data["name"] != db_obj.name:
            if self.get_by_name(db, name=update_data["name"]):
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists.")

        logger.debug(f"Updating fields for source ID {db_obj.id}: {list(update_data.keys())}")
        for field, value in update_data.items():
            setattr(db_obj, field, value)

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Updated Crosswalk Data Source ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
             db.rollback()
             logger.error(f"DB integrity error updating source ID {db_obj.id}: {e}", exc_info=True)
             if "name" in update_data and "uq_crosswalk_data_sources_name" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' conflict.")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error updating source ID {db_obj.id}.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error updating source ID {db_obj.id}: {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error updating source ID {db_obj.id}.")

    def remove(self, db: Session, *, id: int) -> models.CrosswalkDataSource:
        logger.info(f"Deleting Crosswalk Data Source ID: {id}")
        db_obj = self.get(db, id=id)
        if not db_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Source ID {id} not found")
        name = db_obj.name
        db.delete(db_obj)
        try:
            db.commit()
            logger.info(f"Deleted Crosswalk Data Source ID: {id}, Name: {name}")
            return db_obj
        except Exception as e:
            logger.error(f"DB error deleting source ID {id}: {e}", exc_info=True); db.rollback()
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error deleting source ID {id}.")

    # --- Status update methods ---
    def update_sync_status(self, db: Session, *, source_id: int, status: models.CrosswalkSyncStatus, error_message: Optional[str] = None, row_count: Optional[int] = None) -> bool:
        logger.debug(f"Updating sync status for source ID {source_id} to {status.value}")
        values_to_update: Dict[str, Any] = { # Explicit type hint
            "last_sync_status": status,
            "last_sync_time": func.now(), # Use the imported func
            "last_sync_error": error_message,
            "last_sync_row_count": row_count
        }
        if status == models.CrosswalkSyncStatus.SUCCESS:
            values_to_update["last_sync_error"] = None
        elif status != models.CrosswalkSyncStatus.FAILED:
             values_to_update["last_sync_error"] = None

        if status != models.CrosswalkSyncStatus.SUCCESS:
             values_to_update["last_sync_row_count"] = None

        # Remove keys with None values before executing update
        values_to_update = {k: v for k, v in values_to_update.items() if v is not None}

        if not values_to_update:
            logger.warning(f"No values to update for sync status of source ID {source_id}.")
            return False

        stmt = (
            sql_update(models.CrosswalkDataSource)
            .where(models.CrosswalkDataSource.id == source_id)
            .values(**values_to_update)
            .execution_options(synchronize_session=False)
        )
        success = False
        try:
            result = db.execute(stmt)
            # Commit status updates immediately
            db.commit()
            if result.rowcount > 0:
                logger.debug(f"Sync status updated for source ID {source_id}.")
                success = True
            else:
                logger.warning(f"Sync status update failed for source ID {source_id} (not found?).")
        except Exception as e:
            logger.error(f"DB error updating sync status for source ID {source_id}: {e}", exc_info=True)
            db.rollback() # Rollback on error
        return success

# --- CRUD for CrosswalkMap (remains the same) ---
class CRUDCrosswalkMap:

    def get(self, db: Session, id: int) -> Optional[models.CrosswalkMap]:
        return db.query(models.CrosswalkMap).options(joinedload(models.CrosswalkMap.data_source)).filter(models.CrosswalkMap.id == id).first()

    def get_by_name(self, db: Session, name: str) -> Optional[models.CrosswalkMap]:
        stmt = select(models.CrosswalkMap).where(models.CrosswalkMap.name == name)
        return db.execute(stmt).scalar_one_or_none()

    def get_multi(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[models.CrosswalkMap]:
        stmt = select(models.CrosswalkMap).options(joinedload(models.CrosswalkMap.data_source)).order_by(models.CrosswalkMap.name).offset(skip).limit(limit)
        return list(db.execute(stmt).scalars().unique().all())

    def get_by_data_source(self, db: Session, *, data_source_id: int, skip: int = 0, limit: int = 100) -> List[models.CrosswalkMap]:
         stmt = select(models.CrosswalkMap).where(models.CrosswalkMap.data_source_id == data_source_id).options(joinedload(models.CrosswalkMap.data_source)).order_by(models.CrosswalkMap.name).offset(skip).limit(limit)
         return list(db.execute(stmt).scalars().unique().all())


    def create(self, db: Session, *, obj_in: schemas_crosswalk.CrosswalkMapCreate) -> models.CrosswalkMap:
        logger.info(f"Creating Crosswalk Map: {obj_in.name}")
        if self.get_by_name(db, name=obj_in.name):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Map name '{obj_in.name}' already exists.")

        data_source = db.get(models.CrosswalkDataSource, obj_in.data_source_id)
        if not data_source:
             raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Data Source ID {obj_in.data_source_id} not found.")

        try:
            db_obj = models.CrosswalkMap(**obj_in.model_dump())
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            db.refresh(db_obj, attribute_names=['data_source'])
            logger.info(f"Created Crosswalk Map ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
            db.rollback()
            logger.error(f"DB integrity error creating map '{obj_in.name}': {e}", exc_info=True)
            if "uq_crosswalk_maps_name" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Map name '{obj_in.name}' conflict.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error creating map '{obj_in.name}'.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error creating map '{obj_in.name}': {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error creating map '{obj_in.name}'.")


    def update(self, db: Session, *, db_obj: models.CrosswalkMap, obj_in: Union[schemas_crosswalk.CrosswalkMapUpdate, Dict[str, Any]]) -> models.CrosswalkMap:
        logger.info(f"Updating Crosswalk Map ID: {db_obj.id}")
        update_data = obj_in if isinstance(obj_in, dict) else obj_in.model_dump(exclude_unset=True)

        if not update_data:
            logger.warning(f"No update data provided for map ID {db_obj.id}.")
            return db_obj

        if "name" in update_data and update_data["name"] != db_obj.name:
            if self.get_by_name(db, name=update_data["name"]):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Map name '{update_data['name']}' already exists.")

        if "data_source_id" in update_data and update_data["data_source_id"] != db_obj.data_source_id:
             data_source = db.get(models.CrosswalkDataSource, update_data["data_source_id"])
             if not data_source:
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"New Data Source ID {update_data['data_source_id']} not found.")

        logger.debug(f"Updating fields for map ID {db_obj.id}: {list(update_data.keys())}")
        for field, value in update_data.items():
            setattr(db_obj, field, value)

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            db.refresh(db_obj, attribute_names=['data_source'])
            logger.info(f"Updated Crosswalk Map ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
             db.rollback()
             logger.error(f"DB integrity error updating map ID {db_obj.id}: {e}", exc_info=True)
             if "name" in update_data and "uq_crosswalk_maps_name" in str(e.orig):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Map name '{update_data['name']}' conflict.")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error updating map ID {db_obj.id}.")
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error updating map ID {db_obj.id}: {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error updating map ID {db_obj.id}.")

    def remove(self, db: Session, *, id: int) -> models.CrosswalkMap:
        logger.info(f"Deleting Crosswalk Map ID: {id}")
        db_obj = self.get(db, id=id)
        if not db_obj:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Map ID {id} not found")
        name = db_obj.name
        db.delete(db_obj)
        try:
            db.commit()
            logger.info(f"Deleted Crosswalk Map ID: {id}, Name: {name}")
            return db_obj
        except Exception as e:
            logger.error(f"DB error deleting map ID {id}: {e}", exc_info=True); db.rollback()
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error deleting map ID {id}.")


# Create singleton instances
crud_crosswalk_data_source = CRUDCrosswalkDataSource()
crud_crosswalk_map = CRUDCrosswalkMap()
