# app/crud/crud_dimse_qr_source.py
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from sqlalchemy.orm import Session
# Ensure correct imports: select, sql_update
from sqlalchemy import select, update as sql_update
# F is NOT imported
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status

from app.db import models
from app.schemas import dimse_qr_source as schemas_dimse_qr

logger = logging.getLogger(__name__)

class CRUDDimseQueryRetrieveSource:
    """
    CRUD operations for DIMSE Query/Retrieve Source configurations.
    Interacts with the DimseQueryRetrieveSource model.
    """

    # --- Standard CRUD Methods (Readable Format) ---
    def get(self, db: Session, id: int) -> Optional[models.DimseQueryRetrieveSource]:
        logger.debug(f"Querying DIMSE QR source config by ID: {id}")
        result = db.get(models.DimseQueryRetrieveSource, id)
        if result:
            logger.debug(f"Found DIMSE QR source config ID: {id}, Name: {result.name}")
        else:
            logger.debug(f"No DIMSE QR source config found with ID: {id}")
        return result

    def get_by_name(self, db: Session, name: str) -> Optional[models.DimseQueryRetrieveSource]:
        logger.debug(f"Querying DIMSE QR source config by name: {name}")
        statement = select(models.DimseQueryRetrieveSource).where(models.DimseQueryRetrieveSource.name == name)
        result = db.execute(statement).scalar_one_or_none()
        if result:
            logger.debug(f"Found DIMSE QR source config Name: {name}, ID: {result.id}")
        else:
            logger.debug(f"No DIMSE QR source config found with Name: {name}")
        return result

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[models.DimseQueryRetrieveSource]:
        logger.debug(f"Querying multiple DIMSE QR source configs (skip={skip}, limit={limit})")
        statement = (
            select(models.DimseQueryRetrieveSource)
            .order_by(models.DimseQueryRetrieveSource.name)
            .offset(skip)
            .limit(limit)
        )
        results = list(db.execute(statement).scalars().all())
        logger.debug(f"Found {len(results)} DIMSE QR source configs.")
        return results

    def get_enabled_sources(self, db: Session) -> List[models.DimseQueryRetrieveSource]:
        logger.debug("Querying all enabled DIMSE QR source configs")
        statement = (
            select(models.DimseQueryRetrieveSource)
            .where(models.DimseQueryRetrieveSource.is_enabled == True)
            .order_by(models.DimseQueryRetrieveSource.name)
        )
        results = list(db.execute(statement).scalars().all())
        logger.debug(f"Found {len(results)} enabled DIMSE QR source configs.")
        return results

    def create(self, db: Session, *, obj_in: schemas_dimse_qr.DimseQueryRetrieveSourceCreate) -> models.DimseQueryRetrieveSource:
        logger.info(f"Creating DIMSE QR source config: {obj_in.name}")
        if self.get_by_name(db, name=obj_in.name):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists.")
        try:
            db_obj = models.DimseQueryRetrieveSource(**obj_in.model_dump())
            db.add(db_obj); db.commit(); db.refresh(db_obj)
            logger.info(f"Created DIMSE QR source ID: {db_obj.id}, Name: {db_obj.name}")
            return db_obj
        except IntegrityError as e:
            db.rollback(); logger.error(f"DB integrity error creating '{obj_in.name}': {e}", exc_info=True)
            if "uq_dimse_qr_sources_name" in str(e.orig): raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' conflict.")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error creating '{obj_in.name}'.")
        except Exception as e:
             db.rollback(); logger.error(f"Unexpected error creating '{obj_in.name}': {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error creating '{obj_in.name}'.")

    def update(self, db: Session, *, db_obj: models.DimseQueryRetrieveSource, obj_in: Union[schemas_dimse_qr.DimseQueryRetrieveSourceUpdate, Dict[str, Any]]) -> models.DimseQueryRetrieveSource:
        logger.info(f"Updating DIMSE QR source ID: {db_obj.id}, Name: {db_obj.name}")
        update_data = obj_in if isinstance(obj_in, dict) else obj_in.model_dump(exclude_unset=True)
        if not update_data: logger.warning(f"No update data for ID {db_obj.id}."); return db_obj
        if "name" in update_data and update_data["name"] != db_obj.name:
            existing = self.get_by_name(db, name=update_data["name"])
            if existing and existing.id != db_obj.id: raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' exists.")
        logger.debug(f"Updating fields for ID {db_obj.id}: {list(update_data.keys())}")
        for field, value in update_data.items():
            if hasattr(db_obj, field): setattr(db_obj, field, value)
            else: logger.warning(f"Skipping non-existent field '{field}' update for ID {db_obj.id}.")
        db.add(db_obj)
        try:
            db.commit(); db.refresh(db_obj); logger.info(f"Updated DIMSE QR source ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
             db.rollback(); logger.error(f"DB integrity error updating ID {db_obj.id}: {e}", exc_info=True)
             if "name" in update_data and "uq_dimse_qr_sources_name" in str(e.orig): raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' conflict.")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error updating ID {db_obj.id}.")
        except Exception as e:
             db.rollback(); logger.error(f"Unexpected error updating ID {db_obj.id}: {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error updating ID {db_obj.id}.")

    def remove(self, db: Session, *, id: int) -> models.DimseQueryRetrieveSource:
        logger.info(f"Deleting DIMSE QR source config ID: {id}")
        db_obj = self.get(db, id=id)
        if not db_obj: raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"DIMSE QR source ID {id} not found")
        name = db_obj.name; db.delete(db_obj)
        try:
            db.commit(); logger.info(f"Deleted DIMSE QR source ID: {id}, Name: {name}")
            return db_obj
        except Exception as e:
            logger.error(f"DB error deleting ID {id}: {e}", exc_info=True); db.rollback()
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"DB error deleting ID {id}.",)

    # --- Status update methods ---
    def update_query_status( self, db: Session, *, source_id: int, last_successful_query: Optional[datetime] = None, last_error_time: Optional[datetime] = None, last_error_message: Optional[str] = None, ) -> bool:
        update_values = {};
        if last_successful_query: update_values["last_successful_query"]=last_successful_query; update_values["last_error_time"]=None; update_values["last_error_message"]=None
        if last_error_time: update_values["last_error_time"]=last_error_time
        if last_error_message is not None and last_error_time: update_values["last_error_message"]=last_error_message
        elif last_error_message is not None: logger.warning(f"Skipping err msg update for source {source_id} without error time.")
        if not update_values: logger.debug(f"No query status to update for {source_id}."); return False;
        logger.debug(f"Updating query status DIMSE QR source {source_id}: {update_values}")
        stmt = (sql_update(models.DimseQueryRetrieveSource).where(models.DimseQueryRetrieveSource.id == source_id).values(**update_values).execution_options(synchronize_session=False))
        success = False
        try:
            result = db.execute(stmt); db.commit()
            if result.rowcount > 0: logger.debug(f"Query status updated {source_id}."); success = True
            else: logger.warning(f"Query status update failed {source_id} (not found?).")
        except Exception as e: logger.error(f"DB error updating query status {source_id}: {e}", exc_info=True); db.rollback()
        return success

    def update_move_status( self, db: Session, *, source_id: int, last_successful_move: Optional[datetime] = None, last_error_time: Optional[datetime] = None, last_error_message: Optional[str] = None, ) -> bool:
        update_values = {};
        if last_successful_move: update_values["last_successful_move"] = last_successful_move
        if last_error_time: update_values["last_error_time"] = last_error_time
        if last_error_message is not None and last_error_time: update_values["last_error_message"] = last_error_message
        elif last_error_message is not None: logger.warning(f"Skipping err msg update move status {source_id} without error time.")
        if not update_values: logger.debug(f"No move status fields to update for {source_id}."); return False;
        logger.debug(f"Updating move status DIMSE QR source {source_id}: {update_values}")
        stmt = (sql_update(models.DimseQueryRetrieveSource).where(models.DimseQueryRetrieveSource.id == source_id).values(**update_values).execution_options(synchronize_session=False))
        success = False
        try:
            result = db.execute(stmt); db.commit()
            if result.rowcount > 0: logger.debug(f"Move status updated {source_id}."); success = True
            else: logger.warning(f"Move status update failed {source_id} (not found?).")
        except Exception as e: logger.error(f"DB error updating move status {source_id}: {e}", exc_info=True); db.rollback()
        return success

    def increment_found_study_count(self, db: Session, *, source_id: int, count: int = 1) -> bool:
        """Atomically increments the found study count for a DIMSE QR source."""
        if count <= 0: return False
        logger.debug(f"Incrementing found study count source ID {source_id} by {count}")
        stmt = (
            sql_update(models.DimseQueryRetrieveSource)
            .where(models.DimseQueryRetrieveSource.id == source_id)
            # FINAL CORRECT SYNTAX: {ColumnObject: ColumnObject + value}
            .values({
                models.DimseQueryRetrieveSource.found_study_count: models.DimseQueryRetrieveSource.found_study_count + count
             })
            .execution_options(synchronize_session=False)
        )
        try:
            result = db.execute(stmt)
            if result.rowcount > 0: return True
            else: logger.warning(f"Failed increment found study count {source_id}."); return False
        except Exception as e: logger.error(f"DB error incrementing found study count {source_id}: {e}", exc_info=True); return False

    def increment_move_queued_count(self, db: Session, *, source_id: int, count: int = 1) -> bool:
        """Atomically increments the move queued study count for a DIMSE QR source."""
        if count <= 0: return False
        logger.debug(f"Incrementing move queued count source ID {source_id} by {count}")
        stmt = (
            sql_update(models.DimseQueryRetrieveSource)
            .where(models.DimseQueryRetrieveSource.id == source_id)
             # FINAL CORRECT SYNTAX: {ColumnObject: ColumnObject + value}
            .values({
                models.DimseQueryRetrieveSource.move_queued_study_count: models.DimseQueryRetrieveSource.move_queued_study_count + count
             })
            .execution_options(synchronize_session=False)
        )
        try:
            result = db.execute(stmt)
            if result.rowcount > 0: return True
            else: logger.warning(f"Failed increment move queued count {source_id}."); return False
        except Exception as e: logger.error(f"DB error incrementing move queued count {source_id}: {e}", exc_info=True); return False

    def increment_processed_count(self, db: Session, *, source_id: int, count: int = 1) -> bool:
        """Atomically increments the processed instance count for a DIMSE QR source."""
        if count <= 0: return False
        logger.debug(f"Incrementing processed instance count for source ID {source_id} by {count}")
        stmt = (
            sql_update(models.DimseQueryRetrieveSource)
            .where(models.DimseQueryRetrieveSource.id == source_id)
            .values({
                models.DimseQueryRetrieveSource.processed_instance_count: models.DimseQueryRetrieveSource.processed_instance_count + count
             })
            .execution_options(synchronize_session=False)
        )
        try:
            result = db.execute(stmt)
            if result.rowcount > 0: return True
            else: logger.warning(f"Failed increment processed instance count {source_id}."); return False
        except Exception as e: logger.error(f"DB error incrementing processed instance count {source_id}: {e}", exc_info=True); return False
# Create a singleton instance
crud_dimse_qr_source = CRUDDimseQueryRetrieveSource()
