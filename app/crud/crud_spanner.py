# app/crud/crud_spanner.py
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from sqlalchemy.orm import Session, joinedload
from sqlalchemy import select, update as sql_update, delete, func
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status

from app.db import models
from app.schemas import spanner as schemas_spanner

logger = logging.getLogger(__name__)


class CRUDSpannerConfig:
    """CRUD operations for SpannerConfig model."""
    
    def get(self, db: Session, id: int) -> Optional[models.SpannerConfig]:
        """Get a spanner config by ID."""
        return db.get(models.SpannerConfig, id)
    
    def get_by_name(self, db: Session, *, name: str) -> Optional[models.SpannerConfig]:
        """Get a spanner config by name."""
        stmt = select(models.SpannerConfig).where(models.SpannerConfig.name == name)
        return db.execute(stmt).scalar_one_or_none()
    
    def get_multi(
        self, 
        db: Session, 
        *, 
        skip: int = 0, 
        limit: int = 100,
        include_disabled: bool = False
    ) -> List[models.SpannerConfig]:
        """Get multiple spanner configs with pagination."""
        stmt = select(models.SpannerConfig)
        
        if not include_disabled:
            stmt = stmt.where(models.SpannerConfig.is_enabled == True)
            
        stmt = stmt.offset(skip).limit(limit).order_by(models.SpannerConfig.name)
        return list(db.execute(stmt).scalars().all())
    
    def get_with_mappings(self, db: Session, id: int) -> Optional[models.SpannerConfig]:
        """Get a spanner config with all its source mappings loaded."""
        stmt = (
            select(models.SpannerConfig)
            .options(joinedload(models.SpannerConfig.source_mappings))
            .where(models.SpannerConfig.id == id)
        )
        return db.execute(stmt).scalar_one_or_none()
    
    def create(
        self, 
        db: Session, 
        *, 
        obj_in: schemas_spanner.SpannerConfigCreate
    ) -> models.SpannerConfig:
        """Create a new spanner config."""
        logger.info(f"Creating spanner config: {obj_in.name}")
        
        # Check for name conflicts
        if self.get_by_name(db, name=obj_in.name):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Spanner config name '{obj_in.name}' already exists."
            )
        
        try:
            db_obj = models.SpannerConfig(**obj_in.model_dump())
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Created spanner config ID: {db_obj.id}, Name: {db_obj.name}")
            return db_obj
        except IntegrityError as e:
            db.rollback()
            logger.error(f"DB integrity error creating spanner config '{obj_in.name}': {e}", exc_info=True)
            if "uq_spanner_configs_name" in str(e.orig):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Spanner config name '{obj_in.name}' already exists."
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error creating spanner config '{obj_in.name}'."
            )
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error creating spanner config '{obj_in.name}': {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unexpected error creating spanner config '{obj_in.name}'."
            )
    
    def update(
        self, 
        db: Session, 
        *, 
        db_obj: models.SpannerConfig, 
        obj_in: Union[schemas_spanner.SpannerConfigUpdate, Dict[str, Any]]
    ) -> models.SpannerConfig:
        """Update a spanner config."""
        logger.info(f"Updating spanner config ID: {db_obj.id}, Name: {db_obj.name}")
        
        update_data = obj_in if isinstance(obj_in, dict) else obj_in.model_dump(exclude_unset=True)
        if not update_data:
            logger.warning(f"No update data for spanner config ID {db_obj.id}.")
            return db_obj
        
        # Check for name conflicts if name is being updated
        if "name" in update_data and update_data["name"] != db_obj.name:
            existing = self.get_by_name(db, name=update_data["name"])
            if existing and existing.id != db_obj.id:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Spanner config name '{update_data['name']}' already exists."
                )
        
        logger.debug(f"Updating spanner config fields for ID {db_obj.id}: {list(update_data.keys())}")
        
        try:
            for field, value in update_data.items():
                if hasattr(db_obj, field):
                    setattr(db_obj, field, value)
                else:
                    logger.warning(f"Skipping non-existent field '{field}' update for spanner config ID {db_obj.id}.")
            
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Updated spanner config ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
            db.rollback()
            logger.error(f"DB integrity error updating spanner config ID {db_obj.id}: {e}", exc_info=True)
            if "uq_spanner_configs_name" in str(e.orig):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Spanner config name '{update_data.get('name', 'unknown')}' already exists."
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error updating spanner config ID {db_obj.id}."
            )
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error updating spanner config ID {db_obj.id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unexpected error updating spanner config ID {db_obj.id}."
            )
    
    def remove(self, db: Session, *, id: int) -> models.SpannerConfig:
        """Delete a spanner config and all its mappings."""
        logger.info(f"Deleting spanner config ID: {id}")
        
        db_obj = self.get(db, id=id)
        if not db_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config ID {id} not found."
            )
        
        try:
            db.delete(db_obj)
            db.commit()
            logger.info(f"Deleted spanner config ID: {id}, Name: {db_obj.name}")
            return db_obj
        except Exception as e:
            db.rollback()
            logger.error(f"Error deleting spanner config ID {id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error deleting spanner config ID {id}."
            )
    
    def count(self, db: Session, *, include_disabled: bool = False) -> int:
        """Count total spanner configs."""
        stmt = select(func.count(models.SpannerConfig.id))
        if not include_disabled:
            stmt = stmt.where(models.SpannerConfig.is_enabled == True)
        return db.execute(stmt).scalar() or 0
    
    def update_activity(self, db: Session, *, spanner_id: int, query_type: str) -> bool:
        """Update activity counters and timestamp for a spanner config."""
        try:
            update_data = {"last_activity": datetime.utcnow()}
            
            if query_type.upper() in ["C-FIND", "QIDO"]:
                stmt = (
                    sql_update(models.SpannerConfig)
                    .where(models.SpannerConfig.id == spanner_id)
                    .values(
                        total_queries_processed=models.SpannerConfig.total_queries_processed + 1,
                        **update_data
                    )
                )
            elif query_type.upper() in ["C-GET", "C-MOVE", "WADO"]:
                stmt = (
                    sql_update(models.SpannerConfig)
                    .where(models.SpannerConfig.id == spanner_id)
                    .values(
                        total_retrievals_processed=models.SpannerConfig.total_retrievals_processed + 1,
                        **update_data
                    )
                )
            else:
                # Just update timestamp for unknown types
                stmt = (
                    sql_update(models.SpannerConfig)
                    .where(models.SpannerConfig.id == spanner_id)
                    .values(**update_data)
                )
            
            result = db.execute(stmt)
            db.commit()
            return result.rowcount > 0
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating spanner activity for ID {spanner_id}: {e}", exc_info=True)
            return False


class CRUDSpannerSourceMapping:
    """CRUD operations for SpannerSourceMapping model."""
    
    def get(self, db: Session, id: int) -> Optional[models.SpannerSourceMapping]:
        """Get a source mapping by ID."""
        return db.get(models.SpannerSourceMapping, id)
    
    def get_by_spanner_and_source(
        self, 
        db: Session, 
        *, 
        spanner_config_id: int, 
        dimse_qr_source_id: int
    ) -> Optional[models.SpannerSourceMapping]:
        """Get a mapping by spanner and source IDs."""
        stmt = (
            select(models.SpannerSourceMapping)
            .where(
                models.SpannerSourceMapping.spanner_config_id == spanner_config_id,
                models.SpannerSourceMapping.dimse_qr_source_id == dimse_qr_source_id
            )
        )
        return db.execute(stmt).scalar_one_or_none()
    
    def get_multi_by_spanner(
        self, 
        db: Session, 
        *, 
        spanner_config_id: int,
        include_disabled: bool = False,
        order_by_priority: bool = True
    ) -> List[models.SpannerSourceMapping]:
        """Get all mappings for a spanner config."""
        stmt = (
            select(models.SpannerSourceMapping)
            .options(joinedload(models.SpannerSourceMapping.dimse_qr_source))
            .where(models.SpannerSourceMapping.spanner_config_id == spanner_config_id)
        )
        
        if not include_disabled:
            stmt = stmt.where(models.SpannerSourceMapping.is_enabled == True)
        
        if order_by_priority:
            stmt = stmt.order_by(models.SpannerSourceMapping.priority)
        
        return list(db.execute(stmt).scalars().all())
    
    def create(
        self, 
        db: Session, 
        *, 
        spanner_config_id: int,
        obj_in: schemas_spanner.SpannerSourceMappingCreate
    ) -> models.SpannerSourceMapping:
        """Create a new source mapping."""
        logger.info(f"Creating source mapping for spanner {spanner_config_id}, source {obj_in.dimse_qr_source_id}")
        
        # Check if mapping already exists
        existing = self.get_by_spanner_and_source(
            db, 
            spanner_config_id=spanner_config_id, 
            dimse_qr_source_id=obj_in.dimse_qr_source_id
        )
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Source {obj_in.dimse_qr_source_id} is already mapped to spanner {spanner_config_id}."
            )
        
        # Verify spanner config exists
        spanner_config = db.get(models.SpannerConfig, spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found."
            )
        
        # Verify source exists
        source = db.get(models.DimseQueryRetrieveSource, obj_in.dimse_qr_source_id)
        if not source:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"DIMSE Q/R source {obj_in.dimse_qr_source_id} not found."
            )
        
        try:
            mapping_data = obj_in.model_dump()
            mapping_data['spanner_config_id'] = spanner_config_id
            
            db_obj = models.SpannerSourceMapping(**mapping_data)
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Created source mapping ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
            db.rollback()
            logger.error(f"DB integrity error creating source mapping: {e}", exc_info=True)
            if "uq_spanner_source_mapping" in str(e.orig):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Source {obj_in.dimse_qr_source_id} is already mapped to spanner {spanner_config_id}."
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database error creating source mapping."
            )
        except Exception as e:
            db.rollback()
            logger.error(f"Unexpected error creating source mapping: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Unexpected error creating source mapping."
            )
    
    def update(
        self, 
        db: Session, 
        *, 
        db_obj: models.SpannerSourceMapping, 
        obj_in: Union[schemas_spanner.SpannerSourceMappingUpdate, Dict[str, Any]]
    ) -> models.SpannerSourceMapping:
        """Update a source mapping."""
        logger.info(f"Updating source mapping ID: {db_obj.id}")
        
        update_data = obj_in if isinstance(obj_in, dict) else obj_in.model_dump(exclude_unset=True)
        if not update_data:
            logger.warning(f"No update data for source mapping ID {db_obj.id}.")
            return db_obj
        
        try:
            for field, value in update_data.items():
                if hasattr(db_obj, field):
                    setattr(db_obj, field, value)
                else:
                    logger.warning(f"Skipping non-existent field '{field}' update for source mapping ID {db_obj.id}.")
            
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Updated source mapping ID: {db_obj.id}")
            return db_obj
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating source mapping ID {db_obj.id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error updating source mapping ID {db_obj.id}."
            )
    
    def remove(self, db: Session, *, id: int) -> models.SpannerSourceMapping:
        """Delete a source mapping."""
        logger.info(f"Deleting source mapping ID: {id}")
        
        db_obj = self.get(db, id=id)
        if not db_obj:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Source mapping ID {id} not found."
            )
        
        try:
            db.delete(db_obj)
            db.commit()
            logger.info(f"Deleted source mapping ID: {id}")
            return db_obj
        except Exception as e:
            db.rollback()
            logger.error(f"Error deleting source mapping ID {id}: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error deleting source mapping ID {id}."
            )
    
    def update_statistics(
        self, 
        db: Session, 
        *, 
        mapping_id: int, 
        query_sent: bool = False,
        query_successful: bool = False,
        retrieval_sent: bool = False,
        retrieval_successful: bool = False
    ) -> bool:
        """Update statistics for a source mapping."""
        try:
            update_data = {"last_used": datetime.utcnow()}
            
            if query_sent:
                update_data["queries_sent"] = models.SpannerSourceMapping.queries_sent + 1
            if query_successful:
                update_data["queries_successful"] = models.SpannerSourceMapping.queries_successful + 1
            if retrieval_sent:
                update_data["retrievals_sent"] = models.SpannerSourceMapping.retrievals_sent + 1
            if retrieval_successful:
                update_data["retrievals_successful"] = models.SpannerSourceMapping.retrievals_successful + 1
            
            stmt = (
                sql_update(models.SpannerSourceMapping)
                .where(models.SpannerSourceMapping.id == mapping_id)
                .values(**update_data)
            )
            
            result = db.execute(stmt)
            db.commit()
            return result.rowcount > 0
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating mapping statistics for ID {mapping_id}: {e}", exc_info=True)
            return False


class CRUDSpannerQueryLog:
    """CRUD operations for SpannerQueryLog model."""
    
    def create_log(
        self, 
        db: Session, 
        *, 
        spanner_config_id: int,
        query_type: str,
        status: str,
        sources_queried: int = 0,
        sources_successful: int = 0,
        total_results_found: int = 0,
        deduplicated_results: int = 0,
        query_duration_seconds: Optional[float] = None,
        query_level: Optional[str] = None,
        query_filters: Optional[Dict[str, Any]] = None,
        requesting_ae_title: Optional[str] = None,
        requesting_ip: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> models.SpannerQueryLog:
        """Create a query log entry."""
        try:
            db_obj = models.SpannerQueryLog(
                spanner_config_id=spanner_config_id,
                query_type=query_type,
                query_level=query_level,
                query_filters=query_filters,
                requesting_ae_title=requesting_ae_title,
                requesting_ip=requesting_ip,
                sources_queried=sources_queried,
                sources_successful=sources_successful,
                total_results_found=total_results_found,
                deduplicated_results=deduplicated_results,
                query_duration_seconds=str(query_duration_seconds) if query_duration_seconds else None,
                status=status,
                error_message=error_message
            )
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            return db_obj
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating query log: {e}", exc_info=True)
            raise
    
    def get_multi_by_spanner(
        self, 
        db: Session, 
        *, 
        spanner_config_id: int,
        skip: int = 0,
        limit: int = 100
    ) -> List[models.SpannerQueryLog]:
        """Get query logs for a spanner config."""
        stmt = (
            select(models.SpannerQueryLog)
            .where(models.SpannerQueryLog.spanner_config_id == spanner_config_id)
            .order_by(models.SpannerQueryLog.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        return list(db.execute(stmt).scalars().all())


# Create singleton instances
crud_spanner_config = CRUDSpannerConfig()
crud_spanner_source_mapping = CRUDSpannerSourceMapping()
crud_spanner_query_log = CRUDSpannerQueryLog()
