# app/crud/crud_dicomweb_source.py
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.orm import Session
from sqlalchemy import select, func
from fastapi import HTTPException, status
import logging

from app.db import models
from app.schemas import dicomweb as dicomweb_schemas # Import the dicomweb schemas module

logger = logging.getLogger(__name__)

class DicomWebSourceCRUD:
    """
    CRUD operations for DICOMweb Source configurations.
    These operations interact with the DicomWebSourceState model,
    focusing on the configuration aspects.
    """

    def get(self, db: Session, id: int) -> Optional[models.DicomWebSourceState]:
        # ... (get method remains the same) ...
        logger.debug(f"Querying DICOMweb source config by ID: {id}")
        result = db.get(models.DicomWebSourceState, id)
        if result:
            logger.debug(f"Found DICOMweb source config with ID: {id}, Name: {result.source_name}")
        else:
            logger.debug(f"No DICOMweb source config found with ID: {id}")
        return result

    def get_by_name(self, db: Session, name: str) -> Optional[models.DicomWebSourceState]:
        # ... (get_by_name method remains the same) ...
        logger.debug(f"Querying DICOMweb source config by name: {name}")
        statement = select(models.DicomWebSourceState).where(models.DicomWebSourceState.source_name == name)
        result = db.execute(statement).scalar_one_or_none()
        if result:
            logger.debug(f"Found DICOMweb source config with Name: {name}, ID: {result.id}")
        else:
            logger.debug(f"No DICOMweb source config found with Name: {name}")
        return result

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[models.DicomWebSourceState]:
        # ... (get_multi method remains the same) ...
        logger.debug(f"Querying multiple DICOMweb source configs (skip={skip}, limit={limit})")
        statement = (
            select(models.DicomWebSourceState)
            .order_by(models.DicomWebSourceState.source_name)
            .offset(skip)
            .limit(limit)
        )
        results = list(db.execute(statement).scalars().all())
        logger.debug(f"Found {len(results)} DICOMweb source configs.")
        return results

    def create(self, db: Session, *, obj_in: dicomweb_schemas.DicomWebSourceConfigCreate) -> models.DicomWebSourceState:
        """Create a new DICOMweb source configuration."""
        logger.info(f"Attempting to create DICOMweb source config with name: {obj_in.name}")

        # Check if name already exists
        existing = self.get_by_name(db, name=obj_in.name)
        if existing:
            logger.warning(f"DICOMweb source config creation failed: Name '{obj_in.name}' already exists (ID: {existing.id}).")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A DICOMweb source with the name '{obj_in.name}' already exists."
            )

        # --- REVISED MODEL INSTANTIATION ---
        # Explicitly map fields from the Pydantic 'Create' schema (obj_in)
        # to the keyword arguments for the SQLAlchemy model constructor.
        try:
            db_obj = models.DicomWebSourceState(
                source_name=obj_in.name, # Direct mapping: Pydantic 'name' -> SQLAlchemy 'source_name'
                description=obj_in.description,
                base_url=obj_in.base_url,
                qido_prefix=obj_in.qido_prefix,
                wado_prefix=obj_in.wado_prefix,
                polling_interval_seconds=obj_in.polling_interval_seconds,
                is_enabled=obj_in.is_enabled,
                auth_type=obj_in.auth_type,
                auth_config=obj_in.auth_config, # Pass dict/None directly
                search_filters=obj_in.search_filters, # Pass dict/None directly
                # State fields (last_successful_run etc.) should have defaults (e.g., nullable) in the model definition
            )
            logger.debug(f"Successfully instantiated DicomWebSourceState object for '{obj_in.name}'")
        except TypeError as te:
             # Catch potential errors if model __init__ doesn't accept a field
             logger.error(f"TypeError during DicomWebSourceState instantiation for '{obj_in.name}': {te}", exc_info=True)
             raise HTTPException(
                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                 detail=f"Internal error constructing database object: {te}",
             )
        except Exception as model_exc:
             # Catch other potential errors during model instantiation
             logger.error(f"Error instantiating DicomWebSourceState model for '{obj_in.name}': {model_exc}", exc_info=True)
             raise HTTPException(
                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                 detail=f"Internal error creating database object: {model_exc}",
             )
        # --- END REVISED INSTANTIATION ---

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully created DICOMweb source config with ID: {db_obj.id}, Name: {db_obj.source_name}")
            return db_obj
        except Exception as e:
            # (Commit error handling remains the same)
            logger.error(f"Database commit error during DICOMweb source creation for '{db_obj.source_name}': {e}", exc_info=True)
            db.rollback()
            if "unique constraint" in str(e).lower() and "dicomweb_source_state_source_name_key" in str(e).lower():
                 raise HTTPException(
                     status_code=status.HTTP_409_CONFLICT,
                     detail=f"A DICOMweb source with the name '{db_obj.source_name}' already exists (commit conflict)."
                 )
            else:
                 raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Database error occurred while creating source '{db_obj.source_name}'.",
                 )


    def update(
        self,
        db: Session,
        *,
        db_obj: models.DicomWebSourceState,
        obj_in: Union[dicomweb_schemas.DicomWebSourceConfigUpdate, Dict[str, Any]]
    ) -> models.DicomWebSourceState:
        # ... (update method remains the same as the last correct version) ...
        logger.info(f"Attempting to update DICOMweb source config with ID: {db_obj.id}, Name: {db_obj.source_name}")
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)

        if not update_data:
             logger.warning(f"Update request for DICOMweb source ID {db_obj.id} contained no data to update.")
             return db_obj

        original_name = db_obj.source_name # Store original name for comparison

        # Handle potential rename: map 'name' from Pydantic to 'source_name' in DB model
        if "name" in update_data:
             new_name = update_data.pop('name')
             update_data['source_name'] = new_name # Add 'source_name' to the update data dict

             if new_name != original_name:
                 existing = self.get_by_name(db, name=new_name)
                 if existing and existing.id != db_obj.id:
                     logger.warning(f"DICOMweb source config update failed for ID {db_obj.id}: New name '{new_name}' conflicts with existing source ID {existing.id}.")
                     raise HTTPException(
                         status_code=status.HTTP_409_CONFLICT,
                         detail=f"A DICOMweb source with the name '{new_name}' already exists."
                     )

        logger.debug(f"Updating fields for source ID {db_obj.id}: {list(update_data.keys())}")

        # Update the database object attributes directly
        for field, value in update_data.items():
             if hasattr(db_obj, field):
                 setattr(db_obj, field, value)
             else:
                 logger.warning(f"Attempted to update non-existent field '{field}' on DicomWebSourceState for ID {db_obj.id}. Skipping.")


        # --- Post-update Validation for Auth ---
        current_auth_type = getattr(db_obj, 'auth_type')
        current_auth_config = getattr(db_obj, 'auth_config')

        if current_auth_type == "basic":
            if not isinstance(current_auth_config, dict) or 'username' not in current_auth_config or 'password' not in current_auth_config:
                 db.rollback()
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Auth type is 'basic' but 'auth_config' is missing or lacks 'username'/'password'.")
        elif current_auth_type == "bearer":
             if not isinstance(current_auth_config, dict) or 'token' not in current_auth_config:
                 db.rollback()
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Auth type is 'bearer' but 'auth_config' is missing or lacks 'token'.")
        elif current_auth_type == "none":
             if current_auth_config is not None:
                 setattr(db_obj, 'auth_config', None) # Ensure config is None if type is none
        # --- End Post-update Validation ---

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully updated DICOMweb source config ID: {db_obj.id}")
            return db_obj
        except Exception as e:
            logger.error(f"Database error during DICOMweb source update for ID {db_obj.id}: {e}", exc_info=True)
            db.rollback()
            if "name" in update_data and "unique constraint" in str(e).lower() and "dicomweb_source_state_source_name_key" in str(e).lower():
                 raise HTTPException(
                     status_code=status.HTTP_409_CONFLICT,
                     detail=f"A DICOMweb source with the name '{update_data['source_name']}' already exists (commit conflict)."
                 )
            else:
                 raise HTTPException(
                     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                     detail=f"Database error occurred while updating source ID {db_obj.id}.",
                 )


    def remove(self, db: Session, *, id: int) -> models.DicomWebSourceState:
        # ... (remove method remains the same) ...
        logger.info(f"Attempting to delete DICOMweb source config with ID: {id}")
        db_obj = self.get(db, id=id)
        if not db_obj:
            logger.warning(f"DICOMweb source config deletion failed: No source found with ID: {id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"DICOMweb source with ID {id} not found")

        deleted_name = db_obj.source_name
        db.delete(db_obj)
        try:
            db.commit()
            logger.info(f"Successfully deleted DICOMweb source config ID: {id}, Name: {deleted_name}")
            return db_obj
        except Exception as e:
            logger.error(f"Database error during DICOMweb source deletion for ID {id}: {e}", exc_info=True)
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error occurred while deleting source ID {id}.",
            )

# Create an instance of the CRUD class
dicomweb_source = DicomWebSourceCRUD()
