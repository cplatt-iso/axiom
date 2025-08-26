# app/crud/crud_dicomweb_source.py
from typing import Any, Dict, List, Optional, Union

import structlog
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status
import logging

# Assuming models and schemas are correctly imported
from app.db import models
from app.schemas import dicomweb as dicomweb_schemas

logger = structlog.get_logger(__name__)

class DicomWebSourceCRUD:
    """
    CRUD operations for DICOMweb Source configurations.
    Interacts with the DicomWebSourceState model.
    NOTE: This currently targets a combined config/state model. Refactor recommended.
    """

    def get(self, db: Session, id: int) -> Optional[models.DicomWebSourceState]:
        logger.debug(f"Querying DICOMweb source config by ID: {id}")
        result = db.get(models.DicomWebSourceState, id)
        if result:
            logger.debug(f"Found DICOMweb source config ID: {id}, Name: {result.source_name}")
        else:
            logger.debug(f"No DICOMweb source config found with ID: {id}")
        return result

    def get_by_name(self, db: Session, name: str) -> Optional[models.DicomWebSourceState]:
        logger.debug(f"Querying DICOMweb source config by name: {name}")
        statement = select(models.DicomWebSourceState).where(models.DicomWebSourceState.source_name == name)
        result = db.execute(statement).scalar_one_or_none()
        if result:
            logger.debug(f"Found DICOMweb source config Name: {name}, ID: {result.id}")
        else:
            logger.debug(f"No DICOMweb source config found with Name: {name}")
        return result

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[models.DicomWebSourceState]:
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

        existing = self.get_by_name(db, name=obj_in.name)
        if existing:
            logger.warning(f"DICOMweb source config creation failed: Name '{obj_in.name}' already exists (ID: {existing.id}).")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A DICOMweb source with the name '{obj_in.name}' already exists."
            )

        try:
            # Map fields from Pydantic schema to SQLAlchemy model constructor
            db_obj = models.DicomWebSourceState(
                source_name=obj_in.name, # Map 'name' from schema to 'source_name' in model
                description=obj_in.description,
                base_url=obj_in.base_url,
                qido_prefix=obj_in.qido_prefix,
                wado_prefix=obj_in.wado_prefix,
                polling_interval_seconds=obj_in.polling_interval_seconds,
                is_enabled=obj_in.is_enabled,
                is_active=obj_in.is_active, # Make sure is_active is included
                auth_type=obj_in.auth_type,
                auth_config=obj_in.auth_config,
                search_filters=obj_in.search_filters,
                # State fields will use their DB defaults or be None initially
            )
            logger.debug(f"Successfully instantiated DicomWebSourceState object for '{obj_in.name}'")
        except TypeError as te:
             logger.error(f"TypeError during DicomWebSourceState instantiation for '{obj_in.name}': {te}", exc_info=True)
             raise HTTPException(
                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                 detail=f"Internal error constructing database object: {te}",
             )
        except Exception as model_exc:
             logger.error(f"Error instantiating DicomWebSourceState model for '{obj_in.name}': {model_exc}", exc_info=True)
             raise HTTPException(
                 status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                 detail=f"Internal error creating database object: {model_exc}",
             )

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully created DICOMweb source config with ID: {db_obj.id}, Name: {db_obj.source_name}")
            return db_obj
        except IntegrityError as ie:
            logger.error(f"Database integrity error during DICOMweb source creation for '{db_obj.source_name}': {ie}", exc_info=True)
            db.rollback()
            # Use specific constraint name if known and constant
            if "unique constraint" in str(ie).lower() and "dicomweb_source_state_source_name_key" in str(ie).lower():
                 raise HTTPException(
                     status_code=status.HTTP_409_CONFLICT,
                     detail=f"A DICOMweb source with the name '{db_obj.source_name}' already exists (commit conflict)."
                 )
            else:
                 raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Database integrity error occurred while creating source '{db_obj.source_name}'.",
                 )
        except Exception as e:
            logger.error(f"Database commit error during DICOMweb source creation for '{db_obj.source_name}': {e}", exc_info=True)
            db.rollback()
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
        logger.info(f"Attempting to update DICOMweb source config with ID: {db_obj.id}, Name: {db_obj.source_name}")
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            # exclude_unset=True is important for PATCH-like behavior
            update_data = obj_in.model_dump(exclude_unset=True)

        if not update_data:
             logger.warning(f"Update request for DICOMweb source ID {db_obj.id} contained no data to update.")
             return db_obj # Return original object if no changes

        original_name = db_obj.source_name

        # Handle potential rename: map 'name' from Pydantic to 'source_name' in DB model if present
        if "name" in update_data:
             new_name = update_data.pop('name') # Remove 'name' from update_data
             update_data['source_name'] = new_name # Add 'source_name' to update_data

             # Check for name collision only if the name is actually changing
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
        # This loop will now also handle 'is_active' if it's in update_data
        for field, value in update_data.items():
             if hasattr(db_obj, field):
                 setattr(db_obj, field, value)
             else:
                 # Log if trying to set an attribute that doesn't exist on the model
                 logger.warning(f"Attempted to update non-existent field '{field}' on DicomWebSourceState for ID {db_obj.id}. Skipping.")

        # --- Post-update Validation for Auth (Consider moving to schema/API layer) ---
        # This validation might be better placed earlier, but check current state after potential update
        current_auth_type = getattr(db_obj, 'auth_type')
        current_auth_config = getattr(db_obj, 'auth_config')

        # Basic validation based on the potentially updated auth_type
        if current_auth_type == "basic":
            if not isinstance(current_auth_config, dict) or 'username' not in current_auth_config or 'password' not in current_auth_config:
                 db.rollback() # Rollback before raising
                 logger.error(f"Update validation failed for ID {db_obj.id}: Auth type 'basic' but config invalid: {current_auth_config}")
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Auth type is 'basic' but 'auth_config' is missing or lacks 'username'/'password'.")
        elif current_auth_type == "bearer":
             if not isinstance(current_auth_config, dict) or 'token' not in current_auth_config:
                 db.rollback()
                 logger.error(f"Update validation failed for ID {db_obj.id}: Auth type 'bearer' but config invalid: {current_auth_config}")
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Auth type is 'bearer' but 'auth_config' is missing or lacks 'token'.")
        elif current_auth_type == "apikey": # Added validation for apikey
             if not isinstance(current_auth_config, dict) or 'header_name' not in current_auth_config or 'key' not in current_auth_config:
                 db.rollback()
                 logger.error(f"Update validation failed for ID {db_obj.id}: Auth type 'apikey' but config invalid: {current_auth_config}")
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Auth type is 'apikey' but 'auth_config' is missing or lacks 'header_name'/'key'.")
        elif current_auth_type == "none":
             # Ensure config is None if type is none, even if not explicitly provided in update
             if current_auth_config is not None:
                 setattr(db_obj, 'auth_config', None)
        # --- End Post-update Validation ---

        db.add(db_obj) # Add the modified object back to the session
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully updated DICOMweb source config ID: {db_obj.id}")
            return db_obj
        except IntegrityError as ie:
            logger.error(f"Database integrity error during DICOMweb source update for ID {db_obj.id}: {ie}", exc_info=True)
            db.rollback()
            # Check specifically for the name unique constraint if name was part of the update
            if "source_name" in update_data and "unique constraint" in str(ie).lower() and "dicomweb_source_state_source_name_key" in str(ie).lower():
                 raise HTTPException(
                     status_code=status.HTTP_409_CONFLICT,
                     detail=f"A DICOMweb source with the name '{update_data['source_name']}' already exists (commit conflict)."
                 )
            else:
                 raise HTTPException(
                     status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                     detail=f"Database integrity error occurred while updating source ID {db_obj.id}.",
                 )
        except Exception as e:
            logger.error(f"Database error during DICOMweb source update for ID {db_obj.id}: {e}", exc_info=True)
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error occurred while updating source ID {db_obj.id}.",
            )


    def remove(self, db: Session, *, id: int) -> models.DicomWebSourceState:
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
            # The db_obj is technically expired after commit, but returning its previous state might be okay
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
