# app/crud/crud_dicomweb_source.py
from typing import Any, Dict, List, Optional, Union

import structlog
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from fastapi import HTTPException, status
import logging

from app.db import models
from app.schemas import dicomweb as dicomweb_schemas # Import the dicomweb schemas module

logger = structlog.get_logger(__name__)

class DicomWebSourceCRUD:
    """
    CRUD operations for DICOMweb Source configurations.
    These operations interact with the DicomWebSourceState model,
    focusing on the configuration aspects.
    """

    def get(self, db: Session, id: int) -> Optional[models.DicomWebSourceState]:
        """Get a DICOMweb source configuration by its ID."""
        logger.debug(f"Querying DICOMweb source config by ID: {id}")
        result = db.get(models.DicomWebSourceState, id) # Use Session.get for PK lookup
        if result:
            logger.debug(f"Found DICOMweb source config with ID: {id}, Name: {result.source_name}")
        else:
            logger.debug(f"No DICOMweb source config found with ID: {id}")
        return result

    def get_by_name(self, db: Session, name: str) -> Optional[models.DicomWebSourceState]:
        """Get a DICOMweb source configuration by its unique name."""
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
        """Retrieve multiple DICOMweb source configurations."""
        logger.debug(f"Querying multiple DICOMweb source configs (skip={skip}, limit={limit})")
        statement = (
            select(models.DicomWebSourceState)
            .order_by(models.DicomWebSourceState.source_name) # Order by source_name for consistency
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
             # Raise HTTPException here to be handled by the API layer
             raise HTTPException(
                 status_code=status.HTTP_409_CONFLICT,
                 detail=f"A DICOMweb source with the name '{obj_in.name}' already exists."
             )

        # The Pydantic model handles validation of individual fields and auth logic
        create_data = obj_in.model_dump()
        if "name" in create_data: # Map 'name' from schema to 'source_name' for the model
            create_data["source_name"] = create_data.pop("name")
        db_obj = models.DicomWebSourceState(**create_data)

        # Initialize state fields not present in the Create schema
        db_obj.last_successful_run = None
        db_obj.last_error_run = None
        db_obj.last_error_message = ""  # Use empty string instead of None
        db_obj.last_processed_timestamp = None # Or set to a specific initial value if needed

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully created DICOMweb source config with ID: {db_obj.id}, Name: {db_obj.source_name}")
            return db_obj
        except Exception as e:
            logger.error(f"Database error during DICOMweb source creation for '{obj_in.name}': {e}", exc_info=True)
            db.rollback()
            # Re-raise a more generic server error or a specific DB error exception
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error occurred while creating source '{obj_in.name}'.",
            )


    def update(
        self,
        db: Session,
        *,
        db_obj: models.DicomWebSourceState,
        obj_in: Union[dicomweb_schemas.DicomWebSourceConfigUpdate, Dict[str, Any]]
    ) -> models.DicomWebSourceState:
        """Update an existing DICOMweb source configuration."""
        logger.info(f"Attempting to update DICOMweb source config with ID: {db_obj.id}, Name: {db_obj.source_name}")
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            # Use exclude_unset=True to only include fields explicitly passed in the request
            update_data = obj_in.model_dump(exclude_unset=True)

        if not update_data:
             logger.warning(f"Update request for DICOMweb source ID {db_obj.id} contained no data to update.")
             # Return the object unchanged or raise an error? Returning unchanged seems reasonable.
             return db_obj

        # Check for name conflict if the name is being changed
        if "name" in update_data and update_data["name"] != db_obj.source_name:
            existing = self.get_by_name(db, name=update_data["name"])
            if existing and existing.id != db_obj.id:
                logger.warning(f"DICOMweb source config update failed for ID {db_obj.id}: New name '{update_data['name']}' conflicts with existing source ID {existing.id}.")
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"A DICOMweb source with the name '{update_data['name']}' already exists."
                )

        logger.debug(f"Updating fields for source ID {db_obj.id}: {list(update_data.keys())}")

        # Update the database object attributes
        for field, value in update_data.items():
            setattr(db_obj, field, value)

        # --- Post-update Validation for Auth ---
        # The Pydantic validator on DicomWebSourceConfigUpdate checks if provided auth_config matches provided auth_type.
        # Here, we need to check consistency if *only one* of them was updated.
        if "auth_type" in update_data or "auth_config" in update_data:
            current_auth_type = getattr(db_obj, 'auth_type')
            current_auth_config = getattr(db_obj, 'auth_config')

            if current_auth_type == "basic":
                if not isinstance(current_auth_config, dict) or 'username' not in current_auth_config or 'password' not in current_auth_config:
                     db.rollback() # Rollback changes made so far in this transaction
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Auth type is 'basic' but 'auth_config' is missing or lacks 'username'/'password'.")
            elif current_auth_type == "bearer":
                 if not isinstance(current_auth_config, dict) or 'token' not in current_auth_config:
                     db.rollback()
                     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Auth type is 'bearer' but 'auth_config' is missing or lacks 'token'.")
            elif current_auth_type == "none":
                 if current_auth_config is not None:
                     # If type set to none, ensure config is also None
                     setattr(db_obj, 'auth_config', None)
            # else: # Invalid auth_type should have been caught by Pydantic on input if provided

        # --- End Post-update Validation ---

        db.add(db_obj) # Add the modified object to the session
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully updated DICOMweb source config ID: {db_obj.id}")
            return db_obj
        except Exception as e:
            logger.error(f"Database error during DICOMweb source update for ID {db_obj.id}: {e}", exc_info=True)
            db.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error occurred while updating source ID {db_obj.id}.",
            )


    def remove(self, db: Session, *, id: int) -> models.DicomWebSourceState:
        """Delete a DICOMweb source configuration by ID."""
        logger.info(f"Attempting to delete DICOMweb source config with ID: {id}")
        db_obj = self.get(db, id=id) # Use self.get which already handles not found
        if not db_obj:
            logger.warning(f"DICOMweb source config deletion failed: No source found with ID: {id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"DICOMweb source with ID {id} not found")

        deleted_name = db_obj.source_name # Store name for logging before deleting
        db.delete(db_obj)
        try:
            db.commit()
            logger.info(f"Successfully deleted DICOMweb source config ID: {id}, Name: {deleted_name}")
            # Return the deleted object (transient state) as confirmation
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
