# app/crud/crud_storage_backend_config.py
import logging
from typing import Any, Dict, List, Optional, Union, Type # Added Type

from sqlalchemy.orm import Session
from sqlalchemy import select, inspect # Added inspect
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException, status

# Import the specific model subclasses AND the base class
from app.db.models.storage_backend_config import (
    StorageBackendConfig,
    FileSystemBackendConfig,
    GcsBackendConfig,
    CStoreBackendConfig,
    GoogleHealthcareBackendConfig,
    StowRsBackendConfig,
    #ALLOWED_BACKEND_TYPES # Might be useful
)
# Import the specific Pydantic Read/Create/Update schemas
from app.schemas import storage_backend_config as schemas_storage # Use alias

logger = logging.getLogger(__name__)

# --- Mapping from backend_type string to Model Class ---
# This helps in creating the correct SQLAlchemy model instance
MODEL_MAP: Dict[str, Type[StorageBackendConfig]] = {
    "filesystem": FileSystemBackendConfig,
    "gcs": GcsBackendConfig,
    "cstore": CStoreBackendConfig,
    "google_healthcare": GoogleHealthcareBackendConfig,
    "stow_rs": StowRsBackendConfig,
}

class CRUDStorageBackendConfig:
    """
    CRUD operations for Storage Backend configurations using STI.
    Interacts with StorageBackendConfig model hierarchy.
    """

    def get(self, db: Session, id: int) -> Optional[StorageBackendConfig]: # Return type is base class
        """Get a storage backend config by its ID. Returns the specific subclass instance."""
        logger.debug(f"Querying storage backend config by ID: {id}")
        # Use with_polymorphic='*' to load all columns for any potential subclass
        # Or let SQLAlchemy handle loading based on discriminator if eager loading isn't needed here
        result = db.get(StorageBackendConfig, id) # SQLAlchemy handles STI loading
        if result:
            logger.debug(f"Found storage backend config ID: {id}, Name: {result.name}, Type: {result.backend_type}")
        else:
            logger.debug(f"No storage backend config found with ID: {id}")
        return result # Will be instance of FileSystemBackendConfig, CStoreBackendConfig etc.

    def get_by_name(self, db: Session, name: str) -> Optional[StorageBackendConfig]:
        """Get a storage backend config by its unique name. Returns the specific subclass instance."""
        logger.debug(f"Querying storage backend config by name: {name}")
        statement = select(StorageBackendConfig).where(StorageBackendConfig.name == name)
        # SQLAlchemy loads the correct subclass based on the 'backend_type' discriminator
        result = db.execute(statement).scalar_one_or_none()
        if result:
            logger.debug(f"Found storage backend config Name: {name}, ID: {result.id}, Type: {result.backend_type}")
        else:
            logger.debug(f"No storage backend config found with Name: {name}")
        return result

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[StorageBackendConfig]: # Return list of base class, actual elements are subclasses
        """Retrieve multiple storage backend configurations. Returns a list of specific subclass instances."""
        logger.debug(f"Querying multiple storage backend configs (skip={skip}, limit={limit})")
        statement = (
            select(StorageBackendConfig)
            .order_by(StorageBackendConfig.name) # Order by name
            .offset(skip)
            .limit(limit)
        )
        # SQLAlchemy loads correct subclasses
        results = list(db.execute(statement).scalars().all())
        logger.debug(f"Found {len(results)} storage backend configs.")
        return results

    def create(self, db: Session, *, obj_in: schemas_storage.StorageBackendConfigCreate) -> StorageBackendConfig:
        """
        Create a new storage backend configuration.
        Uses the discriminated union schema to determine the correct model subclass.
        """
        logger.info(f"Attempting to create storage backend config with name: {obj_in.name}, type: {obj_in.backend_type}")

        if self.get_by_name(db, name=obj_in.name):
            logger.warning(f"Storage backend config creation failed: Name '{obj_in.name}' already exists.")
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists.")

        # --- STI Handling ---
        # 1. Get the correct SQLAlchemy Model Class based on backend_type
        model_class = MODEL_MAP.get(obj_in.backend_type)
        if not model_class:
            # This should ideally be caught by Pydantic validation first, but double-check
            logger.error(f"Invalid backend_type '{obj_in.backend_type}' provided during creation.")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid backend_type '{obj_in.backend_type}'.")

        # 2. Create instance of the specific model class
        #    The Pydantic discriminated union obj_in should have the correct fields.
        try:
            # Pass the model_dump() which contains all necessary fields (common + specific)
            db_obj = model_class(**obj_in.model_dump())

            # --- Specific Validation (Example for CStore TLS) ---
            # Although Pydantic handles some validation, complex cross-field logic might live here
            if isinstance(db_obj, CStoreBackendConfig):
                 if db_obj.tls_enabled and not db_obj.tls_ca_cert_secret_name:
                      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="C-STORE: CA Cert Secret Name is required when TLS is enabled.")
                 if (db_obj.tls_client_cert_secret_name and not db_obj.tls_client_key_secret_name) or \
                    (not db_obj.tls_client_cert_secret_name and db_obj.tls_client_key_secret_name):
                      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="C-STORE: Provide both Client Cert and Key Secret Names for mTLS, or neither.")

            # --- Add to session and commit ---
            db.add(db_obj)
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully created storage backend config ID: {db_obj.id}, Name: {db_obj.name}, Type: {db_obj.backend_type}")
            return db_obj # Return the specific subclass instance

        except IntegrityError as e:
            db.rollback()
            logger.error(f"Database integrity error during storage backend creation for '{obj_in.name}': {e}", exc_info=True)
            # Check if constraint name is available in the error object's details (DB dependent)
            constraint_name = getattr(getattr(e, 'orig', None), 'diag', None) and getattr(e.orig.diag, 'constraint_name', None)
            if constraint_name == 'uq_storage_backend_configs_name' or "uq_storage_backend_configs_name" in str(e.orig): # Adapt check as needed
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{obj_in.name}' already exists (commit conflict).")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error creating backend '{obj_in.name}'.")
        except HTTPException: # Re-raise HTTPExceptions from validation
            raise
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error during storage backend creation for '{obj_in.name}': {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error creating backend '{obj_in.name}'.")

    def update(
        self,
        db: Session,
        *,
        db_obj: StorageBackendConfig, # Input is the specific subclass instance from get()
        obj_in: Union[schemas_storage.StorageBackendConfigUpdate, Dict[str, Any]]
    ) -> StorageBackendConfig:
        """
        Update an existing storage backend configuration.
        Handles updating common fields and fields specific to the object's type.
        Does NOT support changing the backend_type.
        """
        logger.info(f"Attempting to update storage backend config ID: {db_obj.id}, Name: {db_obj.name}, Type: {db_obj.backend_type}")

        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            # Use exclude_unset=True for proper PATCH semantics
            update_data = obj_in.model_dump(exclude_unset=True)

        if not update_data:
             logger.warning(f"Update request for storage backend config ID {db_obj.id} contained no data.")
             return db_obj # Return unchanged object

        # --- Prevent backend_type change ---
        if "backend_type" in update_data:
            logger.warning(f"Attempted to change backend_type for ID {db_obj.id}. This is not allowed. Ignoring.")
            del update_data["backend_type"]

        # --- Check Name Uniqueness (if changed) ---
        if "name" in update_data and update_data["name"] != db_obj.name:
            existing_name = self.get_by_name(db, name=update_data["name"])
            if existing_name and existing_name.id != db_obj.id:
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists.")

        logger.debug(f"Updating fields for storage backend ID {db_obj.id}: {list(update_data.keys())}")

        # --- Apply updates ---
        # Iterate through the update data and set attributes on the db_obj
        # This works because db_obj is already the correct subclass instance
        updated_fields = []
        current_values = {} # Store current values for validation if needed
        mapper = inspect(db_obj.__class__) # Get mapper for the specific subclass

        for field, value in update_data.items():
            if field in mapper.attrs: # Check if the field exists on the current model instance
                # Store current value before update for validation checks
                current_values[field] = getattr(db_obj, field, None)
                setattr(db_obj, field, value)
                updated_fields.append(field)
            else:
                 logger.warning(f"Attempted to update field '{field}' which is not valid for backend type '{db_obj.backend_type}' (ID: {db_obj.id}). Skipping.")

        if not updated_fields:
             logger.warning(f"Update request for storage backend config ID {db_obj.id} contained no valid fields for type '{db_obj.backend_type}'.")
             return db_obj

        # --- Specific Post-Update Validation (Example for CStore TLS) ---
        # Needs careful checking as we only have partial data in update_data
        if isinstance(db_obj, CStoreBackendConfig):
             # Check if tls_enabled is now True but CA is missing
             if getattr(db_obj,'tls_enabled', False) and not getattr(db_obj, 'tls_ca_cert_secret_name', None):
                  # Rollback potential attribute change before raising
                  # This logic gets complicated quickly with PATCH.
                  # Simpler might be to validate in the service layer or endpoint.
                  # For now, let's assume Pydantic's model validator on CStoreConfig handles creation logic,
                  # and updates need careful handling. Re-adding the check:
                  if db_obj.tls_enabled and not db_obj.tls_ca_cert_secret_name:
                      db.rollback() # Avoid committing invalid state
                      raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="C-STORE: CA Cert Secret Name is required when TLS is enabled.")

             # Check if only one of client cert/key is provided
             cert_provided = 'tls_client_cert_secret_name' in update_data or getattr(db_obj, 'tls_client_cert_secret_name', None)
             key_provided = 'tls_client_key_secret_name' in update_data or getattr(db_obj, 'tls_client_key_secret_name', None)
             if cert_provided != key_provided: # If one is true and the other false
                 db.rollback()
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="C-STORE: Provide both Client Cert and Key Secret Names for mTLS, or neither.")


        # --- Add to session and commit ---
        db.add(db_obj) # Add the modified object back to the session
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully updated storage backend config ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
             db.rollback()
             logger.error(f"Database integrity error during storage backend update for ID {db_obj.id}: {e}", exc_info=True)
             constraint_name = getattr(getattr(e, 'orig', None), 'diag', None) and getattr(e.orig.diag, 'constraint_name', None)
             if "name" in update_data and (constraint_name == 'uq_storage_backend_configs_name' or "uq_storage_backend_configs_name" in str(e.orig)):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists (commit conflict).")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error updating backend ID {db_obj.id}.")
        except HTTPException: # Re-raise validation errors
             raise
        except Exception as e:
             db.rollback()
             logger.error(f"Unexpected error during storage backend update for ID {db_obj.id}: {e}", exc_info=True)
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Unexpected error updating backend ID {db_obj.id}.")


    def remove(self, db: Session, *, id: int) -> StorageBackendConfig:
        """Delete a storage backend configuration by ID."""
        logger.info(f"Attempting to delete storage backend config ID: {id}")
        db_obj = self.get(db, id=id) # Gets the specific subclass instance
        if not db_obj:
            logger.warning(f"Storage backend config deletion failed: No config found with ID: {id}.")
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Storage backend config with ID {id} not found")

        # Check for relationships (Rules) before deleting?
        # You might want to add logic here to prevent deletion if it's used by a Rule.
        # Example check (requires db_obj to have relationship loaded or query it):
        # if db_obj.rules:
        #    logger.warning(f"Deletion failed for ID {id}. It is used by {len(db_obj.rules)} rules.")
        #    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Storage backend config ID {id} is currently used by rules and cannot be deleted.")

        deleted_name = db_obj.name
        deleted_type = db_obj.backend_type
        db.delete(db_obj)
        try:
            db.commit()
            logger.info(f"Successfully deleted storage backend config ID: {id}, Name: {deleted_name}, Type: {deleted_type}")
            # Note: db_obj is expired after commit, but might still hold data. Return it? Or None?
            # Returning it is consistent with previous version, but might be confusing.
            return db_obj
        except Exception as e:
            # Specific check for foreign key violations if rules check wasn't done/failed
            db.rollback()
            logger.error(f"Database error during storage backend deletion for ID {id}: {e}", exc_info=True)
            if isinstance(e, IntegrityError) and "violates foreign key constraint" in str(e.orig).lower():
                 raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Cannot delete backend config ID {id} as it is referenced by other entities (e.g., rules)."
                )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error occurred while deleting backend config ID {id}.",
            )

# Keep the singleton instance
crud_storage_backend_config = CRUDStorageBackendConfig()
