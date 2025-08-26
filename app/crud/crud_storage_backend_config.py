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
import structlog
# Import the specific Pydantic Read/Create/Update schemas
from app.schemas import storage_backend_config as schemas_storage # Use alias

logger = structlog.get_logger(__name__)

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
            constraint_name = None
            # Safely extract constraint_name using getattr
            _orig_exc = getattr(e, 'orig', None)
            if _orig_exc is not None:
                _diag_obj = getattr(_orig_exc, 'diag', None)
                if _diag_obj is not None:
                    _extracted_name = getattr(_diag_obj, 'constraint_name', None)
                    if _extracted_name is not None:
                        constraint_name = _extracted_name
            
            if constraint_name == 'uq_storage_backend_configs_name' or \
               (isinstance(e.orig, Exception) and "uq_storage_backend_configs_name" in str(e.orig)): # Adapt check as needed
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
        db_obj: StorageBackendConfig, 
        obj_in: Union[schemas_storage.StorageBackendConfigUpdate, Dict[str, Any]]
    ) -> StorageBackendConfig:
        logger.info(f"Attempting to update storage backend config ID: {db_obj.id}, Name: {db_obj.name}, Type: {db_obj.backend_type}")

        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)

        if not update_data:
             logger.warning(f"Update request for storage backend config ID {db_obj.id} contained no data.")
             return db_obj 

        if "backend_type" in update_data:
            logger.warning(f"Attempted to change backend_type for ID {db_obj.id}. This is not allowed. Ignoring.")
            del update_data["backend_type"]

        if "name" in update_data and update_data["name"] != db_obj.name:
            existing_name = self.get_by_name(db, name=update_data["name"])
            if existing_name and existing_name.id != db_obj.id:
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists.")

        logger.debug(f"Updating fields for storage backend ID {db_obj.id}: {list(update_data.keys())}")

        updated_fields = []
        current_values = {} 
        mapper = inspect(db_obj.__class__) 

        for field, value in update_data.items():
            if field in mapper.attrs: 
                current_values[field] = getattr(db_obj, field, None)
                setattr(db_obj, field, value)
                updated_fields.append(field)
            else:
                 logger.warning(f"Attempted to update field '{field}' which is not valid for backend type '{db_obj.backend_type}' (ID: {db_obj.id}). Skipping.")

        if not updated_fields:
             logger.warning(f"Update request for storage backend config ID {db_obj.id} contained no valid fields for type '{db_obj.backend_type}'.")
             return db_obj

        # --- Specific Post-Update Validation ---
        if isinstance(db_obj, CStoreBackendConfig):
             if db_obj.tls_enabled and not db_obj.tls_ca_cert_secret_name:
                 db.rollback() 
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="C-STORE: CA Cert Secret Name is required when TLS is enabled.")

             cert_provided_in_update = 'tls_client_cert_secret_name' in update_data
             key_provided_in_update = 'tls_client_key_secret_name' in update_data
             
             current_cert = current_values.get('tls_client_cert_secret_name', db_obj.tls_client_cert_secret_name)
             current_key = current_values.get('tls_client_key_secret_name', db_obj.tls_client_key_secret_name)

             final_cert = update_data.get('tls_client_cert_secret_name', current_cert)
             final_key = update_data.get('tls_client_key_secret_name', current_key)

             if (final_cert and not final_key) or (not final_cert and final_key):
                 db.rollback()
                 raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="C-STORE: Provide both Client Cert and Key Secret Names for mTLS, or neither.")
        
        # +++ ADDED VALIDATION FOR STOW-RS ON UPDATE +++
        if isinstance(db_obj, StowRsBackendConfig):
            # Use db_obj.auth_type as it reflects the value after setattr
            # This means if auth_type is part of update_data, db_obj.auth_type is the NEW type.
            # If auth_type was not in update_data, db_obj.auth_type is the original type.
            
            auth_type_to_validate = db_obj.auth_type # This is the state after setattr
            
            # Retrieve current values for secret names BEFORE potential nulling
            # to check if they were provided in this update if auth_type changes to one that needs them.
            # Or, more simply, just check the state of db_obj after all setattrs.

            if auth_type_to_validate == "basic":
                if not db_obj.basic_auth_username_secret_name or not db_obj.basic_auth_password_secret_name:
                    db.rollback()
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="STOW-RS: If auth_type is 'basic', both username and password secret names are required.")
            elif auth_type_to_validate == "bearer":
                if not db_obj.bearer_token_secret_name:
                    db.rollback()
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="STOW-RS: If auth_type is 'bearer', bearer_token_secret_name is required.")
            elif auth_type_to_validate == "apikey":
                if not db_obj.api_key_secret_name or not db_obj.api_key_header_name_override:
                    db.rollback()
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="STOW-RS: If auth_type is 'apikey', api_key_secret_name and api_key_header_name_override are required.")
            elif auth_type_to_validate == "none" or auth_type_to_validate is None: # If auth_type is being set to "none" or cleared
                # Null out other auth fields if changing to "none" or if auth_type is removed (becomes None)
                # This ensures DB consistency if auth_type is explicitly set to "none" in the update.
                fields_to_null = [
                    'basic_auth_username_secret_name', 'basic_auth_password_secret_name',
                    'bearer_token_secret_name', 'api_key_secret_name', 'api_key_header_name_override'
                ]
                nulled_any = False
                for field_name in fields_to_null:
                    if getattr(db_obj, field_name, None) is not None:
                        # Only set to None if it was not ALREADY None and auth_type is now "none"
                        # This check is implicitly handled by setattr if `value` is None in `update_data`
                        # The explicit check is more about enforcing that if auth_type is "none", these *must* be None.
                        # However, the current loop only sets what's in update_data.
                        # So, if auth_type becomes "none", we should ensure these are nulled.
                        if update_data.get('auth_type') == "none" or update_data.get('auth_type') is None and field_name not in update_data : # if auth_type is set to none/null in update
                             setattr(db_obj, field_name, None) # Force null these fields
                             nulled_any = True
                if nulled_any:
                    logger.info(f"Nulled STOW-RS auth secret names for ID {db_obj.id} due to auth_type change to '{auth_type_to_validate}'.")
        # +++ END ADDED VALIDATION +++

        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
            logger.info(f"Successfully updated storage backend config ID: {db_obj.id}")
            return db_obj
        except IntegrityError as e:
            # ... (existing integrity error handling) ...
             db.rollback()
             logger.error(f"Database integrity error during storage backend update for ID {db_obj.id}: {e}", exc_info=True)
             constraint_name = None
             _orig_exc = getattr(e, 'orig', None)
             if _orig_exc is not None:
                 _diag_obj = getattr(_orig_exc, 'diag', None)
                 if _diag_obj is not None:
                     _extracted_name = getattr(_diag_obj, 'constraint_name', None)
                     if _extracted_name is not None:
                         constraint_name = _extracted_name

             if "name" in update_data and \
                (constraint_name == 'uq_storage_backend_configs_name' or \
                 (isinstance(e.orig, Exception) and "uq_storage_backend_configs_name" in str(e.orig))):
                 raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Name '{update_data['name']}' already exists (commit conflict).")
             raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Database error updating backend ID {db_obj.id}.")
        except HTTPException: 
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
