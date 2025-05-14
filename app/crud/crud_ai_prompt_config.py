# backend/app/crud/crud_ai_prompt_config.py
from typing import List, Optional, Union, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy import select, asc # Added asc, select
import structlog 

from app.crud.base import CRUDBase
from app.db.models.ai_prompt_config import AIPromptConfig # The DB Model
from app.schemas.ai_prompt_config import ( # The Pydantic Schemas
    AIPromptConfigCreate,
    AIPromptConfigUpdate,
    # AIPromptConfigRead # Not directly used in return types here by default
)

logger = structlog.get_logger(__name__)

class CRUDAIPromptConfig(CRUDBase[AIPromptConfig, AIPromptConfigCreate, AIPromptConfigUpdate]):
    
    def get_by_name(self, db: Session, *, name: str) -> Optional[AIPromptConfig]:
        """
        Retrieves an AI Prompt Configuration by its unique name.
        """
        logger.debug("Fetching AI prompt config by name", name=name)
        return db.execute(select(self.model).filter(self.model.name == name)).scalar_one_or_none()

    def create(self, db: Session, *, obj_in: AIPromptConfigCreate) -> AIPromptConfig:
        """
        Creates a new AI Prompt Configuration.
        Name uniqueness is primarily handled by the DB constraint,
        but an early check can be done via get_by_name if desired (as in API layer).
        This CRUD method will rely on the DB for the unique constraint enforcement on commit.
        """
        logger.debug("Attempting to create AI prompt config in CRUD", name=obj_in.name)
        # The API layer now does a pre-check for name uniqueness, 
        # so this CRUD method can be simpler and rely on DB constraints.
        # If a ValueError for uniqueness is desired from CRUD, add the get_by_name check here.
        
        try:
            db_obj = super().create(db, obj_in=obj_in) # Calls CRUDBase.create
            logger.info("Successfully created AI prompt config in CRUD", id=db_obj.id, name=db_obj.name)
            return db_obj
        except ValueError as e: # Catch other ValueErrors from CRUDBase if any
            logger.error("ValueError during AI prompt config creation in CRUD", error=str(e), name=obj_in.name, exc_info=True)
            raise
        # IntegrityError (like unique constraint violation) will be raised by SQLAlchemy/DB driver
        # and should be handled by the API layer (which it does, by catching Exception).

    def update(
        self,
        db: Session,
        *,
        db_obj: AIPromptConfig, 
        obj_in: Union[AIPromptConfigUpdate, Dict[str, Any]]
    ) -> AIPromptConfig:
        """
        Updates an existing AI Prompt Configuration.
        Name uniqueness if name is changed is primarily handled by DB constraint.
        API layer performs a pre-check.
        """
        current_name = db_obj.name
        new_name_in_payload = None
        update_data_dict = {}

        if isinstance(obj_in, dict):
            update_data_dict = obj_in
            new_name_in_payload = obj_in.get("name")
        else: # Pydantic model
            update_data_dict = obj_in.model_dump(exclude_unset=True)
            if "name" in update_data_dict: # Check if name was actually provided for update
                 new_name_in_payload = update_data_dict["name"]
        
        logger.debug("Attempting to update AI prompt config in CRUD", id=db_obj.id, current_name=current_name, new_name_in_payload=new_name_in_payload)

        # The API layer now does a pre-check for name uniqueness if name is changing.
        # This CRUD method relies on DB constraints for final enforcement.

        try:
            # Pass the already prepared update_data_dict to super().update
            updated_db_obj = super().update(db, db_obj=db_obj, obj_in=update_data_dict)
            logger.info("Successfully updated AI prompt config in CRUD", id=updated_db_obj.id, name=updated_db_obj.name)
            return updated_db_obj
        except ValueError as e: 
            logger.error("ValueError during AI prompt config update in CRUD", error=str(e), id=db_obj.id, exc_info=True)
            raise
        # IntegrityError (like unique constraint violation) will be raised by SQLAlchemy/DB driver
        # and should be handled by the API layer.

    def get_multi_filtered(
        self, 
        db: Session, 
        *, 
        skip: int = 0, 
        limit: int = 100,
        is_enabled: Optional[bool] = None,
        dicom_tag_keyword: Optional[str] = None,
        model_identifier: Optional[str] = None
    ) -> List[AIPromptConfig]:
        """
        Retrieves multiple AI Prompt Configurations with optional filtering, pagination, and ordering.
        """
        logger.debug(
            "Fetching multiple AI prompt configs with filters", 
            skip=skip, limit=limit, is_enabled=is_enabled, 
            dicom_tag_keyword=dicom_tag_keyword, model_identifier=model_identifier
        )
        statement = select(self.model)
        
        if is_enabled is not None:
            statement = statement.filter(self.model.is_enabled == is_enabled)
        if dicom_tag_keyword:
            # Using '==' for exact match as per endpoint description
            statement = statement.filter(self.model.dicom_tag_keyword == dicom_tag_keyword)
        if model_identifier:
            statement = statement.filter(self.model.model_identifier == model_identifier)
            
        # Apply ordering for consistent pagination results
        statement = statement.order_by(asc(self.model.id)) # Default order by ID
        
        statement = statement.offset(skip).limit(limit)
        
        results = db.execute(statement).scalars().all()
        logger.debug("Fetched AI prompt configs count", count=len(results))
        return results

# Instantiate the CRUD object for AI Prompt Configurations
crud_ai_prompt_config = CRUDAIPromptConfig(AIPromptConfig)