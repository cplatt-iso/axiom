# backend/app/crud/crud_ai_prompt_config.py
from typing import List, Optional, Union, Dict, Any

from sqlalchemy.orm import Session
import structlog # Using structlog as requested

from app.crud.base import CRUDBase
from app.db.models.ai_prompt_config import AIPromptConfig # The DB Model
from app.schemas.ai_prompt_config import ( # The Pydantic Schemas
    AIPromptConfigCreate,
    AIPromptConfigUpdate,
    AIPromptConfigRead # For potential type hinting or future use, though CRUDBase returns ModelType
)

logger = structlog.get_logger(__name__)

class CRUDAIPromptConfig(CRUDBase[AIPromptConfig, AIPromptConfigCreate, AIPromptConfigUpdate]):
    
    def get_by_name(self, db: Session, *, name: str) -> Optional[AIPromptConfig]:
        """
        Retrieves an AI Prompt Configuration by its unique name.
        """
        return db.query(self.model).filter(self.model.name == name).first()

    def create(self, db: Session, *, obj_in: AIPromptConfigCreate) -> AIPromptConfig:
        """
        Creates a new AI Prompt Configuration.
        Checks for name uniqueness before creation.
        """
        logger.debug("Attempting to create AI prompt config", name=obj_in.name, tag_keyword=obj_in.dicom_tag_keyword)
        existing_config = self.get_by_name(db, name=obj_in.name)
        if existing_config:
            logger.warning("AI prompt config with this name already exists", name=obj_in.name)
            # Consider raising a more specific exception that can be caught by the API layer
            # to return a 409 Conflict or similar, instead of a generic ValueError from CRUDBase.
            # For now, letting CRUDBase's IntegrityError handling catch it if DB constraint exists,
            # or this custom ValueError.
            raise ValueError(f"An AI Prompt Configuration with the name '{obj_in.name}' already exists.")
        
        # Additional business logic check:
        # Ensure that a prompt for a specific dicom_tag_keyword is reasonably unique if needed,
        # e.g., you might not want 10 different prompts all for "BodyPartExamined" unless distinguished by name.
        # This is more of a business rule than a strict DB constraint.
        # For now, only name is strictly unique by DB and this check.

        # Call the parent CRUDBase create method
        try:
            db_obj = super().create(db, obj_in=obj_in)
            logger.info("Successfully created AI prompt config", id=db_obj.id, name=db_obj.name)
            return db_obj
        except ValueError as e: # Catch ValueError from CRUDBase or this class
            logger.error("Error creating AI prompt config", error_details=str(e), name=obj_in.name, exc_info=True)
            raise e # Re-raise to be handled by API layer


    def update(
        self,
        db: Session,
        *,
        db_obj: AIPromptConfig, # The existing DB object instance
        obj_in: Union[AIPromptConfigUpdate, Dict[str, Any]]
    ) -> AIPromptConfig:
        """
        Updates an existing AI Prompt Configuration.
        Checks for name uniqueness if the name is being changed.
        """
        current_name = db_obj.name
        new_name = None

        if isinstance(obj_in, dict):
            new_name = obj_in.get("name")
        else: # Pydantic model
            # obj_in.model_dump(exclude_unset=True) will be used by super().update()
            # We need to check if 'name' is part of the update payload
            if obj_in.model_fields_set and "name" in obj_in.model_fields_set:
                 new_name = obj_in.name
        
        logger.debug("Attempting to update AI prompt config", id=db_obj.id, current_name=current_name, new_name_in_payload=new_name)

        if new_name and new_name != current_name:
            existing_with_new_name = self.get_by_name(db, name=new_name)
            if existing_with_new_name and existing_with_new_name.id != db_obj.id:
                logger.warning("AI prompt config with the new name already exists", new_name=new_name, existing_id=existing_with_new_name.id)
                raise ValueError(f"An AI Prompt Configuration with the name '{new_name}' already exists.")

        # Call the parent CRUDBase update method
        try:
            updated_db_obj = super().update(db, db_obj=db_obj, obj_in=obj_in)
            logger.info("Successfully updated AI prompt config", id=updated_db_obj.id, name=updated_db_obj.name)
            return updated_db_obj
        except ValueError as e: # Catch ValueError from CRUDBase
            logger.error("Error updating AI prompt config", error_details=str(e), id=db_obj.id, exc_info=True)
            raise e # Re-raise
            
    # get, get_multi, remove are inherited from CRUDBase and should work as is.
    # If you need custom logic for them (e.g., special filtering for get_multi),
    # you can override them here.

# Instantiate the CRUD object for AI Prompt Configurations
crud_ai_prompt_config = CRUDAIPromptConfig(AIPromptConfig)