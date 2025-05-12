# backend/app/api/api_v1/endpoints/config_ai_prompts.py
from typing import List, Any, Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Body # Added Body
from sqlalchemy.orm import Session

from app import crud, schemas # Assuming your __init__.py in app/schemas and app/crud are set up
from app.api import deps # For DB session and current user
from app.db.models.user import User # For current_user type hint, if needed for permissions

router = APIRouter()
logger = structlog.get_logger(__name__)

# Define permissions if needed - for now, assuming authenticated user can manage
# You might want to restrict this to admin users, e.g., using deps.get_current_active_superuser

@router.post(
    "/",
    response_model=schemas.AIPromptConfigRead,
    status_code=201, # Correct status code for resource creation
    summary="Create AI Prompt Configuration",
    # dependencies=[Depends(deps.get_current_active_superuser)] # Example: admin only
)
def create_ai_prompt_config(
    *,
    db: Session = Depends(deps.get_db),
    ai_prompt_config_in: schemas.AIPromptConfigCreate,
    # current_user: User = Depends(deps.get_current_active_user), # If you need user info
) -> Any:
    """
    Create a new AI Prompt Configuration.
    - **name**: Must be unique.
    - **dicom_tag_keyword**: The DICOM tag keyword this prompt applies to.
    - **prompt_template**: The template string, must include `{value}`.
    - **model_identifier**: The AI model to use (e.g., "gemini-1.5-flash-001").
    - **model_parameters**: Optional JSON for AI model params.
    """
    log = logger.bind(
        endpoint="create_ai_prompt_config",
        config_name=ai_prompt_config_in.name,
        # user_id=current_user.id if hasattr(current_user, 'id') else "anonymous" # Example if user context is added
    )
    log.info("Attempting to create AI prompt configuration.")
    try:
        ai_prompt_config = crud.crud_ai_prompt_config.create(db=db, obj_in=ai_prompt_config_in)
        log.info("AI prompt configuration created successfully.", config_id=ai_prompt_config.id)
        return ai_prompt_config
    except ValueError as e: # Catch ValueError from CRUD layer (e.g., name exists)
        log.warning("Failed to create AI prompt configuration due to ValueError.", error_details=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e: # Catch any other unexpected errors
        log.error("Unexpected error creating AI prompt configuration.", error_details=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred on the server.")


@router.get(
    "/{config_id}",
    response_model=schemas.AIPromptConfigRead,
    summary="Get AI Prompt Configuration by ID",
    # dependencies=[Depends(deps.get_current_active_user)] # Example: any authenticated user
)
def read_ai_prompt_config(
    *,
    db: Session = Depends(deps.get_db),
    config_id: int, # Assuming int ID from your Base model
    # current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Get a specific AI Prompt Configuration by its ID.
    """
    log = logger.bind(endpoint="read_ai_prompt_config", config_id=config_id)
    log.debug("Attempting to read AI prompt configuration by ID.")
    
    ai_prompt_config = crud.crud_ai_prompt_config.get(db=db, id=config_id)
    if not ai_prompt_config:
        log.warning("AI prompt configuration not found.")
        raise HTTPException(status_code=404, detail="AI Prompt Configuration not found.")
    log.debug("AI prompt configuration retrieved successfully.")
    return ai_prompt_config


@router.get(
    "/",
    response_model=List[schemas.AIPromptConfigRead], # Or AIPromptConfigSummary if you prefer
    summary="List AI Prompt Configurations",
    # dependencies=[Depends(deps.get_current_active_user)]
)
def read_ai_prompt_configs(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=200, description="Maximum number of items to return"),
    # current_user: User = Depends(deps.get_current_active_user),
    # You could add query parameters for filtering here, e.g., by dicom_tag_keyword
    # dicom_tag_keyword: Optional[str] = Query(None, description="Filter by DICOM tag keyword")
) -> Any:
    """
    Retrieve a list of AI Prompt Configurations. Supports pagination.
    """
    log = logger.bind(endpoint="read_ai_prompt_configs", skip=skip, limit=limit)
    log.debug("Attempting to list AI prompt configurations.")
    
    # Add filtering logic here if query parameters like dicom_tag_keyword are implemented
    # For now, just using the basic get_multi
    ai_prompt_configs = crud.crud_ai_prompt_config.get_multi(db, skip=skip, limit=limit)
    log.info("AI prompt configurations listed.", count=len(ai_prompt_configs))
    return ai_prompt_configs


@router.put(
    "/{config_id}",
    response_model=schemas.AIPromptConfigRead,
    summary="Update AI Prompt Configuration",
    # dependencies=[Depends(deps.get_current_active_superuser)] # Example: admin only
)
def update_ai_prompt_config(
    *,
    db: Session = Depends(deps.get_db),
    config_id: int,
    ai_prompt_config_in: schemas.AIPromptConfigUpdate,
    # current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Update an existing AI Prompt Configuration.
    Provide only the fields you want to change.
    """
    log = logger.bind(endpoint="update_ai_prompt_config", config_id=config_id)
    log.info("Attempting to update AI prompt configuration.")

    db_obj = crud.crud_ai_prompt_config.get(db=db, id=config_id)
    if not db_obj:
        log.warning("AI prompt configuration not found for update.")
        raise HTTPException(status_code=404, detail="AI Prompt Configuration not found.")
    
    try:
        updated_config = crud.crud_ai_prompt_config.update(db=db, db_obj=db_obj, obj_in=ai_prompt_config_in)
        log.info("AI prompt configuration updated successfully.")
        return updated_config
    except ValueError as e: # Catch ValueError from CRUD (e.g., name conflict)
        log.warning("Failed to update AI prompt configuration due to ValueError.", error_details=str(e))
        raise HTTPException(status_code=400, detail=str(e)) # Or 409 Conflict if specifically a name conflict
    except Exception as e:
        log.error("Unexpected error updating AI prompt configuration.", error_details=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred on the server.")


@router.delete(
    "/{config_id}",
    response_model=schemas.AIPromptConfigRead, # Returns the deleted object
    summary="Delete AI Prompt Configuration",
    # dependencies=[Depends(deps.get_current_active_superuser)] # Example: admin only
)
def delete_ai_prompt_config(
    *,
    db: Session = Depends(deps.get_db),
    config_id: int,
    # current_user: User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Delete an AI Prompt Configuration by its ID.
    """
    log = logger.bind(endpoint="delete_ai_prompt_config", config_id=config_id)
    log.info("Attempting to delete AI prompt configuration.")

    db_obj = crud.crud_ai_prompt_config.get(db=db, id=config_id)
    if not db_obj:
        log.warning("AI prompt configuration not found for deletion.")
        raise HTTPException(status_code=404, detail="AI Prompt Configuration not found.")
    
    # Business logic check: Is this config_id used in any Rule.ai_prompt_config_ids?
    # If so, should we prevent deletion or nullify the reference in rules?
    # For now, we'll let it delete. If there's a foreign key from Rule to AIPromptConfig
    # (which there ISN'T, it's just a list of IDs in a JSONB field), the DB might prevent it.
    # Since it's just a list of IDs, deleting a config will leave "dangling IDs" in rules.
    # This is something to consider for future enhancements (e.g., a cleanup task or a check here).
    # For now, keeping it simple.

    try:
        deleted_config = crud.crud_ai_prompt_config.remove(db=db, id=config_id)
        # remove() in CRUDBase returns the object or None if not found, but we check above.
        log.info("AI prompt configuration deleted successfully.")
        return deleted_config # Will be the object that was deleted
    except ValueError as e: # Catch from CRUDBase remove if it raises on FK issues (it does)
        log.error("Failed to delete AI prompt configuration due to database constraint.", error_details=str(e), exc_info=True)
        # This might indicate it's still referenced if you had actual FK constraints,
        # but with JSONB list of IDs, this specific ValueError from CRUDBase's remove is less likely
        # unless other IntegrityErrors occur.
        raise HTTPException(status_code=409, detail=f"Cannot delete configuration: {str(e)}")
    except Exception as e:
        log.error("Unexpected error deleting AI prompt configuration.", error_details=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred on the server.")