# backend/app/api/api_v1/endpoints/config_ai_prompts.py
from typing import List, Any, Optional, Dict # Added Dict

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query # Removed Body as it wasn't used directly here
from sqlalchemy.orm import Session

from app import crud, schemas
from app.api import deps
from app.db import models
# from app.db.models.user import User # Not actively used in current functions

# Placeholder for Vertex AI service availability (assuming these are defined elsewhere, e.g. ai_assist_service.py and imported)
# For this file to be self-contained for now, let's define them simply here.
# In a real setup, you'd import these from your actual service module.
try:
    from app.services.ai_assist_service import VERTEX_AI_INITIALIZED_SUCCESSFULLY, VERTEX_AI_AVAILABLE
except ImportError:
    # Fallback if ai_assist_service is not available or these are not defined there
    logger = structlog.get_logger(__name__) # Need a logger instance here for this case
    logger.warning("Could not import VERTEX_AI_INITIALIZED_SUCCESSFULLY, VERTEX_AI_AVAILABLE from ai_assist_service. Using placeholders.")
    VERTEX_AI_INITIALIZED_SUCCESSFULLY = False # Default to False if import fails
    VERTEX_AI_AVAILABLE = False


router = APIRouter()
logger = structlog.get_logger(__name__)


@router.post(
    "/",
    response_model=schemas.AIPromptConfigRead,
    status_code=201,
    summary="Create AI Prompt Configuration",
)
def create_ai_prompt_config(
    *,
    db: Session = Depends(deps.get_db),
    ai_prompt_config_in: schemas.AIPromptConfigCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Create a new AI Prompt Configuration.
    - **name**: Must be unique.
    - **is_enabled**: Whether the configuration is active.
    - **dicom_tag_keyword**: The DICOM tag keyword this prompt applies to.
    - **prompt_template**: The template string, must include `{value}`.
    - **model_identifier**: The AI model to use (e.g., "gemini-1.5-flash-001").
    - **model_parameters**: Optional JSON for AI model params.
    """
    log = logger.bind(
        endpoint="create_ai_prompt_config",
        config_name=ai_prompt_config_in.name,
    )
    log.info("Attempting to create AI prompt configuration.")
    # Check for uniqueness of name if not handled by DB constraint directly in CRUD
    existing_config = crud.crud_ai_prompt_config.get_by_name(db, name=ai_prompt_config_in.name)
    if existing_config:
        log.warning("AI prompt configuration with this name already exists.", config_name=ai_prompt_config_in.name)
        raise HTTPException(
            status_code=409, # 409 Conflict is more appropriate
            detail=f"An AI Prompt Configuration with name '{ai_prompt_config_in.name}' already exists.",
        )
    try:
        ai_prompt_config = crud.crud_ai_prompt_config.create(db=db, obj_in=ai_prompt_config_in)
        log.info("AI prompt configuration created successfully.", config_id=ai_prompt_config.id)
        return ai_prompt_config
    except ValueError as e: 
        log.warning("Failed to create AI prompt configuration due to ValueError.", error_details=str(e))
        # This might be redundant if name check is above, but good for other ValueErrors from CRUD
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e: 
        log.error("Unexpected error creating AI prompt configuration.", error_details=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred on the server.")


@router.get(
    "/{config_id}",
    response_model=schemas.AIPromptConfigRead,
    summary="Get AI Prompt Configuration by ID",
)
def read_ai_prompt_config(
    *,
    db: Session = Depends(deps.get_db),
    config_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
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
    response_model=List[schemas.AIPromptConfigRead],
    summary="List AI Prompt Configurations",
)
def read_ai_prompt_configs(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=200, description="Maximum number of items to return"),
    is_enabled: Optional[bool] = Query(None, description="Filter by enabled status (true/false)"),
    dicom_tag_keyword: Optional[str] = Query(None, description="Filter by DICOM tag keyword (exact match)"),
    model_identifier: Optional[str] = Query(None, description="Filter by AI model identifier (exact match)"),
    current_user: models.User = Depends(deps.get_current_active_user),

) -> Any:
    """
    Retrieve a list of AI Prompt Configurations. Supports pagination and filtering.
    """
    log = logger.bind(endpoint="read_ai_prompt_configs", skip=skip, limit=limit, 
                      filter_is_enabled=is_enabled, filter_dicom_tag_keyword=dicom_tag_keyword,
                      filter_model_identifier=model_identifier)
    log.debug("Attempting to list AI prompt configurations.")
    
    # Pass filters to your CRUD function
    ai_prompt_configs = crud.crud_ai_prompt_config.get_multi_filtered(
        db, 
        skip=skip, 
        limit=limit,
        is_enabled=is_enabled,
        dicom_tag_keyword=dicom_tag_keyword,
        model_identifier=model_identifier
    )
    log.info("AI prompt configurations listed.", count=len(ai_prompt_configs))
    return ai_prompt_configs


@router.put(
    "/{config_id}",
    response_model=schemas.AIPromptConfigRead,
    summary="Update AI Prompt Configuration",
)
def update_ai_prompt_config(
    *,
    db: Session = Depends(deps.get_db),
    config_id: int,
    ai_prompt_config_in: schemas.AIPromptConfigUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
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
    
    # Check if the new name (if provided) conflicts with another existing config
    if ai_prompt_config_in.name and ai_prompt_config_in.name != db_obj.name:
        existing_config_with_new_name = crud.crud_ai_prompt_config.get_by_name(db, name=ai_prompt_config_in.name)
        if existing_config_with_new_name and existing_config_with_new_name.id != config_id:
            log.warning("AI prompt configuration with the new name already exists.", new_name=ai_prompt_config_in.name)
            raise HTTPException(
                status_code=409,
                detail=f"An AI Prompt Configuration with name '{ai_prompt_config_in.name}' already exists.",
            )
    try:
        updated_config = crud.crud_ai_prompt_config.update(db=db, db_obj=db_obj, obj_in=ai_prompt_config_in)
        log.info("AI prompt configuration updated successfully.")
        return updated_config
    except ValueError as e: 
        log.warning("Failed to update AI prompt configuration due to ValueError.", error_details=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error("Unexpected error updating AI prompt configuration.", error_details=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred on the server.")


@router.delete(
    "/{config_id}",
    response_model=schemas.AIPromptConfigRead, 
    summary="Delete AI Prompt Configuration",
)
def delete_ai_prompt_config(
    *,
    db: Session = Depends(deps.get_db),
    config_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
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
    
    try:
        # Before deleting, check if it's used by any rules
        rules_using_config = crud.crud_rule.get_rules_by_ai_prompt_config_id(db, config_id=config_id)
        if rules_using_config:
            rule_names = [rule.name for rule in rules_using_config]
            log.warning(
                "Attempt to delete AI Prompt Config that is currently used by rules.",
                config_id=config_id,
                config_name=db_obj.name,
                rules_using_it=rule_names
            )
            raise HTTPException(
                status_code=409, # Conflict
                detail=f"Cannot delete AI Prompt Configuration '{db_obj.name}' (ID: {config_id}) because it is currently used by the following rules: {', '.join(rule_names)}. Please remove it from these rules first."
            )
        
        deleted_config = crud.crud_ai_prompt_config.remove(db=db, id=config_id)
        log.info("AI prompt configuration deleted successfully.")
        return deleted_config 
    except ValueError as e: 
        log.error("ValueError during AI prompt configuration deletion.", error_details=str(e), exc_info=True)
        raise HTTPException(status_code=409, detail=f"Cannot delete configuration: {str(e)}")
    except HTTPException: # Re-raise HTTPException (like the 409 above)
        raise
    except Exception as e:
        log.error("Unexpected error deleting AI prompt configuration.", error_details=str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred on the server.")

# --- NEW ENDPOINT FOR LISTING AVAILABLE VERTEX AI MODELS ---
# Placeholder function for fetching models - replace with actual SDK call later
def get_vertex_generative_models_placeholder() -> List[Dict[str, str]]:
    """
    Returns a placeholder list of commonly used Vertex AI generative models.
    Replace this with a dynamic SDK call in the future.
    The 'id' should be the string used when initializing GenerativeModel(model_name="YOUR_MODEL_ID").
    """
    logger.debug("Using placeholder list for available Vertex AI models.")
    return [
        {"id": "gemini-2.5-flash-preview-05-20", "display_name": "Gemini 2.5 Flash Preview", "description": "Gemini 2.5 models are thinking models, capable of reasoning through their thoughts before responding, resulting in enhanced performance and improved accuracy."},
        {"id": "gemini-2.0-flash-001 ", "display_name": "Gemini 2.0 Flash", "description": "Gemini 2.0 Flash is a workhorse model that delivers strong overall performance and provides low-latency support for real-time streaming."},
        {"id": "gemini-1.5-pro-001", "display_name": "Gemini 1.5 Pro", "description": "Next generation model that achieves Pro-level quality at a lower cost than 1.0 Pro, with improved instruction following, editing, and more."},
        {"id": "gemini-1.0-pro-002", "display_name": "Gemini 1.0 Pro", "description": "Mid-size multimodal model, performs well at a variety of multimodal tasks."},
        # {"id": "gemini-1.0-pro-vision-001", "display_name": "Gemini 1.0 Pro Vision", "description": "Multimodal model that supports vision tasks."}, # If you plan to use vision
        # Add other known/supported Vertex model IDs here
        # It's crucial that these IDs are what the Vertex AI SDK's GenerativeModel() constructor expects.
    ]

@router.get(
    "/available-models/vertex-ai", # Consider making this path part of an "ai-assist" group later
    response_model=List[Dict[str, str]],
    summary="List Available Vertex AI Generative Models (Placeholder)",
    tags=["AI Configuration", "AI Assist"] # Added tags for Swagger UI grouping
)
def list_available_vertex_ai_models(
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Provides a list of available Vertex AI generative models that can be used
    as the `model_identifier` in AI Prompt Configurations.
    
    **(Note: This currently returns a placeholder list. Dynamic SDK-based listing is a future enhancement.)**
    """
    # Check if Vertex AI is configured/available in your system (using the imported flags)
    if not VERTEX_AI_AVAILABLE:
        log = logger.bind(endpoint="list_available_vertex_ai_models")
        log.warning("Vertex AI SDK not available. Cannot list models.")
        raise HTTPException(
            status_code=503, # Service Unavailable
            detail="Vertex AI SDK is not available in the current environment."
        )
    if not VERTEX_AI_INITIALIZED_SUCCESSFULLY:
        log = logger.bind(endpoint="list_available_vertex_ai_models")
        log.warning("Vertex AI not initialized successfully. Cannot list models.")
        raise HTTPException(
            status_code=503, # Service Unavailable
            detail="Vertex AI has not been initialized successfully. Check configuration and credentials."
        )
        
    try:
        models = get_vertex_generative_models_placeholder()
        return models
    except Exception as e:
        log = logger.bind(endpoint="list_available_vertex_ai_models")
        log.error("Failed to list Vertex AI models (using placeholder).", error_details=str(e), exc_info=True)
        # This error is less likely with a placeholder but good for future SDK integration
        raise HTTPException(status_code=500, detail=f"Failed to retrieve Vertex AI models: {str(e)}")