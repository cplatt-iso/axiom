# app/api/api_v1/endpoints/ai_assist.py
import logging
from fastapi import APIRouter, Depends, HTTPException, status
import structlog

from app import schemas
from app.api import deps
from app.services import ai_assist_service # Import the new service
from app.core.config import settings # Import settings to check API key

logger = structlog.get_logger(__name__)
router = APIRouter()

@router.post(
    "/suggest-rule",
    response_model=schemas.RuleGenResponse,
    summary="Get AI Assistance for Rule Generation",
    description="Takes a natural language prompt and attempts to generate corresponding rule criteria and modifications using an AI model.",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(deps.get_current_active_user)], # Require user login
    tags=["AI Assist"],
    responses={
        status.HTTP_501_NOT_IMPLEMENTED: {"description": "AI service is not configured or available."},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "AI service temporary unavailable or connection error."},
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid request prompt."},
        status.HTTP_429_TOO_MANY_REQUESTS: {"description": "AI service rate limit exceeded."},
    }
)
async def suggest_rule_from_prompt(
    *,
    request_data: schemas.RuleGenRequest,
    # current_user: models.User = Depends(deps.get_current_active_user), # Already included in dependencies
) -> schemas.RuleGenResponse:
    """
    Provides AI-powered suggestions for rule components based on a user's natural language description.
    Requires the OpenAI API key to be configured in the backend environment.
    """
    # Check if the service is available (based on client initialization)
    if not ai_assist_service.OPENAI_AVAILABLE or not ai_assist_service.openai_client:
        logger.warning("AI rule suggestion requested, but AI service is not available/configured.")
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="AI assistance feature is not configured or available."
        )

    try:
        response = await ai_assist_service.generate_rule_suggestion(request_data)

        # Handle specific error types returned by the service to set appropriate HTTP status codes
        if response.error:
            if "rate limit" in response.error.lower():
                raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=response.error)
            elif "connect" in response.error.lower():
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=response.error)
            elif "invalid json" in response.error.lower() or "structure invalid" in response.error.lower():
                 # This might indicate an issue with the AI's response format
                 logger.error(f"AI Response Format Error: {response.error}")
                 # Return 500 as it's an unexpected backend/AI issue, not usually client's fault
                 raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error processing AI response.")
            else:
                # For other errors reported by the AI (e.g., ambiguity), return 400
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=response.error)

        # If no error, return the successful response (should be 200 OK by default)
        return response

    except HTTPException as http_exc:
        # Re-raise known HTTP exceptions
        raise http_exc
    except Exception as e:
        # Catch any unexpected errors during the process
        logger.error(f"Unexpected error in /suggest-rule endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred while generating the rule suggestion."
        )
