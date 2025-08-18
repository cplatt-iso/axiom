# app/api/api_v1/endpoints/auth.py (New File)
from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.orm import Session

from app import crud, schemas # Make sure crud.__init__ exports user CRUD
from app.api import deps # Assuming deps.py exists for get_db
from app.core.security import create_access_token, verify_google_token
# from app.db.models.user import User  # Import User model if needed

router = APIRouter()

@router.post("/google", response_model=schemas.TokenResponse)
async def login_google_access_token(
    *,
    db: Session = Depends(deps.get_db),
    google_token: schemas.GoogleToken = Body(...) # Receive token in request body
):
    """
    OAuth2 compatible token login, get an access token for future requests using Google ID token
    """
    try:
        google_info = await verify_google_token(google_token.token)
        if not google_info:
             # Should not happen if verify_google_token raises ValueError properly
             raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Could not validate Google credentials (no info)",
            )

    except ValueError as e:
         raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Could not validate Google credentials: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )

    google_id = google_info.get("sub")
    email = google_info.get("email")

    if not google_id or not email:
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Google token missing required claims (sub, email).",
        )

    # Try finding user by Google ID first, then by email
    user = crud.crud_user.get_user_by_google_id(db, google_id=google_id)
    if not user:
        user = crud.crud_user.get_user_by_email(db, email=email)
        if user:
            # User exists by email, but Google ID might be missing, update it
            user = crud.crud_user.update_user_google_info(db, db_user=user, google_info=google_info)
        else:
            # User does not exist, create new user from Google info
            try:
                user = crud.crud_user.create_user_from_google(db, google_info=google_info)
            except ValueError as e: # Handle errors like non-verified email
                 raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Could not create user: {e}",
                 )

    if not user or not user.is_active:
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, # Or 403 Forbidden
            detail="User not found or inactive.",
        )

    # Create an access token specific to *our* application
    access_token = create_access_token(
        subject=user.id # Use internal user ID as subject
    )

    # Prepare user info for response (use Pydantic schema for validation)
    user_response = schemas.User.model_validate(user) # Convert User model to User schema


    return schemas.TokenResponse(
        access_token=access_token,
        token_type="bearer",
        user=user_response # Include user details in response
    )
