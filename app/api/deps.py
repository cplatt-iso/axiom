# app/api/deps.py
import logging
from typing import Generator, Optional, Any

from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, APIKeyHeader
from jose import jwt, JWTError
from pydantic import ValidationError, SecretStr # Ensure SecretStr is imported for type hints if needed elsewhere
from sqlalchemy.orm import Session

from app.db import models
from app import crud, schemas # Import top-level packages
from app.core import security
from app.core.config import settings
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

# OAuth2 Password Bearer flow configuration for JWT tokens.
# tokenUrl should point to the endpoint that issues tokens (used for OpenAPI docs).
# auto_error=False allows us to handle missing/invalid tokens manually and try API keys.
reusable_oauth2 = OAuth2PasswordBearer(
    tokenUrl=f"{settings.API_V1_STR}/auth/token", # Example pointing to token endpoint
    scheme_name="JWT Bearer",
    auto_error=False
)

# API Key Header configuration.
# Reads the "Authorization" header. Expects format: "Api-Key <your_key>".
# auto_error=False allows us to handle missing/invalid keys manually.
api_key_header_auth = APIKeyHeader(
    name="Authorization",
    scheme_name="API Key",
    description="Enter the **full API Key prefixed with 'Api-Key '** (e.g., 'Api-Key yourprefix_yourkey')",
    auto_error=False
)

def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency to provide a database session per request.
    Ensures the session is closed afterwards.
    """
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        yield db
    finally:
        if db is not None:
            db.close()

async def get_current_user(
    db: Session = Depends(get_db),
    bearer_token: Optional[str] = Depends(reusable_oauth2),
    api_key_header: Optional[str] = Depends(api_key_header_auth)
) -> models.User:
    """
    Dependency to authenticate the current user.

    Tries Bearer token authentication first. If that fails or is not provided,
    it attempts authentication using an API key from the 'Authorization' header
    (expected format: 'Api-Key <prefix_secret_key>').

    Raises:
        HTTPException(401): If neither method yields a valid, existing user.

    Returns:
        The authenticated User database model instance.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"}, # Indicates Bearer is a supported method
    )
    user: Optional[models.User] = None
    auth_method: Optional[str] = None # For logging which method succeeded

    # 1. Attempt Bearer Token Authentication
    if bearer_token:
        auth_method = "Bearer"
        try:
            # Extract secret value before passing to jwt.decode
            secret_key_value = settings.SECRET_KEY.get_secret_value()
            payload = jwt.decode(
                bearer_token, secret_key_value, algorithms=[settings.ALGORITHM]
            )
            # Validate payload structure and extract subject (user ID)
            token_data = schemas.TokenPayload(**payload)
            user_id = token_data.sub
            if user_id is None:
                logger.warning("[Auth] Bearer token payload missing 'sub' (subject/user ID).")
                raise ValueError("Token payload missing 'sub'") # Will be caught below

            logger.debug(f"[Auth] Attempting lookup for User ID {user_id} from Bearer token.")
            user = db.get(models.User, user_id) # Use Session.get for efficient PK lookup
            if not user:
                 logger.warning(f"[Auth] User ID {user_id} from Bearer token not found in DB.")
                 # Let it fall through to try API key or raise 401 later

        except (JWTError, ValidationError, ValueError, AttributeError) as e:
             # Log specific error but don't raise yet, allow fallback
             logger.debug(f"[Auth] Bearer token validation failed: {type(e).__name__} - {e}")
             user = None # Ensure user is None if token validation fails


    # 2. Attempt API Key Authentication if Bearer failed/missing
    if user is None and api_key_header:
        scheme, _, api_key_value = api_key_header.partition(' ')
        # Check if header uses the expected 'Api-Key' scheme
        if scheme.lower() == "api-key" and api_key_value:
            auth_method = "API Key"
            logger.debug(f"[Auth] Attempting API Key auth. Key starts with: '{api_key_value[:12]}...'")
            try:
                # Basic format check before DB lookup
                if "_" not in api_key_value or len(api_key_value) < security.API_KEY_PREFIX_LENGTH + 2:
                    logger.warning("[Auth] Invalid API Key format received.")
                    raise ValueError("Invalid API Key format")

                prefix = api_key_value[:security.API_KEY_PREFIX_LENGTH]
                logger.debug(f"[Auth] Extracted API Key prefix: {prefix}")

                # Query database for an active key matching the prefix
                db_api_key = crud.crud_api_key.get_active_key_by_prefix(db, prefix=prefix)

                if db_api_key:
                     logger.debug(f"[Auth] Found active API key ID {db_api_key.id} for prefix {prefix}. Verifying key value...")
                     # Verify the full key against the stored hash
                     is_valid = security.verify_api_key(api_key_value, db_api_key.hashed_key)
                     logger.debug(f"[Auth] Verification Result for prefix {prefix}: {is_valid}")

                     if is_valid:
                        logger.debug(f"[Auth] API Key is valid. Associating with User ID: {db_api_key.user_id}")
                        # Access the related user; handle potential loading issues
                        related_user = db_api_key.user
                        if not related_user:
                            logger.error(f"[Auth] API Key {db_api_key.id} valid but user relationship (ID: {db_api_key.user_id}) failed!")
                            # Fail authentication if user cannot be loaded
                            raise credentials_exception
                        else:
                            user = related_user # Assign the authenticated user
                            # Update the key's last used timestamp (fire and forget for this dependency)
                            try:
                                 crud.crud_api_key.update_last_used(db=db, api_key=db_api_key)
                                 # commit might happen later or need explicit handling if critical
                            except Exception as update_err:
                                 logger.warning(f"[Auth] Failed to update last_used for API key {db_api_key.id}: {update_err}")
                            logger.debug(f"[Auth] API Key authentication successful for User ID {user.id}")
                     else:
                         logger.warning(f"[Auth] Verification FAILED for prefix {prefix} (key value mismatch)")
                         # Fall through to raise 401 if user is still None
                else:
                     logger.warning(f"[Auth] No active API key found in DB for prefix: {prefix}")
                     # Fall through

            except ValueError as ve: # Catch specific validation errors
                logger.warning(f"[Auth] Error processing API Key: {ve}")
            except Exception as e: # Catch unexpected errors
                 logger.error(f"[Auth] Unexpected error during API Key processing: {e}", exc_info=True)
                 # Fall through, generic 401 will be raised

        elif scheme.lower() == "bearer":
             # Bearer token was provided but failed validation earlier. Do nothing here.
             pass
        else:
             # Header present but used an unexpected scheme
             logger.debug(f"[Auth] Authorization header ignored. Unsupported scheme: '{scheme}'")


    # 3. Final Check: If user is still None after trying all methods, raise 401
    if user is None:
        logger.warning("[Auth] Authentication failed: No valid user found.")
        raise credentials_exception

    # Authentication succeeded
    logger.info(f"[Auth] Authentication successful for User ID: {user.id} via {auth_method or 'Unknown Method'}")
    return user


def get_current_active_user(
    current_user: models.User = Depends(get_current_user),
) -> models.User:
    """
    Dependency that ensures the authenticated user is currently active.

    Raises:
        HTTPException(403): If the user is marked as inactive.
    """
    if not current_user.is_active:
        logger.warning(f"[Auth] User {current_user.id} ('{current_user.email}') is inactive. Access denied.")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Inactive user")
    return current_user


def get_current_active_superuser(
     current_user: models.User = Depends(get_current_active_user),
) -> models.User:
     """
     Dependency that ensures the authenticated user is active and is a superuser.

     Raises:
         HTTPException(403): If the user is not a superuser.
     """
     if not current_user.is_superuser:
        logger.warning(f"[Auth] User {current_user.id} ('{current_user.email}') is not a superuser. Access denied.")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="The user doesn't have enough privileges"
        )
     return current_user


def require_role(required_role: str):
    """
    Dependency factory function that returns a dependency checking for a specific user role.

    Args:
        required_role: The name of the role required for access.

    Returns:
        A FastAPI dependency function.
    """
    def role_checker(
        current_user: models.User = Depends(get_current_active_user)
    ) -> models.User:
        """Checks if the current user has the specified role."""
        # Ensure roles are loaded (adjust query loading strategy if needed)
        if current_user.roles is None: # Check if relationship wasn't loaded
             logger.error(f"User {current_user.id} roles not loaded for role check '{required_role}'. Check database query loading options.")
             # Re-fetch user with roles eagerly? Or deny access. Denying is safer default.
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cannot verify user roles.")

        has_role = any(role.name == required_role for role in current_user.roles)
        if not has_role:
            user_roles = [r.name for r in current_user.roles]
            logger.warning(f"[Auth] User {current_user.id} ('{current_user.email}') denied access. Missing role: '{required_role}'. User roles: {user_roles}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires '{required_role}' role",
            )
        logger.debug(f"[Auth] User {current_user.id} has required role '{required_role}'. Allowing access.")
        return current_user
    return role_checker
