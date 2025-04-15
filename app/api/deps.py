# app/api/deps.py
import logging
from typing import Generator, Optional

from fastapi import Depends, HTTPException, status, Request # Make sure Request is imported if needed later
from fastapi.security import OAuth2PasswordBearer, APIKeyHeader
from jose import jwt, JWTError
from pydantic import ValidationError
from sqlalchemy.orm import Session

# --- Corrected Model Import ---
from app.db import models
# --- End Correction ---

# Import other necessary modules
from app import crud, schemas
from app.core import security
from app.core.config import settings
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)

reusable_oauth2 = OAuth2PasswordBearer(
    tokenUrl=f"{settings.API_V1_STR}/auth/dummy-token-url",
    auto_error=False # Set auto_error=False to allow trying API key next
)

api_key_header_auth = APIKeyHeader(
    name="Authorization",
    scheme_name="API Key (Header: Authorization)",
    description="Enter the **full API Key prefixed with 'Api-Key '** (e.g., 'Api-Key yourprefix_yourkey')",
    auto_error=False
)

def get_db() -> Generator:
    """Dependency to get a DB session."""
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        yield db
    finally:
        if db is not None:
            db.close()

# --- Get Current User Dependency (Async is correct here if called by async endpoints) ---
async def get_current_user(
    db: Session = Depends(get_db),
    bearer_token: Optional[str] = Depends(reusable_oauth2),
    api_key_header: Optional[str] = Depends(api_key_header_auth)
) -> models.User:
    """
    Authenticates user via Bearer token OR API Key.
    Checks Bearer token first. If invalid or not present, checks for an
    'Authorization: Api-Key <key>' header.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    user: Optional[models.User] = None
    auth_method = "Unknown" # For logging

    # 1. Try Bearer Token
    if bearer_token:
        auth_method = "Bearer"
        try:
            payload = jwt.decode(
                bearer_token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
            )
            user_id_str = payload.get("sub")
            if user_id_str is None: raise ValueError("Token missing 'sub'")
            user_id = int(user_id_str)
            logger.debug(f"[Auth] Attempting lookup for User ID {user_id} from Bearer token.")
            user = db.get(models.User, user_id)
            if not user: logger.warning(f"[Auth] User ID {user_id} from Bearer token not found in DB.")

        except (JWTError, ValueError, ValidationError) as e:
             logger.debug(f"[Auth] Bearer token validation failed: {e}")
             pass # Fall through

    # 2. Try API Key if Bearer token failed or wasn't provided
    if user is None and api_key_header:
        auth_method = "API Key"
        scheme, _, api_key_value = api_key_header.partition(' ')
        logger.debug(f"[Auth] Attempting API Key auth. Header Received: '{api_key_header[:20]}...', Scheme: '{scheme}'") # Log header start

        if scheme.lower() == "api-key" and api_key_value:
            logger.debug(f"[Auth] API Key scheme matched. Value starts with: '{api_key_value[:12]}...'")
            try:
                if "_" not in api_key_value or len(api_key_value) < security.API_KEY_PREFIX_LENGTH + 2:
                     logger.warning(f"[Auth] Invalid API Key format received: {api_key_value[:20]}...")
                     raise ValueError("Invalid API Key format")

                prefix = api_key_value[:security.API_KEY_PREFIX_LENGTH]
                logger.debug(f"[Auth] Extracted API Key prefix: {prefix}")

                # Find active key by prefix
                db_api_key = crud.crud_api_key.get_active_key_by_prefix(db, prefix=prefix)

                if not db_api_key:
                     # CRITICAL LOG POINT 1
                     logger.warning(f"[Auth] >>> DB Lookup FAILED: No active API key found in DB for prefix: {prefix}")
                elif db_api_key:
                     # CRITICAL LOG POINT 2
                     logger.debug(f"[Auth] >>> DB Lookup SUCCESS: Found active API key ID {db_api_key.id} for prefix {prefix}. Verifying key value...")
                     is_valid = security.verify_api_key(api_key_value, db_api_key.hashed_key)
                     # CRITICAL LOG POINT 3
                     logger.warning(f"[Auth] >>> Verification Result for prefix {prefix}: {is_valid}") # Use WARNING level temporarily to ensure visibility

                     if is_valid:
                        # CRITICAL LOG POINT 4
                        logger.debug(f"[Auth] API Key is valid. Associating with User ID: {db_api_key.user_id}")
                        user = db_api_key.user # Access user via relationship (ensure relationship loaded or handle error)
                        if not user: # Check if user relationship loaded correctly
                            logger.error(f"[Auth] API Key {db_api_key.id} is valid but user relationship (ID: {db_api_key.user_id}) failed to load!")
                        else:
                             # Update last used timestamp (moved logging inside update_last_used)
                            crud.crud_api_key.update_last_used(db=db, api_key=db_api_key)
                            logger.debug(f"[Auth] API Key authentication successful for User ID {user.id}")

                     else:
                         # CRITICAL LOG POINT 5
                         logger.warning(f"[Auth] >>> Verification FAILED for prefix {prefix} (key value mismatch)")
                # If still no user, it will fall through to final check

            except Exception as e:
                 logger.error(f"[Auth] >>> Error during API Key processing: {e}", exc_info=True)
                 # Fall through

        else:
             logger.debug(f"[Auth] Authorization header present but scheme was not 'api-key' or no value provided. Scheme: '{scheme}'")

    # 3. Final Check
    if user is None:
        # CRITICAL LOG POINT 6
        logger.warning(f"[Auth] >>> Authentication failed: No valid user found after checking {auth_method} method(s). Raising 401.")
        raise credentials_exception

    # CRITICAL LOG POINT 7
    logger.debug(f"[Auth] Authentication successful for User ID: {user.id} via {auth_method}")
    return user

# --- get_current_active_user ---
def get_current_active_user(
    current_user: models.User = Depends(get_current_user),
) -> models.User:
    if not current_user.is_active:
        logger.warning(f"[Auth] Auth OK, but user {current_user.id} is INACTIVE.")
        raise HTTPException(status_code=403, detail="Inactive user")
    return current_user

# --- get_current_active_superuser ---
def get_current_active_superuser(
     current_user: models.User = Depends(get_current_active_user),
) -> models.User:
     if not current_user.is_superuser:
        logger.warning(f"[Auth] User {current_user.id} is not a superuser. Denying access.")
        raise HTTPException(
            status_code=403, detail="The user doesn't have enough privileges"
        )
     return current_user

# --- require_role ---
def require_role(required_role: str):
    # ... (existing require_role code, add logging if needed) ...
    def role_checker(
        current_user: models.User = Depends(get_current_active_user)
    ) -> models.User:
        has_role = any(role.name == required_role for role in current_user.roles)
        if not has_role:
            logger.warning(f"[Auth] User {current_user.id} denied access. Missing role: '{required_role}'. User roles: {[r.name for r in current_user.roles]}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires '{required_role}' role",
            )
        logger.debug(f"[Auth] User {current_user.id} has required role '{required_role}'. Allowing access.")
        return current_user
    return role_checker
