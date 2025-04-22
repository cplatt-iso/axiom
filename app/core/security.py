# app/core/security.py
import secrets
import string
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Union, Optional, Dict

# Security libraries
from jose import jwt, JWTError # Import base JWTError
from passlib.context import CryptContext
from pydantic import SecretStr

# Application specific imports
from app.core.config import settings
from app.schemas import TokenPayload # Assuming TokenPayload schema exists

# Imports for Google Token Verification
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
import cachecontrol
import requests

logger = logging.getLogger(__name__)

# --- Password Hashing Context ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# --- JWT Settings ---
ALGORITHM = settings.ALGORITHM
SECRET_KEY = settings.SECRET_KEY
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

def create_access_token(subject: Union[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Creates a JWT access token containing the subject (e.g., user ID) and expiry.

    Args:
        subject: The subject of the token (typically user ID or email).
        expires_delta: Optional timedelta for token expiry. Defaults to settings.

    Returns:
        The encoded JWT string.

    Raises:
        RuntimeError: If token encoding fails.
    """
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode = {"exp": expire, "sub": str(subject)}

    # Extract the actual secret string value from SecretStr before encoding
    if isinstance(SECRET_KEY, SecretStr):
        secret_key_value = SECRET_KEY.get_secret_value()
    else:
        secret_key_value = str(SECRET_KEY)
        logger.warning("SECRET_KEY was not loaded as SecretStr, using string conversion.")

    try:
        # Encode the payload into a JWT
        encoded_jwt = jwt.encode(to_encode, secret_key_value, algorithm=ALGORITHM)
        return encoded_jwt
    # Catch base JWTError and general Exception for encoding issues
    except (JWTError, Exception) as e:
         logger.error(f"Error encoding JWT token: {e}", exc_info=True)
         raise RuntimeError(f"Could not create access token due to encoding error.") from e


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a stored hash using the configured context."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Generates a hash for a given plain password."""
    return pwd_context.hash(password)

# --- Google Token Verification ---
# Setup a cached session for Google requests
try:
    _google_session = requests.session()
    _google_cached_session = cachecontrol.CacheControl(_google_session)
    google_request_cached = google_requests.Request(session=_google_cached_session)
    logger.debug("Initialized cached session for Google OAuth requests.")
except Exception as e:
    logger.warning(f"Could not initialize cached session for Google requests: {e}. Token verification might be slower.")
    google_request_cached = google_requests.Request() # Fallback


async def verify_google_token(token: str) -> Optional[Dict[str, Any]]:
    """
    Verifies the Google ID token asynchronously using Google's auth library.

    Args:
        token: The Google ID token string received from the client.

    Returns:
        A dictionary containing the verified token claims if valid, otherwise None.

    Raises:
        ValueError: If Google Client ID is not configured or the token is invalid/expired.
    """
    if not settings.GOOGLE_OAUTH_CLIENT_ID:
        logger.error("Google OAuth Client ID (GOOGLE_OAUTH_CLIENT_ID) is not configured in settings.")
        raise ValueError("Google OAuth Client ID is not configured.")

    try:
        idinfo = id_token.verify_oauth2_token(
            token, google_request_cached, settings.GOOGLE_OAUTH_CLIENT_ID
        )
        logger.debug(f"Google token verified successfully for user sub: {idinfo.get('sub')}")
        return idinfo
    except ValueError as e:
        logger.warning(f"Invalid Google Token received: {e}")
        raise ValueError(f"Invalid Google Token: {e}")
    except Exception as e:
        logger.error(f"Unexpected error verifying Google token: {e}", exc_info=True)
        raise ValueError("Could not verify Google Token due to an unexpected error.")


# --- API Key Configuration ---
API_KEY_PREFIX_LENGTH = 8
API_KEY_SECRET_LENGTH = 32
API_KEY_CHARS = string.ascii_letters + string.digits

# --- API Key Generation and Hashing ---
def generate_api_key_string() -> tuple[str, str]:
    """
    Generates a new API key consisting of a prefix and a secret part.

    Returns:
        A tuple containing (prefix, full_api_key).
    """
    prefix = "".join(secrets.choice(API_KEY_CHARS) for _ in range(API_KEY_PREFIX_LENGTH))
    secret = "".join(secrets.choice(API_KEY_CHARS) for _ in range(API_KEY_SECRET_LENGTH))
    full_key = f"{prefix}_{secret}"
    return prefix, full_key

def get_api_key_hash(api_key: str) -> str:
    """Hashes the full API key using the application's standard password context."""
    return pwd_context.hash(api_key)

def verify_api_key(plain_api_key: str, hashed_api_key: str) -> bool:
    """Verifies a plain API key against its stored hash."""
    return pwd_context.verify(plain_api_key, hashed_api_key)
