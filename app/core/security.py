# app/core/security.py (New File)
import secrets
import string

from datetime import datetime, timedelta, timezone
from typing import Any, Union

from jose import jwt, JWTError
from passlib.context import CryptContext # Keep for potential future password use

from app.core.config import settings


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

ALGORITHM = settings.ALGORITHM
SECRET_KEY = settings.SECRET_KEY
ACCESS_TOKEN_EXPIRE_MINUTES = settings.ACCESS_TOKEN_EXPIRE_MINUTES

def create_access_token(subject: Union[str, Any], expires_delta: timedelta | None = None) -> str:
    """Creates a JWT access token for the application."""
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"exp": expire, "sub": str(subject)}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifies a plain password against a hashed password."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Hashes a password."""
    return pwd_context.hash(password)

# --- Google Token Verification ---
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests
import cachecontrol # Required by google-auth for caching discovery docs
import requests

# Create a cached session for Google requests
session = requests.session()
cached_session = cachecontrol.CacheControl(session)
google_request = google_requests.Request(session=cached_session)

async def verify_google_token(token: str) -> dict | None:
    """
    Verifies the Google ID token.

    Args:
        token: The Google ID token string.

    Returns:
        A dictionary containing the token claims if valid, otherwise None.
        Raises ValueError on invalid token.
    """
    if not settings.GOOGLE_OAUTH_CLIENT_ID:
        raise ValueError("Google OAuth Client ID is not configured in the backend settings.")

    try:
        # Specify the CLIENT_ID of the app that accesses the backend:
        # Verify the token against Google's public keys.
        idinfo = id_token.verify_oauth2_token(
            token, google_request, settings.GOOGLE_OAUTH_CLIENT_ID
        )
        # Or, if multiple clients access the backend:
        # idinfo = id_token.verify_oauth2_token(token, google_request)
        # if idinfo['aud'] not in [CLIENT_ID_1, CLIENT_ID_2, ...]:
        #     raise ValueError('Could not verify audience.')

        # # ID token is valid. Get the user's Google Account ID from the decoded token.
        # userid = idinfo['sub']
        return idinfo
    except ValueError as e:
        # Invalid token
        print(f"Error verifying Google token: {e}")
        raise ValueError(f"Invalid Google Token: {e}")
    except Exception as e:
        print(f"Unexpected error verifying Google token: {e}")
        raise ValueError("Could not verify Google Token due to unexpected error.")


# --- API Key Configuration ---
API_KEY_PREFIX_LENGTH = 8 # Length of the non-sensitive prefix
API_KEY_SECRET_LENGTH = 32 # Length of the random secret part
API_KEY_CHARS = string.ascii_letters + string.digits # Characters allowed in the key

# --- API Key Generation and Hashing ---
def generate_api_key_string() -> tuple[str, str]:
    """Generates a secure API key string (prefix + secret)."""
    prefix = "".join(secrets.choice(API_KEY_CHARS) for _ in range(API_KEY_PREFIX_LENGTH))
    secret = "".join(secrets.choice(API_KEY_CHARS) for _ in range(API_KEY_SECRET_LENGTH))
    full_key = f"{prefix}_{secret}" # e.g., "AbCd1234_xxxxx..."
    return prefix, full_key

def get_api_key_hash(api_key: str) -> str:
    """Hashes an API key using the same context as passwords."""
    # Using the same context is fine, bcrypt is suitable for high-entropy secrets too
    return pwd_context.hash(api_key)

def verify_api_key(plain_api_key: str, hashed_api_key: str) -> bool:
    """Verifies a plain API key against a stored hash."""
    return pwd_context.verify(plain_api_key, hashed_api_key)
