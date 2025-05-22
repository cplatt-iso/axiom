# filename: backend/app/core/gcp_utils.py
import logging
import os
from typing import Dict, Optional
from google.cloud import secretmanager
from google.api_core import exceptions as google_exceptions
from app.core.config import settings

# --- Caching Import ---
try:
    from cachetools import TTLCache
    CACHE_TOOLS_AVAILABLE = True
except ImportError:
    CACHE_TOOLS_AVAILABLE = False
    TTLCache = None # Placeholder if not available

# --- Logger setup ---
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging # Fallback if structlog isn't there (should be, but defensive)
    import structlog
    logger = structlog.wrap_logger(logging.getLogger(__name__))

# --- Custom Exceptions ---
class SecretManagerError(Exception): pass
class SecretNotFoundError(SecretManagerError): pass
class PermissionDeniedError(SecretManagerError): pass

# --- Global Client Variable (Initialize as None) ---
secret_manager_client: Optional[secretmanager.SecretManagerServiceClient] = None
GCP_SECRET_MANAGER_AVAILABLE = False # Start as False until client is successfully created

# --- Global Cache for Secrets ---
# Cache up to 50 secrets, each for 1 hour (3600 seconds)
# Adjust maxsize and ttl as needed. TTL is in seconds.
if CACHE_TOOLS_AVAILABLE and TTLCache: # Ensure TTLCache was imported
    secret_content_cache = TTLCache(maxsize=settings.SECRET_CACHE_MAX_SIZE, ttl=settings.SECRET_CACHE_TTL_SECONDS)
    logger.info(f"Secret cache initialized: maxsize={settings.SECRET_CACHE_MAX_SIZE}, ttl={settings.SECRET_CACHE_TTL_SECONDS}s")
else:
    secret_content_cache = None # No caching if cachetools not installed
    logger.warning("cachetools library not found. Secret caching will be disabled. Consider installing 'cachetools'.")


def _get_sm_client() -> secretmanager.SecretManagerServiceClient:
    """Initializes and returns the Secret Manager client, caching it."""
    global secret_manager_client, GCP_SECRET_MANAGER_AVAILABLE
    if secret_manager_client:
        return secret_manager_client

    log = logger.bind(action="initialize_secret_manager_client")
    log.info("Attempting to initialize Google Secret Manager client on demand...")
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_manager_client = client
        GCP_SECRET_MANAGER_AVAILABLE = True
        log.info("Google Secret Manager client initialized successfully.")
        return client
    except Exception as e:
        log.error(f"Failed to initialize Google Secret Manager client: {e}", exc_info=True)
        secret_manager_client = None
        GCP_SECRET_MANAGER_AVAILABLE = False
        raise SecretManagerError("Failed to initialize Secret Manager client") from e

def _fetch_secret_content_from_gcp(secret_resource_name: str) -> bytes: # Renamed to indicate it's the actual fetch
    """Fetches secret payload directly from GCP using the on-demand client. NOT CACHED here."""
    log = logger.bind(secret_resource_name=secret_resource_name)
    try:
        client = _get_sm_client()
    except SecretManagerError:
         log.error("Cannot fetch secret, client initialization failed.")
         raise

    if not secret_resource_name or not isinstance(secret_resource_name, str) or not secret_resource_name.startswith("projects/"):
        log.warning("Invalid secret resource name provided to _fetch_secret_content_from_gcp.", provided_name=secret_resource_name)
        raise ValueError("Invalid secret_resource_name provided (must be full path).")

    log.debug("Attempting GCP fetch for secret content...", target_secret=secret_resource_name)
    try:
        response = client.access_secret_version(name=secret_resource_name)
        secret_payload = response.payload.data
        if not secret_payload: log.warning("Fetched secret payload from GCP is empty.")
        log.info("Successfully fetched secret content from GCP.", target_secret=secret_resource_name)
        return secret_payload
    except google_exceptions.NotFound as e:
        log.warning("Secret version not found in GCP.", error=str(e))
        raise SecretNotFoundError(f"Secret version not found: {secret_resource_name}") from e
    except google_exceptions.PermissionDenied as e:
        log.error("Permission denied accessing secret in GCP.", error=str(e))
        raise PermissionDeniedError(f"Permission denied: {secret_resource_name}") from e
    except google_exceptions.InvalidArgument as e:
        log.error("Invalid argument to Secret Manager API for GCP.", error=str(e))
        raise SecretManagerError(f"Invalid argument: {secret_resource_name}: {e}") from e
    except google_exceptions.GoogleAPICallError as e:
        log.error("Google API call error for GCP.", error=str(e), exc_info=True)
        raise SecretManagerError(f"API error: {secret_resource_name}: {e}") from e
    except Exception as e:
        log.exception(f"Unexpected error fetching secret {secret_resource_name} from GCP.")
        raise SecretManagerError(f"Unexpected error fetching secret: {e}") from e


def get_secret(secret_id: str, version: str = "latest", project_id: str | None = None) -> str:
    """
    Retrieves secret value using secret name (ID). Initializes client on demand.
    Uses an in-memory TTL cache to reduce calls to Secret Manager.
    """
    log = logger.bind(secret_id=secret_id, version=version)

    if not secret_id or not isinstance(secret_id, str) or "/" in secret_id:
        log.error("Invalid secret_id provided (empty, wrong type, or looks like path).", provided_id=secret_id)
        raise ValueError("Invalid secret_id: Function expects only the secret name.")

    effective_project_id = project_id or settings.VERTEX_AI_PROJECT
    if not effective_project_id:
         log.error("Project ID is required but not found for secret retrieval.")
         raise SecretManagerError("Could not determine GCP Project ID for Secret Manager.")
    
    # --- Cache Key ---
    # Using a tuple as a key is fine for cachetools if all elements are hashable.
    cache_key = (effective_project_id, secret_id, version)

    # --- Check Cache First ---
    if secret_content_cache is not None: # Check if caching is enabled
        try:
            cached_secret_string = secret_content_cache.get(cache_key)
            if cached_secret_string is not None:
                log.info(f"Cache HIT for secret '{secret_id}' (version: {version}).")
                return cached_secret_string
            else:
                log.info(f"Cache MISS for secret '{secret_id}' (version: {version}).")
        except Exception as cache_err: # Should not happen with simple get, but defensive
            log.warning(f"Error accessing secret cache for '{secret_id}': {cache_err}. Fetching from GCP.", exc_info=True)
            # Fall through to fetch from GCP

    # --- If Not in Cache or Caching Disabled, Fetch from GCP ---
    secret_resource_name = f"projects/{effective_project_id}/secrets/{secret_id}/versions/{version}"
    log.debug(f"Constructed secret resource name for GCP fetch: {secret_resource_name}")

    try:
        secret_bytes = _fetch_secret_content_from_gcp(secret_resource_name) # Call the renamed internal function
        secret_string = secret_bytes.decode('utf-8')
        log.info(f"Successfully retrieved and decoded secret '{secret_id}' from GCP.")

        # --- Store in Cache if Enabled ---
        if secret_content_cache is not None:
            try:
                secret_content_cache[cache_key] = secret_string
                log.info(f"Stored secret '{secret_id}' (version: {version}) in cache.")
            except Exception as cache_store_err: # Should not happen with simple set, but defensive
                 log.warning(f"Error storing secret '{secret_id}' in cache: {cache_store_err}.", exc_info=True)
        
        return secret_string
    except (SecretNotFoundError, PermissionDeniedError, SecretManagerError, ValueError) as e: # These are our custom errors or ValueError
        log.error(f"Failed to get secret '{secret_id}' after cache miss or disabled cache.", name=secret_resource_name, error_type=type(e).__name__, error=str(e))
        raise e # Re-raise the specific error
    except Exception as e:
        log.exception(f"Unexpected error getting secret '{secret_id}' after cache miss or disabled cache.")
        raise SecretManagerError(f"Unexpected error getting secret '{secret_id}': {e}") from e

# --- Function to manually clear the secret cache (optional, for API endpoint) ---
def clear_secret_cache(secret_id: Optional[str] = None, version: Optional[str] = None, project_id: Optional[str] = None) -> Dict[str, str]:
    """Clears the secret cache.
    If secret_id, version, and project_id are provided, clears a specific entry.
    Otherwise, clears the entire cache.
    """
    log = logger.bind(action="clear_secret_cache")
    if secret_content_cache is None:
        msg = "Secret caching is not enabled (cachetools not found or TTLCache init failed)."
        log.warning(msg)
        return {"status": "warning", "message": msg}

    if secret_id and version and (project_id or settings.VERTEX_AI_PROJECT):
        effective_project_id = project_id or settings.VERTEX_AI_PROJECT
        cache_key_to_clear = (effective_project_id, secret_id, version)
        if cache_key_to_clear in secret_content_cache:
            del secret_content_cache[cache_key_to_clear]
            msg = f"Cleared specific secret '{secret_id}' (version: {version}, project: {effective_project_id}) from cache."
            log.info(msg)
            return {"status": "success", "message": msg}
        else:
            msg = f"Secret '{secret_id}' (version: {version}, project: {effective_project_id}) not found in cache."
            log.info(msg)
            return {"status": "info", "message": msg}
    else:
        count_before = len(secret_content_cache)
        secret_content_cache.clear()
        count_after = len(secret_content_cache)
        msg = f"Cleared entire secret cache. Items removed: {count_before - count_after}."
        log.info(msg)
        return {"status": "success", "message": msg}