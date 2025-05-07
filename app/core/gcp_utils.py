# filename: backend/app/core/gcp_utils.py
import logging
import os
from typing import Optional # <--- ADDED THIS IMPORT
from google.cloud import secretmanager
from google.api_core import exceptions as google_exceptions
from app.core.config import settings

# --- Logger setup ---
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# --- Custom Exceptions ---
class SecretManagerError(Exception): pass
class SecretNotFoundError(SecretManagerError): pass
class PermissionDeniedError(SecretManagerError): pass

# --- Global Client Variable (Initialize as None) ---
# Now Optional is defined
secret_manager_client: Optional[secretmanager.SecretManagerServiceClient] = None
GCP_SECRET_MANAGER_AVAILABLE = False # Start as False until client is successfully created

def _get_sm_client() -> secretmanager.SecretManagerServiceClient:
    """Initializes and returns the Secret Manager client, caching it."""
    global secret_manager_client, GCP_SECRET_MANAGER_AVAILABLE
    # If client already exists and is valid, return it
    if secret_manager_client:
        # Maybe add a check here if the client can become invalid? Unlikely for this type.
        return secret_manager_client

    # Attempt to create the client
    log = logger.bind(action="initialize_secret_manager_client")
    log.info("Attempting to initialize Google Secret Manager client on demand...")
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_manager_client = client # Assign to global variable
        GCP_SECRET_MANAGER_AVAILABLE = True # Set flag on successful creation
        log.info("Google Secret Manager client initialized successfully.")
        return client
    except Exception as e:
        log.error(f"Failed to initialize Google Secret Manager client: {e}", exc_info=True)
        secret_manager_client = None # Ensure it's None on failure
        GCP_SECRET_MANAGER_AVAILABLE = False # Ensure flag is False
        raise SecretManagerError("Failed to initialize Secret Manager client") from e

# --- Internal fetch function ---
def _fetch_secret_content(secret_resource_name: str) -> bytes:
    """Fetches secret payload using the on-demand client."""
    log = logger.bind(secret_resource_name=secret_resource_name)
    try:
        client = _get_sm_client() # Get or create client
    except SecretManagerError:
         log.error("Cannot fetch secret, client initialization failed.")
         raise # Re-raise the init error

    # Basic validation for full path format
    if not secret_resource_name or not isinstance(secret_resource_name, str) or not secret_resource_name.startswith("projects/"):
        log.warning("Invalid secret resource name provided to _fetch_secret_content.", provided_name=secret_resource_name)
        raise ValueError("Invalid secret_resource_name provided (must be full path).")

    log.debug("Attempting internal fetch for secret content...")
    try:
        response = client.access_secret_version(name=secret_resource_name)
        # ... (rest of the fetch logic is the same) ...
        secret_payload = response.payload.data
        if not secret_payload: log.warning("Fetched secret payload is empty.")
        log.info("Successfully fetched secret content via internal call.")
        return secret_payload
    # ... (rest of the exception handling is the same) ...
    except google_exceptions.NotFound as e:
        log.warning("Secret version not found.", error=str(e))
        raise SecretNotFoundError(f"Secret version not found: {secret_resource_name}") from e
    except google_exceptions.PermissionDenied as e:
        log.error("Permission denied accessing secret.", error=str(e))
        raise PermissionDeniedError(f"Permission denied: {secret_resource_name}") from e
    except google_exceptions.InvalidArgument as e:
        log.error("Invalid argument to Secret Manager API.", error=str(e))
        raise SecretManagerError(f"Invalid argument: {secret_resource_name}: {e}") from e
    except google_exceptions.GoogleAPICallError as e:
        log.error("Google API call error.", error=str(e), exc_info=True)
        raise SecretManagerError(f"API error: {secret_resource_name}: {e}") from e
    except Exception as e:
        log.exception(f"Unexpected error fetching secret {secret_resource_name}.")
        raise SecretManagerError(f"Unexpected error fetching secret: {e}") from e


# --- Public get_secret function ---
def get_secret(secret_id: str, version: str = "latest", project_id: str | None = None) -> str:
    """Retrieves secret value using secret name (ID). Initializes client on demand."""
    log = logger.bind(secret_id=secret_id, version=version)

    # --- Input Validation ---
    if not secret_id or not isinstance(secret_id, str) or "/" in secret_id:
        log.error("Invalid secret_id provided (empty, wrong type, or looks like path).", provided_id=secret_id)
        raise ValueError("Invalid secret_id: Function expects only the secret name.")
    # --- End Validation ---

    # --- Determine Project ID ---
    effective_project_id = project_id or settings.VERTEX_AI_PROJECT
    if not effective_project_id:
         log.error("Project ID is required but not found.")
         raise SecretManagerError("Could not determine GCP Project ID for Secret Manager.")
    log.debug("Using project ID for secret path.", gcp_project_id=effective_project_id)
    # --- End Project ID ---

    # --- Construct Full Resource Name ---
    secret_resource_name = f"projects/{effective_project_id}/secrets/{secret_id}/versions/{version}"
    log.debug(f"Constructed secret resource name: {secret_resource_name}")
    # --- End Construction ---

    # --- Fetch and Decode ---
    try:
        # _fetch_secret_content will ensure client is initialized via _get_sm_client()
        secret_bytes = _fetch_secret_content(secret_resource_name)
        secret_string = secret_bytes.decode('utf-8')
        log.info(f"Successfully retrieved and decoded secret '{secret_id}'.")
        return secret_string
    # --- Exception handling remains the same ---
    except (SecretNotFoundError, PermissionDeniedError, SecretManagerError, ValueError) as e:
        log.error(f"Failed to get secret '{secret_id}'.", name=secret_resource_name, error_type=type(e).__name__, error=str(e))
        raise e
    except Exception as e:
        # Catch any other unexpected errors
        log.exception(f"Unexpected error getting secret '{secret_id}'.")
        raise SecretManagerError(f"Unexpected error getting secret '{secret_id}': {e}") from e
    # --- End Fetch ---
