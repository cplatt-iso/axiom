# app/core/gcp_utils.py
import logging
from google.cloud import secretmanager
from google.api_core import exceptions as google_exceptions

# Use structlog if available, otherwise fallback
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

# Initialize the Secret Manager client.
# Reuse the client instance for efficiency.
# Authentication uses Application Default Credentials (ADC) automatically.
try:
    secret_manager_client = secretmanager.SecretManagerServiceClient()
    GCP_SECRET_MANAGER_AVAILABLE = True
    logger.info("Google Secret Manager client initialized successfully.")
except Exception as e:
    # Log an error if client creation fails (e.g., library not installed, auth setup issues)
    logger.error(f"Failed to initialize Google Secret Manager client: {e}", exc_info=True)
    secret_manager_client = None
    GCP_SECRET_MANAGER_AVAILABLE = False

class SecretManagerError(Exception):
    """Custom exception for Secret Manager errors."""
    pass

class SecretNotFoundError(SecretManagerError):
    """Exception raised when a secret is not found."""
    pass

class PermissionDeniedError(SecretManagerError):
    """Exception raised for permission issues."""
    pass


def fetch_secret_content(secret_resource_name: str) -> bytes:
    """
    Fetches the payload of a secret version from Google Secret Manager.

    Args:
        secret_resource_name: The full resource name of the secret version
                              (e.g., "projects/*/secrets/*/versions/latest").

    Returns:
        The secret payload as bytes.

    Raises:
        SecretNotFoundError: If the secret version doesn't exist.
        PermissionDeniedError: If access is denied.
        SecretManagerError: For other GCP API errors or if client is unavailable.
    """
    log = logger.bind(secret_resource_name=secret_resource_name)
    if not GCP_SECRET_MANAGER_AVAILABLE or not secret_manager_client:
        log.error("Secret Manager client is not available.")
        raise SecretManagerError("Secret Manager client is not initialized.")

    if not secret_resource_name or not isinstance(secret_resource_name, str):
        # Added basic validation
        log.warning("Invalid secret resource name provided.")
        raise ValueError("Invalid secret_resource_name provided.")

    log.debug("Attempting to fetch secret content from Google Secret Manager...")
    try:
        response = secret_manager_client.access_secret_version(name=secret_resource_name)
        secret_payload = response.payload.data
        # Maybe add validation here? Check if empty?
        if not secret_payload:
            log.warning("Fetched secret payload is empty.")
            # Decide: raise error or return empty bytes? Returning empty for now.
            # raise SecretManagerError("Fetched secret payload is empty.")

        log.info("Successfully fetched secret content.")
        return secret_payload

    except google_exceptions.NotFound as e:
        log.warning("Secret version not found in Secret Manager.", error=str(e))
        raise SecretNotFoundError(f"Secret version not found: {secret_resource_name}") from e
    except google_exceptions.PermissionDenied as e:
        log.error("Permission denied accessing secret in Secret Manager. Check IAM roles.", error=str(e))
        raise PermissionDeniedError(f"Permission denied for secret: {secret_resource_name}. Ensure service account has 'Secret Manager Secret Accessor' role.") from e
    except google_exceptions.GoogleAPICallError as e:
        log.error("Google API call error accessing Secret Manager.", error=str(e), exc_info=True)
        raise SecretManagerError(f"Google API error accessing secret {secret_resource_name}: {e}") from e
    except Exception as e:
        # Catch any other unexpected exceptions
        log.exception(f"Unexpected error fetching secret {secret_resource_name}.")
        raise SecretManagerError(f"Unexpected error fetching secret {secret_resource_name}: {e}") from e
