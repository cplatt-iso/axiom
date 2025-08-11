# app/services/connection_test_service.py
"""
Connection testing services for scraper sources.
Provides connection testing for DICOMWeb, DIMSE Q/R, and Google Healthcare sources.
"""

import asyncio
import socket
import ssl
import tempfile
import os
from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, List
import httpx
import structlog
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import Verification

from app.core.config import settings
from app.db.models.dicomweb_source_state import DicomWebSourceState
from app.db.models.dimse_qr_source import DimseQueryRetrieveSource
from app.db.models.google_healthcare_source import GoogleHealthcareSource
from app.schemas.enums import HealthStatus

# Import GCP utils for secret management (conditional import like other services)
try:
    from app.core import gcp_utils
    GCP_UTILS_AVAILABLE = True
except ImportError:
    GCP_UTILS_AVAILABLE = False

logger = structlog.get_logger(__name__)


class TlsConfigError(Exception):
    """Exception raised when TLS configuration fails."""
    def __init__(self, message: str, details: Optional[str] = None):
        self.message = message
        self.details = details
        super().__init__(f"{message}: {details}" if details else message)


def _fetch_and_write_secret(secret_name: str, suffix: str) -> str:
    """Fetch secret from GCP Secret Manager and write to temporary file."""
    if not GCP_UTILS_AVAILABLE:
        raise TlsConfigError("GCP utils not available for secret fetching")
    
    secret_content = gcp_utils.get_secret(secret_name)
    temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix=suffix)
    temp_file.write(secret_content)
    temp_file.flush()
    temp_file.close()
    return temp_file.name


def _prepare_tls_context(
    tls_ca_cert_secret: Optional[str],
    tls_client_cert_secret: Optional[str],
    tls_client_key_secret: Optional[str]
) -> Tuple[Optional[ssl.SSLContext], List[str]]:
    """
    Prepare SSL context for DIMSE TLS connection.
    Returns tuple of (ssl_context, temp_files_to_cleanup)
    """
    temp_files_created: List[str] = []
    
    if not tls_ca_cert_secret:
        raise TlsConfigError("TLS CA certificate secret name is required for TLS verification")
    
    try:
        # Fetch CA certificate
        ca_cert_file = _fetch_and_write_secret(tls_ca_cert_secret, "-ca.pem")
        temp_files_created.append(ca_cert_file)
        
        # Fetch client certificate and key if provided
        client_cert_file = None
        client_key_file = None
        
        has_client_cert = bool(tls_client_cert_secret)
        has_client_key = bool(tls_client_key_secret)
        
        if has_client_cert and has_client_key:
            client_cert_file = _fetch_and_write_secret(tls_client_cert_secret, "-cert.pem")
            temp_files_created.append(client_cert_file)
            client_key_file = _fetch_and_write_secret(tls_client_key_secret, "-key.pem")
            temp_files_created.append(client_key_file)
        elif has_client_cert != has_client_key:
            raise TlsConfigError("Both client cert and key secrets required for mTLS, or neither")
        
        # Create SSL context
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_file)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        
        # Load client certificate if provided
        if client_cert_file and client_key_file:
            ssl_context.load_cert_chain(certfile=client_cert_file, keyfile=client_key_file)
        
        logger.info("TLS context configured successfully for connection test")
        return ssl_context, temp_files_created
        
    except (TlsConfigError, ssl.SSLError) as e:
        # Cleanup temp files on error
        for path in temp_files_created:
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass
        if isinstance(e, TlsConfigError):
            raise
        else:
            raise TlsConfigError("SSL configuration error", details=str(e)) from e
    except Exception as e:
        # Cleanup temp files on error
        for path in temp_files_created:
            try:
                if path and os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass
        raise TlsConfigError("Unexpected TLS setup error", details=str(e)) from e


logger = structlog.get_logger(__name__)

class ConnectionTestService:
    """Service for testing connections to scraper sources."""

    @staticmethod
    async def test_dicomweb_connection(source: DicomWebSourceState) -> Tuple[HealthStatus, Optional[str]]:
        """
        Test connection to a DICOMWeb source.
        
        Args:
            source: DICOMWeb source configuration
            
        Returns:
            Tuple of (health_status, error_message)
        """
        try:
            # Build the QIDO-RS endpoint URL for studies
            qido_url = f"{source.base_url.rstrip('/')}/{source.qido_prefix.strip('/')}/studies"
            
            # Prepare headers and auth
            headers = {"Accept": "application/dicom+json"}
            auth = None
            
            if source.auth_type == "basic" and source.auth_config:
                auth = httpx.BasicAuth(
                    source.auth_config.get("username", ""),
                    source.auth_config.get("password", "")
                )
            elif source.auth_type == "bearer" and source.auth_config:
                headers["Authorization"] = f"Bearer {source.auth_config.get('token', '')}"
            elif source.auth_type == "apikey" and source.auth_config:
                header_name = source.auth_config.get("header_name", "X-API-Key")
                headers[header_name] = source.auth_config.get("key", "")
            
            # Test connection with a simple query (limit to 1 study)
            params = {"limit": 1}
            if source.search_filters:
                params.update(source.search_filters)
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    qido_url,
                    headers=headers,
                    auth=auth,
                    params=params
                )
                
                if response.status_code in [200, 204]:
                    logger.info(f"DICOMWeb connection test successful for {source.source_name}")
                    return HealthStatus.OK, None
                elif response.status_code in [401, 403]:
                    error_msg = f"Authentication failed: {response.status_code}"
                    logger.warning(f"DICOMWeb connection test failed for {source.source_name}: {error_msg}")
                    return HealthStatus.DOWN, error_msg
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                    logger.warning(f"DICOMWeb connection test failed for {source.source_name}: {error_msg}")
                    return HealthStatus.ERROR, error_msg
                    
        except httpx.ConnectError as e:
            error_msg = f"Connection failed: {str(e)}"
            logger.warning(f"DICOMWeb connection test failed for {source.source_name}: {error_msg}")
            return HealthStatus.DOWN, error_msg
        except httpx.TimeoutException:
            error_msg = "Request timeout"
            logger.warning(f"DICOMWeb connection test failed for {source.source_name}: {error_msg}")
            return HealthStatus.DOWN, error_msg
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"DICOMWeb connection test error for {source.source_name}: {error_msg}", exc_info=True)
            return HealthStatus.ERROR, error_msg

    @staticmethod
    async def test_dimse_qr_connection(source: DimseQueryRetrieveSource) -> Tuple[HealthStatus, Optional[str]]:
        """
        Test connection to a DIMSE Q/R source using C-ECHO.
        
        Args:
            source: DIMSE Q/R source configuration
            
        Returns:
            Tuple of (health_status, error_message)
        """
        temp_files_to_cleanup: List[str] = []
        
        try:
            # Create an Application Entity
            ae = AE(ae_title=source.local_ae_title)
            # For C-ECHO (verification), we need to request the presentation context as SCU
            ae.add_requested_context(Verification)
            
            # Prepare TLS if enabled
            ssl_context = None
            if source.tls_enabled:
                if not GCP_UTILS_AVAILABLE:
                    error_msg = "TLS enabled but GCP utils not available for secret fetching"
                    logger.warning(f"DIMSE Q/R connection test failed for {source.name}: {error_msg}")
                    return HealthStatus.ERROR, error_msg
                
                try:
                    ssl_context, temp_files_to_cleanup = _prepare_tls_context(
                        tls_ca_cert_secret=source.tls_ca_cert_secret_name,
                        tls_client_cert_secret=source.tls_client_cert_secret_name,
                        tls_client_key_secret=source.tls_client_key_secret_name
                    )
                    logger.info(f"TLS context prepared for connection test to {source.name}")
                except TlsConfigError as e:
                    error_msg = f"TLS configuration failed: {str(e)}"
                    logger.warning(f"DIMSE Q/R connection test failed for {source.name}: {error_msg}")
                    return HealthStatus.ERROR, error_msg
            
            # Create association arguments
            assoc_args = {
                'addr': source.remote_host,
                'port': source.remote_port,
                'ae_title': source.remote_ae_title
            }
            
            # Add TLS arguments if TLS is enabled
            if ssl_context:
                # For pynetdicom, tls_args expects (SSLContext, hostname_str) or None
                tls_args = (ssl_context, source.remote_host if ssl_context.check_hostname else None)
                assoc_args['tls_args'] = tls_args
            
            # Run the connection test in a thread to avoid blocking
            def _test_connection():
                try:
                    assoc = ae.associate(**assoc_args)
                    if assoc.is_established:
                        # Send C-ECHO
                        status = assoc.send_c_echo()
                        assoc.release()
                        
                        if status and status.Status == 0x0000:  # Success
                            return True, None
                        else:
                            return False, f"C-ECHO failed with status: {status.Status if status else 'None'}"
                    else:
                        reason = "Unknown"
                        if assoc.is_rejected:
                            reason = f"Rejected by {getattr(assoc, 'result_source', 'N/A')}, code {getattr(assoc, 'result_reason', 'N/A')}"
                        elif assoc.is_aborted:
                            reason = "Aborted"
                        return False, f"Failed to establish association: {reason}"
                except Exception as e:
                    return False, str(e)
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            success, error = await loop.run_in_executor(None, _test_connection)
            
            if success:
                logger.info(f"DIMSE Q/R connection test successful for {source.name}")
                return HealthStatus.OK, None
            else:
                error_msg = f"DIMSE connection failed: {error}"
                logger.warning(f"DIMSE Q/R connection test failed for {source.name}: {error_msg}")
                return HealthStatus.DOWN, error_msg
                
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"DIMSE Q/R connection test error for {source.name}: {error_msg}", exc_info=True)
            return HealthStatus.ERROR, error_msg
        finally:
            # Cleanup temporary TLS files
            for file_path in temp_files_to_cleanup:
                try:
                    if file_path and os.path.exists(file_path):
                        os.remove(file_path)
                        logger.debug(f"Cleaned up temporary TLS file: {file_path}")
                except OSError as e:
                    logger.warning(f"Failed to cleanup temporary TLS file {file_path}: {e}")

    @staticmethod
    async def test_google_healthcare_connection(source: GoogleHealthcareSource) -> Tuple[HealthStatus, Optional[str]]:
        """
        Test connection to a Google Healthcare source using HTTP API calls.
        
        Args:
            source: Google Healthcare source configuration
            
        Returns:
            Tuple of (health_status, error_message)
        """
        try:
            # Import Google auth libraries (basic auth, not healthcare_v1)
            try:
                import google.auth
                import google.auth.transport.requests
                import httpx
                GOOGLE_AUTH_AVAILABLE = True
            except ImportError as e:
                error_msg = f"Google auth libraries not available: {str(e)}"
                logger.warning(f"Google Healthcare connection test failed for {source.name}: {error_msg}")
                return HealthStatus.ERROR, error_msg
            
            # Run the connection test in a thread to avoid blocking
            def _test_connection():
                try:
                    # Get default credentials
                    creds, project_id = google.auth.default(
                        scopes=["https://www.googleapis.com/auth/cloud-platform"]
                    )
                    
                    # Ensure we have a valid token
                    if not creds.token or (hasattr(creds, 'expired') and creds.expired):
                        # Refresh the credentials
                        auth_request = google.auth.transport.requests.Request()
                        creds.refresh(auth_request)
                    
                    if not creds.token:
                        return False, "Failed to obtain valid authentication token"
                    
                    # Build the Healthcare API URL for a minimal test (search for studies with limit 1)
                    base_api_url = "https://healthcare.googleapis.com/v1"
                    dicom_store_path = (
                        f"{base_api_url}/projects/{source.gcp_project_id}/locations/{source.gcp_location}"
                        f"/datasets/{source.gcp_dataset_id}/dicomStores/{source.gcp_dicom_store_id}/dicomWeb"
                    )
                    test_url = f"{dicom_store_path}/studies"
                    
                    # Prepare headers and minimal query params
                    headers = {
                        "Authorization": f"Bearer {creds.token}",
                        "Accept": "application/dicom+json, application/json"
                    }
                    params = {"limit": "1"}  # Minimal query to test connectivity
                    
                    # Make the HTTP request using httpx (sync client)
                    with httpx.Client(timeout=30.0) as client:
                        response = client.get(test_url, headers=headers, params=params)
                        response.raise_for_status()
                        
                        # If we get here without exception, the connection works
                        return True, None
                        
                except google.auth.exceptions.DefaultCredentialsError as e:
                    return False, f"Authentication failed - no default credentials: {str(e)}"
                except httpx.HTTPStatusError as e:
                    # Check for common HTTP error codes
                    if e.response.status_code == 403:
                        return False, f"Permission denied (HTTP 403): Check IAM permissions for Healthcare API"
                    elif e.response.status_code == 404:
                        return False, f"Resource not found (HTTP 404): Check project/dataset/store IDs"
                    elif e.response.status_code == 401:
                        return False, f"Authentication failed (HTTP 401): Invalid credentials"
                    else:
                        error_body = e.response.text[:200] if e.response.text else "No error details"
                        return False, f"HTTP {e.response.status_code}: {error_body}"
                except httpx.RequestError as e:
                    return False, f"Network error: {str(e)}"
                except Exception as e:
                    return False, f"Unexpected error: {str(e)}"
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            success, error = await loop.run_in_executor(None, _test_connection)
            
            if success:
                logger.info(f"Google Healthcare connection test successful for {source.name}")
                return HealthStatus.OK, None
            else:
                # Classify error types
                if "Permission denied" in str(error) or "Authentication failed" in str(error):
                    logger.warning(f"Google Healthcare connection test failed for {source.name}: {error}")
                    return HealthStatus.DOWN, error
                else:
                    logger.warning(f"Google Healthcare connection test failed for {source.name}: {error}")
                    return HealthStatus.ERROR, error
                
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"Google Healthcare connection test error for {source.name}: {error_msg}", exc_info=True)
            return HealthStatus.ERROR, error_msg

    @staticmethod
    async def update_source_health_status(
        db_session,
        source_type: str,
        source_id: int,
        health_status: HealthStatus,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Update the health status of a source in the database.
        
        Args:
            db_session: Database session
            source_type: Type of source (dicomweb, dimse_qr, google_healthcare)
            source_id: ID of the source
            health_status: New health status
            error_message: Error message if status is not OK
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            now = datetime.now(timezone.utc)
            
            if source_type == "dicomweb":
                source = db_session.get(DicomWebSourceState, source_id)
            elif source_type == "dimse_qr":
                source = db_session.get(DimseQueryRetrieveSource, source_id)
            elif source_type == "google_healthcare":
                source = db_session.get(GoogleHealthcareSource, source_id)
            else:
                logger.error(f"Unknown source type: {source_type}")
                return False
            
            if not source:
                logger.error(f"Source not found: {source_type}:{source_id}")
                return False
            
            # Update health status fields
            source.health_status = health_status.value
            source.last_health_check = now
            source.last_health_error = error_message if health_status != HealthStatus.OK else None
            
            db_session.commit()
            logger.debug(f"Updated health status for {source_type}:{source_id} to {health_status.value}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update health status for {source_type}:{source_id}: {str(e)}", exc_info=True)
            db_session.rollback()
            return False
