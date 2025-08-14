# backend/app/services/storage_backends/dicom_cstore.py
import structlog
from pydicom.dataset import Dataset
from typing import Any, Dict, Optional
from pathlib import Path

from app.schemas.storage_backend_config import CStoreBackendConfig
from .base_backend import BaseStorageBackend, StorageBackendError
from ..network.dimse.scu_service import store_dataset, DimseScuError, TlsConfigError

logger = structlog.get_logger(__name__)

class CStoreStorage(BaseStorageBackend):
    """
    A storage backend that sends DICOM datasets to a remote Application Entity (AE)
    using the C-STORE protocol. This class acts as a wrapper around the centralized
    `store_dataset` service, which handles the underlying logic for both pynetdicom
    and dcm4che senders.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the CStoreStorage backend.

        Args:
            config: A dictionary containing the configuration for this backend,
                    which will be validated against the CStoreBackendConfig schema.

        Raises:
            ValueError: If the provided configuration is invalid.
        """
        log = logger.bind(config_received_name=config.get("name", "UnnamedCStore"))
        log.debug("Initializing CStoreStorage backend...")

        try:
            # Validate and structure the config using the Pydantic model
            self.config_model = CStoreBackendConfig(**config)
        except Exception as e:  # Catches Pydantic validation errors
            log.error("CStoreStorage configuration validation failed.", validation_errors=str(e))
            raise ValueError(f"[{config.get('name', 'Unknown')}] Configuration errors: {e}")

        self.backend_type = "cstore"
        self.name = self.config_model.name
        self._validate_config()
        
        log.debug(f"CStoreStorage backend '{self.name}' initialized.", redacted_config=self.get_redacted_config())

    def _validate_config(self):
        """
        Performs additional validation that isn't covered by the Pydantic model,
        such as checking for the availability of required external modules (e.g., for TLS).
        """
        # The scu_service now handles TLS secret fetching, so we don't need to check
        # for gcp_utils here. This method is kept for any future complex validation.
        pass

    def get_redacted_config(self) -> Dict[str, Any]:
        """Returns a redacted version of the configuration for logging purposes."""
        config_dict = self.config_model.model_dump()
        if config_dict.get("tls_client_cert_secret_name"):
            config_dict["tls_client_cert_secret_name"] = "[REDACTED]"
        if config_dict.get("tls_client_key_secret_name"):
            config_dict["tls_client_key_secret_name"] = "[REDACTED]"
        return config_dict

    def store(
        self, modified_ds: Dataset, original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None, source_identifier: Optional[str] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Stores a DICOM dataset by sending it to the configured C-STORE destination.

        This method delegates the C-STORE operation to the `store_dataset` service,
        which handles transfer syntax negotiation, TLS, and sender type logic.

        Args:
            modified_ds: The DICOM dataset to be stored.
            original_filepath: The original path of the file (not used by C-STORE).
            filename_context: A string for logging/identification purposes.
            source_identifier: A string identifying the source of the study.

        Returns:
            A dictionary containing the result of the store operation.

        Raises:
            StorageBackendError: If the C-STORE operation fails for any reason.
        """
        log_identifier = "Unknown"
        try:
            sop_instance_uid = modified_ds.SOPInstanceUID
            log_identifier = f"SOP {sop_instance_uid}"
        except AttributeError:
            pass
        
        log = logger.bind(
            backend_name=self.name, 
            source_identifier=source_identifier,
            log_identifier=filename_context or log_identifier
        )
        log.info("Executing C-STORE via scu_service")

        try:
            # Delegate directly to the centralized SCU service
            result = store_dataset(
                config=self.config_model,
                dataset=modified_ds,
                transfer_syntax_strategy=self.config_model.transfer_syntax_strategy,
                max_retries=self.config_model.max_association_retries
            )
            # The result from store_dataset is already a dict in the desired format
            log.info("C-STORE operation completed via scu_service.", status=result.get("status"))
            return result
        except (DimseScuError, TlsConfigError) as e:
            log.warning("C-STORE failed in scu_service", error=str(e))
            # Map the specific error from the service layer to the backend's exception type
            raise StorageBackendError(f"C-STORE to {self.config_model.remote_ae_title} failed: {e}") from e
        except Exception as e:
            log.error("An unexpected error occurred during C-STORE via scu_service", error=str(e), exc_info=True)
            raise StorageBackendError(f"Unexpected error during C-STORE to {self.config_model.remote_ae_title}: {e}") from e

    def __repr__(self) -> str:
        """Provides a developer-friendly representation of the CStoreStorage object."""
        return (f"<{self.__class__.__name__}(name='{self.config_model.name}', ae='{self.config_model.remote_ae_title}', "
                f"host='{self.config_model.remote_host}:{self.config_model.remote_port}', tls={self.config_model.tls_enabled}, "
                f"sender='{self.config_model.sender_type}')>")
