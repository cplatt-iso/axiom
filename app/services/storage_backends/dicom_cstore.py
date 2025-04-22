# app/services/storage_backends/dicom_cstore.py

import logging
import socket
from pathlib import Path
from typing import Any, Dict, Optional

from pydicom.dataset import Dataset
from pynetdicom import AE
from pynetdicom._globals import ALL_TRANSFER_SYNTAXES # Consider narrowing if appropriate

from .base_backend import BaseStorageBackend, StorageBackendError

logger = logging.getLogger(__name__)

class CStoreStorage(BaseStorageBackend):
    """Sends DICOM datasets via C-STORE SCU."""

    def _validate_config(self):
        """Validate C-STORE configuration."""
        required_keys = ["ae_title", "host", "port"]
        if not all(key in self.config for key in required_keys):
            raise ValueError(f"C-STORE storage config requires: {', '.join(required_keys)}")
        try:
            self.dest_ae = {
                "ae_title": self.config["ae_title"],
                "host": self.config["host"],
                "port": int(self.config["port"])
            }
        except (TypeError, ValueError) as e:
             raise ValueError(f"Invalid port number in C-STORE config: {e}")

        self.calling_ae_title = self.config.get("calling_ae_title", "AXIOM_SCU")
        self.network_timeout = self.config.get("network_timeout", 30)
        self.acse_timeout = self.config.get("acse_timeout", 30)
        self.dimse_timeout = self.config.get("dimse_timeout", 60)
        self.max_pdu_size = self.config.get("max_pdu_size", 0) # 0 for unlimited/default

    # Update signature to match BaseStorageBackend explicitly
    def store(
        self,
        dataset_to_send: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None, # Add source_identifier
        **kwargs: Any                              # Keep kwargs
    ) -> Dict[str, Any]:
        """
        Sends the provided DICOM dataset via C-STORE SCU.

        Args:
            dataset_to_send: The pydicom Dataset object to send.
            original_filepath: The original path of the file (optional, unused).
            filename_context: A filename or identifier for logging (optional).
            source_identifier: Identifier of the source system (currently unused by this backend).
            **kwargs: Additional keyword arguments (ignored).

        Returns:
            A dictionary with status information.

        Raises:
            StorageBackendError: If the C-STORE operation fails.
        """
        # Log if source_identifier was provided, even if unused
        if source_identifier:
            logger.debug(f"CStoreStorage received source_identifier: {source_identifier} (unused)")

        # Get UIDs for logging and context negotiation
        try:
            sop_instance_uid = dataset_to_send.SOPInstanceUID
            sop_class_uid = dataset_to_send.SOPClassUID
            log_identifier = f"SOPInstanceUID {sop_instance_uid}"
        except AttributeError as e:
            err_msg = f"Cannot perform C-STORE: Dataset is missing required UID attribute ({e})"
            logger.error(err_msg)
            raise StorageBackendError(err_msg)

        # Initialize Application Entity (AE)
        ae = AE(ae_title=self.calling_ae_title)
        ae.network_timeout = self.network_timeout
        ae.acse_timeout = self.acse_timeout
        ae.dimse_timeout = self.dimse_timeout
        ae.maximum_pdu_size = self.max_pdu_size

        # Add requested presentation context for the specific SOP Class
        # Offer multiple transfer syntaxes for better compatibility
        ae.add_requested_context(sop_class_uid, transfer_syntax=ALL_TRANSFER_SYNTAXES)

        logger.debug(f"Attempting C-STORE for {log_identifier} to AE '{self.dest_ae['ae_title']}' at "
                     f"{self.dest_ae['host']}:{self.dest_ae['port']} (SOP Class: {sop_class_uid})")

        assoc = None
        try:
            logger.debug(f"Requesting association with {self.dest_ae['ae_title']}...")
            assoc = ae.associate(
                self.dest_ae["host"],
                self.dest_ae["port"],
                ae_title=self.dest_ae["ae_title"]
            )

            if assoc.is_established:
                logger.info(f"Association established with {self.dest_ae['ae_title']} for {log_identifier}")
                # Verify the context was actually accepted
                accepted_contexts = [ctx for ctx in assoc.accepted_contexts if ctx.abstract_syntax == sop_class_uid]
                if not accepted_contexts:
                     assoc.release()
                     err_msg = (f"C-STORE failed: Remote AE {self.dest_ae['ae_title']} did not accept proposed "
                                f"presentation context for SOP Class {sop_class_uid}")
                     logger.error(err_msg)
                     raise StorageBackendError(err_msg)

                logger.debug(f"Sending C-STORE request for {log_identifier}...")
                status_store = assoc.send_c_store(dataset_to_send)

                if status_store:
                     status_hex = f"0x{status_store.Status:04X}"
                     logger.info(f"C-STORE response received for {log_identifier} - Status: {status_hex}")
                     assoc.release()
                     logger.debug(f"Association released with {self.dest_ae['ae_title']}")

                     if status_store.Status == 0x0000: # Success status
                         return {
                             "status": "success",
                             "message": f"C-STORE successful for {log_identifier}",
                             "remote_status": status_hex
                         }
                     else: # Failed status from remote AE
                         err_msg = (f"C-STORE rejected by remote AE '{self.dest_ae['ae_title']}' for {log_identifier} "
                                    f"with status {status_hex}")
                         logger.error(err_msg)
                         raise StorageBackendError(err_msg, remote_status=status_hex)
                else:
                    # No status received - likely DIMSE timeout or network issue during C-STORE
                    logger.error(f"C-STORE failed for {log_identifier} - No response received from {self.dest_ae['ae_title']}")
                    assoc.abort() # Abort if no status received
                    err_msg = f"C-STORE to {self.dest_ae['ae_title']} failed - No response"
                    raise StorageBackendError(err_msg)
            else:
                # Association was rejected or aborted immediately upon connection
                err_msg = (f"Association rejected or aborted by {self.dest_ae['ae_title']} "
                           f"at {self.dest_ae['host']}:{self.dest_ae['port']}")
                logger.error(err_msg)
                raise StorageBackendError(err_msg)

        # Specific Exception Handling
        except (socket.timeout, TimeoutError) as e:
             err_msg = f"Network timeout during C-STORE to {self.dest_ae['host']}:{self.dest_ae['port']} for {log_identifier}"
             logger.error(f"{err_msg}: {e}", exc_info=False) # Log less detail for common timeouts
             raise StorageBackendError(err_msg) from e
        except ConnectionRefusedError as e:
             err_msg = f"C-STORE connection refused by {self.dest_ae['host']}:{self.dest_ae['port']} for {log_identifier}"
             logger.error(f"{err_msg}.", exc_info=False)
             raise StorageBackendError(err_msg) from e
        except OSError as e:
             # Includes errors like "No route to host"
             err_msg = f"Network/OS error during C-STORE to {self.dest_ae['host']}:{self.dest_ae['port']} for {log_identifier}"
             logger.error(f"{err_msg}: {e}", exc_info=False)
             raise StorageBackendError(f"{err_msg}: {e}") from e
        except StorageBackendError: # Re-raise specific errors we generated
             raise
        except Exception as e: # Catch unexpected errors
            err_msg = f"C-STORE operation unexpectedly failed for {log_identifier} to {self.dest_ae['ae_title']}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            if assoc and not assoc.is_released and not assoc.is_aborted: assoc.abort()
            raise StorageBackendError(f"{err_msg}: {e}") from e
        finally:
            # Ensure association is closed if something went wrong mid-process
            if assoc and not assoc.is_released and not assoc.is_aborted:
                logger.warning(f"Association with {self.dest_ae['ae_title']} was not properly closed, aborting.")
                assoc.abort()
