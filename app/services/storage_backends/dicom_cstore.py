# app/services/storage_backends/dicom_cstore.py

import logging
import tempfile # <-- Keep for potential future use, though not needed now
import os       # <-- Keep for potential future use
from pathlib import Path
from typing import Any, Dict, Optional # <-- Add Optional
import socket # For potential timeout errors

from pydicom.dataset import Dataset
from pydicom.uid import UID

from pynetdicom import AE # AE is top-level
from pynetdicom._globals import ALL_TRANSFER_SYNTAXES # Import common transfer syntaxes

# Removed AssociationSocket import as it's not needed for the exception
# from pynetdicom.transport import AssociationSocket
#from pynetdicom.errors import PynetdicomError # Import base pynetdicom error if needed

from .base_backend import BaseStorageBackend
from . import StorageBackendError

logger = logging.getLogger(__name__)

class CStoreStorage(BaseStorageBackend):
    """Sends DICOM datasets via C-STORE SCU."""

    def _validate_config(self):
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

        # Use configured calling AET or a default
        self.calling_ae_title = self.config.get("calling_ae_title", "AXIOM_SCU")
        # Get timeouts from config or use defaults
        self.network_timeout = self.config.get("network_timeout", 30)
        self.acse_timeout = self.config.get("acse_timeout", 30)
        self.dimse_timeout = self.config.get("dimse_timeout", 60)
        self.max_pdu_size = self.config.get("max_pdu_size", 0) # 0 means unlimited or use pynetdicom default

    def store(
        self,
        dataset_to_send: Dataset,
        original_filepath: Optional[Path] = None, # Make optional
        filename_context: Optional[str] = None,   # Add for logging context if needed
        **kwargs: Any                               # Accept extra args
    ) -> Dict[str, Any]:                            # Return dict
        """
        Sends the provided DICOM dataset via C-STORE.

        Args:
            dataset_to_send: The pydicom Dataset object to send.
            original_filepath: The original path of the file (optional, used for context).
            filename_context: A filename or identifier for logging (optional).
            **kwargs: Additional keyword arguments (ignored by this backend).

        Returns:
            A dictionary with status information (e.g.,
            {"status": "success", "remote_status": 0x0000, "message": "C-STORE successful"} or
            {"status": "error", "message": "Reason for failure..."}).
        """
        # --- Get Identifiers for Logging ---
        try:
            sop_instance_uid = dataset_to_send.SOPInstanceUID
            sop_class_uid = dataset_to_send.SOPClassUID
            log_identifier = f"SOPInstanceUID {sop_instance_uid}"
        except AttributeError as e:
            err_msg = f"Cannot perform C-STORE: Dataset is missing required UID attribute ({e})"
            logger.error(err_msg)
            raise StorageBackendError(err_msg) # Raise error, cannot proceed

        # --- Initialize AE ---
        ae = AE(ae_title=self.calling_ae_title)
        # Configure timeouts on the AE instance
        ae.network_timeout = self.network_timeout
        ae.acse_timeout = self.acse_timeout
        ae.dimse_timeout = self.dimse_timeout
        ae.maximum_pdu_size = self.max_pdu_size

        # --- Add Presentation Contexts ---
        # Add context for the specific SOP Class
        ae.add_requested_context(sop_class_uid)
        # Optionally add common storage contexts or configure to accept all
        # Example: Add common CT/MR storage contexts
        # ae.add_requested_context(pydicom.uid.CTImageStorage)
        # ae.add_requested_context(pydicom.uid.MRImageStorage)
        # Or, more flexibly, offer many transfer syntaxes for the main SOP Class:
        # ae.add_requested_context(sop_class_uid, transfer_syntax=ALL_TRANSFER_SYNTAXES)

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
                # Check if the required presentation context was accepted
                accepted_contexts = [ctx for ctx in assoc.accepted_contexts if ctx.abstract_syntax == sop_class_uid]
                if not accepted_contexts:
                     assoc.release()
                     err_msg = (f"C-STORE failed: Remote AE {self.dest_ae['ae_title']} did not accept proposed "
                                f"presentation context for SOP Class {sop_class_uid}")
                     logger.error(err_msg)
                     raise StorageBackendError(err_msg)

                logger.debug(f"Sending C-STORE request for {log_identifier}...")
                # Send the dataset object directly
                status_store = assoc.send_c_store(dataset_to_send)

                if status_store:
                     status_hex = f"0x{status_store.Status:04X}"
                     logger.info(f"C-STORE response received for {log_identifier} - Status: {status_hex}")
                     assoc.release()
                     logger.debug(f"Association released with {self.dest_ae['ae_title']}")

                     if status_store.Status == 0x0000:
                         return {
                             "status": "success",
                             "message": f"C-STORE successful for {log_identifier}",
                             "remote_status": status_hex
                         }
                     else:
                         # Failed status from remote AE
                         err_msg = (f"C-STORE rejected by remote AE '{self.dest_ae['ae_title']}' for {log_identifier} "
                                    f"with status {status_hex}")
                         logger.error(err_msg)
                         raise StorageBackendError(err_msg, remote_status=status_hex) # Add status to error
                else:
                    # No status received, likely DIMSE timeout or network issue during C-STORE
                    logger.error(f"C-STORE failed for {log_identifier} - No response received from {self.dest_ae['ae_title']}")
                    assoc.abort()
                    err_msg = f"C-STORE to {self.dest_ae['ae_title']} failed - No response"
                    raise StorageBackendError(err_msg)
            else:
                # Association was rejected or aborted immediately
                err_msg = (f"Association rejected or aborted by {self.dest_ae['ae_title']} "
                           f"at {self.dest_ae['host']}:{self.dest_ae['port']}")
                logger.error(err_msg)
                raise StorageBackendError(err_msg)

        # --- Exception Handling ---
        except (socket.timeout, TimeoutError) as e:
             err_msg = f"Network timeout during C-STORE to {self.dest_ae['host']}:{self.dest_ae['port']} for {log_identifier}"
             logger.error(f"{err_msg}: {e}", exc_info=True)
             # No need to abort assoc if it wasn't established or timed out
             raise StorageBackendError(err_msg) from e
        except ConnectionRefusedError as e:
             err_msg = f"C-STORE connection refused by {self.dest_ae['host']}:{self.dest_ae['port']} for {log_identifier}"
             logger.error(f"{err_msg}.", exc_info=True)
             raise StorageBackendError(err_msg) from e
        except OSError as e:
             err_msg = f"Network/OS error during C-STORE to {self.dest_ae['host']}:{self.dest_ae['port']} for {log_identifier}"
             logger.error(f"{err_msg}: {e}", exc_info=True)
             raise StorageBackendError(f"{err_msg}: {e}") from e
        # Example of catching specific pynetdicom error if needed
        # except ae.ACSETimeoutError as e: # Be specific if possible
        #    logger.error(f"ACSE Timeout during C-STORE association with {self.dest_ae['ae_title']}: {e}", exc_info=True)
        #    raise StorageBackendError(f"Timeout associating with {self.dest_ae['ae_title']}") from e
        except StorageBackendError as e: # Re-raise specific StorageBackendErrors we generated
             # Log here or let the caller handle logging of the re-raised error
             logger.debug(f"Re-raising StorageBackendError for {log_identifier}: {e}")
             raise e
        except Exception as e: # Catch any other unexpected exceptions
            err_msg = f"C-STORE operation unexpectedly failed for {log_identifier} to {self.dest_ae['ae_title']}"
            logger.error(f"{err_msg}: {e}", exc_info=True)
            if assoc and not assoc.is_released and not assoc.is_aborted: assoc.abort()
            raise StorageBackendError(f"{err_msg}: {e}") from e
        finally:
            # Final safety net: Ensure association is closed if it exists and wasn't handled
            if assoc and not assoc.is_released and not assoc.is_aborted:
                logger.warning(f"Association with {self.dest_ae['ae_title']} was not properly closed in CStoreStorage.store, aborting.")
                assoc.abort()
