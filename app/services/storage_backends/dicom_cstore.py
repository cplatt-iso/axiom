# app/services/storage_backends/dicom_cstore.py

import logging
from pathlib import Path
from typing import Any, Dict
import socket # For potential timeout errors

from pydicom.dataset import Dataset
from pydicom.uid import UID

from pynetdicom import AE # AE is top-level
# Removed AssociationSocket import as it's not needed for the exception
# from pynetdicom.transport import AssociationSocket
#from pynetdicom.errors import PynetdicomError # Import base pynetdicom error if needed

from .base_backend import BaseStorageBackend
from . import StorageBackendError

logger = logging.getLogger(__name__)

class CStoreStorage(BaseStorageBackend):
    """Sends DICOM files via C-STORE SCU."""

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

        self.calling_ae_title = self.config.get("calling_ae_title", "AXIOM_SCU") # Updated default AET
        self.network_timeout = self.config.get("network_timeout", 30)
        self.acse_timeout = self.config.get("acse_timeout", 30)
        # Note: pynetdicom uses dimse_timeout, not pdu_timeout for DIMSE operations
        self.dimse_timeout = self.config.get("dimse_timeout", 60) # Renamed config key if needed
        self.max_pdu_size = self.config.get("max_pdu_size", 16384)

    def store(self, modified_ds: Dataset, original_filepath: Path) -> bool:
        ae = AE(ae_title=self.calling_ae_title)
        # Configure timeouts on the AE instance
        ae.network_timeout = self.network_timeout
        ae.acse_timeout = self.acse_timeout
        ae.dimse_timeout = self.dimse_timeout
        ae.maximum_pdu_size = self.max_pdu_size

        sop_class_uid_obj = modified_ds.get("SOPClassUID")
        if not sop_class_uid_obj:
             logger.error(f"Cannot perform C-STORE for {original_filepath.name}: SOPClassUID is missing")
             raise StorageBackendError("Missing SOPClassUID in dataset for C-STORE")
        sop_class_uid_str = str(sop_class_uid_obj) # Use string form for context
        ae.add_requested_context(sop_class_uid_str)

        logger.debug(f"Attempting C-STORE to AE '{self.dest_ae['ae_title']}' at "
                     f"{self.dest_ae['host']}:{self.dest_ae['port']} for SOP Class {sop_class_uid_str}")

        assoc = None
        try:
            logger.debug(f"Requesting association with {self.dest_ae['ae_title']}...")
            assoc = ae.associate(
                self.dest_ae["host"],
                self.dest_ae["port"],
                ae_title=self.dest_ae["ae_title"]
                # Timeouts are set on AE object now
            )

            if assoc.is_established:
                logger.info(f"Association established with {self.dest_ae['ae_title']}")
                logger.debug(f"Sending C-STORE request for {original_filepath.name}...")
                status_store = assoc.send_c_store(modified_ds)

                if status_store:
                     logger.info(f"C-STORE response received for {original_filepath.name} - Status: 0x{status_store.Status:04X}")
                     if status_store.Status == 0x0000:
                         assoc.release()
                         logger.info(f"Association released with {self.dest_ae['ae_title']}")
                         return True # Explicit success
                     else:
                         # Failed status from remote AE
                         assoc.release() # Still release cleanly
                         raise StorageBackendError(f"C-STORE rejected by remote AE '{self.dest_ae['ae_title']}' with status 0x{status_store.Status:04X}")
                else:
                    # No status received, likely DIMSE timeout or network issue during C-STORE
                    logger.error(f"C-STORE failed for {original_filepath.name} - No response received from {self.dest_ae['ae_title']}")
                    assoc.abort()
                    raise StorageBackendError(f"C-STORE to {self.dest_ae['ae_title']} failed - No response")
            else:
                # Association was rejected or aborted immediately
                logger.error(f"Association rejected or aborted by {self.dest_ae['ae_title']}")
                # The reason might be logged by pynetdicom already
                raise StorageBackendError(f"Could not associate with '{self.dest_ae['ae_title']}' at {self.dest_ae['host']}:{self.dest_ae['port']}")

        # --- Corrected Exception Handling ---
        except (socket.timeout, TimeoutError) as e: # Catch standard socket/builtin timeouts
             logger.error(f"Network timeout during C-STORE to {self.dest_ae['host']}:{self.dest_ae['port']}: {e}", exc_info=True)
             # No need to abort assoc if it wasn't established or timed out during operation
             raise StorageBackendError(f"Network timeout connecting/communicating with {self.dest_ae['host']}:{self.dest_ae['port']}") from e
        except ConnectionRefusedError as e:
             logger.error(f"C-STORE connection refused by {self.dest_ae['host']}:{self.dest_ae['port']}.", exc_info=True)
             raise StorageBackendError(f"Connection refused by {self.dest_ae['host']}:{self.dest_ae['port']}") from e
        except OSError as e: # Catch other OS-level errors like "Network is unreachable"
             logger.error(f"Network/OS error during C-STORE to {self.dest_ae['host']}:{self.dest_ae['port']}: {e}", exc_info=True)
             raise StorageBackendError(f"Network error connecting to {self.dest_ae['host']}:{self.dest_ae['port']}: {e}") from e
        #except PynetdicomError as e: # Catch base pynetdicom errors specifically
        #    logger.error(f"Pynetdicom library error during C-STORE to {self.dest_ae['ae_title']}: {e}", exc_info=True)
        #    if assoc and not assoc.is_released and not assoc.is_aborted: assoc.abort()
        #    raise StorageBackendError(f"C-STORE protocol/library error: {e}") from e
        except StorageBackendError as e: # Re-raise specific StorageBackendErrors we generated
             raise e
        except Exception as e: # Catch any other unexpected exceptions
            logger.error(f"C-STORE operation unexpectedly failed for {original_filepath.name} "
                         f"to {self.dest_ae['ae_title']}: {e}", exc_info=True)
            if assoc and not assoc.is_released and not assoc.is_aborted: assoc.abort()
            raise StorageBackendError(f"C-STORE unexpected error: {e}") from e
        finally:
            # Final safety net: Ensure association is closed if it exists and wasn't handled
            if assoc and not assoc.is_released and not assoc.is_aborted:
                logger.warning(f"Association with {self.dest_ae['ae_title']} was not properly closed in CStoreStorage.store, aborting.")
                assoc.abort()

        # Should only be reached if an error path didn't raise StorageBackendError
        return False
