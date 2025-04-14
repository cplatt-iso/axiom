# app/services/storage_backends/dicom_cstore.py

import logging
from pathlib import Path
from typing import Any, Dict

from pydicom.dataset import Dataset
from pynetdicom import AE, VerificationPresentationContexts, StoragePresentationContexts
from pynetdicom.sop_class import VerificationSOPClass

from .base_backend import BaseStorageBackend
from . import StorageBackendError

logger = logging.getLogger(__name__)

class CStoreStorage(BaseStorageBackend):
    """Sends DICOM files via C-STORE SCU."""

    def _validate_config(self):
        """Validate C-STORE configuration."""
        required_keys = ["ae_title", "host", "port"]
        if not all(key in self.config for key in required_keys):
            raise ValueError(f"C-STORE storage config requires: {', '.join(required_keys)}")
        self.dest_ae = {
            "ae_title": self.config["ae_title"],
            "host": self.config["host"],
            "port": int(self.config["port"])
        }
        # Optional: Configure calling AET, timeouts etc. from config
        self.calling_ae_title = self.config.get("calling_ae_title", "DICOM_PROCESSOR_SCU")


    def store(self, modified_ds: Dataset, original_filepath: Path) -> bool:
        """
        Sends the modified DICOM dataset via C-STORE.

        Returns:
            True if the C-STORE operation was successful, False otherwise.
        """
        ae = AE(ae_title=self.calling_ae_title)

        # Add presentation contexts based on the modified dataset's SOP Class UID
        sop_class_uid = modified_ds.SOPClassUID
        if not sop_class_uid:
             logger.error(f"Cannot perform C-STORE for {original_filepath.name}: SOPClassUID is missing from modified dataset.")
             raise StorageBackendError("Missing SOPClassUID in dataset for C-STORE")

        # Offer default transfer syntaxes for the specific SOP class
        ae.add_requested_context(sop_class_uid) # Default: Explicit VR LE, Implicit VR LE
        # Optionally add Verification context for echo test before store
        ae.add_requested_context(VerificationSOPClass)

        logger.debug(f"Attempting C-STORE to AE '{self.dest_ae['ae_title']}' at "
                     f"{self.dest_ae['host']}:{self.dest_ae['port']} for {original_filepath.name}")

        assoc = None # Initialize assoc to None
        try:
            assoc = ae.associate(
                self.dest_ae["host"],
                self.dest_ae["port"],
                ae_title=self.dest_ae["ae_title"]
            )

            if assoc.is_established:
                logger.info(f"Association established with {self.dest_ae['ae_title']}")

                # Optionally perform C-ECHO test first
                # status_echo = assoc.send_c_echo()
                # if status_echo and status_echo.Status == 0x0000:
                #     logger.info("C-ECHO successful")
                # else:
                #     logger.warning(f"C-ECHO failed or status not success: {status_echo}")
                #     raise StorageBackendError(f"C-ECHO failed to {self.dest_ae['ae_title']}")

                # Send the C-STORE request
                status_store = assoc.send_c_store(modified_ds)

                # Check the status of the storage request
                if status_store and status_store.Status == 0x0000:
                    logger.info(f"C-STORE successful for {original_filepath.name} "
                                f"(Status: {status_store.Status:04x})")
                    assoc.release()
                    logger.debug(f"Association released with {self.dest_ae['ae_title']}")
                    return True
                else:
                    status_val = status_store.Status if status_store else "Unknown"
                    logger.error(f"C-STORE failed for {original_filepath.name} - Status: {status_val}")
                    assoc.abort() # Abort on failure
                    raise StorageBackendError(f"C-STORE to {self.dest_ae['ae_title']} failed with status {status_val}")

            else:
                logger.error(f"Association rejected or aborted for {self.dest_ae['ae_title']}")
                raise StorageBackendError(f"Could not associate with {self.dest_ae['ae_title']}")

        except Exception as e:
            logger.error(f"C-STORE operation failed for {original_filepath.name} "
                         f"to {self.dest_ae['ae_title']}: {e}", exc_info=True)
            if assoc and assoc.is_established:
                assoc.abort() # Ensure association is aborted on any error
            # Re-raise as our custom error type
            if not isinstance(e, StorageBackendError):
                 raise StorageBackendError(f"C-STORE network/protocol error: {e}") from e
            else:
                 raise e
        finally:
             # Although release/abort should be called, ensure it's not left dangling
             if assoc and not assoc.is_released and not assoc.is_aborted:
                logger.warning(f"Association with {self.dest_ae['ae_title']} was not properly closed, aborting.")
                assoc.abort()

        return False # Should not be reached if errors are raised
