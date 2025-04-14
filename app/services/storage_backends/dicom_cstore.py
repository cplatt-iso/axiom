# app/services/storage_backends/dicom_cstore.py

import logging
from pathlib import Path
from typing import Any, Dict

from pydicom.dataset import Dataset
# Import only what's needed
from pynetdicom import AE # Removed unused PresentationContexts imports
# from pynetdicom.sop_class import VerificationSOPClass # REMOVE THIS LINE

# Import Base AND Error from base_backend
from .base_backend import BaseStorageBackend, StorageBackendError

logger = logging.getLogger(__name__)

# --- Default value for calling AET ---
# Define once, potentially configurable later via settings if needed
DEFAULT_CALLING_AET = "AXIOM_FLOW_SCU"

class CStoreStorage(BaseStorageBackend):
    """Sends DICOM files via C-STORE SCU."""

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
        except ValueError as e:
             raise ValueError(f"Invalid C-STORE config - port must be an integer: {e}")

        self.calling_ae_title = self.config.get("calling_ae_title", DEFAULT_CALLING_AET)
        # Add timeouts from config if needed
        self.connect_timeout = self.config.get("connect_timeout", 10) # seconds
        self.response_timeout = self.config.get("response_timeout", 30) # seconds
        self.acse_timeout = self.config.get("acse_timeout", 30) # seconds


    def store(self, modified_ds: Dataset, original_filepath: Path) -> bool:
        """
        Sends the modified DICOM dataset via C-STORE.

        Returns:
            True if the C-STORE operation was successful, False otherwise (raises Exception on failure).
        """
        ae = AE(ae_title=self.calling_ae_title)

        # Add presentation context for the specific dataset being sent
        sop_class_uid = modified_ds.file_meta.MediaStorageSOPClassUID # Get from file_meta
        if not sop_class_uid:
             logger.error(f"Cannot perform C-STORE for {original_filepath.name}: MediaStorageSOPClassUID is missing from modified dataset's file meta.")
             raise StorageBackendError("Missing MediaStorageSOPClassUID in dataset for C-STORE")

        # Add requested context for the dataset's SOP Class
        # Using default transfer syntaxes (Implicit/Explicit Little Endian)
        ae.add_requested_context(sop_class_uid)
        logger.debug(f"Added requested context for SOP Class: {sop_class_uid}")

        # --- REMOVED C-ECHO LOGIC ---
        # logger.debug("Adding Verification context for optional pre-send C-ECHO test")
        # ae.add_requested_context('1.2.840.10008.1.1') # Add by UID
        # --- END REMOVED C-ECHO LOGIC ---

        logger.debug(f"Attempting C-STORE to AE '{self.dest_ae['ae_title']}' at "
                     f"{self.dest_ae['host']}:{self.dest_ae['port']} for {original_filepath.name}")

        assoc = None # Initialize assoc to None
        try:
            assoc = ae.associate(
                self.dest_ae["host"],
                self.dest_ae["port"],
                ae_title=self.dest_ae["ae_title"],
                # Pass configured timeouts
                acse_timeout=self.acse_timeout,
                connect_timeout=self.connect_timeout
            )

            if assoc.is_established:
                logger.info(f"Association established with {self.dest_ae['ae_title']}")

                # --- REMOVED C-ECHO TEST ---

                # Send the C-STORE request with response timeout
                status_store = assoc.send_c_store(modified_ds, dimse_timeout=self.response_timeout)

                # Check the status of the storage request
                if status_store:
                    status_val = status_store.Status
                    logger.debug(f"C-STORE Response Status: 0x{status_val:04x}")
                    if status_val == 0x0000: # Success
                        logger.info(f"C-STORE successful for {original_filepath.name}")
                        assoc.release()
                        logger.debug(f"Association released with {self.dest_ae['ae_title']}")
                        return True
                    # Specific warning/error codes (optional detailed handling)
                    elif status_val in [0xB000, 0xB006, 0xB007]: # Coercion related warnings
                        logger.warning(f"C-STORE for {original_filepath.name} reported success with warning: {status_val:04x}")
                        assoc.release()
                        logger.debug(f"Association released with {self.dest_ae['ae_title']} after warning.")
                        return True # Treat warnings as success for this backend? Configurable?
                    else: # Failure or other status
                        logger.error(f"C-STORE failed for {original_filepath.name} - Status: 0x{status_val:04x}")
                        assoc.abort() # Abort on failure
                        raise StorageBackendError(f"C-STORE to {self.dest_ae['ae_title']} failed with status 0x{status_val:04x}")
                else:
                    logger.error(f"C-STORE failed for {original_filepath.name} - No response status received (timeout?).")
                    assoc.abort() # Abort on failure
                    raise StorageBackendError(f"C-STORE to {self.dest_ae['ae_title']} failed - No response status (timeout?)")

            else:
                logger.error(f"Association rejected or aborted with {self.dest_ae['ae_title']}")
                raise StorageBackendError(f"Could not associate with {self.dest_ae['ae_title']}")

        except Exception as e:
            logger.error(f"C-STORE operation failed during execution for {original_filepath.name} "
                         f"to {self.dest_ae['ae_title']}: {e}", exc_info=True)
            if assoc and assoc.is_established:
                assoc.abort() # Ensure association is aborted on any error
            # Re-raise as our custom error type
            if not isinstance(e, StorageBackendError):
                 raise StorageBackendError(f"C-STORE network/protocol error: {e}") from e
            else:
                 raise e
        finally:
             # Ensure association state is cleaned up
             if assoc and not assoc.is_released and not assoc.is_aborted:
                logger.warning(f"Association with {self.dest_ae['ae_title']} was not properly closed, aborting.")
                assoc.abort()

        # This line should theoretically not be reached if errors are raised correctly
        return False
