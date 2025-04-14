# app/services/storage_backends/dicom_cstore.py

import logging
from pathlib import Path
from typing import Any, Dict

from pydicom.dataset import Dataset
from pynetdicom import AE, debug_logger
from pynetdicom.sop_class import VerificationSOPClass
from pynetdicom.presentation import build_context # To build contexts dynamically

from .base_backend import BaseStorageBackend
from . import StorageBackendError

# Get logger for this module
logger = logging.getLogger(__name__)
# Uncomment to enable verbose pynetdicom debugging
# debug_logger()


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
        self.calling_ae_title = self.config.get("calling_ae_title", "AXIOM_FLOW_SCU") # Updated name
        # Network timeouts (can be added back carefully if needed)
        self.assoc_timeout = self.config.get("assoc_timeout", 30) # Timeout for associate request
        self.dimse_timeout = self.config.get("dimse_timeout", 60) # Timeout for DIMSE messages (C-STORE)


    def store(self, modified_ds: Dataset, original_filepath: Path) -> bool:
        """
        Sends the modified DICOM dataset via C-STORE.

        Returns:
            True if the C-STORE operation was successful, False otherwise.
        """
        ae = AE(ae_title=self.calling_ae_title)

        # Add presentation context based on the modified dataset's SOP Class UID
        sop_class_uid = modified_ds.file_meta.MediaStorageSOPClassUID # Get from file_meta
        if not sop_class_uid:
             # Try getting from main dataset as fallback
             sop_class_uid = modified_ds.get("SOPClassUID", None)
             if not sop_class_uid:
                  logger.error(f"Cannot perform C-STORE for {original_filepath.name}: SOPClassUID is missing from dataset.")
                  raise StorageBackendError("Missing SOPClassUID in dataset for C-STORE")
             else:
                 logger.warning(f"Using SOPClassUID from main dataset for {original_filepath.name}, file meta preferred.")

        # Offer default transfer syntaxes for the specific SOP class
        # (Implicit Little Endian, Explicit Little Endian)
        context = build_context(sop_class_uid) # Build context with default syntaxes
        ae.add_requested_context(context)
        # ae.add_requested_context(sop_class_uid) # Simpler form also works for defaults

        # Optionally add Verification context for echo test before store
        ae.add_requested_context(VerificationSOPClass)

        target_desc = f"AE '{self.dest_ae['ae_title']}' at {self.dest_ae['host']}:{self.dest_ae['port']}"
        logger.debug(f"Attempting C-STORE to {target_desc} for {original_filepath.name}")

        assoc = None # Initialize assoc to None
        try:
            # Associate with the remote AE
            # Pass timeouts here if needed and supported by the version/call signature
            assoc = ae.associate(
                self.dest_ae["host"],
                self.dest_ae["port"],
                ae_title=self.dest_ae["ae_title"],
                # Add timeouts back carefully:
                acse_timeout=self.assoc_timeout,
                dimse_timeout=self.dimse_timeout,
                # bind_address=(settings.LISTEN_IP, 0) # Optionally bind to specific interface/port
            )

            if assoc.is_established:
                logger.info(f"Association established with {target_desc}")

                # Optional: C-ECHO Test (Good practice before sending lots of data)
                # try:
                #     status_echo = assoc.send_c_echo()
                #     if status_echo and status_echo.Status == 0x0000:
                #         logger.info(f"C-ECHO successful to {target_desc}")
                #     else:
                #         echo_status_val = status_echo.Status if status_echo else 'Unknown'
                #         logger.warning(f"C-ECHO to {target_desc} failed or status not success: {echo_status_val}")
                #         raise StorageBackendError(f"C-ECHO failed to {self.dest_ae['ae_title']} with status {echo_status_val}")
                # except Exception as echo_exc:
                #     logger.error(f"C-ECHO to {target_desc} raised an exception: {echo_exc}", exc_info=True)
                #     raise StorageBackendError(f"C-ECHO to {self.dest_ae['ae_title']} failed: {echo_exc}") from echo_exc

                # Send the C-STORE request using the modified dataset
                logger.debug(f"Sending C-STORE request for {original_filepath.name} (SOP UID: {modified_ds.SOPInstanceUID})...")
                status_store = assoc.send_c_store(modified_ds)

                # Check the status of the storage request
                if status_store and status_store.Status == 0x0000:
                    logger.info(f"C-STORE successful for {original_filepath.name} "
                                f"(Status: {status_store.Status:04x})")
                    # Release the association gracefully
                    assoc.release()
                    logger.debug(f"Association released with {target_desc}")
                    return True
                else:
                    # Storage failed, log details and abort
                    status_val = status_store.Status if status_store else "No Response Status"
                    status_repr = f"0x{status_val:04X}" if isinstance(status_val, int) else str(status_val)
                    logger.error(f"C-STORE failed for {original_filepath.name} - Status: {status_repr}")
                    assoc.abort() # Abort on failure
                    logger.debug(f"Association aborted with {target_desc} due to C-STORE failure.")
                    raise StorageBackendError(f"C-STORE to {self.dest_ae['ae_title']} failed with status {status_repr}")

            else:
                # Association failed
                logger.error(f"Association rejected or aborted connecting to {target_desc}")
                # Examine assoc.acceptor details if needed: assoc.acceptor.primitive
                raise StorageBackendError(f"Could not associate with {self.dest_ae['ae_title']}")

        except OSError as e:
             # Catch specific network errors like connection refused
             logger.error(f"C-STORE OS Error connecting to {target_desc}: {e}", exc_info=False)
             raise StorageBackendError(f"Network error connecting to {self.dest_ae['ae_title']}: {e}") from e
        except Exception as e:
             # Catch any other exceptions during association or C-STORE
             logger.error(f"C-STORE operation failed for {original_filepath.name} "
                          f"to {target_desc}: {e}", exc_info=True)
             if assoc and assoc.is_active:
                 # Try to abort if association seems established but error occurred
                 assoc.abort()
                 logger.debug(f"Association aborted with {target_desc} due to exception.")

             # Re-raise wrapped as our custom error type if it wasn't already
             if not isinstance(e, StorageBackendError):
                  raise StorageBackendError(f"C-STORE network/protocol error: {e}") from e
             else:
                  raise e # Re-raise the original StorageBackendError
        finally:
             # Ensure association is closed even if logic above missed something
             if assoc and assoc.is_active:
                 logger.warning(f"Association with {target_desc} was left active in finally block, aborting.")
                 assoc.abort()

        return False # Should not be reached if errors are properly raised
