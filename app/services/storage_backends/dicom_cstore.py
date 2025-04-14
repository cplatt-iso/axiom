# app/services/storage_backends/dicom_cstore.py

import logging
from pathlib import Path
from typing import Any, Dict

from pydicom.dataset import Dataset
from pynetdicom import AE, debug_logger
# REMOVED: from pynetdicom.sop_class import VerificationSOPClass # problematic import
from pynetdicom.presentation import build_context # To build contexts dynamically
# Import UID definition directly if preferred over string
from pynetdicom import AllStoragePresentationContexts, VerificationPresentationContexts
# Or just use the UID string directly:
VERIFICATION_SOP_CLASS_UID = '1.2.840.10008.1.1'


from .base_backend import BaseStorageBackend
from . import StorageBackendError # Import custom error

# Get logger for this module
logger = logging.getLogger(__name__)
# Uncomment to enable verbose pynetdicom debugging for this specific module/process
# logging.getLogger('pynetdicom').setLevel(logging.DEBUG)


class CStoreStorage(BaseStorageBackend):
    """Sends DICOM files via C-STORE SCU."""

    def _validate_config(self):
        """Validate C-STORE configuration."""
        required_keys = ["ae_title", "host", "port"]
        if not all(key in self.config for key in required_keys):
            raise ValueError(f"C-STORE storage config requires: {', '.join(required_keys)}")

        try:
             port = int(self.config["port"])
        except ValueError:
             raise ValueError("C-STORE storage config 'port' must be an integer.")

        self.dest_ae = {
            "ae_title": self.config["ae_title"],
            "host": self.config["host"],
            "port": port
        }
        # Optional: Configure calling AET, timeouts etc. from config
        self.calling_ae_title = self.config.get("calling_ae_title", "AXIOM_FLOW_SCU") # Use project name
        # Network timeouts
        try:
             self.assoc_timeout = int(self.config.get("assoc_timeout", 30))
             self.dimse_timeout = int(self.config.get("dimse_timeout", 60))
        except ValueError:
             logger.warning("Invalid timeout values in C-STORE config, using defaults (30s/60s)")
             self.assoc_timeout = 30
             self.dimse_timeout = 60


    def store(self, modified_ds: Dataset, original_filepath: Path) -> bool:
        """
        Sends the modified DICOM dataset via C-STORE.

        Returns:
            True if the C-STORE operation was successful. Raises StorageBackendError on failure.
        """
        ae = AE(ae_title=self.calling_ae_title)

        # --- Determine SOP Class UID ---
        sop_class_uid = None
        if modified_ds.file_meta and modified_ds.file_meta.get("MediaStorageSOPClassUID"):
             sop_class_uid = modified_ds.file_meta.MediaStorageSOPClassUID
             logger.debug(f"Using MediaStorageSOPClassUID ({sop_class_uid}) for C-STORE context.")
        elif modified_ds.get("SOPClassUID"):
             sop_class_uid = modified_ds.SOPClassUID
             logger.warning(f"Using SOPClassUID ({sop_class_uid}) from main dataset for C-STORE context (file_meta preferred).")
        else:
             logger.error(f"Cannot perform C-STORE for {original_filepath.name}: SOPClassUID is missing from both file_meta and dataset.")
             raise StorageBackendError("Missing SOPClassUID in dataset for C-STORE")

        # --- Add Presentation Contexts ---
        # Build context for the specific storage SOP Class with default transfer syntaxes
        storage_context = build_context(sop_class_uid)
        ae.add_requested_context(storage_context)

        # Optionally add Verification context for C-ECHO test, using the UID string
        ae.add_requested_context(VERIFICATION_SOP_CLASS_UID) # Referencing by UID string

        target_desc = f"AE '{self.dest_ae['ae_title']}' at {self.dest_ae['host']}:{self.dest_ae['port']}"
        log_prefix = f"CStoreStorage ({original_filepath.name} to {target_desc})" # Prefix logs for clarity
        logger.debug(f"{log_prefix}: Attempting C-STORE...")

        assoc = None # Initialize assoc to None
        try:
            # --- Association ---
            logger.debug(f"{log_prefix}: Initiating association...")
            assoc = ae.associate(
                self.dest_ae["host"],
                self.dest_ae["port"],
                ae_title=self.dest_ae["ae_title"],
                acse_timeout=self.assoc_timeout,
                dimse_timeout=self.dimse_timeout,
            )

            if assoc.is_established:
                logger.info(f"{log_prefix}: Association established.")

                # --- Optional: C-ECHO Test (Uncomment if needed) ---
                # echo_success = False
                # try:
                #     logger.debug(f"{log_prefix}: Sending C-ECHO verification...")
                #     status_echo = assoc.send_c_echo() # This uses the context added above
                #     if status_echo and status_echo.Status == 0x0000:
                #         logger.info(f"{log_prefix}: C-ECHO successful.")
                #         echo_success = True
                #     else:
                #         echo_status_val = getattr(status_echo, 'Status', 'Unknown Status')
                #         logger.warning(f"{log_prefix}: C-ECHO failed or status not success: {echo_status_val:#04x}")
                # except Exception as echo_exc:
                #     logger.error(f"{log_prefix}: C-ECHO raised an exception: {echo_exc}", exc_info=True)

                # --- C-STORE ---
                logger.debug(f"{log_prefix}: Sending C-STORE request (SOP UID: {modified_ds.SOPInstanceUID})...")
                status_store = assoc.send_c_store(modified_ds)

                # Check the status of the storage request
                if status_store and status_store.Status == 0x0000:
                    logger.info(f"{log_prefix}: C-STORE successful (Status: {status_store.Status:#04x}).")
                    assoc.release()
                    logger.debug(f"{log_prefix}: Association released.")
                    return True
                else:
                    status_val = getattr(status_store, 'Status', 'No Response Status') # Safely get status
                    status_repr = f"0x{status_val:04X}" if isinstance(status_val, int) else str(status_val)
                    error_comment = getattr(status_store, 'ErrorComment', None)
                    log_msg = f"{log_prefix}: C-STORE failed - Status: {status_repr}"
                    if error_comment: log_msg += f" Comment: {error_comment}"
                    logger.error(log_msg)
                    assoc.abort() # Abort on failure
                    logger.debug(f"{log_prefix}: Association aborted due to C-STORE failure.")
                    raise StorageBackendError(f"C-STORE to {self.dest_ae['ae_title']} failed with status {status_repr}")

            else:
                # Association failed
                logger.error(f"{log_prefix}: Association rejected or aborted connecting to {target_desc}")
                raise StorageBackendError(f"Could not associate with {self.dest_ae['ae_title']} (rejected/aborted).")

        except OSError as e:
             logger.error(f"{log_prefix}: Network OS Error during C-STORE: {e}", exc_info=False)
             raise StorageBackendError(f"Network error connecting to {self.dest_ae['ae_title']}: {e}") from e
        except Exception as e:
             logger.error(f"{log_prefix}: Unexpected error during C-STORE operation: {e}", exc_info=True)
             if assoc and assoc.is_active:
                 assoc.abort()
                 logger.debug(f"{log_prefix}: Association aborted due to unexpected exception.")
             if not isinstance(e, StorageBackendError):
                  raise StorageBackendError(f"C-STORE unexpected error: {e}") from e
             else:
                  raise e
        finally:
             if assoc and assoc.is_active:
                 logger.warning(f"{log_prefix}: Association left active in finally block, aborting.")
                 assoc.abort()

        return False
