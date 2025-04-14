# app/services/storage_backends/dicom_cstore.py

import logging
from pathlib import Path
from typing import Any, Dict
import socket # For potential timeout errors

from pydicom.dataset import Dataset
from pydicom.uid import UID

from pynetdicom import AE # AE is top-level
from pynetdicom.transport import AssociationSocket # Import from transport submodule
# No import for PynetdicomError for now

# Logging setup
# ...

from .base_backend import BaseStorageBackend
from . import StorageBackendError

logger = logging.getLogger(__name__)

class CStoreStorage(BaseStorageBackend):
    """Sends DICOM files via C-STORE SCU."""

    def _validate_config(self):
        # ... (as before) ...
        required_keys = ["ae_title", "host", "port"]
        if not all(key in self.config for key in required_keys):
            raise ValueError(f"C-STORE storage config requires: {', '.join(required_keys)}")
        self.dest_ae = {
            "ae_title": self.config["ae_title"],
            "host": self.config["host"],
            "port": int(self.config["port"])
        }
        self.calling_ae_title = self.config.get("calling_ae_title", "AXIOM_SCU")
        self.network_timeout = self.config.get("network_timeout", 30)
        self.acse_timeout = self.config.get("acse_timeout", 30)
        self.pdu_timeout = self.config.get("pdu_timeout", 60)
        self.max_pdu_size = self.config.get("max_pdu_size", 16384)

    def store(self, modified_ds: Dataset, original_filepath: Path) -> bool:
        # ... (Setup AE, get SOP Class UID as before) ...
        ae = AE(ae_title=self.calling_ae_title)
        ae.network_timeout = self.network_timeout
        ae.acse_timeout = self.acse_timeout
        ae.dimse_timeout = self.pdu_timeout
        ae.maximum_pdu_size = self.max_pdu_size

        sop_class_uid_obj = modified_ds.get("SOPClassUID")
        if not sop_class_uid_obj:
             logger.error(f"Cannot perform C-STORE for {original_filepath.name}: SOPClassUID is missing")
             raise StorageBackendError("Missing SOPClassUID in dataset for C-STORE")
        sop_class_uid_str = str(sop_class_uid_obj)
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
            )

            if assoc.is_established:
                logger.info(f"Association established with {self.dest_ae['ae_title']}")
                # ... Optional C-ECHO ...
                logger.debug(f"Sending C-STORE request for {original_filepath.name}...")
                status_store = assoc.send_c_store(modified_ds)

                if status_store:
                     logger.info(f"C-STORE response received for {original_filepath.name} - Status: 0x{status_store.Status:04X}")
                     if status_store.Status == 0x0000:
                         assoc.release()
                         logger.info(f"Association released with {self.dest_ae['ae_title']}")
                         return True
                     else:
                         assoc.release()
                         raise StorageBackendError(f"C-STORE failed by remote AE '{self.dest_ae['ae_title']}' with status 0x{status_store.Status:04X}")
                else:
                    logger.error(f"C-STORE failed for {original_filepath.name} - No response received from {self.dest_ae['ae_title']}")
                    assoc.abort()
                    raise StorageBackendError(f"C-STORE to {self.dest_ae['ae_title']} failed - No response")
            else:
                logger.error(f"Association rejected or aborted by {self.dest_ae['ae_title']}")
                raise StorageBackendError(f"Could not associate with '{self.dest_ae['ae_title']}' at {self.dest_ae['host']}:{self.dest_ae['port']}")

        except ConnectionRefusedError as e:
            logger.error(f"C-STORE connection refused by {self.dest_ae['host']}:{self.dest_ae['port']}.", exc_info=True)
            raise StorageBackendError(f"Connection refused by {self.dest_ae['host']}:{self.dest_ae['port']}") from e
        # Catch specific timeout error from pynetdicom if it's this class
        except AssociationSocket.Timeout as e:
             logger.error(f"C-STORE association/network timeout to {self.dest_ae['host']}:{self.dest_ae['port']}: {e}", exc_info=True)
             if assoc and assoc.is_established: assoc.abort()
             raise StorageBackendError(f"Timeout connecting/associating with {self.dest_ae['host']}:{self.dest_ae['port']}") from e
        # Catch generic socket timeout as well
        except socket.timeout as e:
            logger.error(f"C-STORE socket timeout to {self.dest_ae['host']}:{self.dest_ae['port']}: {e}", exc_info=True)
            if assoc and assoc.is_established: assoc.abort()
            raise StorageBackendError(f"Socket timeout with {self.dest_ae['host']}:{self.dest_ae['port']}") from e
        # Catch generic exceptions last
        except Exception as e:
            # This might catch PynetdicomError implicitly
            logger.error(f"C-STORE operation unexpectedly failed for {original_filepath.name} "
                         f"to {self.dest_ae['ae_title']}: {e}", exc_info=True)
            if assoc and not assoc.is_released:
                assoc.abort()
            if not isinstance(e, StorageBackendError):
                 # Check class name to see if it's a pynetdicom error for logging clarity
                 if 'Pynetdicom' in e.__class__.__name__:
                     raise StorageBackendError(f"C-STORE pynetdicom error: {e}") from e
                 else:
                     raise StorageBackendError(f"C-STORE unexpected error: {e}") from e
            else:
                 raise e
        finally:
            if assoc and not assoc.is_released and not assoc.is_aborted:
                logger.warning(f"Association with {self.dest_ae['ae_title']} was not properly closed in CStoreStorage.store, aborting.")
                assoc.abort()

        return False
