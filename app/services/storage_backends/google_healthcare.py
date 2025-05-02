# app/services/storage_backends/google_healthcare.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Union
import uuid

import requests
import pydicom
from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

try:
    import google.auth
    import google.auth.transport.requests
except ImportError:
    google = None
    logging.getLogger(__name__).warning("google-auth library not found. Google Healthcare backend unusable.")

try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

from .base_backend import BaseStorageBackend, StorageBackendError

class GoogleHealthcareDicomStoreStorage(BaseStorageBackend):
    """Sends DICOM files to a Google Cloud Healthcare DICOM Store via STOW-RS."""

    def __init__(self, config: Dict[str, Any]):
        self.backend_type = config.get("type", "google_healthcare")
        self.name = config.get("name", "Unnamed GHC Backend")
        self.project_id = config.get("gcp_project_id")
        self.location = config.get("gcp_location")
        self.dataset_id = config.get("gcp_dataset_id")
        self.dicom_store_id = config.get("gcp_dicom_store_id")

        if google is None:
            raise StorageBackendError("google-auth library is not installed.")

        required_keys = ["gcp_project_id", "gcp_location", "gcp_dataset_id", "gcp_dicom_store_id"]
        missing = [key for key in required_keys if not getattr(self, key.replace("gcp_", ""), None)] # Check instance attrs
        if missing:
            raise ValueError(f"Google Healthcare storage config missing required keys: {', '.join(missing)}")

        self.base_url = (
            f"https://healthcare.googleapis.com/v1/projects/{self.project_id}"
            f"/locations/{self.location}/datasets/{self.dataset_id}"
            f"/dicomStores/{self.dicom_store_id}/dicomWeb"
        )
        self.stow_url = f"{self.base_url}/studies"
        self.credentials = None # Initialize
        self._validate_config()

    def _validate_config(self):
        try:
            credentials, project = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-healthcare'])
            auth_req = google.auth.transport.requests.Request()
            if not credentials or not credentials.valid:
                 logger.info("GHC: Google Cloud credentials require refresh...")
                 try:
                     if credentials:
                          credentials.refresh(auth_req)
                          logger.info("GHC: Google Cloud credentials refreshed.")
                     else:
                           raise StorageBackendError("Cannot refresh None credentials.")
                 except Exception as refresh_error:
                     logger.error("Failed to refresh GHC credentials", error=str(refresh_error), exc_info=False)
                     raise StorageBackendError("Failed to refresh GHC credentials.") from refresh_error
            if not credentials or not credentials.valid:
                 logger.error("Could not obtain valid GHC credentials.")
                 raise StorageBackendError("Could not obtain valid GHC credentials.")
            logger.info("GHC: Google Cloud credentials obtained and valid", project_id=(project or 'Default'))
            self.credentials = credentials
        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.error("GHC: Failed to find Google Cloud default credentials.", error=str(e), exc_info=False)
            raise StorageBackendError("Could not find Google Cloud credentials.") from e
        except StorageBackendError as e: raise e
        except Exception as e:
            logger.error("GHC: Failed to initialize Google Cloud credentials", error=str(e), exc_info=True)
            raise StorageBackendError(f"GHC credential initialization failed: {e}") from e

    def _get_auth_token(self) -> str:
        if not hasattr(self, 'credentials') or not self.credentials:
             raise StorageBackendError("GHC credentials not initialized.")
        try:
            auth_req = google.auth.transport.requests.Request()
            if not self.credentials.valid:
                logger.info("Refreshing GHC access token...")
                self.credentials.refresh(auth_req)
                logger.info("GHC access token refreshed.")
            if not self.credentials.valid or not self.credentials.token:
                 raise StorageBackendError("Could not obtain valid GHC access token.")
            return self.credentials.token
        except Exception as e:
            logger.error("Failed to get/refresh GHC access token", error=str(e), exc_info=True)
            raise StorageBackendError("Failed to obtain GHC access token.") from e

    def store(
        self,
        modified_ds: Dataset,
        original_filepath: Optional[Path] = None,
        filename_context: Optional[str] = None,
        source_identifier: Optional[str] = None,
        **kwargs: Any
    ) -> Union[Dict[str, Any], str]:
        sop_instance_uid = getattr(modified_ds, 'SOPInstanceUID', 'Unknown_SOPInstanceUID')
        study_instance_uid = getattr(modified_ds, 'StudyInstanceUID', 'Unknown_StudyInstanceUID')
        log = logger.bind(
            sop_instance_uid=sop_instance_uid, study_instance_uid=study_instance_uid,
            google_dicom_store=f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}",
            storage_backend_type=self.backend_type
        )
        log.debug("Attempting GHC STOW-RS upload", stow_url=self.stow_url)
        buffer = None
        try:
            access_token = self._get_auth_token()
            if not hasattr(modified_ds, 'file_meta') or not modified_ds.file_meta:
                 log.warning("Dataset missing file_meta, creating default for GHC STOW.")
                 file_meta = pydicom.dataset.FileMetaDataset(); file_meta.FileMetaInformationVersion = b'\x00\x01'
                 sop_class_uid_data = modified_ds.get("SOPClassUID"); sop_class_uid_val_list = getattr(sop_class_uid_data, 'value', ['1.2.840.10008.5.1.4.1.1.2'])
                 sop_class_uid_val = sop_class_uid_val_list[0] if isinstance(sop_class_uid_val_list, list) and sop_class_uid_val_list else '1.2.840.10008.5.1.4.1.1.2'
                 file_meta.MediaStorageSOPClassUID = sop_class_uid_val; file_meta.MediaStorageSOPInstanceUID = sop_instance_uid
                 transfer_syntax = getattr(modified_ds.get("TransferSyntaxUID"), 'value', [pydicom.uid.ImplicitVRLittleEndian])[0]
                 if transfer_syntax not in pydicom.uid.TRANSFER_SYNTAXES: transfer_syntax = pydicom.uid.ImplicitVRLittleEndian; log.debug("Using default Implicit VR LE.")
                 file_meta.TransferSyntaxUID = transfer_syntax; file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
                 file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"; modified_ds.file_meta = file_meta

            buffer = BytesIO(); dcmwrite(buffer, modified_ds, write_like_original=False); buffer.seek(0)
            dicom_bytes = buffer.getvalue()
            boundary = uuid.uuid4().hex
            headers = { "Authorization": f"Bearer {access_token}", "Accept": "application/dicom+json", "Content-Type": f'multipart/related; type="application/dicom"; boundary="{boundary}"' }
            body = b""; body += f"--{boundary}\r\n".encode('utf-8'); body += b"Content-Type: application/dicom\r\n\r\n"; body += dicom_bytes; body += b"\r\n"; body += f"--{boundary}--\r\n".encode('utf-8')
            response = requests.post(self.stow_url, headers=headers, data=body, timeout=120)

            if 200 <= response.status_code < 300:
                log.info("GHC STOW-RS successful", status_code=response.status_code)
                try: return response.json()
                except requests.exceptions.JSONDecodeError: log.warning("GHC STOW success but non-JSON response."); return {"status": "success", "message": "Upload successful, non-JSON response."}
            elif response.status_code == 409:
                failure_details = ""; failure_reason = "Conflict"
                try:
                    response_json = response.json(); failed_seq_val = response_json.get("00081198", {}).get("Value", [{}]); failed_seq = failed_seq_val[0] if isinstance(failed_seq_val, list) and failed_seq_val else {}
                    reason_code_data = failed_seq.get("00081197", {}); reason_code_val = getattr(reason_code_data, 'Value', [None]); reason_code = reason_code_val[0] if isinstance(reason_code_val, list) and reason_code_val else None
                    if reason_code == 272: failure_reason = "Duplicate SOPInstanceUID"
                    elif reason_code == 43264: failure_reason = "Instance Coercion"
                    retrieve_url_data = response_json.get("00081190", {}); retrieve_url_val = getattr(retrieve_url_data, 'Value', [None]); retrieve_url = retrieve_url_val[0] if isinstance(retrieve_url_val, list) and retrieve_url_val else None
                    failure_details = f"ReasonCode: {reason_code}, ConflictingStudyURL: {retrieve_url}"
                except Exception as parse_exc: log.warning("Failed parse 409 Conflict details", parse_error=str(parse_exc)); failure_details = response.text[:200]
                log.warning("GHC STOW-RS Conflict (Duplicate Instance?)", status_code=response.status_code, reason=failure_reason, details=failure_details)
                return "duplicate"
            else:
                error_details_str = response.text
                try: error_json = response.json(); error_details_str = str(error_json.get("error", {}).get("message", error_json))
                except requests.exceptions.JSONDecodeError: pass
                err_msg = f"STOW-RS to GHC failed. Status: {response.status_code}"
                log.error(err_msg, status_code=response.status_code, details=error_details_str[:500])
                raise StorageBackendError(f"{err_msg}, Details: {error_details_str[:500]}")
        except requests.exceptions.Timeout as e: raise StorageBackendError(f"Network timeout during GHC STOW ({self.stow_url})") from e
        except requests.exceptions.RequestException as e: raise StorageBackendError(f"Network error during GHC STOW ({self.stow_url}): {e}") from e
        except StorageBackendError as e: raise e
        except Exception as e: raise StorageBackendError(f"Unexpected error storing via GHC STOW: {e}") from e
        finally:
            if buffer: buffer.close()

    def __repr__(self) -> str:
        store_path = f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}"
        return f"<{self.__class__.__name__}(dicom_store={store_path})>"
