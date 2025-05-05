# app/services/storage_backends/google_healthcare.py

import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Union, List
import uuid
import json
from urllib.parse import urlencode

import httpx # Use httpx for async requests
import pydicom
from pydicom.dataset import Dataset
from pydicom.filewriter import dcmwrite

try:
    import google.auth
    import google.auth.transport.requests # Still needed for sync refresh check? Or use async refresh?
    # Check if google.auth.transport.aiohttp or similar exists for native async refresh
    # google-auth >= 2.17.0 supports native async refresh
    google_auth_version = tuple(map(int, google.auth.__version__.split('.')[:2]))
    NATIVE_ASYNC_GOOGLE_AUTH = google_auth_version >= (2, 17)

except ImportError:
    google = None
    logging.getLogger(__name__).warning("google-auth library not found. Google Healthcare backend unusable.")
    NATIVE_ASYNC_GOOGLE_AUTH = False

try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

from .base_backend import BaseStorageBackend, StorageBackendError
from app.schemas.data_browser import StudyResultItem # Assuming these exist for typing

class GoogleHealthcareDicomStoreStorage(BaseStorageBackend):

    def __init__(self, config: Dict[str, Any]):
        self.backend_type = config.get("type", "google_healthcare")
        self.name = config.get("name", "Unnamed GHC Backend")
        self.project_id = config.get("gcp_project_id")
        self.location = config.get("gcp_location")
        self.dataset_id = config.get("gcp_dataset_id")
        self.dicom_store_id = config.get("gcp_dicom_store_id")
        self._client: Optional[httpx.AsyncClient] = None # Initialize httpx client later

        if google is None:
            raise StorageBackendError("google-auth library is not installed.")

        required_keys = ["gcp_project_id", "gcp_location", "gcp_dataset_id", "gcp_dicom_store_id"]
        missing = [key for key in required_keys if not getattr(self, key.replace("gcp_", ""), None)]
        if missing:
            raise ValueError(f"Google Healthcare storage config missing required keys: {', '.join(missing)}")

        self.base_url = (
            f"https://healthcare.googleapis.com/v1/projects/{self.project_id}"
            f"/locations/{self.location}/datasets/{self.dataset_id}"
            f"/dicomStores/{self.dicom_store_id}/dicomWeb"
        )
        self.stow_url = f"{self.base_url}/studies"
        self.qido_studies_url = f"{self.base_url}/studies"
        self.qido_series_url = f"{self.base_url}/series"
        self.qido_instances_url = f"{self.base_url}/instances"

        self.credentials = None
        # Validation now happens in async method initialize_client
        # We cannot call async methods in __init__ directly

    @property
    def client(self) -> httpx.AsyncClient:
         if self._client is None:
              # Consider client lifecycle management if needed (e.g., closing)
              self._client = httpx.AsyncClient(timeout=120.0) # Default timeout for requests
         return self._client

    async def initialize_client(self):
        if self.credentials: # Already initialized
            return
        try:
            creds, project = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-healthcare'])
            if not creds:
                raise StorageBackendError("Could not obtain default Google Cloud credentials.")

            if not creds.valid:
                 logger.info("GHC: Google Cloud credentials require refresh...")
                 try:
                     if NATIVE_ASYNC_GOOGLE_AUTH:
                         # Use native async refresh if available
                         import google.auth.transport.aiohttp # Requires aiohttp
                         request = google.auth.transport.aiohttp.Request()
                         await creds.refresh(request)
                         logger.info("GHC: Google Cloud credentials refreshed (async).")
                     else:
                         # Fallback to sync refresh in executor if needed (might block event loop)
                         # This path is less ideal. Ensure google-auth is updated.
                         logger.warning("GHC: Using sync credential refresh. Update google-auth >= 2.17.0 for async.")
                         auth_req_sync = google.auth.transport.requests.Request()
                         # TODO: Wrap in asyncio.to_thread if running in async context
                         # For now, assume it might block or work depending on environment
                         creds.refresh(auth_req_sync)
                         logger.info("GHC: Google Cloud credentials refreshed (sync fallback).")

                 except Exception as refresh_error:
                     logger.error("Failed to refresh GHC credentials", error=str(refresh_error), exc_info=False)
                     raise StorageBackendError("Failed to refresh GHC credentials.") from refresh_error

            if not creds.valid:
                 logger.error("Could not obtain valid GHC credentials after refresh attempt.")
                 raise StorageBackendError("Could not obtain valid GHC credentials.")

            logger.info("GHC: Google Cloud credentials obtained and valid", project_id=(project or 'Default'))
            self.credentials = creds
        except google.auth.exceptions.DefaultCredentialsError as e:
            logger.error("GHC: Failed to find Google Cloud default credentials.", error=str(e), exc_info=False)
            raise StorageBackendError("Could not find Google Cloud credentials.") from e
        except StorageBackendError as e: raise e
        except Exception as e:
            logger.error("GHC: Failed to initialize Google Cloud credentials", error=str(e), exc_info=True)
            raise StorageBackendError(f"GHC credential initialization failed: {e}") from e

    async def _get_auth_headers(self) -> Dict[str, str]:
        if not self.credentials:
            await self.initialize_client() # Ensure initialized

        if not self.credentials: # Check again after initialization attempt
             raise StorageBackendError("GHC credentials not initialized after attempt.")

        try:
             # Check validity and refresh if needed (async preferred)
             if not self.credentials.valid:
                  logger.info("Refreshing GHC access token (async)...")
                  if NATIVE_ASYNC_GOOGLE_AUTH:
                        import google.auth.transport.aiohttp
                        request = google.auth.transport.aiohttp.Request()
                        await self.credentials.refresh(request)
                  else:
                        logger.warning("GHC: Using sync credential refresh in async context. Update google-auth.")
                        auth_req_sync = google.auth.transport.requests.Request()
                        # TODO: Wrap in asyncio.to_thread
                        self.credentials.refresh(auth_req_sync)
                  logger.info("GHC access token refreshed.")

             if not self.credentials.valid or not self.credentials.token:
                  raise StorageBackendError("Could not obtain valid GHC access token after refresh.")

             return {"Authorization": f"Bearer {self.credentials.token}"}
        except Exception as e:
            logger.error("Failed to get/refresh GHC access token", error=str(e), exc_info=True)
            raise StorageBackendError("Failed to obtain GHC access token.") from e

    async def store(
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
            auth_headers = await self._get_auth_headers()

            if not hasattr(modified_ds, 'file_meta') or not modified_ds.file_meta:
                 log.warning("Dataset missing file_meta, creating default for GHC STOW.")
                 file_meta = pydicom.dataset.FileMetaDataset()
                 file_meta.FileMetaInformationVersion = b'\x00\x01'
                 sop_class_uid_data = modified_ds.get("SOPClassUID")
                 sop_class_uid_val_list = getattr(sop_class_uid_data, 'value', ['1.2.840.10008.5.1.4.1.1.2'])
                 sop_class_uid_val = sop_class_uid_val_list[0] if isinstance(sop_class_uid_val_list, list) and sop_class_uid_val_list else '1.2.840.10008.5.1.4.1.1.2'
                 file_meta.MediaStorageSOPClassUID = sop_class_uid_val
                 file_meta.MediaStorageSOPInstanceUID = sop_instance_uid
                 transfer_syntax_data = modified_ds.get("TransferSyntaxUID")
                 transfer_syntax_val_list = getattr(transfer_syntax_data, 'value', [pydicom.uid.ImplicitVRLittleEndian])
                 transfer_syntax = transfer_syntax_val_list[0] if isinstance(transfer_syntax_val_list, list) and transfer_syntax_val_list else pydicom.uid.ImplicitVRLittleEndian

                 if transfer_syntax not in pydicom.uid.TRANSFER_SYNTAXES:
                     transfer_syntax = pydicom.uid.ImplicitVRLittleEndian
                     log.debug("Using default Implicit VR LE.")
                 file_meta.TransferSyntaxUID = transfer_syntax
                 file_meta.ImplementationClassUID = pydicom.uid.PYDICOM_IMPLEMENTATION_UID
                 file_meta.ImplementationVersionName = f"pydicom {pydicom.__version__}"
                 modified_ds.file_meta = file_meta

            buffer = BytesIO()
            dcmwrite(buffer, modified_ds, write_like_original=False)
            buffer.seek(0)
            dicom_bytes = buffer.getvalue()

            boundary = uuid.uuid4().hex
            headers = {
                **auth_headers,
                "Accept": "application/dicom+json",
                "Content-Type": f'multipart/related; type="application/dicom"; boundary="{boundary}"'
            }

            body = b""
            body += f"--{boundary}\r\n".encode('utf-8')
            body += b"Content-Type: application/dicom\r\n\r\n"
            body += dicom_bytes
            body += b"\r\n"
            body += f"--{boundary}--\r\n".encode('utf-8')

            response = await self.client.post(self.stow_url, headers=headers, content=body) # Use httpx async post

            if 200 <= response.status_code < 300:
                log.info("GHC STOW-RS successful", status_code=response.status_code)
                try:
                    return response.json()
                except json.JSONDecodeError:
                    log.warning("GHC STOW success but non-JSON response.")
                    return {"status": "success", "message": "Upload successful, non-JSON response."}
            elif response.status_code == 409:
                failure_details = ""
                failure_reason = "Conflict"
                try:
                    response_json = response.json()
                    failed_seq_val = response_json.get("00081198", {}).get("Value", [{}])
                    failed_seq = failed_seq_val[0] if isinstance(failed_seq_val, list) and failed_seq_val else {}
                    reason_code_data = failed_seq.get("00081197", {})
                    reason_code_val = getattr(reason_code_data, 'Value', [None])
                    reason_code = reason_code_val[0] if isinstance(reason_code_val, list) and reason_code_val else None
                    if reason_code == 272: failure_reason = "Duplicate SOPInstanceUID"
                    elif reason_code == 43264: failure_reason = "Instance Coercion"
                    retrieve_url_data = response_json.get("00081190", {})
                    retrieve_url_val = getattr(retrieve_url_data, 'Value', [None])
                    retrieve_url = retrieve_url_val[0] if isinstance(retrieve_url_val, list) and retrieve_url_val else None
                    failure_details = f"ReasonCode: {reason_code}, ConflictingStudyURL: {retrieve_url}"
                except Exception as parse_exc:
                    log.warning("Failed parse 409 Conflict details", parse_error=str(parse_exc))
                    failure_details = response.text[:200]
                log.warning("GHC STOW-RS Conflict (Duplicate Instance?)", status_code=response.status_code, reason=failure_reason, details=failure_details)
                return "duplicate"
            else:
                error_details_str = response.text
                try:
                    error_json = response.json()
                    error_details_str = str(error_json.get("error", {}).get("message", error_json))
                except json.JSONDecodeError:
                    pass
                err_msg = f"STOW-RS to GHC failed. Status: {response.status_code}"
                log.error(err_msg, status_code=response.status_code, details=error_details_str[:500])
                raise StorageBackendError(f"{err_msg}, Details: {error_details_str[:500]}")
        except httpx.TimeoutException as e:
             raise StorageBackendError(f"Network timeout during GHC STOW ({self.stow_url})") from e
        except httpx.RequestError as e:
             raise StorageBackendError(f"Network error during GHC STOW ({self.stow_url}): {e}") from e
        except StorageBackendError as e:
             raise e
        except Exception as e:
             raise StorageBackendError(f"Unexpected error storing via GHC STOW: {e}") from e
        finally:
            if buffer:
                 buffer.close()

    async def search_studies(
        self,
        query_params: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0,
        fuzzymatching: bool = False,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:

        log = logger.bind(
            google_dicom_store=f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}",
            query_type="QIDO_Studies"
        )
        params = {
            "limit": str(limit),
            "offset": str(offset),
        }
        if fuzzymatching:
            params["fuzzymatching"] = "true"
        if fields:
            # Google uses 'includefield' repeatedly, not comma-separated
            # httpx handles duplicate query params correctly
            params["includefield"] = fields # Pass list directly
        else:
             # Default fields to include if none specified (adjust as needed)
             params["includefield"] = ["00080020", "00080030", "00080050", "00080061", "00080090", "00100010", "00100020", "0020000D", "00200010", "00201206", "00201208"]

        if query_params:
            # Add specific query filters
            params.update(query_params)

        log.debug("Querying GHC studies (QIDO-RS)", url=self.qido_studies_url, params=params)
        try:
            auth_headers = await self._get_auth_headers()
            headers = {
                **auth_headers,
                "Accept": "application/dicom+json",
            }
            response = await self.client.get(self.qido_studies_url, headers=headers, params=params)

            if response.status_code == 200:
                studies_json = response.json()
                log.info(f"GHC QIDO studies successful. Found {len(studies_json)} studies.", count=len(studies_json))
                # Convert DICOM JSON to our StudyMetadata schema (requires a mapping function)
                # Placeholder: Return raw JSON list for now
                # return [self._map_dicom_json_to_study_metadata(s) for s in studies_json]
                return studies_json # Return raw list of dicts for now
            elif response.status_code == 204: # No content
                log.info("GHC QIDO studies successful. No studies found.", count=0)
                return []
            else:
                error_details_str = response.text
                try: error_json = response.json(); error_details_str = str(error_json.get("error", {}).get("message", error_json))
                except json.JSONDecodeError: pass
                err_msg = f"QIDO-RS studies query to GHC failed. Status: {response.status_code}"
                log.error(err_msg, status_code=response.status_code, details=error_details_str[:500])
                raise StorageBackendError(f"{err_msg}, Details: {error_details_str[:500]}")

        except httpx.TimeoutException as e:
             raise StorageBackendError(f"Network timeout during GHC QIDO studies ({self.qido_studies_url})") from e
        except httpx.RequestError as e:
             raise StorageBackendError(f"Network error during GHC QIDO studies ({self.qido_studies_url}): {e}") from e
        except StorageBackendError as e:
             raise e
        except Exception as e:
             raise StorageBackendError(f"Unexpected error querying GHC studies: {e}") from e

    async def search_series(
        self,
        study_instance_uid: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
        limit: int = 1000, # Higher default limit for series within a study
        offset: int = 0,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:

        log = logger.bind(
            google_dicom_store=f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}",
            query_type="QIDO_Series",
            study_instance_uid=study_instance_uid
        )
        params = {
            "limit": str(limit),
            "offset": str(offset),
        }
        if fields:
            params["includefield"] = fields
        else:
            # Default series fields
             params["includefield"] = ["00080060", "0008103E", "0020000E", "00200011", "00201209"]

        if query_params:
            params.update(query_params)

        # Construct URL: either base series URL or study-specific series URL
        if study_instance_uid:
             search_url = f"{self.base_url}/studies/{study_instance_uid}/series"
        else:
             search_url = self.qido_series_url

        log.debug("Querying GHC series (QIDO-RS)", url=search_url, params=params)
        try:
            auth_headers = await self._get_auth_headers()
            headers = {
                **auth_headers,
                "Accept": "application/dicom+json",
            }
            response = await self.client.get(search_url, headers=headers, params=params)

            if response.status_code == 200:
                series_json = response.json()
                log.info(f"GHC QIDO series successful. Found {len(series_json)} series.", count=len(series_json))
                # Placeholder: Return raw JSON list
                # return [self._map_dicom_json_to_series_metadata(s) for s in series_json]
                return series_json
            elif response.status_code == 204:
                log.info("GHC QIDO series successful. No series found.", count=0)
                return []
            else:
                error_details_str = response.text
                try: error_json = response.json(); error_details_str = str(error_json.get("error", {}).get("message", error_json))
                except json.JSONDecodeError: pass
                err_msg = f"QIDO-RS series query to GHC failed. Status: {response.status_code}"
                log.error(err_msg, status_code=response.status_code, details=error_details_str[:500])
                raise StorageBackendError(f"{err_msg}, Details: {error_details_str[:500]}")
        except httpx.TimeoutException as e:
             raise StorageBackendError(f"Network timeout during GHC QIDO series ({search_url})") from e
        except httpx.RequestError as e:
             raise StorageBackendError(f"Network error during GHC QIDO series ({search_url}): {e}") from e
        except StorageBackendError as e:
             raise e
        except Exception as e:
             raise StorageBackendError(f"Unexpected error querying GHC series: {e}") from e

    async def search_instances(
        self,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
        limit: int = 1000,
        offset: int = 0,
        fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:


        log = logger.bind(
            google_dicom_store=f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}",
            query_type="QIDO_Instances",
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid
        )
        params = {
            "limit": str(limit),
            "offset": str(offset),
        }
        if fields:
            params["includefield"] = fields
        else:
            # Default instance fields
             params["includefield"] = ["00080016", "00080018", "00200013"]

        if query_params:
            params.update(query_params)

        # Construct URL based on provided UIDs
        if study_instance_uid and series_instance_uid:
             search_url = f"{self.base_url}/studies/{study_instance_uid}/series/{series_instance_uid}/instances"
        elif study_instance_uid:
             search_url = f"{self.base_url}/studies/{study_instance_uid}/instances"
        else:
             search_url = self.qido_instances_url # Search all instances

        log.debug("Querying GHC instances (QIDO-RS)", url=search_url, params=params)
        try:
            auth_headers = await self._get_auth_headers()
            headers = {
                **auth_headers,
                "Accept": "application/dicom+json",
            }
            response = await self.client.get(search_url, headers=headers, params=params)

            if response.status_code == 200:
                instances_json = response.json()
                log.info(f"GHC QIDO instances successful. Found {len(instances_json)} instances.", count=len(instances_json))
                # Placeholder: Return raw JSON list
                # return [self._map_dicom_json_to_instance_metadata(i) for i in instances_json]
                return instances_json
            elif response.status_code == 204:
                log.info("GHC QIDO instances successful. No instances found.", count=0)
                return []
            else:
                error_details_str = response.text
                try: error_json = response.json(); error_details_str = str(error_json.get("error", {}).get("message", error_json))
                except json.JSONDecodeError: pass
                err_msg = f"QIDO-RS instances query to GHC failed. Status: {response.status_code}"
                log.error(err_msg, status_code=response.status_code, details=error_details_str[:500])
                raise StorageBackendError(f"{err_msg}, Details: {error_details_str[:500]}")
        except httpx.TimeoutException as e:
             raise StorageBackendError(f"Network timeout during GHC QIDO instances ({search_url})") from e
        except httpx.RequestError as e:
             raise StorageBackendError(f"Network error during GHC QIDO instances ({search_url}): {e}") from e
        except StorageBackendError as e:
             raise e
        except Exception as e:
             raise StorageBackendError(f"Unexpected error querying GHC instances: {e}") from e

    # TODO: Implement mapping functions if needed to convert DICOM JSON to specific schemas
    # def _map_dicom_json_to_study_metadata(self, dicom_json: Dict[str, Any]) -> StudyMetadata: ...
    # def _map_dicom_json_to_series_metadata(self, dicom_json: Dict[str, Any]) -> SeriesMetadata: ...
    # def _map_dicom_json_to_instance_metadata(self, dicom_json: Dict[str, Any]) -> InstanceMetadata: ...


    def __repr__(self) -> str:
        store_path = f"{self.project_id}/{self.location}/{self.dataset_id}/{self.dicom_store_id}"
        return f"<{self.__class__.__name__}(dicom_store={store_path})>"
