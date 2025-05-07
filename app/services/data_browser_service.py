# app/services/data_browser_service.py

import asyncio
import logging
import re
from typing import List, Dict, Any, Optional, Literal
from datetime import date, timedelta, datetime, timezone

from pydicom.dataset import Dataset as PydicomDataset
from pydicom.tag import Tag
from sqlalchemy.orm import Session

from app import crud
from app.db import models
from app.schemas.data_browser import (
    DataBrowserQueryParam,
    DataBrowserQueryResponse,
    StudyResultItem,
    QueryLevel
)
from app.core.config import settings
from app.services import dicomweb_client # Keep this import style
from app.worker.dimse_qr_poller import _resolve_dynamic_date_filter
from app.services.network.dimse.scu_service import (
    find_studies,
    DimseScuError,
    AssociationError,
    DimseCommandError,
    TlsConfigError
)
# --- CORRECTED IMPORT - Import functions directly ---
from app.services.google_healthcare_service import (
    search_for_studies, # Use correct name
    search_for_series, # Use correct name
    search_for_instances, # Use correct name
    GoogleHealthcareQueryError
)
# --- END CORRECTION ---
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,
    PatientRootQueryRetrieveInformationModelFind
)

try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)


class QueryServiceError(Exception):
    def __init__(self, message: str, source_type: Optional[str] = None, source_id: Optional[int] = None):
        self.source_type = source_type
        self.source_id = source_id
        super().__init__(f"{source_type or 'Unknown Source'} (ID: {source_id or 'N/A'}): {message}")

class SourceNotFoundError(QueryServiceError): pass
class InvalidParameterError(QueryServiceError): pass
class RemoteConnectionError(QueryServiceError): pass
class RemoteQueryError(QueryServiceError): pass


def _build_find_identifier(query_params: List[DataBrowserQueryParam], query_level: QueryLevel) -> PydicomDataset:
    identifier = PydicomDataset()
    identifier.QueryRetrieveLevel = query_level.value
    log = logger.bind(query_level=query_level.value) if hasattr(logger, 'bind') else logger
    if query_level == QueryLevel.STUDY:
        default_return_keys = [
            "PatientID", "PatientName", "StudyInstanceUID", "StudyDate", "StudyTime",
            "AccessionNumber", "ModalitiesInStudy", "ReferringPhysicianName",
            "PatientBirthDate", "StudyDescription", "NumberOfStudyRelatedSeries",
            "NumberOfStudyRelatedInstances", "InstitutionName"
        ]
    elif query_level == QueryLevel.SERIES:
         default_return_keys = ["SeriesInstanceUID", "SeriesNumber", "Modality", "SeriesDescription", "NumberOfSeriesRelatedInstances", "StudyInstanceUID", "InstitutionName"]
    elif query_level == QueryLevel.INSTANCE:
         default_return_keys = ["SOPInstanceUID", "InstanceNumber", "SOPClassUID", "StudyInstanceUID", "SeriesInstanceUID", "InstitutionName"]
    else: default_return_keys = []

    filter_keys_used = set()
    for param in query_params:
        value_to_set = str(param.value) if param.value is not None else ""
        if param.field.lower() == "studydate":
            resolved_value = _resolve_dynamic_date_filter(param.value)
            if resolved_value is None: log.warning("C-FIND: Skipping invalid dynamic date filter", field=param.field, value=param.value); continue
            value_to_set = resolved_value
        try:
            keyword = param.field
            try:
                 if re.match(r"^[0-9a-fA-F]{8}$", keyword.replace(",", "")): tag = Tag(f"({keyword[:4]},{keyword[4:]})"); keyword = tag.keyword or keyword
                 elif not Tag(keyword).is_valid: raise ValueError("Not a valid keyword")
            except Exception: log.warning("C-FIND: Field not standard DICOM keyword/tag, using as provided.", field=param.field)
            setattr(identifier, keyword, value_to_set)
            filter_keys_used.add(keyword)
            log.debug("C-FIND Filter Added", keyword=keyword, value=value_to_set)
        except Exception as e: log.warning("C-FIND: Error setting filter attribute", field=param.field, value=value_to_set, error=str(e))

    for key in default_return_keys:
        if key not in filter_keys_used:
            try: setattr(identifier, key, "")
            except Exception as e: log.warning("C-FIND: Could not set return key", key=key, error=str(e))

    log.debug("Constructed C-FIND Identifier", identifier_str=str(identifier))
    return identifier


def _build_qido_params(query_params: List[DataBrowserQueryParam]) -> Dict[str, str]:
    qido_dict: Dict[str, str] = {}
    for param in query_params:
        value_to_set = str(param.value) if param.value is not None else ""
        if param.field.lower() == "studydate":
            resolved_value = _resolve_dynamic_date_filter(param.value)
            if resolved_value is None: logger.warning("QIDO: Skipping invalid dynamic date filter", field=param.field, value=param.value); continue
            value_to_set = resolved_value
        qido_dict[param.field] = value_to_set
    logger.debug("Constructed QIDO parameters", qido_params=qido_dict)
    return qido_dict


async def _execute_cfind_query(
    source_config: models.DimseQueryRetrieveSource,
    query_identifier: PydicomDataset,
    query_level: QueryLevel
) -> List[Dict[str, Any]]:
    log = logger.bind(
        remote_ae=source_config.remote_ae_title,
        source_name=source_config.name,
        source_id=source_config.id,
        query_level=query_level.value,
        tls_enabled=source_config.tls_enabled
    ) if hasattr(logger, 'bind') else logger
    log.info("Executing C-FIND via scu_service")

    results: List[Dict[str, Any]] = []

    scu_config_dict = {
        "remote_host": source_config.remote_host,
        "remote_port": source_config.remote_port,
        "remote_ae_title": source_config.remote_ae_title,
        "local_ae_title": source_config.local_ae_title,
        "tls_enabled": source_config.tls_enabled,
        "tls_ca_cert_secret_name": source_config.tls_ca_cert_secret_name,
        "tls_client_cert_secret_name": source_config.tls_client_cert_secret_name,
        "tls_client_key_secret_name": source_config.tls_client_key_secret_name,
    }

    if query_level == QueryLevel.STUDY:
        find_sop_class = StudyRootQueryRetrieveInformationModelFind
    else:
        log.warning("C-FIND query level is Series/Instance, using StudyRootQueryRetrieveInformationModelFind SOP Class.", requested_level=query_level.value)
        find_sop_class = StudyRootQueryRetrieveInformationModelFind

    try:
        log.info("Calling scu_service.find_studies", find_sop_class=str(find_sop_class))
        found_datasets: List[PydicomDataset] = find_studies(
            config=scu_config_dict,
            identifier=query_identifier,
            find_sop_class=find_sop_class
        )
        log.info("scu_service.find_studies completed", result_count=len(found_datasets))

        for ds in found_datasets:
            try:
                result_dict = ds.to_json_dict()
                result_dict["source_id"] = source_config.id
                result_dict["source_name"] = source_config.name
                result_dict["source_type"] = "dimse-qr"
                results.append(result_dict)
            except Exception as json_err:
                log.warning("Failed to convert C-FIND result dataset to JSON", error=str(json_err), dataset_info=str(ds)[:200])

        return results

    except (TlsConfigError, AssociationError, DimseCommandError) as scu_err:
        log.error("DIMSE SCU service failed during C-FIND", error=str(scu_err), exc_info=True)
        if isinstance(scu_err, TlsConfigError): raise QueryServiceError(f"TLS Configuration Error: {scu_err}", source_type="dimse-qr", source_id=source_config.id) from scu_err
        elif isinstance(scu_err, AssociationError): raise RemoteConnectionError(f"Association Error: {scu_err}", source_type="dimse-qr", source_id=source_config.id) from scu_err
        elif isinstance(scu_err, DimseCommandError): raise RemoteQueryError(f"Remote Query Error: {scu_err}", source_type="dimse-qr", source_id=source_config.id) from scu_err
        else: raise QueryServiceError(f"DIMSE SCU Error: {scu_err}", source_type="dimse-qr", source_id=source_config.id) from scu_err
    except ValueError as val_err:
        log.error("Value error during C-FIND via service", error=str(val_err), exc_info=True)
        raise InvalidParameterError(f"Configuration or Parameter Error: {val_err}", source_type="dimse-qr", source_id=source_config.id) from val_err
    except Exception as e:
        log.error("Unexpected error executing C-FIND via service", error=str(e), exc_info=True)
        raise QueryServiceError(f"Unexpected C-FIND Error: {e}", source_type="dimse-qr", source_id=source_config.id) from e


async def _execute_qido_query(
    source_config: models.DicomWebSourceState,
    query_params: Dict[str, str],
    query_level: QueryLevel,
    prioritize_custom_params: bool = False
) -> List[Dict[str, Any]]:
    log = logger.bind(source_name=source_config.source_name, source_id=source_config.id, base_url=source_config.base_url, query_level=query_level.value) if hasattr(logger, 'bind') else logger
    log.info("Attempting DICOMweb QIDO query")

    # Note: Removed include_fields map and parameter from the call below

    try:
        qido_results = await asyncio.to_thread(
             dicomweb_client.query_qido,
             config=source_config,
             level=query_level.value,
             custom_params=query_params,
             # include_fields=include_fields, # REMOVED THIS ARGUMENT
             prioritize_custom_params=prioritize_custom_params
        )
        log.info("QIDO query successful.", result_count=len(qido_results))
        for result in qido_results:
             result["source_id"] = source_config.id
             result["source_name"] = source_config.source_name
             result["source_type"] = "dicomweb"
        return qido_results
    except dicomweb_client.DicomWebClientError as e:
         log.error("Error during QIDO query", status_code=e.status_code, error=str(e), exc_info=False)
         if e.status_code and 400 <= e.status_code < 500: raise InvalidParameterError(f"QIDO Error ({e.status_code}): {e}", source_type="dicomweb", source_id=source_config.id) from e
         elif e.status_code and e.status_code >= 500: raise RemoteQueryError(f"QIDO Remote Error ({e.status_code}): {e}", source_type="dicomweb", source_id=source_config.id) from e
         else: raise RemoteConnectionError(f"QIDO Connection Error: {e}", source_type="dicomweb", source_id=source_config.id) from e
    except TypeError as te: # Catch the specific TypeError
         log.error("Type error calling query_qido (likely unexpected argument)", error=str(te), exc_info=True)
         raise InvalidParameterError(f"Internal configuration error calling QIDO function: {te}", source_type="dicomweb", source_id=source_config.id) from te
    except Exception as e:
         log.error("Unexpected error during QIDO query", error=str(e), exc_info=True)
         raise QueryServiceError(f"QIDO Error: {e}", source_type="dicomweb", source_id=source_config.id) from e


async def _execute_google_healthcare_query(
    source_config: models.GoogleHealthcareSource,
    query_params: Dict[str, str],
    query_level: QueryLevel
) -> List[Dict[str, Any]]:
    log = logger.bind(source_name=source_config.name, source_id=source_config.id, query_level=query_level.value) if hasattr(logger, 'bind') else logger
    log.info("Attempting Google Healthcare QIDO query")

    default_fields_map = {
         QueryLevel.STUDY: ["00100010", "00100020", "0020000D", "00080020", "00080030", "00080050", "00080061", "00080090", "00100030", "00081030", "00201206", "00201208", "00080080"],
         QueryLevel.SERIES: ["0020000E", "00080060", "00200011", "0008103E", "00201209"],
         QueryLevel.INSTANCE: ["00080018", "00080016", "00200013"]
    }
    include_fields = default_fields_map.get(query_level, [])

    try:
        # --- Use correct function names ---
        if query_level == QueryLevel.STUDY:
            raw_results = await search_for_studies( # Corrected name
                gcp_project_id=source_config.gcp_project_id,
                gcp_location=source_config.gcp_location,
                gcp_dataset_id=source_config.gcp_dataset_id,
                gcp_dicom_store_id=source_config.gcp_dicom_store_id,
                query_params=query_params,
                fields=include_fields
            )
        elif query_level == QueryLevel.SERIES:
            study_uid_filter = query_params.get("StudyInstanceUID") or query_params.get("0020000D")
            raw_results = await search_for_series( # Corrected name
                gcp_project_id=source_config.gcp_project_id,
                gcp_location=source_config.gcp_location,
                gcp_dataset_id=source_config.gcp_dataset_id,
                gcp_dicom_store_id=source_config.gcp_dicom_store_id,
                study_instance_uid=study_uid_filter,
                query_params=query_params,
                fields=include_fields
            )
        elif query_level == QueryLevel.INSTANCE:
            study_uid_filter = query_params.get("StudyInstanceUID") or query_params.get("0020000D")
            series_uid_filter = query_params.get("SeriesInstanceUID") or query_params.get("0020000E")
            if not study_uid_filter or not series_uid_filter:
                 raise InvalidParameterError("StudyInstanceUID and SeriesInstanceUID filters required for INSTANCE level query on Google Healthcare source.", source_type="google_healthcare", source_id=source_config.id)
            raw_results = await search_for_instances( # Corrected name
                gcp_project_id=source_config.gcp_project_id,
                gcp_location=source_config.gcp_location,
                gcp_dataset_id=source_config.gcp_dataset_id,
                gcp_dicom_store_id=source_config.gcp_dicom_store_id,
                study_instance_uid=study_uid_filter,
                series_instance_uid=series_uid_filter,
                query_params=query_params,
                fields=include_fields
            )
        # --- End correction ---
        else:
             raise InvalidParameterError(f"Unsupported query level '{query_level.value}' for Google Healthcare.", source_type="google_healthcare", source_id=source_config.id)

        log.info("Google Healthcare QIDO query successful", result_count=len(raw_results))
        for result in raw_results:
             result["source_id"] = source_config.id
             result["source_name"] = source_config.name
             result["source_type"] = "google_healthcare"
        return raw_results

    except GoogleHealthcareQueryError as e:
        log.error("Error during Google Healthcare QIDO query", error=str(e), exc_info=False)
        raise RemoteQueryError(f"Google Healthcare Query Error: {e}", source_type="google_healthcare", source_id=source_config.id) from e
    except InvalidParameterError as e: raise e
    except Exception as e:
        log.error("Unexpected error during Google Healthcare QIDO query", error=str(e), exc_info=True)
        raise QueryServiceError(f"Google Healthcare Error: {e}", source_type="google_healthcare", source_id=source_config.id) from e


def get_source_info_for_response(db: Session, source_id: int, source_type: Literal["dicomweb", "dimse-qr", "google_healthcare"]) -> Dict[str, Any]:
    info = {"name": "Unknown", "type": source_type, "id": source_id}
    try:
        if source_type == "dimse-qr": config = crud.crud_dimse_qr_source.get(db, id=source_id); info["name"] = config.name if config else "Unknown"
        elif source_type == "dicomweb": config = crud.dicomweb_source.get(db, id=source_id); info["name"] = config.source_name if config else "Unknown"
        elif source_type == "google_healthcare": config = crud.google_healthcare_source.get(db, id=source_id); info["name"] = config.name if config else "Unknown"
        else: logger.warning("get_source_info called with invalid source_type", provided_type=source_type, source_id=source_id); info["type"] = "Unknown"
    except Exception as e: logger.error("Failed to retrieve source info for error response", source_id=source_id, source_type=source_type, error=str(e))
    return info


async def execute_query(
    db: Session,
    source_id: int,
    source_type: Literal["dicomweb", "dimse-qr", "google_healthcare"],
    query_params: List[DataBrowserQueryParam],
    query_level: QueryLevel
) -> DataBrowserQueryResponse:
    source_config: Any = None
    source_name = "Unknown"
    log = logger.bind(source_id=source_id, source_type=source_type, query_level=query_level.value) if hasattr(logger, 'bind') else logger
    log.info("Executing data browser query")

    try:
        if source_type == "dimse-qr":
            source_config = crud.crud_dimse_qr_source.get(db, id=source_id)
            if source_config: source_name = source_config.name
        elif source_type == "dicomweb":
            source_config = crud.dicomweb_source.get(db, id=source_id)
            if source_config: source_name = source_config.source_name
        elif source_type == "google_healthcare":
            source_config = crud.google_healthcare_source.get(db, id=source_id)
            if source_config: source_name = source_config.name
        else:
            raise InvalidParameterError(f"Unsupported source_type: {source_type}", source_type=source_type, source_id=source_id)

        if source_config is None:
            raise SourceNotFoundError(f"Source configuration not found.", source_type=source_type, source_id=source_id)

        log = logger.bind(source_name=source_name) if hasattr(logger, 'bind') else logger
        log.debug("Source configuration found")

        if not source_config.is_enabled:
             log.warning("Query attempt on disabled source")
             return DataBrowserQueryResponse(query_status="success", message="Source is disabled, no query performed.", source_id=source_id, source_name=source_name, source_type=source_type, results=[])

    except (SourceNotFoundError, InvalidParameterError) as e: raise e
    except Exception as db_exc:
        log.error("Database error fetching source configuration", error=str(db_exc), exc_info=True)
        raise QueryServiceError("Failed to fetch source configuration from database", source_type=source_type, source_id=source_id) from db_exc

    results_list: List[Dict[str, Any]] = []
    message = "Query executed successfully."
    query_status: Literal["success", "error", "partial"] = "success"

    try:
        if source_type == "dimse-qr":
             log.debug("Preparing C-FIND identifier")
             find_identifier = _build_find_identifier(query_params, query_level)
             if not isinstance(source_config, models.DimseQueryRetrieveSource): raise TypeError("Config type mismatch for DIMSE QR")
             results_list = await _execute_cfind_query(source_config, find_identifier, query_level)
        elif source_type == "dicomweb":
            log.debug("Preparing QIDO parameters")
            qido_params = _build_qido_params(query_params)
            if not isinstance(source_config, models.DicomWebSourceState):
                log.error("Configuration type mismatch for DICOMweb source", actual_type=type(source_config))
                raise TypeError("Config type mismatch for DICOMweb")
            results_list = await _execute_qido_query(
                source_config, qido_params, query_level, prioritize_custom_params=True
            )
        elif source_type == "google_healthcare":
            log.debug("Preparing QIDO parameters for Google Healthcare")
            qido_params = _build_qido_params(query_params)
            if not isinstance(source_config, models.GoogleHealthcareSource): raise TypeError("Config type mismatch for Google Healthcare")
            results_list = await _execute_google_healthcare_query(
                 source_config, qido_params, query_level
            )

    except (QueryServiceError, RemoteConnectionError, RemoteQueryError, InvalidParameterError, SourceNotFoundError) as e:
        query_status = "error"
        message = str(e)
        results_list = []
    except Exception as e:
        query_status = "error"
        message = f"Unexpected error during query execution: {e}"
        results_list = []
        log.exception("Unexpected error during query execution")

    formatted_results: List[StudyResultItem] = []
    log.debug("Formatting results for response", raw_result_count=len(results_list))
    for result_dict in results_list:
        try:
            # Add source info before validation if not already present
            if "source_id" not in result_dict: result_dict["source_id"] = source_id
            if "source_name" not in result_dict: result_dict["source_name"] = source_name
            if "source_type" not in result_dict: result_dict["source_type"] = source_type
            formatted_item = StudyResultItem.model_validate(result_dict)
            formatted_results.append(formatted_item)
        except Exception as validation_err:
            log.warning("Failed to validate/format result item", validation_error=str(validation_err), raw_item=str(result_dict)[:500], exc_info=True)

    log.info("Query execution complete", final_status=query_status, formatted_result_count=len(formatted_results))
    return DataBrowserQueryResponse(
        query_status=query_status,
        message=message if query_status != "success" else None,
        source_id=source_id,
        source_name=source_name,
        source_type=source_type,
        results=formatted_results
    )
