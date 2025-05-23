# tests/worker/test_task_utils.py

from typing import Optional, Dict, Any # Added Dict, Any
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
import uuid # Added import for uuid

from pydicom.dataset import Dataset

# The functions we're testing
from app.worker.task_utils import create_exception_log_entry, _safe_get_dicom_value, build_storage_backend_config_dict # Added build_storage_backend_config_dict
# Schemas and Enums needed for creating test data and asserting types
from app.schemas.enums import ProcessedStudySourceType, ExceptionProcessingStage, ExceptionStatus
from app.schemas.dicom_exception_log import DicomExceptionLogCreate
# DB Models for type hinting if needed by mocks, and for the destination model
from app.db.models.storage_backend_config import StorageBackendConfig as DBStorageBackendConfigModel
from app.db.models.storage_backend_config import StowRsBackendConfig as DBStowRsModel # Added for new tests
from app.db.models.dicom_exception_log import DicomExceptionLog as DBDicomExceptionLogModel
# Schemas for new tests
from app.schemas.storage_backend_config import StowRsAuthType # Added for new tests
# Settings for default retry delay
from app.core.config import settings


# --- Test _safe_get_dicom_value (Helper, good to test separately) ---

def test_safe_get_dicom_value_exists():
    ds = Dataset()
    ds.PatientName = "Test^Patient"
    assert _safe_get_dicom_value(ds, "PatientName") == "Test^Patient"

def test_safe_get_dicom_value_not_exists():
    ds = Dataset()
    assert _safe_get_dicom_value(ds, "PatientName", default="Unknown") == "Unknown"

def test_safe_get_dicom_value_multivalue():
    ds = Dataset()
    ds.OtherPatientIDsSequence = [Dataset(), Dataset()] # Just to make it a sequence
    ds.OtherPatientIDsSequence[0].PatientID = "123"
    ds.OtherPatientIDsSequence[0].IssuerOfPatientID = "SiteA"
    ds.OtherPatientIDsSequence[1].PatientID = "456"
    ds.OtherPatientIDsSequence[1].IssuerOfPatientID = "SiteB"
    # Test a simple multi-value string tag
    ds.ImageType = ["DERIVED", "PRIMARY", "VOLUME"]
    assert _safe_get_dicom_value(ds, "ImageType") == "DERIVED\\PRIMARY\\VOLUME"

def test_safe_get_dicom_value_empty_multivalue():
    ds = Dataset()
    ds.ImageType = []
    assert _safe_get_dicom_value(ds, "ImageType", default="EMPTY") == "EMPTY"
    ds.ImageType = [""] # list with one empty string
    assert _safe_get_dicom_value(ds, "ImageType") == ""


# --- Tests for create_exception_log_entry ---

@pytest.fixture
def mock_db_session():
    """Mocks the SQLAlchemy Session."""
    return MagicMock()

@pytest.fixture
def mock_crud_dicom_exception_log_create():
    """Mocks the crud.dicom_exception_log.create method."""
    with patch("app.crud.dicom_exception_log.create") as mock_create:
        mock_db_obj = MagicMock(spec=DBDicomExceptionLogModel)
        mock_db_obj.id = 123
        mock_db_obj.exception_uuid = uuid.uuid4()
        mock_create.return_value = mock_db_obj
        yield mock_create

def test_create_exception_log_entry_minimal(mock_db_session, mock_crud_dicom_exception_log_create):
    test_exc_message = "Something broke"
    try:
        raise ValueError(test_exc_message)
    except ValueError as e:
        test_exc = e

    stage = ExceptionProcessingStage.INGESTION
    
    db_log_entry = create_exception_log_entry(
        db=mock_db_session,
        exc=test_exc,
        processing_stage=stage
    )

    assert db_log_entry is not None
    mock_crud_dicom_exception_log_create.assert_called_once()
    
    args, kwargs = mock_crud_dicom_exception_log_create.call_args
    created_obj_in = kwargs['obj_in']
    
    assert isinstance(created_obj_in, DicomExceptionLogCreate)
    assert created_obj_in.error_message == "Something broke"
    assert created_obj_in.processing_stage == stage
    assert created_obj_in.status == ExceptionStatus.NEW
    assert created_obj_in.retry_count == 0
    assert created_obj_in.next_retry_attempt_at is None
    assert "Traceback (most recent call last)" in (created_obj_in.error_details or "")

def test_create_exception_log_entry_with_dataset(mock_db_session, mock_crud_dicom_exception_log_create):
    test_exc = RuntimeError("Dataset error")
    stage = ExceptionProcessingStage.TAG_MORPHING
    ds = Dataset()
    ds.PatientName = "Doe^John"
    ds.PatientID = "12345"
    ds.AccessionNumber = "A6789"
    ds.Modality = "CT"
    ds.StudyInstanceUID = "1.2.3"
    ds.SeriesInstanceUID = "1.2.3.4"
    ds.SOPInstanceUID = "1.2.3.4.5"

    create_exception_log_entry(
        db=mock_db_session,
        exc=test_exc,
        processing_stage=stage,
        dataset=ds
    )
    
    args, kwargs = mock_crud_dicom_exception_log_create.call_args
    created_obj_in = kwargs['obj_in']
    
    assert created_obj_in.patient_name == "Doe^John"
    assert created_obj_in.patient_id == "12345"
    assert created_obj_in.accession_number == "A6789"
    assert created_obj_in.modality == "CT"
    assert created_obj_in.study_instance_uid == "1.2.3"
    assert created_obj_in.series_instance_uid == "1.2.3.4"
    assert created_obj_in.sop_instance_uid == "1.2.3.4.5"

def test_create_exception_log_entry_with_destination(mock_db_session, mock_crud_dicom_exception_log_create):
    # Using your defined StorageBackendError placeholder for this test
    class TestStorageBackendError(Exception): 
        pass
    test_exc = TestStorageBackendError("Destination unreachable")
    stage = ExceptionProcessingStage.DESTINATION_SEND
    
    mock_dest_model = MagicMock(spec=DBStorageBackendConfigModel)
    mock_dest_model.id = 42
    mock_dest_model.name = "Test PACS"

    create_exception_log_entry(
        db=mock_db_session,
        exc=test_exc,
        processing_stage=stage,
        target_destination_db_model=mock_dest_model
    )

    args, kwargs = mock_crud_dicom_exception_log_create.call_args
    created_obj_in = kwargs['obj_in']
    assert created_obj_in.target_destination_id == 42
    assert created_obj_in.target_destination_name == "Test PACS"

def test_create_exception_log_entry_retryable(mock_db_session, mock_crud_dicom_exception_log_create):
    test_exc = ConnectionError("Network hiccup")
    stage = ExceptionProcessingStage.DESTINATION_SEND
    
    start_time = datetime.now(timezone.utc)
    create_exception_log_entry(
        db=mock_db_session,
        exc=test_exc,
        processing_stage=stage,
        initial_status=ExceptionStatus.RETRY_PENDING,
        retryable=True,
        retry_delay_seconds=settings.EXCEPTION_RETRY_DELAY_SECONDS
    )
    end_time = datetime.now(timezone.utc)
    
    args, kwargs = mock_crud_dicom_exception_log_create.call_args
    created_obj_in = kwargs['obj_in']
        
    assert created_obj_in.status == ExceptionStatus.RETRY_PENDING
    assert created_obj_in.next_retry_attempt_at is not None
    
    expected_retry_time_min = start_time + timedelta(seconds=settings.EXCEPTION_RETRY_DELAY_SECONDS - 1)
    expected_retry_time_max = end_time + timedelta(seconds=settings.EXCEPTION_RETRY_DELAY_SECONDS + 1) # Allow for slight clock drift / execution time
    assert expected_retry_time_min <= created_obj_in.next_retry_attempt_at <= expected_retry_time_max


def test_create_exception_log_entry_explicit_uids(mock_db_session, mock_crud_dicom_exception_log_create):
    test_exc = ValueError("Fetch error, no dataset")
    stage = ExceptionProcessingStage.INGESTION
    
    create_exception_log_entry(
        db=mock_db_session,
        exc=test_exc,
        processing_stage=stage,
        dataset=None,
        study_instance_uid_str="study.123",
        series_instance_uid_str="series.456",
        sop_instance_uid_str="sop.789"
    )
    
    args, kwargs = mock_crud_dicom_exception_log_create.call_args
    created_obj_in = kwargs['obj_in']
    
    assert created_obj_in.study_instance_uid == "study.123"
    assert created_obj_in.series_instance_uid == "series.456"
    assert created_obj_in.sop_instance_uid == "sop.789"

def test_create_exception_log_entry_with_commit(mock_db_session, mock_crud_dicom_exception_log_create):
    test_exc = ValueError("Commit this one")
    stage = ExceptionProcessingStage.POST_PROCESSING
    
    db_log_entry = create_exception_log_entry(
        db=mock_db_session,
        exc=test_exc,
        processing_stage=stage,
        commit_on_success=True
    )
    
    assert db_log_entry is not None
    mock_crud_dicom_exception_log_create.assert_called_once()
    mock_db_session.commit.assert_called_once()
    mock_db_session.refresh.assert_called_once_with(db_log_entry)

def test_create_exception_log_entry_commit_fails(mock_db_session, mock_crud_dicom_exception_log_create):
    mock_db_session.commit.side_effect = Exception("DB commit boom!")
    
    test_exc = ValueError("This should log, but commit will fail")
    stage = ExceptionProcessingStage.DATABASE_INTERACTION
    
    db_log_entry = create_exception_log_entry(
        db=mock_db_session,
        exc=test_exc,
        processing_stage=stage,
        commit_on_success=True
    )
    
    assert db_log_entry is None
    mock_crud_dicom_exception_log_create.assert_called_once()
    mock_db_session.commit.assert_called_once()
    mock_db_session.rollback.assert_called_once()
    mock_db_session.refresh.assert_not_called()


# Your StorageBackendError placeholder, can be removed if the actual one is importable
# from app.services.storage_backends.base_backend import StorageBackendError
# For now, let's keep your placeholder if the above import isn't set up for tests easily
class StorageBackendError(Exception):
    pass


# --- Tests for build_storage_backend_config_dict ---

# Helper to create a mock StowRS DB model instance for the new tests
def create_mock_stowrs_db_model_for_build_config( # Renamed to avoid conflict
    id_val: int = 1,
    name: str = "Test STOW-RS",
    base_url: str = "https://stow.example.com/dicom-web",
    auth_type_val: Optional[str] = StowRsAuthType.NONE.value,
    basic_user_secret: Optional[str] = None,
    basic_pass_secret: Optional[str] = None,
    bearer_token_secret: Optional[str] = None,
    api_key_secret: Optional[str] = None,
    api_key_header: Optional[str] = None,
    tls_ca_secret: Optional[str] = None,
    is_enabled_val: bool = True,
    created_at_val: Optional[datetime] = None,
    updated_at_val: Optional[datetime] = None,
) -> DBStowRsModel:
    now = datetime.now(timezone.utc) # Use timezone aware datetime
    return DBStowRsModel(
        id=id_val,
        name=name,
        description="A test STOW-RS backend for build_config_dict",
        backend_type="stow_rs",
        is_enabled=is_enabled_val,
        base_url=base_url,
        auth_type=auth_type_val,
        basic_auth_username_secret_name=basic_user_secret,
        basic_auth_password_secret_name=basic_pass_secret,
        bearer_token_secret_name=bearer_token_secret,
        api_key_secret_name=api_key_secret,
        api_key_header_name_override=api_key_header,
        tls_ca_cert_secret_name=tls_ca_secret,
        created_at=created_at_val if created_at_val else now,
        updated_at=updated_at_val if updated_at_val else now
    )

@pytest.mark.parametrize(
    "test_id, db_model_params, expect_config_dict_to_be_none, expected_specific_config_keys, unexpected_specific_config_keys",
    [
        (
            "basic_auth_valid", # Renamed for clarity
            {
                "auth_type_val": StowRsAuthType.BASIC.value,
                "basic_user_secret": "user-secret-basic",
                "basic_pass_secret": "pass-secret-basic",
                "tls_ca_secret": "ca-secret-basic",
            },
            False, # Expect config_dict NOT to be None
            ["auth_type", "basic_auth_username_secret_name", "basic_auth_password_secret_name", "tls_ca_cert_secret_name"],
            ["bearer_token_secret_name", "api_key_secret_name", "api_key_header_name_override"],
        ),
        (
            "bearer_auth_valid", # Renamed
            {
                "auth_type_val": StowRsAuthType.BEARER.value,
                "bearer_token_secret": "token-secret-bearer",
            },
            False, # Expect config_dict NOT to be None
            ["auth_type", "bearer_token_secret_name"],
            ["basic_auth_username_secret_name", "basic_auth_password_secret_name", "api_key_secret_name"],
        ),
        (
            "apikey_auth_valid", # Renamed
            {
                "auth_type_val": StowRsAuthType.APIKEY.value,
                "api_key_secret": "key-secret-apikey",
                "api_key_header": "X-Custom-Header",
            },
            False, # Expect config_dict NOT to be None
            ["auth_type", "api_key_secret_name", "api_key_header_name_override"],
            ["basic_auth_username_secret_name", "bearer_token_secret_name"],
        ),
        (
            "auth_none_valid", # Renamed
            {
                "auth_type_val": StowRsAuthType.NONE.value,
                 "tls_ca_secret": "ca-for-none-auth",
            },
            False, # Expect config_dict NOT to be None
            ["auth_type", "tls_ca_cert_secret_name"],
            ["basic_auth_username_secret_name", "bearer_token_secret_name", "api_key_secret_name"],
        ),
         (
            "basic_auth_invalid_missing_secrets", # Renamed to reflect it's testing an invalid case due to Pydantic validation
            {
                "auth_type_val": StowRsAuthType.BASIC.value,
                "basic_user_secret": None, # This will trigger Pydantic StowRsConfig validation
                "basic_pass_secret": None, # This will trigger Pydantic StowRsConfig validation
            },
            True, # EXPECT config_dict TO BE None due to Pydantic validation error
            [], # No specific keys expected if None
            [], # No specific keys unexpected if None
        ),
    ],
)
def test_build_storage_backend_config_dict_for_stowrs(
    test_id: str,
    db_model_params: Dict[str, Any],
    expect_config_dict_to_be_none: bool, # New parameter
    expected_specific_config_keys: list[str],
    unexpected_specific_config_keys: list[str],
):
    mock_db_model = create_mock_stowrs_db_model_for_build_config(**db_model_params)
    
    config_dict = build_storage_backend_config_dict(mock_db_model, task_id=f"test-{test_id}")

    if expect_config_dict_to_be_none:
        assert config_dict is None, f"Expected config_dict to be None for test '{test_id}' due to validation, but it was not."
        return # Stop further checks if None was expected

    assert config_dict is not None, f"Expected config_dict to be populated for test '{test_id}', but it was None."
    
    assert config_dict.get("id") == mock_db_model.id
    assert config_dict.get("name") == mock_db_model.name
    assert config_dict.get("description") == mock_db_model.description
    assert config_dict.get("is_enabled") == mock_db_model.is_enabled
    assert config_dict.get("base_url") == mock_db_model.base_url
    assert config_dict.get("backend_type") == "stow_rs" 
    assert config_dict.get("type") == "stow_rs"         

    for key in expected_specific_config_keys:
        assert key in config_dict, f"Expected key '{key}' not found for test '{test_id}'"
        original_param_key_map = {
            "auth_type": "auth_type_val", "basic_auth_username_secret_name": "basic_user_secret",
            "basic_auth_password_secret_name": "basic_pass_secret", "bearer_token_secret_name": "bearer_token_secret",
            "api_key_secret_name": "api_key_secret", "api_key_header_name_override": "api_key_header",
            "tls_ca_cert_secret_name": "tls_ca_secret",
        }
        if key in original_param_key_map and original_param_key_map[key] in db_model_params:
             expected_value = db_model_params[original_param_key_map[key]]
             if isinstance(expected_value, StowRsAuthType):
                 assert config_dict.get(key) == expected_value.value
             else:
                 assert config_dict.get(key) == expected_value

    for key in unexpected_specific_config_keys:
        assert key not in config_dict, f"Unexpected key '{key}' found for test '{test_id}'"

    assert "created_at" in config_dict
    assert "updated_at" in config_dict
    assert isinstance(config_dict["created_at"], datetime)
    assert isinstance(config_dict["updated_at"], datetime)


def test_build_storage_backend_config_dict_invalid_db_model_type_handled_by_pydantic():
    """
    Tests that Pydantic's schema validation (via TypeAdapter) catches an incompatible DB model.
    build_storage_backend_config_dict should then return None.
    """
    class MockDifferentDBModel: # Not a StorageBackendConfig
        # Provide attributes accessed *before* Pydantic validation in build_storage_backend_config_dict
        id = 99 
        name = "Invalid Type Test"
        # This backend_type will cause Pydantic's discriminated union to fail
        backend_type = "super_secret_new_type_not_in_schema_union" 
        
        # Add other fields that StorageBackendConfigRead might expect if it were valid,
        # to ensure the failure is specifically due to the backend_type discriminator.
        # These might not be strictly necessary if backend_type itself causes immediate failure.
        is_enabled = True
        description = "A model that Pydantic's union should reject"
        # Pydantic Read schemas typically expect created_at and updated_at
        created_at = datetime.now(timezone.utc)
        updated_at = datetime.now(timezone.utc)

    mock_invalid_model = MockDifferentDBModel()
    
    config_dict = build_storage_backend_config_dict(mock_invalid_model) # type: ignore
    assert config_dict is None