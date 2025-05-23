# backend/app/tests/services/storage_backends/test_stow_rs.py

from pathlib import Path
import requests # Import the whole requests library
from requests.auth import HTTPBasicAuth # Import HTTPBasicAuth directly
from requests.exceptions import Timeout, ConnectionError, JSONDecodeError as RequestsJSONDecodeError # Explicitly import requests' JSONDecodeError
import pytest
from unittest.mock import patch, MagicMock, mock_open, call
import tempfile
import os

from app.services.storage_backends.stow_rs import StowRsStorage
from app.services.storage_backends.base_backend import StorageBackendError
from app.core.gcp_utils import SecretNotFoundError, SecretManagerError # Make sure this path is correct
from pydicom import Dataset
from pydicom.dataset import FileMetaDataset
from pydicom.uid import UID, ImplicitVRLittleEndian, ExplicitVRLittleEndian, CTImageStorage, generate_uid # Added generate_uid for more robust default UIDs

# --- Test Data ---
MOCK_BASE_URL = "https://dicom.example.com/dicomweb"

# --- Pytest Fixtures ---
@pytest.fixture
def mock_dataset() -> Dataset:
    """Creates a minimal mock pydicom Dataset with required file meta."""
    ds = Dataset()
    
    # Main dataset elements
    ds.PatientName = "Test^Patient"
    ds.PatientID = "12345"
    ds.SOPInstanceUID = generate_uid() # Use pydicom's generator for unique UIDs
    ds.StudyInstanceUID = generate_uid()
    ds.SeriesInstanceUID = generate_uid()
    ds.Modality = "CT"
    ds.SOPClassUID = CTImageStorage # Example SOP Class UID

    # File Meta Information - CRITICAL for dcmwrite to Part 10 format
    ds.file_meta = FileMetaDataset()
    ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
    ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID # Must match the dataset's SOPInstanceUID
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian # Common choice
    # pydicom adds these if they are missing and write_like_original=False, but good to be explicit for tests
    ds.file_meta.ImplementationClassUID = generate_uid() # Your application's UID or a generated one
    ds.file_meta.ImplementationVersionName = "AXIOM_TEST_1.0"
    
    # These flags are important for dcmwrite
    ds.is_little_endian = True
    ds.is_implicit_VR = False # STOW-RS usually expects Explicit VR
    return ds

# --- Tests ---

class TestStowRsStorageInitialization:
    @patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
    def test_init_no_auth_successful(self, mock_get_secret: MagicMock):
        config = {
            "type": "stow_rs",
            "name": "TestNoAuth",
            "base_url": MOCK_BASE_URL,
            "auth_type": "none"
        }
        backend = StowRsStorage(config)
        assert backend.base_url == MOCK_BASE_URL
        assert backend.stow_url == f"{MOCK_BASE_URL}/studies"
        assert backend.auth_type == "none"
        assert backend.auth_config == {}
        mock_get_secret.assert_not_called()

    @patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
    def test_init_basic_auth_successful(self, mock_get_secret: MagicMock):
        mock_get_secret.side_effect = ["testuser", "testpass"]
        config = {
            "type": "stow_rs",
            "name": "TestBasicAuth",
            "base_url": MOCK_BASE_URL,
            "auth_type": "basic",
            "basic_auth_username_secret_name": "user-secret",
            "basic_auth_password_secret_name": "pass-secret"
        }
        backend = StowRsStorage(config)
        assert backend.auth_type == "basic"
        assert backend.auth_config == {"username": "testuser", "password": "testpass"}
        mock_get_secret.assert_any_call("user-secret")
        mock_get_secret.assert_any_call("pass-secret")
        assert mock_get_secret.call_count == 2

    @patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
    def test_init_bearer_auth_successful(self, mock_get_secret: MagicMock):
        mock_get_secret.return_value = "testtoken"
        config = {
            "type": "stow_rs",
            "name": "TestBearerAuth",
            "base_url": MOCK_BASE_URL,
            "auth_type": "bearer",
            "bearer_token_secret_name": "token-secret"
        }
        backend = StowRsStorage(config)
        assert backend.auth_type == "bearer"
        assert backend.auth_config == {"token": "testtoken"}
        mock_get_secret.assert_called_once_with("token-secret")

    @patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
    def test_init_apikey_auth_successful(self, mock_get_secret: MagicMock):
        mock_get_secret.return_value = "testapikey"
        config = {
            "type": "stow_rs",
            "name": "TestApiKeyAuth",
            "base_url": MOCK_BASE_URL,
            "auth_type": "apikey",
            "api_key_secret_name": "key-secret",
            "api_key_header_name_override": "X-Custom-Api-Key"
        }
        backend = StowRsStorage(config)
        assert backend.auth_type == "apikey"
        assert backend.auth_config == {"key": "testapikey", "header_name": "X-Custom-Api-Key"}
        mock_get_secret.assert_called_once_with("key-secret")

    @patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
    def test_init_basic_auth_missing_secret_name_raises_value_error(self, mock_get_secret: MagicMock):
        config = {
            "type": "stow_rs",
            "name": "TestBasicAuthFail",
            "base_url": MOCK_BASE_URL,
            "auth_type": "basic",
            "basic_auth_password_secret_name": "pass-secret" # Username secret missing
        }
        with pytest.raises(ValueError, match="Basic auth requires 'basic_auth_username_secret_name' and 'basic_auth_password_secret_name'"):
            StowRsStorage(config)
        mock_get_secret.assert_not_called()

    @patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
    def test_init_secret_not_found_raises_storage_backend_error(self, mock_get_secret: MagicMock):
        mock_get_secret.side_effect = SecretNotFoundError("Secret 'user-secret' not found.")
        config = {
            "type": "stow_rs",
            "name": "TestSecretNotFound",
            "base_url": MOCK_BASE_URL,
            "auth_type": "basic",
            "basic_auth_username_secret_name": "user-secret",
            "basic_auth_password_secret_name": "pass-secret"
        }
        with pytest.raises(StorageBackendError, match="Failed to initialize STOW-RS backend: Secret not found"):
            StowRsStorage(config)
        mock_get_secret.assert_called_once_with("user-secret")

    @patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
    @patch('app.services.storage_backends.stow_rs.tempfile.NamedTemporaryFile')
    def test_init_with_tls_ca_cert_successful(self, mock_named_temp_file: MagicMock, mock_get_secret: MagicMock):
        mock_get_secret.return_value = "---BEGIN CERTIFICATE---...---END CERTIFICATE---"
        
        mock_file_obj = MagicMock() # Removed spec for simplicity, MagicMock will create methods on access
        temp_file_name = "/tmp/fake_ca_cert.pem"
        mock_file_obj.name = temp_file_name
        mock_named_temp_file.return_value = mock_file_obj
        
        config = {
            "type": "stow_rs",
            "name": "TestTlsCaCert",
            "base_url": MOCK_BASE_URL,
            "auth_type": "none",
            "tls_ca_cert_secret_name": "ca-cert-secret"
        }
        backend = StowRsStorage(config)
        
        mock_get_secret.assert_called_once_with("ca-cert-secret")
        mock_named_temp_file.assert_called_once_with(mode='w+', delete=False, suffix=".pem")
        mock_file_obj.write.assert_called_once_with("---BEGIN CERTIFICATE---...---END CERTIFICATE---")
        mock_file_obj.flush.assert_called_once()
        assert backend.tls_ca_cert_path == temp_file_name
        assert backend._temp_ca_file == mock_file_obj

        # Test cleanup logic
        mock_path_instance = MagicMock(spec=Path)
        mock_path_instance.exists.return_value = True

        with patch('app.services.storage_backends.stow_rs.Path', return_value=mock_path_instance) as mock_path_constructor, \
             patch('app.services.storage_backends.stow_rs.os.unlink') as mock_os_unlink:
            
            backend._cleanup_temp_ca_file()

            mock_file_obj.close.assert_called_once()
            mock_path_constructor.assert_called_once_with(temp_file_name)
            mock_path_instance.exists.assert_called_once()
            mock_os_unlink.assert_called_once_with(temp_file_name)

    @patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
    def test_init_tls_ca_cert_secret_not_found(self, mock_get_secret: MagicMock):
        mock_get_secret.side_effect = SecretNotFoundError("CA cert secret not found")
        config = {
            "type": "stow_rs",
            "name": "TestTlsCaCertFail",
            "base_url": MOCK_BASE_URL,
            "auth_type": "none",
            "tls_ca_cert_secret_name": "ca-cert-secret-missing"
        }
        with pytest.raises(StorageBackendError, match="Custom CA cert secret 'ca-cert-secret-missing' not found"):
            StowRsStorage(config)
        mock_get_secret.assert_called_once_with("ca-cert-secret-missing")


class TestStowRsStorageStore:
    @pytest.fixture(autouse=True)
    def patch_dependencies(self, mocker): # Use pytest-mock's 'mocker' fixture for convenience
        self.mock_post = mocker.patch('app.services.storage_backends.stow_rs.requests.post')
        self.mock_get_secret = mocker.patch('app.services.storage_backends.stow_rs.gcp_utils.get_secret')
        self.mock_temp_file = mocker.patch('app.services.storage_backends.stow_rs.tempfile.NamedTemporaryFile')
        
        self.mock_get_secret.return_value = "default_secret_value"
        
        self.mock_file_obj_for_store = MagicMock() # Use a distinct name if needed
        self.mock_file_obj_for_store.name = "/tmp/fake_ca_cert_test_store.pem"
        self.mock_temp_file.return_value = self.mock_file_obj_for_store
        # No yield needed if not using 'with' context for the fixture itself

    def test_store_successful_no_auth(self, mock_dataset: Dataset):
        config = {"type": "stow_rs", "name": "TestStoreNoAuth", "base_url": MOCK_BASE_URL, "auth_type": "none"}
        backend = StowRsStorage(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success"}
        self.mock_post.return_value = mock_response

        result = backend.store(mock_dataset) # This will use the mock_dataset fixture

        assert result == {"status": "success"}
        self.mock_post.assert_called_once()
        args, kwargs = self.mock_post.call_args
        assert args[0] == f"{MOCK_BASE_URL}/studies"
        assert kwargs['auth'] is None
        assert "multipart/related" in kwargs['headers']['Content-Type']
        assert kwargs['verify'] is True

    def test_store_successful_basic_auth(self, mock_dataset: Dataset):
        self.mock_get_secret.side_effect = ["theuser", "thepass"]
        config = {
            "type": "stow_rs", "name": "TestStoreBasic", "base_url": MOCK_BASE_URL,
            "auth_type": "basic",
            "basic_auth_username_secret_name": "user", "basic_auth_password_secret_name": "pass"
        }
        backend = StowRsStorage(config)
        
        mock_response = MagicMock(); mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        self.mock_post.return_value = mock_response

        backend.store(mock_dataset)
        
        self.mock_post.assert_called_once()
        args, kwargs = self.mock_post.call_args
        assert isinstance(kwargs['auth'], HTTPBasicAuth)
        assert kwargs['auth'].username == "theuser"
        assert kwargs['auth'].password == "thepass"

    def test_store_successful_bearer_auth(self, mock_dataset: Dataset):
        self.mock_get_secret.return_value = "thetoken"
        config = {
            "type": "stow_rs", "name": "TestStoreBearer", "base_url": MOCK_BASE_URL,
            "auth_type": "bearer", "bearer_token_secret_name": "token"
        }
        backend = StowRsStorage(config)

        mock_response = MagicMock(); mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        self.mock_post.return_value = mock_response

        backend.store(mock_dataset)

        self.mock_post.assert_called_once()
        args, kwargs = self.mock_post.call_args
        assert kwargs['headers']['Authorization'] == "Bearer thetoken"

    def test_store_successful_apikey_auth(self, mock_dataset: Dataset):
        self.mock_get_secret.return_value = "theapikey"
        header_name = "X-My-Api-Key"
        config = {
            "type": "stow_rs", "name": "TestStoreApiKey", "base_url": MOCK_BASE_URL,
            "auth_type": "apikey", "api_key_secret_name": "key",
            "api_key_header_name_override": header_name
        }
        backend = StowRsStorage(config)

        mock_response = MagicMock(); mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        self.mock_post.return_value = mock_response

        backend.store(mock_dataset)

        self.mock_post.assert_called_once()
        args, kwargs = self.mock_post.call_args
        assert kwargs['headers'][header_name] == "theapikey"

    def test_store_successful_with_custom_ca(self, mock_dataset: Dataset):
        self.mock_get_secret.side_effect = ["---CA CERT DATA---"] # Only one call for CA cert
        config = {
            "type": "stow_rs", "name": "TestStoreCustomCA", "base_url": MOCK_BASE_URL,
            "auth_type": "none", "tls_ca_cert_secret_name": "ca-secret"
        }
        backend = StowRsStorage(config)

        mock_response = MagicMock(); mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        self.mock_post.return_value = mock_response

        backend.store(mock_dataset)

        self.mock_post.assert_called_once()
        args, kwargs = self.mock_post.call_args
        # self.mock_file_obj_for_store is set by patch_dependencies
        assert kwargs['verify'] == self.mock_file_obj_for_store.name 
        self.mock_get_secret.assert_called_once_with("ca-secret")
        self.mock_temp_file.assert_called_once() # Called during backend init

    def test_store_http_401_unauthorized_raises_storage_backend_error(self, mock_dataset: Dataset):
        config = {"type": "stow_rs", "name": "TestStore401", "base_url": MOCK_BASE_URL, "auth_type": "none"}
        backend = StowRsStorage(config)

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized access"
        mock_response.json.side_effect = RequestsJSONDecodeError("err", "doc", 0) # Use requests' version
        self.mock_post.return_value = mock_response

        # This test should now pass IF dcmwrite is fixed, as it will hit the mock_post
        with pytest.raises(StorageBackendError, match="STOW-RS to .* failed. Status: 401. Details: Unauthorized access") as excinfo:
            backend.store(mock_dataset)
        assert excinfo.value.status_code == 401

    def test_store_http_500_server_error_raises_storage_backend_error(self, mock_dataset: Dataset):
        config = {"type": "stow_rs", "name": "TestStore500", "base_url": MOCK_BASE_URL, "auth_type": "none"}
        backend = StowRsStorage(config)

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Server blew up"
        mock_response.json.side_effect = RequestsJSONDecodeError("err", "doc", 0)
        self.mock_post.return_value = mock_response

        with pytest.raises(StorageBackendError, match="STOW-RS to .* failed. Status: 500. Details: Server blew up") as excinfo:
            backend.store(mock_dataset)
        assert excinfo.value.status_code == 500
        
    def test_store_requests_timeout_raises_storage_backend_error(self, mock_dataset: Dataset):
        config = {"type": "stow_rs", "name": "TestStoreTimeout", "base_url": MOCK_BASE_URL, "auth_type": "none"}
        backend = StowRsStorage(config)
        
        self.mock_post.side_effect = Timeout("Connection timed out")
        
        with pytest.raises(StorageBackendError, match="Timeout during STOW-RS") as excinfo: # Check match
            backend.store(mock_dataset)
        assert excinfo.value.status_code is None # Timeout doesn't inherently have HTTP status

    def test_store_requests_connection_error_raises_storage_backend_error(self, mock_dataset: Dataset):
        config = {"type": "stow_rs", "name": "TestStoreConnectionError", "base_url": MOCK_BASE_URL, "auth_type": "none"}
        backend = StowRsStorage(config)
        
        mock_response_on_error = MagicMock()
        mock_response_on_error.status_code = None
        
        self.mock_post.side_effect = ConnectionError(
            "Failed to connect", response=mock_response_on_error
        )
        
        with pytest.raises(StorageBackendError, match="Network error during STOW-RS") as excinfo: # Check match
            backend.store(mock_dataset)
        assert excinfo.value.status_code is None # ConnectionError might not have HTTP status