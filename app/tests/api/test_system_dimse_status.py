# app/tests/api/test_system_dimse_status.py
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from unittest.mock import MagicMock, patch
from datetime import datetime

from app.main import app
from app import crud
from app.db.models.dimse_listener_config import DimseListenerConfig
from app.db.models.dimse_listener_state import DimseListenerState
from app.db.models.user import User
from app.api import deps


class TestDimseListenersStatusAPI:
    """Test suite for the DIMSE Listeners Status API endpoint."""

    @pytest.fixture
    def mock_user(self):
        """Mock authenticated user."""
        user = MagicMock(spec=User)
        user.id = 1
        user.email = "test@example.com"
        user.is_active = True
        return user

    @pytest.fixture
    def mock_db(self):
        """Mock database session."""
        return MagicMock(spec=Session)

    @pytest.fixture
    def client(self, mock_user, mock_db):
        """Create a test client with dependency overrides."""
        app.dependency_overrides[deps.get_current_active_user] = lambda: mock_user
        app.dependency_overrides[deps.get_db] = lambda: mock_db
        
        client = TestClient(app)
        yield client
        
        # Clean up overrides
        app.dependency_overrides.clear()

    @pytest.fixture
    def mock_listener_config_pynetdicom(self):
        """Mock pynetdicom listener configuration."""
        config = MagicMock(spec=DimseListenerConfig)
        config.id = 1
        config.name = "pynetdicom-listener-1"
        config.description = "PyNetDICOM Test Listener"
        config.listener_type = "pynetdicom"
        config.ae_title = "PYNET01"
        config.port = 11112
        config.is_enabled = True
        config.instance_id = "pynet_instance_1"
        config.tls_enabled = False
        return config

    @pytest.fixture
    def mock_listener_config_dcm4che(self):
        """Mock dcm4che listener configuration."""
        config = MagicMock(spec=DimseListenerConfig)
        config.id = 2
        config.name = "dcm4che-listener-1"
        config.description = "DCM4CHE Test Listener"
        config.listener_type = "dcm4che"
        config.ae_title = "DCM4CHE01"
        config.port = 11113
        config.is_enabled = True
        config.instance_id = "dcm4che_instance_1"
        config.tls_enabled = False
        return config

    @pytest.fixture
    def mock_listener_state_active(self):
        """Mock active listener state."""
        state = MagicMock(spec=DimseListenerState)
        state.id = 1
        state.listener_id = "pynet_instance_1"
        state.status = "active"
        state.status_message = "Listening for connections"
        state.host = "0.0.0.0"
        state.port = 11112
        state.ae_title = "PYNET01"
        state.last_heartbeat = datetime.now()
        state.received_instance_count = 42
        state.processed_instance_count = 40
        return state

    def test_get_dimse_listeners_status_success(
        self, 
        client, 
        mock_db,
        mock_listener_config_pynetdicom,
        mock_listener_config_dcm4che,
        mock_listener_state_active
    ):
        """Test successful retrieval of DIMSE listeners status."""
        
        # Mock the CRUD method to return both configs and status
        mock_data = [
            (mock_listener_config_pynetdicom, mock_listener_state_active),
            (mock_listener_config_dcm4che, None),  # dcm4che not currently reporting
        ]
        
        with patch.object(crud.crud_dimse_listener_state, 'get_all_listeners_with_status', return_value=mock_data):
            response = client.get("/api/v1/system/dimse-listeners/status")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "listeners" in data
        assert len(data["listeners"]) == 2
        
        # Check pynetdicom listener (with status)
        pynet_listener = data["listeners"][0]
        assert pynet_listener["name"] == "pynetdicom-listener-1"
        assert pynet_listener["listener_type"] == "pynetdicom"
        assert pynet_listener["ae_title"] == "PYNET01"
        assert pynet_listener["port"] == 11112
        assert pynet_listener["is_enabled"] == True
        assert pynet_listener["runtime_status"] == "active"
        assert pynet_listener["received_instance_count"] == 42
        assert pynet_listener["processed_instance_count"] == 40
        
        # Check dcm4che listener (without status)
        dcm4che_listener = data["listeners"][1]
        assert dcm4che_listener["name"] == "dcm4che-listener-1"
        assert dcm4che_listener["listener_type"] == "dcm4che"
        assert dcm4che_listener["ae_title"] == "DCM4CHE01"
        assert dcm4che_listener["port"] == 11113
        assert dcm4che_listener["is_enabled"] == True
        assert dcm4che_listener["runtime_status"] is None
        assert dcm4che_listener["received_instance_count"] == 0
        assert dcm4che_listener["processed_instance_count"] == 0

    def test_get_dimse_listeners_status_crud_error(self, client, mock_db):
        """Test handling of CRUD errors."""
        
        with patch.object(crud.crud_dimse_listener_state, 'get_all_listeners_with_status', side_effect=Exception("Database error")):
            response = client.get("/api/v1/system/dimse-listeners/status")
        
        assert response.status_code == 500
        data = response.json()
        assert "Database error retrieving listener status" in data["detail"]

    def test_get_dimse_listeners_status_empty_result(self, client, mock_db):
        """Test handling of empty results."""
        
        with patch.object(crud.crud_dimse_listener_state, 'get_all_listeners_with_status', return_value=[]):
            response = client.get("/api/v1/system/dimse-listeners/status")
        
        assert response.status_code == 200
        data = response.json()
        assert "listeners" in data
        assert len(data["listeners"]) == 0
