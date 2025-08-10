# app/tests/api/test_orders.py
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session
from unittest.mock import MagicMock, patch

from app.main import app
from app import crud, schemas
from app.db.models.imaging_order import ImagingOrder
from app.db.models.user import User
from app.schemas.enums import OrderStatus
from app.api import deps


class TestOrdersAPI:
    """Test suite for the Orders API endpoints."""

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
    def mock_rabbitmq_connection(self):
        """Mock RabbitMQ connection."""
        return MagicMock()

    @pytest.fixture
    def client(self, mock_user, mock_db, mock_rabbitmq_connection):
        """Create a test client with dependency overrides."""
        app.dependency_overrides[deps.get_current_active_user] = lambda: mock_user
        app.dependency_overrides[deps.get_db] = lambda: mock_db
        app.dependency_overrides[deps.get_rabbitmq_connection] = lambda: mock_rabbitmq_connection
        
        client = TestClient(app)
        yield client
        
        # Clean up overrides
        app.dependency_overrides.clear()

    @pytest.fixture
    def sample_order(self):
        """Sample imaging order for testing."""
        order = MagicMock(spec=ImagingOrder)
        order.id = 1
        order.accession_number = "ACC123456"
        order.patient_name = "Test Patient"
        order.patient_id = "MRN123"
        order.modality = "CT"
        order.order_status = OrderStatus.SCHEDULED
        order.created_at = "2024-01-01T10:00:00Z"
        order.updated_at = "2024-01-01T10:00:00Z"
        order.order_received_at = "2024-01-01T10:00:00Z"
        return order

    def test_delete_order_success(self, client, mock_db, sample_order):
        """Test successful order deletion."""
        # Mock CRUD operations
        with patch("app.crud.imaging_order.get") as mock_get, \
             patch("app.crud.imaging_order.remove") as mock_remove, \
             patch("app.events.publish_order_event") as mock_publish:
            
            mock_get.return_value = sample_order
            mock_remove.return_value = sample_order

            # Make the request
            response = client.delete("/api/v1/orders/1")

            # Assertions
            assert response.status_code == 204
            assert response.content == b""
            
            # Verify CRUD calls
            mock_get.assert_called_once_with(mock_db, id=1)
            mock_remove.assert_called_once_with(mock_db, id=1)
            
            # Verify event was published
            mock_publish.assert_called_once()
            args, kwargs = mock_publish.call_args
            assert kwargs["event_type"] == "order_deleted"

    def test_delete_order_not_found(self, client, mock_db):
        """Test deletion of non-existent order."""
        # Mock CRUD operations
        with patch("app.crud.imaging_order.get") as mock_get:
            mock_get.return_value = None

            # Make the request
            response = client.delete("/api/v1/orders/999")

            # Assertions
            assert response.status_code == 404
            assert "Imaging order not found" in response.json()["detail"]

    def test_delete_order_database_constraint_error(self, client, mock_db, sample_order):
        """Test deletion with database constraint violation."""
        # Mock CRUD operations
        with patch("app.crud.imaging_order.get") as mock_get, \
             patch("app.crud.imaging_order.remove") as mock_remove:
            
            mock_get.return_value = sample_order
            mock_remove.side_effect = ValueError("Foreign key constraint violation")

            # Make the request
            response = client.delete("/api/v1/orders/1")

            # Assertions
            assert response.status_code == 400
            assert "Cannot delete order" in response.json()["detail"]
            assert "Foreign key constraint violation" in response.json()["detail"]
