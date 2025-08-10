"""
Unit tests for modality DMWL access validation functionality.
These tests validate the modality access control logic without requiring database setup.
"""

import pytest
from unittest.mock import Mock, MagicMock
from sqlalchemy.orm import Session
from app.services.network.dimse.handlers import _validate_modality_dmwl_access
from app.db.models.modality import Modality
from app.db.models.facility import Facility
from app.crud import crud_modality


class TestModalityDmwlValidation:
    """Test cases for modality DMWL access validation."""

    def test_validate_authorized_modality(self):
        """Test that an authorized modality passes validation."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock facility
        mock_facility = Mock(spec=Facility)
        mock_facility.id = 1
        mock_facility.is_active = True
        
        # Create mock modality
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "AUTHORIZED_CT"
        mock_modality.ip_address = "192.168.1.100"
        mock_modality.is_active = True
        mock_modality.is_dmwl_enabled = True
        mock_modality.modality_type = "CT"
        mock_modality.facility_id = 1
        mock_modality.department = "Radiology"
        mock_modality.facility = mock_facility
        
        # Mock the CRUD function to return authorized modality
        crud_modality.can_query_dmwl = Mock(return_value=(True, mock_modality))
        
        # Act
        is_allowed, reason, modality_info = _validate_modality_dmwl_access(
            mock_db, "AUTHORIZED_CT", "192.168.1.100"
        )
        
        # Assert
        assert is_allowed is True
        assert reason is None
        assert modality_info is not None
        assert modality_info["id"] == 1
        assert modality_info["ae_title"] == "AUTHORIZED_CT"
        assert modality_info["modality_type"] == "CT"

    def test_validate_unknown_modality(self):
        """Test that an unknown modality is denied access."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Mock the CRUD functions
        crud_modality.can_query_dmwl = Mock(return_value=(False, None))
        crud_modality.get_by_ae_title = Mock(return_value=None)
        
        # Act
        is_allowed, reason, modality_info = _validate_modality_dmwl_access(
            mock_db, "UNKNOWN_CT", "192.168.1.999"
        )
        
        # Assert
        assert is_allowed is False
        assert reason is not None
        assert "not configured" in reason
        assert modality_info is None

    def test_validate_inactive_modality(self):
        """Test that an inactive modality is denied access."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock modality (inactive)
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "INACTIVE_CT"
        mock_modality.is_active = False
        mock_modality.is_dmwl_enabled = True
        mock_modality.ip_address = "192.168.1.101"
        
        # Mock the CRUD functions
        crud_modality.can_query_dmwl = Mock(return_value=(False, None))
        crud_modality.get_by_ae_title = Mock(return_value=mock_modality)
        
        # Act
        is_allowed, reason, modality_info = _validate_modality_dmwl_access(
            mock_db, "INACTIVE_CT", "192.168.1.101"
        )
        
        # Assert
        assert is_allowed is False
        assert reason is not None
        assert "inactive" in reason.lower()
        assert modality_info is None

    def test_validate_dmwl_disabled_modality(self):
        """Test that a modality with DMWL disabled is denied access."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock modality (DMWL disabled)
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "DMWL_DISABLED_CT"
        mock_modality.is_active = True
        mock_modality.is_dmwl_enabled = False
        mock_modality.ip_address = "192.168.1.102"
        
        # Mock the CRUD functions
        crud_modality.can_query_dmwl = Mock(return_value=(False, None))
        crud_modality.get_by_ae_title = Mock(return_value=mock_modality)
        
        # Act
        is_allowed, reason, modality_info = _validate_modality_dmwl_access(
            mock_db, "DMWL_DISABLED_CT", "192.168.1.102"
        )
        
        # Assert
        assert is_allowed is False
        assert reason is not None
        assert "not enabled for DMWL" in reason
        assert modality_info is None

    def test_validate_ip_mismatch(self):
        """Test that a modality querying from wrong IP is denied access."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock modality (different IP)
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "IP_MISMATCH_CT"
        mock_modality.is_active = True
        mock_modality.is_dmwl_enabled = True
        mock_modality.ip_address = "192.168.1.103"  # Expected IP
        
        # Mock the CRUD functions
        crud_modality.can_query_dmwl = Mock(return_value=(False, None))
        crud_modality.get_by_ae_title = Mock(return_value=mock_modality)
        
        # Act
        is_allowed, reason, modality_info = _validate_modality_dmwl_access(
            mock_db, "IP_MISMATCH_CT", "192.168.1.999"  # Different IP
        )
        
        # Assert
        assert is_allowed is False
        assert reason is not None
        assert "IP address mismatch" in reason
        assert "192.168.1.103" in reason  # Expected IP
        assert "192.168.1.999" in reason  # Actual IP
        assert modality_info is None

    def test_validate_inactive_facility(self):
        """Test that a modality from an inactive facility is denied access."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock facility (inactive)
        mock_facility = Mock(spec=Facility)
        mock_facility.id = 1
        mock_facility.is_active = False
        
        # Create mock modality
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "INACTIVE_FACILITY_CT"
        mock_modality.is_active = True
        mock_modality.is_dmwl_enabled = True
        mock_modality.ip_address = "192.168.1.104"
        mock_modality.facility = mock_facility
        
        # Mock the CRUD functions
        crud_modality.can_query_dmwl = Mock(return_value=(False, None))
        crud_modality.get_by_ae_title = Mock(return_value=mock_modality)
        
        # Act
        is_allowed, reason, modality_info = _validate_modality_dmwl_access(
            mock_db, "INACTIVE_FACILITY_CT", "192.168.1.104"
        )
        
        # Assert
        assert is_allowed is False
        assert reason is not None
        assert "facility" in reason.lower()
        assert "inactive" in reason.lower()
        assert modality_info is None

    def test_validate_missing_facility(self):
        """Test that a modality with no facility is denied access."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock modality (no facility)
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "NO_FACILITY_CT"
        mock_modality.is_active = True
        mock_modality.is_dmwl_enabled = True
        mock_modality.ip_address = "192.168.1.105"
        mock_modality.facility = None
        
        # Mock the CRUD functions
        crud_modality.can_query_dmwl = Mock(return_value=(False, None))
        crud_modality.get_by_ae_title = Mock(return_value=mock_modality)
        
        # Act
        is_allowed, reason, modality_info = _validate_modality_dmwl_access(
            mock_db, "NO_FACILITY_CT", "192.168.1.105"
        )
        
        # Assert
        assert is_allowed is False
        assert reason is not None
        assert "facility" in reason.lower()
        assert "inactive" in reason.lower()
        assert modality_info is None
