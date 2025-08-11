"""
Unit tests for the modality CRUD bypass IP validation functionality.
"""

import pytest
from unittest.mock import Mock, MagicMock
from sqlalchemy.orm import Session
from app.crud.crud_modality import can_query_dmwl
from app.db.models.modality import Modality
from app.db.models.facility import Facility


class TestModalityBypassCrud:
    """Test cases for modality bypass IP validation in CRUD layer."""

    def test_can_query_dmwl_normal_case(self):
        """Test normal case where AE title and IP both match."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock facility
        mock_facility = Mock(spec=Facility)
        mock_facility.id = 1
        mock_facility.is_active = True
        
        # Create mock modality
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "NORMAL_CT"
        mock_modality.ip_address = "192.168.1.100"
        mock_modality.is_active = True
        mock_modality.is_dmwl_enabled = True
        mock_modality.bypass_ip_validation = False
        mock_modality.facility = mock_facility
        
        # Mock scalar to return modality on first call (normal case)
        mock_db.scalar.return_value = mock_modality
        
        # Act
        can_access, modality = can_query_dmwl(
            mock_db, ae_title="NORMAL_CT", ip_address="192.168.1.100"
        )
        
        # Assert
        assert can_access is True
        assert modality == mock_modality
        assert mock_db.scalar.call_count == 1  # Only one query needed

    def test_can_query_dmwl_bypass_case(self):
        """Test bypass case where AE title matches but IP is different, but bypass is enabled."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock facility
        mock_facility = Mock(spec=Facility)
        mock_facility.id = 1
        mock_facility.is_active = True
        
        # Create mock modality with bypass enabled
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "BYPASS_CT"
        mock_modality.ip_address = "192.168.1.100"
        mock_modality.is_active = True
        mock_modality.is_dmwl_enabled = True
        mock_modality.bypass_ip_validation = True
        mock_modality.facility = mock_facility
        
        # Mock scalar to return None on first call (normal case fails), 
        # then modality on second call (bypass case succeeds)
        mock_db.scalar.side_effect = [None, mock_modality]
        
        # Act
        can_access, modality = can_query_dmwl(
            mock_db, ae_title="BYPASS_CT", ip_address="192.168.1.999"  # Different IP
        )
        
        # Assert
        assert can_access is True
        assert modality == mock_modality
        assert mock_db.scalar.call_count == 2  # Two queries: normal then bypass

    def test_can_query_dmwl_no_bypass_ip_mismatch(self):
        """Test case where AE title matches but IP is different and bypass is disabled."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Mock scalar to return None for both normal and bypass cases
        mock_db.scalar.side_effect = [None, None]
        
        # Act
        can_access, modality = can_query_dmwl(
            mock_db, ae_title="NO_BYPASS_CT", ip_address="192.168.1.999"
        )
        
        # Assert
        assert can_access is False
        assert modality is None
        assert mock_db.scalar.call_count == 2  # Two queries attempted

    def test_can_query_dmwl_no_ip_provided(self):
        """Test case where no IP is provided."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Create mock modality
        mock_modality = Mock(spec=Modality)
        mock_modality.id = 1
        mock_modality.ae_title = "ANY_IP_CT"
        
        # Mock scalar to return modality
        mock_db.scalar.return_value = mock_modality
        
        # Act
        can_access, modality = can_query_dmwl(
            mock_db, ae_title="ANY_IP_CT", ip_address=None
        )
        
        # Assert
        assert can_access is True
        assert modality == mock_modality
        assert mock_db.scalar.call_count == 1  # Only one query needed

    def test_can_query_dmwl_unknown_ae_title(self):
        """Test case where AE title is completely unknown."""
        # Arrange
        mock_db = Mock(spec=Session)
        
        # Mock scalar to return None for both cases
        mock_db.scalar.side_effect = [None, None]
        
        # Act
        can_access, modality = can_query_dmwl(
            mock_db, ae_title="UNKNOWN_CT", ip_address="192.168.1.100"
        )
        
        # Assert
        assert can_access is False
        assert modality is None
        assert mock_db.scalar.call_count == 2  # Both queries attempted
