"""
Tests for the first user becomes admin functionality.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy.orm import Session

from app.crud.crud_user import create_user_from_google, get_user_count


class TestFirstUserAdmin:
    """Test suite for first user admin functionality."""

    def test_get_user_count_empty_database(self):
        """Test get_user_count returns 0 for empty database."""
        mock_db = Mock(spec=Session)
        mock_db.execute.return_value.scalar_one.return_value = 0
        
        count = get_user_count(mock_db)
        assert count == 0

    def test_get_user_count_with_users(self):
        """Test get_user_count returns correct count."""
        mock_db = Mock(spec=Session)
        mock_db.execute.return_value.scalar_one.return_value = 5
        
        count = get_user_count(mock_db)
        assert count == 5

    @patch('app.crud.crud_user.get_user_count')
    @patch('app.crud.crud_user.get_password_hash')
    def test_first_user_gets_admin_role(self, mock_get_password_hash, mock_get_user_count):
        """Test that the first user gets Admin role and superuser status."""
        # Setup mocks
        mock_db = Mock(spec=Session)
        mock_get_user_count.return_value = 0  # First user
        mock_get_password_hash.return_value = "hashed_password"
        
        # Mock the Admin role
        mock_admin_role = Mock()
        mock_admin_role.name = "Admin"
        mock_db.query.return_value.filter.return_value.first.return_value = mock_admin_role
        
        # Mock the user creation process
        mock_user = Mock()
        mock_user.roles = Mock()  # Make roles a mock object with append method
        mock_user.id = 1
        mock_user.email = "first@example.com"
        
        # Mock database operations
        mock_db.commit.return_value = None
        mock_db.refresh.return_value = None
        
        google_info = {
            "email": "first@example.com",
            "sub": "google_id_123",
            "name": "First User",
            "picture": "http://example.com/pic.jpg",
            "email_verified": True
        }
        
        with patch('app.crud.crud_user.User') as mock_user_class:
            mock_user_class.return_value = mock_user
            
            result = create_user_from_google(mock_db, google_info=google_info)
            
            # Verify user was created with correct properties
            mock_user_class.assert_called_once()
            call_args = mock_user_class.call_args[1]
            assert call_args['email'] == "first@example.com"
            assert call_args['is_superuser'] == True  # First user should be superuser
            
            # Verify Admin role was assigned
            mock_user.roles.append.assert_called_once_with(mock_admin_role)
            
            # Verify database operations
            mock_db.add.assert_called_once_with(mock_user)
            mock_db.commit.assert_called_once()
            mock_db.refresh.assert_called_once_with(mock_user)

    @patch('app.crud.crud_user.get_user_count')
    @patch('app.crud.crud_user.get_password_hash')
    def test_second_user_gets_user_role(self, mock_get_password_hash, mock_get_user_count):
        """Test that subsequent users get User role and not superuser status."""
        # Setup mocks
        mock_db = Mock(spec=Session)
        mock_get_user_count.return_value = 1  # Second user
        mock_get_password_hash.return_value = "hashed_password"
        
        # Mock the User role
        mock_user_role = Mock()
        mock_user_role.name = "User"
        mock_db.query.return_value.filter.return_value.first.return_value = mock_user_role
        
        # Mock the user creation process
        mock_user = Mock()
        mock_user.roles = Mock()  # Make roles a mock object with append method
        mock_user.id = 2
        mock_user.email = "second@example.com"
        
        # Mock database operations
        mock_db.commit.return_value = None
        mock_db.refresh.return_value = None
        
        google_info = {
            "email": "second@example.com",
            "sub": "google_id_456",
            "name": "Second User",
            "picture": "http://example.com/pic2.jpg",
            "email_verified": True
        }
        
        with patch('app.crud.crud_user.User') as mock_user_class:
            mock_user_class.return_value = mock_user
            
            result = create_user_from_google(mock_db, google_info=google_info)
            
            # Verify user was created with correct properties
            mock_user_class.assert_called_once()
            call_args = mock_user_class.call_args[1]
            assert call_args['email'] == "second@example.com"
            assert call_args['is_superuser'] == False  # Subsequent users should not be superuser
            
            # Verify User role was assigned
            mock_user.roles.append.assert_called_once_with(mock_user_role)

    @patch('app.crud.crud_user.get_user_count')
    def test_first_user_missing_admin_role_raises_error(self, mock_get_user_count):
        """Test that missing Admin role for first user raises critical error."""
        # Setup mocks
        mock_db = Mock(spec=Session)
        mock_get_user_count.return_value = 0  # First user
        
        # Mock missing Admin role
        mock_db.query.return_value.filter.return_value.first.return_value = None
        
        google_info = {
            "email": "first@example.com",
            "sub": "google_id_123",
            "name": "First User",
            "picture": "http://example.com/pic.jpg",
            "email_verified": True
        }
        
        with patch('app.crud.crud_user.User'), \
             patch('app.crud.crud_user.get_password_hash'):
            
            with pytest.raises(ValueError, match="Critical: Admin role.*missing"):
                create_user_from_google(mock_db, google_info=google_info)

    def test_invalid_google_info_raises_error(self):
        """Test that invalid Google info raises appropriate errors."""
        mock_db = Mock(spec=Session)
        
        # Test missing email
        with pytest.raises(ValueError, match="missing required fields"):
            create_user_from_google(mock_db, google_info={"sub": "123"})
        
        # Test missing sub
        with pytest.raises(ValueError, match="missing required fields"):
            create_user_from_google(mock_db, google_info={"email": "test@example.com"})
        
        # Test unverified email
        with pytest.raises(ValueError, match="not verified"):
            create_user_from_google(mock_db, google_info={
                "email": "test@example.com",
                "sub": "123",
                "email_verified": False
            })
