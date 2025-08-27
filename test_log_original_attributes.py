#!/usr/bin/env python3
"""
Test script to verify LOG_ORIGINAL_ATTRIBUTES configuration is respected.
This tests the fix for the dynamic configuration system integration.
"""
import sys
import os
from unittest.mock import MagicMock
from pydicom import Dataset, DataElement
from pydicom.tag import Tag

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.utils.config_helpers import get_config_value
from app.worker.standard_modification_handler import _add_original_attribute

def test_log_original_attributes_false():
    """Test that _add_original_attribute respects LOG_ORIGINAL_ATTRIBUTES=False"""
    
    # Create a test dataset
    dataset = Dataset()
    dataset.StudyInstanceUID = "1.2.3.4.5"
    
    # Create mock database session
    mock_db = MagicMock()
    
    # Mock config_helpers.get_config_value to return False
    original_get_config_value = get_config_value
    
    def mock_get_config_value(db_session, key, default):
        if key == "LOG_ORIGINAL_ATTRIBUTES":
            return False
        return original_get_config_value(db_session, key, default)
    
    # Temporarily replace the function in the handler module  
    import app.worker.standard_modification_handler as handler_module
    original_handler_get_config_value = handler_module.get_config_value
    handler_module.get_config_value = mock_get_config_value
    
    try:
        # Create test original element
        original_element = DataElement(Tag(0x0008, 0x0020), 'DA', '20240101')
        
        # Call _add_original_attribute with correct parameters
        _add_original_attribute(
            ds=dataset,
            original_element=original_element,
            description="Test modification",
            source_id="test_source",
            db_session=mock_db
        )
        
        # Check that OriginalAttributesSequence was NOT added
        original_attrs_tag = Tag(0x0400, 0x0561)  # OriginalAttributesSequence
        
        if original_attrs_tag in dataset:
            print("❌ FAILED: OriginalAttributesSequence was added even though LOG_ORIGINAL_ATTRIBUTES=False")
            return False
        else:
            print("✅ PASSED: OriginalAttributesSequence was NOT added when LOG_ORIGINAL_ATTRIBUTES=False")
            return True
            
    finally:
        # Restore original function
        config_helpers.get_config_value = original_get_config_value

def test_log_original_attributes_true():
    """Test that _add_original_attribute respects LOG_ORIGINAL_ATTRIBUTES=True"""
    
    # Create a test dataset
    dataset = Dataset()
    dataset.StudyInstanceUID = "1.2.3.4.5"
    
    # Create mock database session
    mock_db = MagicMock()
    
    # Mock config_helpers.get_config_value to return True
    original_get_config_value = get_config_value
    
    def mock_get_config_value(db_session, key, default):
        if key == "LOG_ORIGINAL_ATTRIBUTES":
            return True
        return original_get_config_value(db_session, key, default)
    
    # Temporarily replace the function
    import app.utils.config_helpers as config_helpers
    config_helpers.get_config_value = mock_get_config_value
    
    try:
        # Create test original element
        original_element = DataElement(Tag(0x0008, 0x0020), 'DA', '20240101')
        
        # Call _add_original_attribute with correct parameters
        _add_original_attribute(
            ds=dataset,
            original_element=original_element,
            description="Test modification",
            source_id="test_source",
            db_session=mock_db
        )
        
        # Check that OriginalAttributesSequence was added
        original_attrs_tag = Tag(0x0400, 0x0561)  # OriginalAttributesSequence
        
        if original_attrs_tag in dataset:
            print("✅ PASSED: OriginalAttributesSequence was added when LOG_ORIGINAL_ATTRIBUTES=True")
            return True
        else:
            print("❌ FAILED: OriginalAttributesSequence was NOT added even though LOG_ORIGINAL_ATTRIBUTES=True")
            return False
            
    finally:
        # Restore original function
        config_helpers.get_config_value = original_get_config_value

if __name__ == "__main__":
    print("Testing LOG_ORIGINAL_ATTRIBUTES configuration fix...\n")
    
    test1_passed = test_log_original_attributes_false()
    print()
    test2_passed = test_log_original_attributes_true()
    print()
    
    if test1_passed and test2_passed:
        print("🎉 ALL TESTS PASSED - Configuration fix is working correctly!")
        sys.exit(0)
    else:
        print("💥 SOME TESTS FAILED - Configuration fix needs more work")
        sys.exit(1)
