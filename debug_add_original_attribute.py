#!/usr/bin/env python3
"""
Debug script to understand what's happening in _add_original_attribute function.
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

def debug_function_behavior():
    """Debug what the function is actually doing"""
    
    # Create a test dataset
    dataset = Dataset()
    dataset.StudyInstanceUID = "1.2.3.4.5"
    
    # Create mock database session
    mock_db = MagicMock()
    
    # Create test original element
    original_element = DataElement(Tag(0x0008, 0x0020), 'DA', '20240101')
    
    print("=== Testing with mocked config returning True ===")
    
    # Mock get_config_value in the actual module that imports it
    import app.worker.standard_modification_handler as handler_module
    original_get_config_value = handler_module.get_config_value
    
    def mock_get_config_value_true(db_session, key, default):
        print(f"Mock called with key='{key}', default={default}")
        if key == "LOG_ORIGINAL_ATTRIBUTES":
            print("Returning True")
            return True
        print(f"Returning default {default}")
        return default
    
    # Patch the function in the handler module
    handler_module.get_config_value = mock_get_config_value_true
    
    try:
        print("\nBefore calling _add_original_attribute:")
        print(f"Dataset tags: {list(dataset.keys())}")
        
        # Call _add_original_attribute 
        _add_original_attribute(
            ds=dataset,
            original_element=original_element,
            description="Test modification",
            source_id="test_source",
            db_session=mock_db
        )
        
        print("\nAfter calling _add_original_attribute:")
        print(f"Dataset tags: {list(dataset.keys())}")
        
        # Check for OriginalAttributesSequence
        original_attrs_tag = Tag(0x0400, 0x0561)
        if original_attrs_tag in dataset:
            print(f"✅ OriginalAttributesSequence found with {len(dataset[original_attrs_tag].value)} items")
            for i, item in enumerate(dataset[original_attrs_tag].value):
                print(f"  Item {i}: {dict(item)}")
        else:
            print("❌ OriginalAttributesSequence NOT found")
            
    finally:
        # Restore original function
        handler_module.get_config_value = original_get_config_value

if __name__ == "__main__":
    debug_function_behavior()
