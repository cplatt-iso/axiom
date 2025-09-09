#!/usr/bin/env python3
"""
Test script to verify the DICOM-compliant OriginalAttributesSequence implementation.
This tests the updated _add_original_attribute function.
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

def test_coerce_case():
    """Test COERCE case - replacing existing value"""
    
    # Create a test dataset
    dataset = Dataset()
    dataset.StudyInstanceUID = "1.2.3.4.5"
    
    # Create mock database session
    mock_db = MagicMock()
    
    # Mock get_config_value to return True
    import app.worker.standard_modification_handler as handler_module
    original_handler_get_config_value = handler_module.get_config_value
    handler_module.get_config_value = lambda db, key, default: True if key == "LOG_ORIGINAL_ATTRIBUTES" else default
    
    try:
        # Create test original element (old value)
        original_element = DataElement(Tag(0x0008, 0x0080), 'LO', 'Gen Hospital')
        
        # Call _add_original_attribute 
        _add_original_attribute(
            ds=dataset,
            original_element=original_element,
            description="Set InstitutionName to TEST_INSTITUTION",
            source_id="AXIOM_TAGMORPHER/1.2.3",
            db_session=mock_db,
            source_of_previous_values="SENDING_AET@ExternalHospital"
        )
        
        # Check that OriginalAttributesSequence was added
        oas_tag = Tag(0x0400, 0x0561)
        assert oas_tag in dataset, "OriginalAttributesSequence not found"
        
        oas_sequence = dataset[oas_tag].value
        assert len(oas_sequence) == 1, f"Expected 1 OAS item, got {len(oas_sequence)}"
        
        oas_item = oas_sequence[0]
        
        # Check required fields
        assert Tag(0x0400, 0x0564) in oas_item, "SourceOfPreviousValues missing"
        assert oas_item[Tag(0x0400, 0x0564)].value == "SENDING_AET@ExternalHospital"
        
        assert Tag(0x0400, 0x0562) in oas_item, "AttributeModificationDateTime missing"
        
        assert Tag(0x0400, 0x0563) in oas_item, "ModifyingSystem missing"
        assert oas_item[Tag(0x0400, 0x0563)].value == "AXIOM_TAGMORPHER/1.2.3"
        
        assert Tag(0x0400, 0x0565) in oas_item, "ReasonForTheAttributeModification missing"
        reason = oas_item[Tag(0x0400, 0x0565)]
        assert reason.VR == "CS", f"Reason VR should be CS, got {reason.VR}"
        assert reason.value == "COERCE", f"Reason should be COERCE, got {reason.value}"
        
        # Check ModifiedAttributesSequence
        assert Tag(0x0400, 0x0550) in oas_item, "ModifiedAttributesSequence missing"
        mas_sequence = oas_item[Tag(0x0400, 0x0550)].value
        assert len(mas_sequence) == 1, f"Expected 1 MAS item, got {len(mas_sequence)}"
        
        mas_item = mas_sequence[0]
        assert Tag(0x0008, 0x0080) in mas_item, "Original InstitutionName not in MAS"
        assert mas_item[Tag(0x0008, 0x0080)].value == "Gen Hospital"
        
        print("✅ COERCE case test passed!")
        return True
        
    finally:
        handler_module.get_config_value = original_handler_get_config_value

def test_add_case():
    """Test ADD case - adding new value where none existed"""
    
    # Create a test dataset
    dataset = Dataset()
    dataset.StudyInstanceUID = "1.2.3.4.5"
    
    # Create mock database session
    mock_db = MagicMock()
    
    # Mock get_config_value to return True
    import app.worker.standard_modification_handler as handler_module
    original_handler_get_config_value = handler_module.get_config_value
    handler_module.get_config_value = lambda db, key, default: True if key == "LOG_ORIGINAL_ATTRIBUTES" else default
    
    try:
        # Call _add_original_attribute with no original element (ADD case)
        _add_original_attribute(
            ds=dataset,
            original_element=None,  # No original element = ADD case
            description="Added missing InstitutionName",
            source_id="AXIOM_TAGMORPHER/1.2.3",
            db_session=mock_db,
            source_of_previous_values=""  # Empty = unknown source
        )
        
        # Check that OriginalAttributesSequence was added
        oas_tag = Tag(0x0400, 0x0561)
        assert oas_tag in dataset, "OriginalAttributesSequence not found"
        
        oas_sequence = dataset[oas_tag].value
        assert len(oas_sequence) == 1, f"Expected 1 OAS item, got {len(oas_sequence)}"
        
        oas_item = oas_sequence[0]
        
        # Check ReasonForTheAttributeModification is ADD
        assert Tag(0x0400, 0x0565) in oas_item, "ReasonForTheAttributeModification missing"
        reason = oas_item[Tag(0x0400, 0x0565)]
        assert reason.VR == "CS", f"Reason VR should be CS, got {reason.VR}"
        assert reason.value == "ADD", f"Reason should be ADD, got {reason.value}"
        
        # Check SourceOfPreviousValues is empty (unknown)
        assert Tag(0x0400, 0x0564) in oas_item, "SourceOfPreviousValues missing"
        assert oas_item[Tag(0x0400, 0x0564)].value == ""
        
        # Check ModifiedAttributesSequence exists but is empty (indicating previously missing)
        assert Tag(0x0400, 0x0550) in oas_item, "ModifiedAttributesSequence missing"
        mas_sequence = oas_item[Tag(0x0400, 0x0550)].value
        assert len(mas_sequence) == 1, f"Expected 1 MAS item, got {len(mas_sequence)}"
        
        mas_item = mas_sequence[0]
        # MAS item should be empty (no tags) indicating the element was previously missing
        assert len(mas_item) == 0, f"MAS item should be empty for ADD case, got {len(mas_item)} elements"
        
        print("✅ ADD case test passed!")
        return True
        
    finally:
        handler_module.get_config_value = original_handler_get_config_value

def print_oas_structure(dataset):
    """Helper function to print the OAS structure for debugging"""
    oas_tag = Tag(0x0400, 0x0561)
    if oas_tag not in dataset:
        print("No OriginalAttributesSequence found")
        return
    
    print("\n=== OriginalAttributesSequence Structure ===")
    oas_sequence = dataset[oas_tag].value
    for i, oas_item in enumerate(oas_sequence):
        print(f"\nItem {i+1}:")
        for tag, element in oas_item.items():
            print(f"  {tag} {element.keyword}: {element.VR} = '{element.value}'")
            if element.VR == "SQ":  # ModifiedAttributesSequence
                for j, mas_item in enumerate(element.value):
                    print(f"    >> MAS Item {j+1}:")
                    for mas_tag, mas_element in mas_item.items():
                        print(f"      {mas_tag} {mas_element.keyword}: {mas_element.VR} = '{mas_element.value}'")

if __name__ == "__main__":
    print("Testing DICOM-compliant OriginalAttributesSequence implementation...\n")
    
    test1_passed = test_coerce_case()
    print()
    test2_passed = test_add_case()
    print()
    
    if test1_passed and test2_passed:
        print("🎉 ALL TESTS PASSED - DICOM-compliant OAS implementation working correctly!")
        
        # Show an example of the structure
        print("\n" + "="*60)
        print("Example COERCE case structure:")
        dataset = Dataset()
        mock_db = MagicMock()
        
        import app.worker.standard_modification_handler as handler_module
        original = handler_module.get_config_value
        handler_module.get_config_value = lambda db, key, default: True if key == "LOG_ORIGINAL_ATTRIBUTES" else default
        
        try:
            original_element = DataElement(Tag(0x0008, 0x0080), 'LO', 'Gen Hospital')
            _add_original_attribute(
                ds=dataset,
                original_element=original_element,
                description="Institution coercion rule",
                source_id="AXIOM_TAGMORPHER/1.2.3",
                db_session=mock_db,
                source_of_previous_values="SENDING_AET@ExternalHospital"
            )
            print_oas_structure(dataset)
        finally:
            handler_module.get_config_value = original
        
        sys.exit(0)
    else:
        print("💥 SOME TESTS FAILED")
        sys.exit(1)
