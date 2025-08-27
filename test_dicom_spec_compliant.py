#!/usr/bin/env python3
"""
Test script to verify DICOM spec-compliant OriginalAttributesSequence implementation.
Tests both COERCE and ADD cases according to DICOM PS3.17 2025a specification.
"""
import sys
import os
from unittest.mock import MagicMock
from pydicom import Dataset, DataElement
from pydicom.tag import Tag

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.worker.standard_modification_handler import _add_original_attribute

def test_coerce_case():
    """Test COERCE case - replacing existing value (matches DICOM spec example)"""
    
    print("=== Testing COERCE Case ===")
    
    # Create a dataset with existing Patient's Name
    dataset = Dataset()
    dataset.StudyInstanceUID = "1.2.3.4.5"
    dataset.PatientName = "Doe^Jane"  # Original name
    
    mock_db = MagicMock()
    
    # Mock get_config_value
    import app.worker.standard_modification_handler as handler_module
    original = handler_module.get_config_value
    handler_module.get_config_value = lambda db, key, default: True if key == "LOG_ORIGINAL_ATTRIBUTES" else default
    
    try:
        # Create original element (what was there before)
        original_element = DataElement(Tag(0x0010, 0x0010), 'PN', 'Doe^Jane')
        
        # Call _add_original_attribute before making the change
        _add_original_attribute(
            ds=dataset,
            original_element=original_element,
            description="Patient name correction",
            source_id="GinHealthSystem PACS", 
            db_session=mock_db,
            source_of_previous_values="",  # Unknown source
            modified_tag=Tag(0x0010, 0x0010)
        )
        
        # Now apply the actual change (this would be done by caller)
        dataset.PatientName = "Smith^Jane"  # New corrected name
        
        # Verify the audit trail
        oas_tag = Tag(0x0400, 0x0561)
        assert oas_tag in dataset, "OriginalAttributesSequence not found"
        
        oas_sequence = dataset[oas_tag].value
        assert len(oas_sequence) == 1, f"Expected 1 OAS item, got {len(oas_sequence)}"
        
        oas_item = oas_sequence[0]
        
        # Check according to DICOM spec table YYYY.7-2c
        assert Tag(0x0400, 0x0564) in oas_item, "SourceOfPreviousValues missing"
        assert oas_item[Tag(0x0400, 0x0564)].value == "", "SourceOfPreviousValues should be empty (unknown)"
        
        assert Tag(0x0400, 0x0565) in oas_item, "ReasonForTheAttributeModification missing"
        reason = oas_item[Tag(0x0400, 0x0565)]
        assert reason.VR == "CS", f"Reason VR should be CS, got {reason.VR}"
        assert reason.value == "COERCE", f"Reason should be COERCE, got {reason.value}"
        
        # Check ModifiedAttributesSequence contains original value
        assert Tag(0x0400, 0x0550) in oas_item, "ModifiedAttributesSequence missing"
        mas_sequence = oas_item[Tag(0x0400, 0x0550)].value
        assert len(mas_sequence) == 1, f"Expected 1 MAS item, got {len(mas_sequence)}"
        
        mas_item = mas_sequence[0]
        assert Tag(0x0010, 0x0010) in mas_item, "Original Patient's Name not in MAS"
        assert mas_item[Tag(0x0010, 0x0010)].value == "Doe^Jane", "MAS should contain prior name 'Doe^Jane'"
        
        # Verify current dataset shows new value
        assert dataset.PatientName == "Smith^Jane", "Current PatientName should be 'Smith^Jane'"
        
        print("✅ COERCE case follows DICOM spec correctly")
        print(f"   - Current Patient's Name: '{dataset.PatientName}' (new value)")
        print(f"   - MAS contains prior value: '{mas_item[Tag(0x0010, 0x0010)].value}'")
        print(f"   - Reason: {reason.value} (CS VR)")
        print(f"   - Source: '{oas_item[Tag(0x0400, 0x0564)].value}' (unknown)")
        
        return True
        
    finally:
        handler_module.get_config_value = original

def test_add_case():
    """Test ADD case - adding missing value (matches DICOM spec example)"""
    
    print("\n=== Testing ADD Case ===")
    
    # Create a dataset without Body Part Examined
    dataset = Dataset()
    dataset.StudyInstanceUID = "1.2.3.4.5"
    dataset.SeriesInstanceUID = "1.2.3.4.5.1"
    # Note: Body Part Examined is intentionally missing
    
    mock_db = MagicMock()
    
    # Mock get_config_value
    import app.worker.standard_modification_handler as handler_module
    original = handler_module.get_config_value
    handler_module.get_config_value = lambda db, key, default: True if key == "LOG_ORIGINAL_ATTRIBUTES" else default
    
    try:
        # Call _add_original_attribute for ADD case (no original element)
        _add_original_attribute(
            ds=dataset,
            original_element=None,  # No original element = ADD case
            description="Added missing body part",
            source_id="GinHealthSystem PACS",
            db_session=mock_db,
            source_of_previous_values="",  # Unknown source
            modified_tag=Tag(0x0018, 0x0015)  # Body Part Examined
        )
        
        # Now apply the actual change (this would be done by caller)
        dataset.BodyPartExamined = "LIVER"  # New value being added
        
        # Verify the audit trail
        oas_tag = Tag(0x0400, 0x0561)
        assert oas_tag in dataset, "OriginalAttributesSequence not found"
        
        oas_sequence = dataset[oas_tag].value
        assert len(oas_sequence) == 1, f"Expected 1 OAS item, got {len(oas_sequence)}"
        
        oas_item = oas_sequence[0]
        
        # Check according to DICOM spec table YYYY.7-2c
        assert Tag(0x0400, 0x0565) in oas_item, "ReasonForTheAttributeModification missing"
        reason = oas_item[Tag(0x0400, 0x0565)]
        assert reason.VR == "CS", f"Reason VR should be CS, got {reason.VR}"
        assert reason.value == "ADD", f"Reason should be ADD, got {reason.value}"
        
        # Check ModifiedAttributesSequence contains empty value for the tag
        assert Tag(0x0400, 0x0550) in oas_item, "ModifiedAttributesSequence missing"
        mas_sequence = oas_item[Tag(0x0400, 0x0550)].value
        assert len(mas_sequence) == 1, f"Expected 1 MAS item, got {len(mas_sequence)}"
        
        mas_item = mas_sequence[0]
        assert Tag(0x0018, 0x0015) in mas_item, "Body Part Examined not in MAS"
        
        # According to DICOM spec, MAS should contain the tag with empty value indicating it was missing
        mas_bpe = mas_item[Tag(0x0018, 0x0015)]
        assert mas_bpe.VR == "CS", f"MAS Body Part Examined should have CS VR, got {mas_bpe.VR}"
        assert mas_bpe.value == "", "MAS should contain empty value indicating prior value was missing"
        
        # Verify current dataset shows new value
        assert dataset.BodyPartExamined == "LIVER", "Current BodyPartExamined should be 'LIVER'"
        
        print("✅ ADD case follows DICOM spec correctly")
        print(f"   - Current Body Part Examined: '{dataset.BodyPartExamined}' (new value)")
        print(f"   - MAS contains empty value: '{mas_bpe.value}' (indicating prior value missing)")
        print(f"   - Reason: {reason.value} (CS VR)")
        
        return True
        
    finally:
        handler_module.get_config_value = original

def print_oas_structure_detailed(dataset, title):
    """Print detailed OAS structure for verification"""
    print(f"\n=== {title} ===")
    oas_tag = Tag(0x0400, 0x0561)
    if oas_tag not in dataset:
        print("No OriginalAttributesSequence found")
        return
    
    oas_sequence = dataset[oas_tag].value
    print("(0400,0561) OriginalAttributesSequence  SQ")
    
    for i, oas_item in enumerate(oas_sequence):
        print(f"  > Item {i+1}")
        
        # Print in order matching DICOM spec table
        if Tag(0x0400, 0x0564) in oas_item:
            src = oas_item[Tag(0x0400, 0x0564)].value
            print(f"    (0400,0564) SourceOfPreviousValues      LO  \"{src}\"   ; {'unknown' if not src else src}")
        
        if Tag(0x0400, 0x0562) in oas_item:
            dt = oas_item[Tag(0x0400, 0x0562)].value
            print(f"    (0400,0562) AttributeModificationDateTime DT  {dt}")
        
        if Tag(0x0400, 0x0563) in oas_item:
            sys_name = oas_item[Tag(0x0400, 0x0563)].value
            print(f"    (0400,0563) ModifyingSystem             LO  \"{sys_name}\"")
        
        if Tag(0x0400, 0x0565) in oas_item:
            reason = oas_item[Tag(0x0400, 0x0565)].value
            print(f"    (0400,0565) ReasonForTheAttributeModification CS  {reason}")
        
        if Tag(0x0400, 0x0550) in oas_item:
            mas_seq = oas_item[Tag(0x0400, 0x0550)].value
            print(f"    (0400,0550) ModifiedAttributesSequence  SQ")
            for j, mas_item in enumerate(mas_seq):
                print(f"      >> Item {j+1}")
                for tag, element in mas_item.items():
                    comment = ""
                    if element.value == "":
                        comment = "   ; prior value missing"
                    elif element.keyword == "PatientName" and element.value == "Doe^Jane":
                        comment = "   ; prior name"
                    print(f"         {tag} {element.keyword}: {element.VR} = '{element.value}'{comment}")

if __name__ == "__main__":
    print("🔧 Testing DICOM PS3.17 2025a Compliant OriginalAttributesSequence\n")
    
    test1_passed = test_coerce_case()
    test2_passed = test_add_case()
    
    if test1_passed and test2_passed:
        print("\n🎉 ALL TESTS PASSED - Implementation matches DICOM PS3.17 2025a specification!")
        
        # Show detailed structures
        print("\n" + "="*70)
        print("DETAILED STRUCTURES (matching DICOM spec Table YYYY.7-2c format)")
        
        # Example COERCE
        dataset1 = Dataset()
        dataset1.PatientName = "Smith^Jane"
        mock_db = MagicMock()
        
        import app.worker.standard_modification_handler as handler_module
        original = handler_module.get_config_value
        handler_module.get_config_value = lambda db, key, default: True if key == "LOG_ORIGINAL_ATTRIBUTES" else default
        
        try:
            original_element = DataElement(Tag(0x0010, 0x0010), 'PN', 'Doe^Jane')
            _add_original_attribute(
                ds=dataset1,
                original_element=original_element,
                description="Patient name correction",
                source_id="GinHealthSystem PACS",
                db_session=mock_db,
                source_of_previous_values="",
                modified_tag=Tag(0x0010, 0x0010)
            )
            print_oas_structure_detailed(dataset1, "COERCE Example (Patient's Name correction)")
            
            # Example ADD
            dataset2 = Dataset()
            dataset2.BodyPartExamined = "LIVER"
            _add_original_attribute(
                ds=dataset2,
                original_element=None,
                description="Added missing body part",
                source_id="GinHealthSystem PACS",
                db_session=mock_db,
                source_of_previous_values="",
                modified_tag=Tag(0x0018, 0x0015)
            )
            print_oas_structure_detailed(dataset2, "ADD Example (Body Part Examined addition)")
            
        finally:
            handler_module.get_config_value = original
        
        sys.exit(0)
    else:
        print("\n💥 TESTS FAILED")
        sys.exit(1)
