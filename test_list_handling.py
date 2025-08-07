#!/usr/bin/env python3
"""
Test for the specific transfer syntax list handling fixes.
"""

import sys
import os
sys.path.append('/app')

from app.services.network.dimse.transfer_syntax_negotiation import (
    find_compatible_transfer_syntax,
    analyze_accepted_contexts,
    handle_cstore_context_error
)

from pydicom.dataset import Dataset
from pydicom.uid import CTImageStorage, ImplicitVRLittleEndian
from pynetdicom.presentation import build_context

def create_mock_accepted_context():
    """Create a mock context that mimics pynetdicom's accepted context structure."""
    class MockContext:
        def __init__(self, context_id, abstract_syntax, transfer_syntax):
            self.context_id = context_id
            self.abstract_syntax = abstract_syntax
            # This is a list as it comes from pynetdicom
            self.transfer_syntax = transfer_syntax if isinstance(transfer_syntax, list) else [transfer_syntax]
    
    return MockContext(1, CTImageStorage, [ImplicitVRLittleEndian])

def create_mock_association():
    """Create a mock association."""
    class MockAssociation:
        def __init__(self):
            self.is_established = True
            self.accepted_contexts = [create_mock_accepted_context()]
            # Don't include requested_contexts to mimic the real pynetdicom behavior
            # where this attribute may not be available on Association objects
            self.acceptor = MockAcceptor()
    
    class MockAcceptor:
        def __init__(self):
            self.ae_title = "TEST_SCP"
    
    return MockAssociation()

def test_find_compatible_transfer_syntax():
    """Test the fixed find_compatible_transfer_syntax function."""
    print("Testing find_compatible_transfer_syntax with list handling...")
    
    # Create mock dataset
    ds = Dataset()
    ds.SOPClassUID = CTImageStorage
    ds.SOPInstanceUID = "1.2.3.4.5.6.7.8.9.10"
    
    # Add file meta with transfer syntax
    from pydicom.dataset import FileMetaDataset
    ds.file_meta = FileMetaDataset()
    ds.file_meta.TransferSyntaxUID = ImplicitVRLittleEndian
    
    # Create mock accepted contexts (with transfer_syntax as list)
    accepted_contexts = [create_mock_accepted_context()]
    
    try:
        result = find_compatible_transfer_syntax(
            dataset=ds,
            accepted_contexts=accepted_contexts,
            sop_class_uid=CTImageStorage
        )
        
        if result:
            context, reason = result
            print(f"✓ Found compatible context: ID {context.context_id}, reason: {reason}")
            print(f"✓ Context transfer syntax: {context.transfer_syntax}")
        else:
            print("✗ No compatible context found")
            return False
            
    except Exception as e:
        print(f"✗ Error in find_compatible_transfer_syntax: {e}")
        return False
    
    return True

def test_analyze_accepted_contexts():
    """Test the fixed analyze_accepted_contexts function."""
    print("\nTesting analyze_accepted_contexts with list handling...")
    
    mock_assoc = create_mock_association()
    
    try:
        analysis = analyze_accepted_contexts(mock_assoc)
        
        print(f"✓ Analysis completed: {analysis['accepted_count']} accepted contexts")
        for ctx in analysis['accepted_contexts']:
            print(f"✓ Context {ctx['context_id']}: {ctx['transfer_syntax_name']}")
            
    except Exception as e:
        print(f"✗ Error in analyze_accepted_contexts: {e}")
        return False
    
    return True

def test_handle_cstore_context_error():
    """Test the fixed handle_cstore_context_error function."""
    print("\nTesting handle_cstore_context_error with list handling...")
    
    mock_assoc = create_mock_association()
    
    try:
        error_analysis = handle_cstore_context_error(
            sop_class_uid=CTImageStorage,
            required_transfer_syntax=ImplicitVRLittleEndian,
            association=mock_assoc
        )
        
        print(f"✓ Error analysis completed for {error_analysis['sop_class_name']}")
        print(f"✓ Found {len(error_analysis['accepted_contexts_for_sop'])} accepted contexts")
        
    except Exception as e:
        print(f"✗ Error in handle_cstore_context_error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Testing Transfer Syntax List Handling Fixes")
    print("=" * 50)
    
    success = True
    success &= test_find_compatible_transfer_syntax()
    success &= test_analyze_accepted_contexts()
    success &= test_handle_cstore_context_error()
    
    if success:
        print("\n✓ All tests passed! The 'unhashable type: list' error should be fixed.")
    else:
        print("\n✗ Some tests failed. Please check the implementation.")
