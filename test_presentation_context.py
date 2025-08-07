#!/usr/bin/env python3

"""
Test script for presentation context creation
"""

try:
    print("Starting presentation context test...")
    
    from pynetdicom.presentation import PresentationContext
    from pydicom import uid
    print("Imports successful")
    
    # Test direct creation
    context = PresentationContext()
    context.context_id = 1
    context.abstract_syntax = uid.CTImageStorage  
    context.transfer_syntax = [uid.ImplicitVRLittleEndian]
    
    print(f"Context created: ID={context.context_id}, SOP={context.abstract_syntax}")
    print("Test successful!")
    
except Exception as e:
    import traceback
    print(f"Error: {e}")
    traceback.print_exc()
