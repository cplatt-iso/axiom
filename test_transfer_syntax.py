#!/usr/bin/env python3

"""
Test script for transfer syntax negotiation
"""

import sys
sys.path.append('/home/icculus/axiom/backend')

try:
    print("Testing transfer syntax negotiation...")
    
    from app.services.network.dimse.transfer_syntax_negotiation import (
        create_presentation_contexts_with_fallback,
        CONSERVATIVE_TRANSFER_SYNTAXES
    )
    from pydicom import uid
    
    print("Imports successful")
    print("Conservative transfer syntaxes:", CONSERVATIVE_TRANSFER_SYNTAXES)
    
    # Test the function
    contexts = create_presentation_contexts_with_fallback(
        sop_class_uid=uid.CTImageStorage,
        strategies=['conservative'], 
        max_contexts_per_strategy=3
    )
    
    print(f"Created {len(contexts)} presentation contexts:")
    for ctx in contexts:
        print(f"  Context ID: {ctx.context_id}, SOP: {ctx.abstract_syntax}, TS: {ctx.transfer_syntax}")
        
    print("Test successful!")
    
except Exception as e:
    import traceback
    print(f"Error: {e}")
    traceback.print_exc()
