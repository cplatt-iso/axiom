#!/usr/bin/env python3
"""
Test if the current Orthanc setup supports TLS properly
"""

import logging
from pynetdicom import AE  # type: ignore[attr-defined]
from pydicom import Dataset
import ssl
import tempfile
import os

# Simple TLS test
def test_orthanc_tls():
    """Test TLS connection to Orthanc"""
    
    ae = AE()
    ae.ae_title = "AXIOM_QR_SCU"
    
    # Add supported context
    from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind  # type: ignore
    ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
    
    print("Testing non-TLS connection first...")
    try:
        # Test regular connection first
        assoc = ae.associate("orthanc", 4242, ae_title="ORTHANC_TEST")
        if assoc.is_established:
            print("✅ Non-TLS connection successful")
            assoc.release()
        else:
            print("❌ Non-TLS connection failed")
    except Exception as e:
        print(f"❌ Non-TLS connection error: {e}")
    
    print("\nTesting TLS connection...")
    try:
        # Create a basic SSL context for testing
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # For testing only
        
        # Test TLS connection
        tls_args = (ssl_context, "")  # Use empty string instead of None
        assoc = ae.associate("orthanc", 4242, ae_title="ORTHANC_TEST", tls_args=tls_args)
        if assoc.is_established:
            print("✅ TLS connection successful")
            assoc.release()
        else:
            print("❌ TLS connection failed")
    except Exception as e:
        print(f"❌ TLS connection error: {e}")

if __name__ == "__main__":
    test_orthanc_tls()
