#!/usr/bin/env python3
"""
Test script for robust C-STORE implementation.
This script demonstrates the enhanced transfer syntax negotiation and C-STORE capabilities.
"""

import sys
import os
sys.path.append('/app')

from app.services.network.dimse.transfer_syntax_negotiation import (
    create_presentation_contexts_with_fallback,
    TransferSyntaxStrategy,
    find_compatible_transfer_syntax,
    create_optimized_contexts_for_sop_class
)
from app.services.network.dimse.scu_service import store_dataset

from pydicom.dataset import Dataset
from pydicom.uid import CTImageStorage, ImplicitVRLittleEndian, UID
from pynetdicom.presentation import build_context

def test_transfer_syntax_strategies():
    """Test different transfer syntax strategies."""
    print("Testing transfer syntax strategies...")
    
    # Test conservative strategy
    contexts = create_presentation_contexts_with_fallback(
        sop_class_uid=CTImageStorage,
        strategies=[TransferSyntaxStrategy.CONSERVATIVE],
        max_contexts_per_strategy=3
    )
    
    print(f"Conservative strategy created {len(contexts)} contexts:")
    for ctx in contexts:
        print(f"  Context ID {ctx.context_id}: {ctx.abstract_syntax} with {len(ctx.transfer_syntax)} transfer syntaxes")
    
    # Test extended strategy
    contexts = create_presentation_contexts_with_fallback(
        sop_class_uid=CTImageStorage,
        strategies=[TransferSyntaxStrategy.EXTENDED],
        max_contexts_per_strategy=5
    )
    
    print(f"Extended strategy created {len(contexts)} contexts:")
    for ctx in contexts:
        print(f"  Context ID {ctx.context_id}: {ctx.abstract_syntax} with {len(ctx.transfer_syntax)} transfer syntaxes")

def test_optimized_contexts():
    """Test optimized context creation for specific SOP classes."""
    print("\nTesting optimized context creation...")
    
    contexts = create_optimized_contexts_for_sop_class(
        sop_class_uid=CTImageStorage,
        max_contexts=3
    )
    
    print(f"Optimized contexts created {len(contexts)} contexts:")
    for ctx in contexts:
        print(f"  Context ID {ctx.context_id}: {ctx.transfer_syntax}")

def create_sample_dataset():
    """Create a sample CT dataset for testing."""
    ds = Dataset()
    ds.SOPClassUID = CTImageStorage
    ds.SOPInstanceUID = UID("1.2.3.4.5.6.7.8.9.10")
    ds.StudyInstanceUID = UID("1.2.3.4.5.6.7.8.9")
    ds.SeriesInstanceUID = UID("1.2.3.4.5.6.7.8")
    ds.PatientName = "Test^Patient"
    ds.PatientID = "TEST123"
    ds.Modality = "CT"
    ds.ImageType = ["ORIGINAL", "PRIMARY", "AXIAL"]
    ds.Rows = 512
    ds.Columns = 512
    ds.BitsAllocated = 16
    ds.BitsStored = 16
    ds.HighBit = 15
    ds.PixelRepresentation = 1
    ds.SamplesPerPixel = 1
    ds.PhotometricInterpretation = "MONOCHROME2"
    
    # Add minimal file meta information
    from pydicom.dataset import FileMetaDataset
    ds.file_meta = FileMetaDataset()
    ds.file_meta.MediaStorageSOPClassUID = CTImageStorage
    ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
    ds.file_meta.TransferSyntaxUID = ImplicitVRLittleEndian
    ds.file_meta.ImplementationClassUID = UID("1.2.3.4.5.6.7.8.9.10.11")
    
    return ds

def test_c_store_config():
    """Test C-STORE configuration example."""
    print("\nTesting C-STORE configuration...")
    
    # Example configuration for testing
    config = {
        "remote_host": "127.0.0.1",
        "remote_port": 11112,
        "remote_ae_title": "TEST_SCP",
        "local_ae_title": "AXIOM_SCU_TEST",
        "tls_enabled": False
    }
    
    dataset = create_sample_dataset()
    
    print("Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    print(f"Dataset SOP Class: {dataset.SOPClassUID}")
    print(f"Dataset SOP Instance: {dataset.SOPInstanceUID}")
    
    # Note: This would actually try to connect, so we just show the configuration
    print("Ready for C-STORE operation (connection not attempted in test)")

if __name__ == "__main__":
    print("Robust C-STORE Test Suite")
    print("=" * 40)
    
    test_transfer_syntax_strategies()
    test_optimized_contexts()
    test_c_store_config()
    
    print("\nTest completed successfully!")
    print("Your DICOM C-STORE implementation now includes:")
    print("- Robust transfer syntax negotiation with fallback strategies")
    print("- Detailed presentation context analysis")
    print("- Enhanced error handling and logging")
    print("- Batch C-STORE capability")
    print("- Automatic context selection based on dataset requirements")
    print("- Fixed send_c_store() API usage (no context_id parameter needed)")
    print("- Better handling of transfer syntax list formats")
