#!/usr/bin/env python3
"""
Validation script to test the multi-source spanner schema without database connection.
This tests our schema validation logic and CRUD create method logic.
"""

import sys
import os
sys.path.append('/home/icculus/axiom/backend')

from app.schemas import spanner as schemas_spanner
from pydantic import ValidationError

def test_schema_validation():
    """Test the updated SpannerSourceMappingCreate schema validation."""
    print("Testing SpannerSourceMappingCreate schema validation...")
    
    # Test 1: Valid DIMSE source
    try:
        dimse_data = {
            "priority": 50,
            "is_enabled": True,
            "weight": 1,
            "enable_failover": True,
            "max_retries": 2,
            "retry_delay_seconds": 5,
            "source_type": "dimse-qr",
            "dimse_qr_source_id": 1,
            "dicomweb_source_id": None,
            "google_healthcare_source_id": None
        }
        dimse_obj = schemas_spanner.SpannerSourceMappingCreate(**dimse_data)
        print("✅ DIMSE source validation: PASSED")
        print(f"   Source type: {dimse_obj.source_type}")
        print(f"   DIMSE ID: {dimse_obj.dimse_qr_source_id}")
    except ValidationError as e:
        print(f"❌ DIMSE source validation: FAILED - {e}")
    
    # Test 2: Valid DICOMweb source
    try:
        dicomweb_data = {
            "priority": 60,
            "is_enabled": True,
            "weight": 1,
            "enable_failover": True,
            "max_retries": 2,
            "retry_delay_seconds": 5,
            "source_type": "dicomweb",
            "dimse_qr_source_id": None,
            "dicomweb_source_id": 1,
            "google_healthcare_source_id": None
        }
        dicomweb_obj = schemas_spanner.SpannerSourceMappingCreate(**dicomweb_data)
        print("✅ DICOMweb source validation: PASSED")
        print(f"   Source type: {dicomweb_obj.source_type}")
        print(f"   DICOMweb ID: {dicomweb_obj.dicomweb_source_id}")
    except ValidationError as e:
        print(f"❌ DICOMweb source validation: FAILED - {e}")
    
    # Test 3: Valid Google Healthcare source
    try:
        ghc_data = {
            "priority": 70,
            "is_enabled": True,
            "weight": 1,
            "enable_failover": True,
            "max_retries": 2,
            "retry_delay_seconds": 5,
            "source_type": "google_healthcare",
            "dimse_qr_source_id": None,
            "dicomweb_source_id": None,
            "google_healthcare_source_id": 1
        }
        ghc_obj = schemas_spanner.SpannerSourceMappingCreate(**ghc_data)
        print("✅ Google Healthcare source validation: PASSED")
        print(f"   Source type: {ghc_obj.source_type}")
        print(f"   Google Healthcare ID: {ghc_obj.google_healthcare_source_id}")
    except ValidationError as e:
        print(f"❌ Google Healthcare source validation: FAILED - {e}")
    
    # Test 4: Invalid - multiple sources specified
    try:
        invalid_data = {
            "priority": 80,
            "is_enabled": True,
            "weight": 1,
            "enable_failover": True,
            "max_retries": 2,
            "retry_delay_seconds": 5,
            "source_type": "dimse-qr",
            "dimse_qr_source_id": 1,
            "dicomweb_source_id": 1,  # This should cause validation error
            "google_healthcare_source_id": None
        }
        invalid_obj = schemas_spanner.SpannerSourceMappingCreate(**invalid_data)
        print("❌ Multiple sources validation: FAILED (should have been rejected)")
    except ValidationError as e:
        print("✅ Multiple sources validation: PASSED (correctly rejected)")
        print(f"   Error: {e}")
    
    # Test 5: Invalid - no source specified 
    try:
        no_source_data = {
            "priority": 90,
            "is_enabled": True,
            "weight": 1,
            "enable_failover": True,
            "max_retries": 2,
            "retry_delay_seconds": 5,
            "source_type": "dimse-qr",
            "dimse_qr_source_id": None,
            "dicomweb_source_id": None,
            "google_healthcare_source_id": None
        }
        no_source_obj = schemas_spanner.SpannerSourceMappingCreate(**no_source_data)
        print("❌ No source validation: FAILED (should have been rejected)")
    except ValidationError as e:
        print("✅ No source validation: PASSED (correctly rejected)")
        print(f"   Error: {e}")

def test_migration_sql():
    """Check if our migration SQL looks correct."""
    print("\nTesting migration SQL structure...")
    
    # Read the migration file
    migration_file = "/home/icculus/axiom/backend/alembic/versions/a11c63999d7d_add_multi_source_support_to_spanner_.py"
    try:
        with open(migration_file, 'r') as f:
            content = f.read()
        
        # Check for key components
        checks = [
            ("source_type column", "add_column('spanner_source_mappings', sa.Column('source_type'"),
            ("dicomweb_source_id column", "add_column('spanner_source_mappings', sa.Column('dicomweb_source_id'"),
            ("google_healthcare_source_id column", "add_column('spanner_source_mappings', sa.Column('google_healthcare_source_id'"),
            ("constraint updates", "drop_constraint"),
            ("new constraints", "create_check_constraint"),
            ("foreign keys", "create_foreign_key")
        ]
        
        for check_name, check_pattern in checks:
            if check_pattern in content:
                print(f"✅ {check_name}: FOUND")
            else:
                print(f"❌ {check_name}: NOT FOUND")
                
    except FileNotFoundError:
        print("❌ Migration file not found")

if __name__ == "__main__":
    print("=== Multi-Source Spanner Validation ===")
    test_schema_validation()
    test_migration_sql()
    print("\n=== Validation Complete ===")
