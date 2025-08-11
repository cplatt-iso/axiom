#!/usr/bin/env python3
"""
Script to create a test modality with bypass IP validation enabled.
This is useful for testing and containerized environments where IP addresses change.

Usage:
    python create_test_modality.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app import crud
from app.schemas.modality import ModalityCreate
from app.db.models.facility import Facility


def create_test_modality_with_bypass():
    """Create a test modality with IP bypass enabled."""
    db: Session = SessionLocal()
    
    try:
        # First, find or create a test facility
        test_facility = db.query(Facility).filter(Facility.name == "Test Facility").first()
        if not test_facility:
            print("No 'Test Facility' found. Please create a facility first.")
            return
        
        # Create the test modality
        modality_data = ModalityCreate(
            name="Test Container CT",
            description="Test modality for containerized testing with IP bypass",
            ae_title="TEST_CT_BYPASS",
            ip_address="192.168.1.100",  # This will be ignored due to bypass
            port=11112,
            modality_type="CT",
            facility_id=test_facility.id,
            is_active=True,
            is_dmwl_enabled=True,
            bypass_ip_validation=True,  # This is the magic!
            manufacturer="TEST",
            model="Container CT",
            software_version="1.0",
            station_name="TEST_CT_01",
            department="Testing",
            location="Container Lab"
        )
        
        # Check if modality already exists
        existing = crud.crud_modality.get_by_ae_title(db, ae_title="TEST_CT_BYPASS")
        if existing:
            print(f"Modality with AE title 'TEST_CT_BYPASS' already exists (ID: {existing.id})")
            print(f"  Bypass IP Validation: {existing.bypass_ip_validation}")
            return
        
        # Create the new modality
        modality = crud.crud_modality.create(db, obj_in=modality_data)
        print(f"✅ Created test modality:")
        print(f"  ID: {modality.id}")
        print(f"  AE Title: {modality.ae_title}")
        print(f"  Configured IP: {modality.ip_address}")
        print(f"  Bypass IP Validation: {modality.bypass_ip_validation}")
        print(f"  DMWL Enabled: {modality.is_dmwl_enabled}")
        print()
        print("🧪 This modality can now query DMWL from ANY IP address!")
        print("   Perfect for testing and containerized environments.")
        
    except Exception as e:
        print(f"❌ Error creating test modality: {e}")
        db.rollback()
    finally:
        db.close()


def show_bypass_modalities():
    """Show all modalities with bypass IP validation enabled."""
    db: Session = SessionLocal()
    
    try:
        # Query for modalities with bypass enabled
        modalities = db.query(Modality).filter(Modality.bypass_ip_validation == True).all()
        
        if not modalities:
            print("📋 No modalities with IP bypass validation found.")
            return
        
        print("📋 Modalities with IP Bypass Validation:")
        print("-" * 60)
        for modality in modalities:
            print(f"  AE Title: {modality.ae_title}")
            print(f"  Configured IP: {modality.ip_address}")
            print(f"  Active: {modality.is_active}")
            print(f"  DMWL Enabled: {modality.is_dmwl_enabled}")
            print(f"  Facility: {modality.facility.name if modality.facility else 'None'}")
            print("-" * 40)
            
    except Exception as e:
        print(f"❌ Error querying modalities: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    print("🏥 DICOM Modality Bypass IP Validation Setup")
    print("=" * 50)
    
    if len(sys.argv) > 1 and sys.argv[1] == "list":
        show_bypass_modalities()
    else:
        create_test_modality_with_bypass()
        print()
        show_bypass_modalities()
