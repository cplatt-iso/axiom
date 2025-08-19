#!/usr/bin/env python3
"""
Direct test of evidence creation with SSE events using internal methods.
"""

import sys
import os
from datetime import datetime

# Add the app directory to the path
sys.path.insert(0, '/home/icculus/axiom/backend')

def test_evidence_creation():
    """Test evidence creation with SSE events using internal CRUD methods."""
    
    try:
        from app.db.session import SessionLocal
        from app.crud.crud_order_dicom_evidence import crud_order_dicom_evidence
        from app.crud.crud_imaging_order import imaging_order
        
        print("✅ Successfully imported modules")
        
        # Create database session
        db = SessionLocal()
        
        try:
            # Get an existing order
            order = db.query(imaging_order.model).first()
            if not order:
                print("❌ No orders found in database")
                return False
                
            print(f"✅ Found order ID: {order.id}, Accession: {order.accession_number}")
            
            # Create evidence with SSE events
            timestamp = datetime.now().timestamp()
            evidence = crud_order_dicom_evidence.create_or_update_evidence_with_events(
                db=db,
                imaging_order_id=order.id,
                sop_instance_uid=f"1.2.826.0.1.test.{timestamp}",
                study_instance_uid=f"1.2.826.0.1.test.study.{timestamp}",
                series_instance_uid=f"1.2.826.0.1.test.series.{timestamp}",
                accession_number=order.accession_number,
                match_rule="TEST_SSE_RULE",
                processing_successful=True,
                source_identifier="TEST_SSE_SOURCE"
            )
            
            print(f"✅ Evidence created successfully!")
            print(f"   Evidence ID: {evidence.id}")
            print(f"   SOP Instance UID: {evidence.sop_instance_uid}")
            print(f"   Match Rule: {evidence.match_rule}")
            print(f"   Order ID: {evidence.imaging_order_id}")
            
            # Commit the transaction
            db.commit()
            
            print("📡 SSE event should have been published!")
            return True
            
        except Exception as e:
            print(f"❌ Error creating evidence: {e}")
            import traceback
            traceback.print_exc()
            db.rollback()
            return False
        finally:
            db.close()
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Direct Evidence SSE Event Test")
    print("=" * 50)
    success = test_evidence_creation()
    if success:
        print("\n✅ Evidence created with SSE events!")
        print("🔍 Check the SSE monitor for 'order_evidence_created' events")
    else:
        print("\n❌ Test failed")
        sys.exit(1)
