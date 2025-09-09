#!/usr/bin/env python3
"""
Test script to create evidence and verify SSE events are published.
"""

import requests
import json
import sys
from datetime import datetime

API_KEY = "9HXqH14f_IA59fdhJdDNpdLesTXz2CRVWCPfojxGs"
BASE_URL = "http://localhost:8001/api/v1"
HEADERS = {"Authorization": f"Api-Key {API_KEY}", "Content-Type": "application/json"}

def create_test_evidence():
    """Create a test evidence record directly via API to trigger SSE event."""
    
    # First, let's get an existing order to use
    orders_response = requests.get(f"{BASE_URL}/orders/?limit=1", headers=HEADERS)
    if orders_response.status_code != 200:
        print(f"Failed to get orders: {orders_response.text}")
        return False
    
    orders_data = orders_response.json()
    orders = orders_data.get("items", [])
    if not orders:
        print("No orders found to test with")
        return False
    
    order = orders[0]
    print(f"Using order ID {order['id']} with accession {order.get('accession_number')}")
    
    # Create evidence data
    evidence_data = {
        "imaging_order_id": order["id"],
        "sop_instance_uid": f"1.2.826.0.1.test.{datetime.now().timestamp()}",
        "study_instance_uid": f"1.2.826.0.1.test.study.{datetime.now().timestamp()}",
        "series_instance_uid": f"1.2.826.0.1.test.series.{datetime.now().timestamp()}",
        "accession_number": order.get("accession_number", "TEST_ACC"),
        "match_rule": "TEST_RULE",
        "processing_successful": True,
        "source_identifier": "TEST_SOURCE"
    }
    
    print("Creating evidence record...")
    print(f"Evidence data: {json.dumps(evidence_data, indent=2)}")
    
    # Create evidence
    response = requests.post(f"{BASE_URL}/order-evidence/", 
                           headers=HEADERS, 
                           json=evidence_data)
    
    if response.status_code in [200, 201]:
        evidence = response.json()
        print(f"✅ Evidence created successfully with ID: {evidence['id']}")
        print(f"   SOP Instance UID: {evidence['sop_instance_uid']}")
        print(f"   Order ID: {evidence['imaging_order_id']}")
        return True
    else:
        print(f"❌ Failed to create evidence: {response.status_code} - {response.text}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Evidence SSE Events")
    print("=" * 50)
    success = create_test_evidence()
    if success:
        print("\n✅ Test completed successfully!")
        print("📡 Check the SSE stream for order_evidence_created events")
    else:
        print("\n❌ Test failed")
        sys.exit(1)
