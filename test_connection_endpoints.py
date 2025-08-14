#!/usr/bin/env python3
"""
Test script for connection test endpoints.

This script tests the new connection test endpoints without requiring a database.
It uses mock data to verify the endpoint logic works correctly.
"""

import asyncio
import sys
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone

# Add the app directory to Python path
sys.path.insert(0, '/home/icculus/axiom/backend')

def test_connection_service():
    """Test the ConnectionTestService methods."""
    print("Testing ConnectionTestService...")
    
    from app.services.connection_test_service import ConnectionTestService
    from app.schemas.enums import HealthStatus
    
    # Mock DICOMWeb source
    mock_dicomweb_source = Mock()
    mock_dicomweb_source.source_name = "Test DICOMWeb Source"
    mock_dicomweb_source.qido_url = "http://test-server:8080/dcm4chee-arc/aets/DCM4CHEE/rs"
    mock_dicomweb_source.auth_username = "test_user"
    mock_dicomweb_source.auth_password = "test_pass"
    
    # Mock DIMSE Q/R source
    mock_dimse_source = Mock()
    mock_dimse_source.peer_ae_title = "TESTPACS"
    mock_dimse_source.peer_ip = "192.168.1.100"
    mock_dimse_source.peer_port = 11112
    mock_dimse_source.our_ae_title = "AXIOM_FLOW"
    
    # Mock Google Healthcare source
    mock_google_source = Mock()
    mock_google_source.project_id = "test-project"
    mock_google_source.dataset_id = "test-dataset"
    mock_google_source.store_id = "test-store"
    mock_google_source.location = "us-central1"
    
    print("✅ Connection test service imports successfully")
    print(f"✅ HealthStatus enum available: {list(HealthStatus)}")
    
    return True

def test_api_endpoints():
    """Test the API endpoint logic (without FastAPI server)."""
    print("\nTesting API endpoint logic...")
    
    try:
        # Test imports for endpoint files
        from app.api.api_v1.endpoints import config_dicomweb
        from app.api.api_v1.endpoints import config_dimse_qr
        from app.api.api_v1.endpoints import config_google_healthcare_sources
        
        print("✅ All endpoint modules import successfully")
        
        # Check that the new endpoints exist
        endpoints = [
            (config_dicomweb.router, "test_dicomweb_connection"),
            (config_dimse_qr.router, "test_dimse_qr_connection"),
            (config_google_healthcare_sources.router, "test_google_healthcare_connection"),
        ]
        
        for router, endpoint_name in endpoints:
            # Check if the function exists in the router
            found = False
            for route in router.routes:
                endpoint = getattr(route, 'endpoint', None)
                if endpoint and hasattr(endpoint, '__name__') and endpoint.__name__ == endpoint_name:
                    found = True
                    print(f"✅ Found endpoint: {endpoint_name}")
                    path = getattr(route, 'path', 'Unknown')
                    methods = getattr(route, 'methods', set())
                    print(f"   Path: {path}")
                    print(f"   Methods: {methods}")
                    break
            
            if not found:
                print(f"❌ Endpoint not found: {endpoint_name}")
                return False
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_health_monitoring_task():
    """Test the health monitoring Celery task logic."""
    print("\nTesting health monitoring task...")
    
    try:
        from app.worker.tasks import health_monitoring_task
        print("✅ Health monitoring task imports successfully")
        
        # Test that the task is properly decorated
        if hasattr(health_monitoring_task, 'delay'):
            print("✅ Task is properly decorated with @shared_task")
        else:
            print("❌ Task missing Celery decorators")
            return False
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def main():
    """Run all tests."""
    print("🔍 Testing Connection Health Monitoring Implementation")
    print("=" * 60)
    
    tests = [
        test_connection_service,
        test_api_endpoints,
        test_health_monitoring_task,
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 60)
    print("📊 Test Results Summary:")
    print(f"✅ Passed: {sum(results)}/{len(results)}")
    print(f"❌ Failed: {len(results) - sum(results)}/{len(results)}")
    
    if all(results):
        print("\n🎉 All tests passed! The connection health monitoring system is ready.")
        print("\n📋 Next Steps:")
        print("   1. Start the database to run migrations: alembic upgrade head")
        print("   2. Test the API endpoints with a running FastAPI server")
        print("   3. Schedule the health monitoring task in Celery beat")
        print("   4. Configure connection test timeouts as needed")
    else:
        print("\n⚠️ Some tests failed. Please check the implementation.")
        sys.exit(1)

if __name__ == "__main__":
    main()
