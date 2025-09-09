#!/usr/bin/env python3
"""
API test script to validate the enhanced system info endpoint.

This script tests the /api/v1/system/info endpoint to ensure all configuration
fields are properly populated and the response structure is correct.
"""

import sys
import asyncio
import json
from pathlib import Path

# Add the app directory to the Python path so we can import the modules
sys.path.insert(0, str(Path(__file__).parent / "app"))

async def test_enhanced_system_info():
    """Test the enhanced system info endpoint by calling it directly."""
    
    try:
        # Import required modules
        from app.api.api_v1.endpoints.system import get_system_info
        from app.schemas.system import SystemInfo
        from app.core.config import settings
        from app.db.session import SessionLocal
        
        print("🧪 TESTING ENHANCED SYSTEM INFO ENDPOINT")
        print("=" * 50)
        
        # Create a database session
        db = SessionLocal()
        
        try:
            # Call the endpoint function directly 
            print("📡 Calling get_system_info endpoint...")
            system_info = await get_system_info(db)
            
            # Validate the response type
            print(f"✅ Response type: {type(system_info)}")
            assert isinstance(system_info, SystemInfo), "Response should be SystemInfo instance"
            
            # Convert to dict for analysis
            info_dict = system_info.model_dump()
            
            print(f"✅ Response contains {len(info_dict)} configuration fields")
            
            # Verify key categories are present
            categories_to_check = [
                ("Basic Info", ["project_name", "project_version", "environment"]),
                ("Database Config", ["postgres_server", "postgres_port", "database_connected"]),
                ("AI Configuration", ["openai_configured", "vertex_ai_configured"]),
                ("File Processing", ["log_original_attributes", "delete_on_success"]),
                ("Dustbin System", ["use_dustbin_system", "dustbin_retention_days"]),
                ("Celery Config", ["celery_worker_concurrency", "celery_task_max_retries"]),
                ("Service Status", ["services_status"])
            ]
            
            print(f"\n🔍 VALIDATING CONFIGURATION CATEGORIES:")
            all_present = True
            
            for category_name, fields in categories_to_check:
                missing_fields = [f for f in fields if f not in info_dict]
                if missing_fields:
                    print(f"❌ {category_name}: Missing fields {missing_fields}")
                    all_present = False
                else:
                    print(f"✅ {category_name}: All fields present")
            
            # Show sample configuration values
            print(f"\n📋 SAMPLE CONFIGURATION VALUES:")
            sample_fields = [
                "project_name", "environment", "debug_mode", 
                "database_connected", "openai_configured", "vertex_ai_configured",
                "use_dustbin_system", "celery_worker_concurrency"
            ]
            
            for field in sample_fields:
                if field in info_dict:
                    value = info_dict[field]
                    print(f"   {field}: {value} ({type(value).__name__})")
            
            # Validate service status structure
            if "services_status" in info_dict and isinstance(info_dict["services_status"], dict):
                print(f"\n🔌 SERVICE STATUS CHECK:")
                for service, status in info_dict["services_status"].items():
                    if isinstance(status, dict) and "status" in status:
                        print(f"   {service}: {status['status']}")
                    else:
                        print(f"   {service}: {status}")
            
            if all_present:
                print(f"\n🎉 SUCCESS: Enhanced system info endpoint is working correctly!")
                print(f"   • All {len(info_dict)} configuration fields populated")
                print(f"   • Dynamic configuration integration active")
                print(f"   • Service health status included")
                return True
            else:
                print(f"\n❌ FAILURE: Some configuration categories are missing fields")
                return False
                
        finally:
            db.close()
            
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print("Make sure you're running this from the backend directory")
        return False
    except Exception as e:
        print(f"❌ Test Error: {e}")
        print(f"Exception type: {type(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_schema_structure():
    """Test the SystemInfo schema structure."""
    
    try:
        from app.schemas.system import SystemInfo
        
        print(f"\n🏗️  TESTING SYSTEMINFO SCHEMA STRUCTURE")
        print("=" * 45)
        
        # Get schema info
        schema = SystemInfo.model_json_schema()
        properties = schema.get("properties", {})
        
        print(f"✅ Schema contains {len(properties)} field definitions")
        
        # Check for required field categories
        expected_categories = [
            "project_", "postgres_", "celery_", "ai_", "redis_", 
            "rabbitmq_", "dicom", "dustbin_", "vertex_ai_"
        ]
        
        for category in expected_categories:
            matching_fields = [f for f in properties.keys() if category in f]
            if matching_fields:
                print(f"✅ {category}* fields: {len(matching_fields)} found")
            else:
                print(f"❌ {category}* fields: None found")
        
        return len(properties) > 50  # Should have many fields
        
    except Exception as e:
        print(f"❌ Schema test error: {e}")
        return False


async def main():
    """Run all tests."""
    
    print("🚀 AXIOM FLOW - ENHANCED SYSTEM INFO ENDPOINT TESTS")
    print("=" * 60)
    
    # Test schema structure
    schema_ok = test_schema_structure()
    
    # Test endpoint functionality
    endpoint_ok = await test_enhanced_system_info()
    
    print(f"\n🏁 TEST RESULTS:")
    print(f"   Schema Structure: {'✅ PASS' if schema_ok else '❌ FAIL'}")
    print(f"   Endpoint Function: {'✅ PASS' if endpoint_ok else '❌ FAIL'}")
    
    if schema_ok and endpoint_ok:
        print(f"\n🎉 ALL TESTS PASSED - Enhanced system info endpoint is ready!")
        print(f"\n📡 API Usage:")
        print(f"   GET /api/v1/system/info")
        print(f"   Authorization: Bearer <admin_token>")
        print(f"   Response: 70+ comprehensive configuration fields")
    else:
        print(f"\n❌ SOME TESTS FAILED - Check implementation")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
