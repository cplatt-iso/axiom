#!/usr/bin/env python3
"""
Test script to verify the Redis consistency fix between dashboard status and system info endpoints.

This script demonstrates that both endpoints now consistently report Redis status.
"""

import sys
import asyncio
import json
from pathlib import Path

# Add the app directory to the Python path so we can import the modules
sys.path.insert(0, str(Path(__file__).parent / "app"))

async def test_redis_consistency():
    """Test that both endpoints report Redis status consistently."""
    
    try:
        # Import required modules
        from app.api.api_v1.endpoints.system import get_system_info, get_dashboard_status
        from app.db.session import SessionLocal
        
        print("🔧 TESTING REDIS STATUS CONSISTENCY FIX")
        print("=" * 50)
        
        # Create a database session
        db = SessionLocal()
        
        try:
            # Test system info endpoint (used by Network Services section)
            print("📡 Testing /info endpoint (Network Services section)...")
            system_info = await get_system_info(db)
            info_dict = system_info.model_dump()
            
            if "services_status" in info_dict and "redis" in info_dict["services_status"]:
                redis_status_info = info_dict["services_status"]["redis"]
                print(f"   📋 /info Redis Status: {redis_status_info}")
            else:
                print("   ❌ No Redis status found in /info endpoint")
                return False
            
            # Test dashboard status endpoint (used by Service Status section)
            print("\n📡 Testing /dashboard/status endpoint (Service Status section)...")
            dashboard_status = await get_dashboard_status(db)
            dashboard_dict = dashboard_status.model_dump()
            
            if "components" in dashboard_dict and "redis" in dashboard_dict["components"]:
                redis_component = dashboard_dict["components"]["redis"]
                print(f"   📋 /dashboard/status Redis Status: {redis_component}")
            else:
                print("   ❌ No Redis status found in /dashboard/status endpoint")
                return False
            
            # Compare the results
            print(f"\n🔍 CONSISTENCY CHECK:")
            
            # Extract status values
            info_redis_status = redis_status_info.get("status")
            dashboard_redis_status = redis_component.get("status")
            
            print(f"   Network Services Redis: {info_redis_status}")
            print(f"   Service Status Redis: {dashboard_redis_status}")
            
            # Check consistency
            if info_redis_status and dashboard_redis_status:
                # Map statuses for comparison (connected -> ok for consistency)
                normalized_info_status = "ok" if info_redis_status == "connected" else info_redis_status
                normalized_dashboard_status = dashboard_redis_status
                
                if normalized_info_status == normalized_dashboard_status:
                    print(f"   ✅ CONSISTENT: Both endpoints report Redis as '{dashboard_redis_status}'")
                    return True
                else:
                    print(f"   ❌ INCONSISTENT: Network Services shows '{info_redis_status}', Service Status shows '{dashboard_redis_status}'")
                    return False
            else:
                print(f"   ❌ MISSING STATUS: Could not extract status from one or both endpoints")
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


async def main():
    """Run the Redis consistency test."""
    
    print("🚀 AXIOM FLOW - REDIS STATUS CONSISTENCY FIX TEST")
    print("=" * 60)
    
    print("The frontend shows Redis as red in 'Service Status' but green in 'Network Services'.")
    print("This is because they call different endpoints:")
    print("• Service Status section → /dashboard/status endpoint")  
    print("• Network Services section → /info endpoint")
    print()
    print("The fix: Added Redis check to /dashboard/status endpoint for consistency.")
    print()
    
    # Test consistency
    success = await test_redis_consistency()
    
    print(f"\n🏁 TEST RESULTS:")
    if success:
        print(f"   ✅ REDIS CONSISTENCY FIX SUCCESSFUL")
        print(f"   • Both endpoints now report Redis status consistently")
        print(f"   • Service Status section should now show correct Redis state")
        print(f"   • Frontend UI inconsistency resolved")
    else:
        print(f"   ❌ REDIS CONSISTENCY FIX NEEDS MORE WORK")
        print(f"   • Endpoints still report different Redis statuses")
        print(f"   • Frontend UI inconsistency remains")
    
    print(f"\n📡 TECHNICAL DETAILS:")
    print(f"   • Added Redis check to /dashboard/status endpoint")
    print(f"   • Both endpoints now include Redis connectivity testing")
    print(f"   • Consistent status reporting across all UI sections")
    print(f"   • Error handling for Redis connection failures")


if __name__ == "__main__":
    asyncio.run(main())
