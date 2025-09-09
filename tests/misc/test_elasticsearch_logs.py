#!/usr/bin/env python3
"""
Quick test script for Elasticsearch log integration.
Because debugging auth issues is a pain in the ass through an API.
"""
import os
import sys
from datetime import datetime, timedelta

# Add the app to the Python path
sys.path.insert(0, '/home/icculus/axiom/backend')

from app.services.log_service import log_service, LogQueryParams


def test_elasticsearch_connection():
    """Test basic Elasticsearch connectivity."""
    print("🔍 Testing Elasticsearch connection...")
    
    health = log_service.health_check()
    print(f"Health check result: {health}")
    
    if health.get("status") == "ok":
        print("✅ Elasticsearch connection successful!")
        return True
    else:
        print("❌ Elasticsearch connection failed!")
        print(f"Error: {health.get('message', 'Unknown error')}")
        return False


def test_log_query():
    """Test querying logs."""
    print("\n📋 Testing log query...")
    
    # Query for recent logs
    params = LogQueryParams(
        start_time=datetime.utcnow() - timedelta(hours=1),
        limit=5
    )
    
    result = log_service.query_logs(params)
    
    print(f"Query result keys: {list(result.keys())}")
    print(f"Total logs found: {result.get('total', 0)}")
    print(f"Entries returned: {len(result.get('entries', []))}")
    
    if result.get('error'):
        print(f"❌ Query error: {result['error']}")
        return False
    
    # Print first entry if available
    entries = result.get('entries', [])
    if entries:
        print("\n📄 Sample log entry:")
        entry = entries[0]
        print(f"  Timestamp: {entry.get('timestamp')}")
        print(f"  Level: {entry.get('level')}")
        print(f"  Message: {entry.get('message', '')[:100]}...")
        print(f"  Container: {entry.get('container_name')}")
        print("✅ Log query successful!")
        return True
    else:
        print("⚠️  No log entries found (might be normal if no recent logs)")
        return True


def main():
    print("🚀 Testing Axiom Elasticsearch Log Integration")
    print("=" * 50)
    
    # Check if we have the required environment variables
    elastic_password = os.getenv('ELASTICSEARCH_PASSWORD') or os.getenv('ELASTIC_PASSWORD')
    if not elastic_password:
        print("⚠️  Warning: No ELASTICSEARCH_PASSWORD or ELASTIC_PASSWORD found in environment")
        print("   You may need to set this for authentication to work")
    
    # Override host for local testing (since we're not running in Docker)
    print("🔧 Overriding Elasticsearch host to localhost for local testing...")
    log_service.es_host = "localhost"
    log_service._initialize_client()  # Reinitialize with new host
    
    # Test connection
    if not test_elasticsearch_connection():
        print("\n💥 Connection test failed. Check your Elasticsearch setup.")
        return 1
    
    # Test querying
    if not test_log_query():
        print("\n💥 Log query test failed.")
        return 1
    
    print("\n🎉 All tests passed! Log integration looks good.")
    print("\nNext steps:")
    print("1. Start your API server")
    print("2. Test the endpoints:")
    print("   GET /api/v1/logs/health")
    print("   GET /api/v1/logs/recent")
    print("   GET /api/v1/logs/?limit=10")
    
    return 0


if __name__ == "__main__":
    exit(main())
