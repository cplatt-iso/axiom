#!/usr/bin/env python3
"""
Debug script to see the raw Elasticsearch log structure.
"""
import os
import sys
from datetime import datetime, timedelta
import json

# Add the app to the Python path
sys.path.insert(0, '/home/icculus/axiom/backend')

from app.services.log_service import log_service, LogQueryParams


def examine_log_structure():
    """Look at raw log entries to understand the structure."""
    print("🔍 Examining raw log structure...")
    
    # Override for local testing
    log_service.es_host = "localhost"
    log_service._initialize_client()
    
    # Query recent logs
    params = LogQueryParams(
        start_time=datetime.utcnow() - timedelta(minutes=30),
        limit=3
    )
    
    result = log_service.query_logs(params)
    
    if result.get('entries'):
        print(f"\n📄 Found {len(result['entries'])} entries. Raw structure of first entry:")
        print(json.dumps(result['entries'][0], indent=2, default=str))
        
        print(f"\n📄 Raw structure of second entry:")
        print(json.dumps(result['entries'][1], indent=2, default=str))
    else:
        print("No entries found")


if __name__ == "__main__":
    os.environ['ELASTICSEARCH_PASSWORD'] = "furt-murt-dirt-dink"
    examine_log_structure()
