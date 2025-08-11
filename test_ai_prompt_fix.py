#!/usr/bin/env python3
"""
Test script to verify the fix for get_rules_by_ai_prompt_config_id function.
This tests the JSONB query compatibility with older PostgreSQL versions.
"""

import os
import sys
import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add the app directory to Python path
sys.path.insert(0, '/app')

from app.crud.crud_rule import get_rules_by_ai_prompt_config_id
from app.db.models import Rule

# Database connection using the same environment variables as the app
DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER', 'dicom_processor_user')}:{os.getenv('POSTGRES_PASSWORD', 'changeme')}@db:5432/{os.getenv('POSTGRES_DB', 'dicom_processor_db')}"

def test_function():
    """Test the fixed get_rules_by_ai_prompt_config_id function."""
    print("Testing get_rules_by_ai_prompt_config_id function...")
    
    # Create database engine and session
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    
    try:
        # Test with a config_id that doesn't exist (should return empty list)
        print("Testing with non-existent config_id...")
        result = get_rules_by_ai_prompt_config_id(db, config_id=99999)
        print(f"✅ Function executed successfully. Found {len(result)} rules.")
        
        # Test with config_id 1 (might exist)
        print("Testing with config_id=1...")
        result = get_rules_by_ai_prompt_config_id(db, config_id=1)
        print(f"✅ Function executed successfully. Found {len(result)} rules for config_id=1.")
        
        # Check PostgreSQL version for reference
        pg_version = db.execute(text("SELECT version()")).scalar()
        print(f"PostgreSQL version: {pg_version}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
        
    finally:
        db.close()

if __name__ == "__main__":
    success = test_function()
    sys.exit(0 if success else 1)
