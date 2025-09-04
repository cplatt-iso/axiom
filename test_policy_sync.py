#!/usr/bin/env python3
"""
Test script to synchronize retention policies to Elasticsearch.

This script tests the core functionality of our log management system
by syncing database policies to Elasticsearch ILM policies.
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.crud import log_management as crud
from app.services.elasticsearch_manager import ElasticsearchManager
from app.log_management.policy_engine import PolicyEngine


async def test_sync_policies():
    """Test syncing retention policies to Elasticsearch."""
    print("🚀 Starting policy synchronization test...")
    
    # Initialize services
    es_manager = ElasticsearchManager()
    policy_engine = PolicyEngine()
    
    # Get database session
    db = SessionLocal()
    
    try:
        # Check Elasticsearch connection
        print("📡 Testing Elasticsearch connection...")
        health = await es_manager.health_check()
        print(f"✅ Elasticsearch health: {health}")
        
        # Get active policies from database
        print("📊 Fetching active retention policies from database...")
        policies = crud.get_retention_policies(db=db, active_only=True)
        print(f"✅ Found {len(policies)} active policies:")
        
        for policy in policies:
            print(f"  - {policy.name} ({policy.tier.value}): {policy.service_pattern}")
        
        # Sync policies to Elasticsearch
        print("\n🔄 Syncing policies to Elasticsearch...")
        synced_policies = []
        
        for policy in policies:
            policy_name = f"axiom-{policy.name.lower().replace(' ', '-')}"
            ilm_policy = policy_engine.generate_ilm_policy_from_db(policy)
            
            print(f"  📝 Creating ILM policy: {policy_name}")
            result = await es_manager.create_ilm_policy(
                policy_name=policy_name,
                policy=ilm_policy
            )
            
            synced_policies.append({
                "database_id": policy.id,
                "database_name": policy.name,
                "elasticsearch_policy_name": policy_name,
                "tier": policy.tier.value,
                "result": result
            })
            print(f"  ✅ Created: {policy_name}")
        
        # List all ILM policies in Elasticsearch
        print("\n📋 Listing all ILM policies in Elasticsearch...")
        all_policies = await es_manager.list_ilm_policies()
        axiom_policies = [p for p in all_policies if p.startswith('axiom-')]
        print(f"✅ Found {len(axiom_policies)} axiom policies:")
        for policy in axiom_policies:
            print(f"  - {policy}")
        
        print(f"\n✨ Successfully synchronized {len(synced_policies)} policies!")
        return synced_policies
        
    except Exception as e:
        print(f"❌ Error during policy sync: {str(e)}")
        import traceback
        traceback.print_exc()
        return []
    
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(test_sync_policies())
