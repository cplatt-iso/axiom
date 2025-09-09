#!/usr/bin/env python3
"""
Test Elasticsearch integration without database dependency.

This script tests the Elasticsearch manager directly with hardcoded policies
to verify the core ILM policy management functionality.
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.elasticsearch_manager import ElasticsearchManager
from app.log_management.policy_engine import PolicyEngine
from app.log_management.models import RetentionTier


async def test_elasticsearch_management():
    """Test Elasticsearch ILM policy management directly."""
    print("🚀 Starting Elasticsearch management test...")
    
    # Initialize services
    es_manager = ElasticsearchManager()
    policy_engine = PolicyEngine()
    
    try:
        # Check Elasticsearch connection
        print("📡 Testing Elasticsearch connection...")
        health = await es_manager.health_check()
        print(f"✅ Elasticsearch health: {health}")
        
        # List current ILM policies
        print("\n📋 Current ILM policies:")
        current_policies = await es_manager.list_ilm_policy_names()
        axiom_policies = [p for p in current_policies if p.startswith('axiom-')]
        print(f"Found {len(axiom_policies)} existing axiom policies:")
        for policy in axiom_policies:
            print(f"  - {policy}")
        
        # Create a test medical imaging ILM policy
        print("\n📝 Creating test medical imaging ILM policy...")
        test_policy_name = "axiom-medical-imaging-test"
        
        # Generate a sample ILM policy for medical imaging
        test_ilm_policy = {
            "phases": {
                "hot": {
                    "actions": {
                        "rollover": {
                            "max_size": "50gb",
                            "max_age": "30d"
                        }
                    }
                },
                "warm": {
                    "min_age": "30d",
                    "actions": {
                        "allocate": {
                            "number_of_replicas": 0
                        }
                    }
                },
                "cold": {
                    "min_age": "90d",
                    "actions": {
                        "allocate": {
                            "number_of_replicas": 0
                        }
                    }
                },
                "delete": {
                    "min_age": "2557d",  # 7 years for medical imaging compliance
                    "actions": {
                        "delete": {}
                    }
                }
            }
        }
        
        result = await es_manager.create_ilm_policy(
            policy_name=test_policy_name,
            policy=test_ilm_policy
        )
        print(f"✅ Created test policy: {result}")
        
        # Create index template for the test policy
        print("\n📄 Creating test index template...")
        test_template_name = "axiom-medical-test-template"
        
        template = policy_engine.generate_index_template(
            template_name=test_template_name,
            index_pattern="axiom-medical-test-*",
            ilm_policy_name=test_policy_name
        )
        
        template_result = await es_manager.create_index_template(
            template_name=test_template_name,
            template=template
        )
        print(f"✅ Created test template: {template_result}")
        
        # List indices that match our pattern
        print("\n📊 Current indices matching axiom patterns:")
        indices_stats = await es_manager.get_indices_stats(index_pattern="axiom*")
        print(f"Found {len(indices_stats)} indices")
        
        # Show cluster statistics
        print("\n📈 Cluster statistics:")
        cluster_stats = await es_manager.get_cluster_stats()
        print(f"✅ Cluster stats: nodes={cluster_stats.get('number_of_nodes')}, "
              f"shards={cluster_stats.get('active_primary_shards')}")
        
        # List final policies to confirm our test policy exists
        print("\n🔍 Final policy list:")
        final_policies = await es_manager.list_ilm_policy_names()
        axiom_final = [p for p in final_policies if p.startswith('axiom-')]
        print(f"Found {len(axiom_final)} axiom policies:")
        for policy in axiom_final:
            print(f"  - {policy}")
        
        print("\n✨ Elasticsearch management test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error during Elasticsearch test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_elasticsearch_management())
    sys.exit(0 if success else 1)
