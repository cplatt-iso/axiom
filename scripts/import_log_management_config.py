#!/usr/bin/env python3
"""
Import Log Management Configuration
Restores retention policies and archival rules from YAML/JSON backup
"""

import asyncio
import yaml
import json
import sys
from pathlib import Path
from sqlalchemy.orm import Session
from app.db.database import SessionLocal
from app.crud.log_management import create_retention_policy, create_archival_rule
from app.schemas.log_management import RetentionPolicyCreate, ArchivalRuleCreate
from app.log_management.models import RetentionTier, StorageBackend, CompressionType


def import_retention_policies(db: Session, policies: list) -> int:
    """Import retention policies from config."""
    imported_count = 0
    
    for policy_data in policies:
        try:
            policy = RetentionPolicyCreate(
                name=policy_data["name"],
                description=policy_data.get("description"),
                service_pattern=policy_data["service_pattern"],
                log_level_filter=policy_data.get("log_level_filter"),
                tier=RetentionTier(policy_data["tier"]),
                hot_days=policy_data["hot_days"],
                warm_days=policy_data["warm_days"],
                cold_days=policy_data["cold_days"],
                delete_days=policy_data["delete_days"],
                max_index_size_gb=policy_data["max_index_size_gb"],
                max_index_age_days=policy_data["max_index_age_days"],
                storage_class_hot=policy_data["storage_class_hot"],
                storage_class_warm=policy_data["storage_class_warm"],
                storage_class_cold=policy_data["storage_class_cold"],
                is_active=policy_data.get("is_active", True)
            )
            
            create_retention_policy(db=db, policy=policy)
            imported_count += 1
            print(f"  ✅ Imported policy: {policy_data['name']}")
            
        except Exception as e:
            print(f"  ❌ Failed to import policy {policy_data['name']}: {e}")
    
    return imported_count


def import_archival_rules(db: Session, rules: list) -> int:
    """Import archival rules from config."""
    imported_count = 0
    
    for rule_data in rules:
        try:
            rule = ArchivalRuleCreate(
                name=rule_data["name"],
                description=rule_data.get("description"),
                source_pattern=rule_data["source_pattern"],
                archive_schedule=rule_data["archive_schedule"],
                storage_backend=StorageBackend(rule_data["storage_backend"]),
                compression_type=CompressionType(rule_data["compression_type"]),
                retention_months=rule_data["retention_months"],
                is_active=rule_data.get("is_active", True)
            )
            
            create_archival_rule(db=db, rule=rule)
            imported_count += 1
            print(f"  ✅ Imported rule: {rule_data['name']}")
            
        except Exception as e:
            print(f"  ❌ Failed to import rule {rule_data['name']}: {e}")
    
    return imported_count


def main():
    """Import log management configuration from file."""
    if len(sys.argv) != 2:
        print("Usage: python import_log_management_config.py <config_file>")
        print("Config file can be YAML or JSON format")
        sys.exit(1)
    
    config_file = Path(sys.argv[1])
    if not config_file.exists():
        print(f"❌ Config file not found: {config_file}")
        sys.exit(1)
    
    # Load configuration
    with open(config_file, 'r') as f:
        if config_file.suffix.lower() in ['.yaml', '.yml']:
            config = yaml.safe_load(f)
        else:
            config = json.load(f)
    
    db = SessionLocal()
    
    try:
        # Import retention policies
        if "retention_policies" in config:
            print(f"📥 Importing {len(config['retention_policies'])} retention policies...")
            imported_policies = import_retention_policies(db, config["retention_policies"])
            print(f"✅ Successfully imported {imported_policies} retention policies")
        
        # Import archival rules  
        if "archival_rules" in config:
            print(f"📥 Importing {len(config['archival_rules'])} archival rules...")
            imported_rules = import_archival_rules(db, config["archival_rules"])
            print(f"✅ Successfully imported {imported_rules} archival rules")
        
        print("\n🎉 Configuration import completed!")
        print("🔄 Run sync-policies and apply-templates to activate in Elasticsearch")
        
    finally:
        db.close()


if __name__ == "__main__":
    main()
