#!/usr/bin/env python3
"""
Export Log Management Configuration
Backs up retention policies and archival rules to YAML for version control
"""

import asyncio
import yaml
import json
from datetime import datetime
from pathlib import Path
from sqlalchemy.orm import Session
from app.db.database import SessionLocal
from app.crud.log_management import get_retention_policies, get_archival_rules


def export_retention_policies(db: Session) -> dict:
    """Export all retention policies to dict format."""
    policies = get_retention_policies(db=db, active_only=False)
    
    exported_policies = []
    for policy in policies:
        exported_policies.append({
            "name": policy.name,
            "description": policy.description,
            "service_pattern": policy.service_pattern,
            "log_level_filter": policy.log_level_filter,
            "tier": policy.tier.value,
            "hot_days": policy.hot_days,
            "warm_days": policy.warm_days,
            "cold_days": policy.cold_days,
            "delete_days": policy.delete_days,
            "max_index_size_gb": policy.max_index_size_gb,
            "max_index_age_days": policy.max_index_age_days,
            "storage_class_hot": policy.storage_class_hot,
            "storage_class_warm": policy.storage_class_warm,
            "storage_class_cold": policy.storage_class_cold,
            "is_active": policy.is_active
        })
    
    return {
        "version": "1.0.0",
        "exported_at": datetime.utcnow().isoformat(),
        "retention_policies": exported_policies
    }


def export_archival_rules(db: Session) -> dict:
    """Export all archival rules to dict format."""
    rules = get_archival_rules(db=db)
    
    exported_rules = []
    for rule in rules:
        exported_rules.append({
            "name": rule.name,
            "description": rule.description,
            "source_pattern": rule.source_pattern,
            "archive_schedule": rule.archive_schedule,
            "storage_backend": rule.storage_backend.value,
            "compression_type": rule.compression_type.value,
            "retention_months": rule.retention_months,
            "is_active": rule.is_active
        })
    
    return {
        "version": "1.0.0", 
        "exported_at": datetime.utcnow().isoformat(),
        "archival_rules": exported_rules
    }


def main():
    """Export all log management configuration."""
    db = SessionLocal()
    
    try:
        # Export retention policies
        policies_config = export_retention_policies(db)
        policies_file = Path("config/log_management_policies.yaml")
        policies_file.parent.mkdir(exist_ok=True)
        
        with open(policies_file, 'w') as f:
            yaml.dump(policies_config, f, default_flow_style=False, sort_keys=False)
        
        print(f"✅ Exported {len(policies_config['retention_policies'])} retention policies to {policies_file}")
        
        # Export archival rules
        rules_config = export_archival_rules(db)
        rules_file = Path("config/log_management_archival.yaml")
        
        with open(rules_file, 'w') as f:
            yaml.dump(rules_config, f, default_flow_style=False, sort_keys=False)
        
        print(f"✅ Exported {len(rules_config['archival_rules'])} archival rules to {rules_file}")
        
        # Export complete config as JSON for API import
        complete_config = {
            "retention_policies": policies_config["retention_policies"],
            "archival_rules": rules_config["archival_rules"]
        }
        
        json_file = Path("config/log_management_complete.json")
        with open(json_file, 'w') as f:
            json.dump(complete_config, f, indent=2)
        
        print(f"✅ Exported complete configuration to {json_file}")
        
    finally:
        db.close()


if __name__ == "__main__":
    main()
