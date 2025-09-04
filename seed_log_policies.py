#!/usr/bin/env python3
"""
Seed default log management policies for Axiom Flow
Run this after the log management tables are created
"""

import sys
import os
sys.path.append('/app')

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.log_management import models, schemas
from app.log_management.policy_engine import PolicyEngine

def seed_default_policies():
    """Create default retention policies for common medical imaging scenarios"""
    
    db = SessionLocal()
    try:
        # Check if policies already exist
        existing_count = db.query(models.LogRetentionPolicy).count()
        if existing_count > 0:
            print(f"Found {existing_count} existing policies, skipping seed")
            return
        
        print("Seeding default log management policies...")
        
        # 1. Critical DICOM Transaction Logs
        critical_policy = models.LogRetentionPolicy(
            name="Critical DICOM Transactions",
            description="DICOM C-STORE, C-FIND, C-MOVE operations - HIPAA compliance requires 7+ year retention",
            service_pattern="axiom.dcm4che-*,axiom.storescp-*,axiom.pynetdicom-*",
            log_level_filter=None,  # All log levels
            tier=models.RetentionTier.CRITICAL,
            hot_days=30,
            warm_days=180,
            cold_days=365,
            delete_days=2555,  # 7 years
            max_index_size_gb=10,
            max_index_age_days=30,
            storage_class_hot="fast-ssd",
            storage_class_warm="standard", 
            storage_class_cold="cold-storage",
            compliance_required=True,
            ilm_policy_json=PolicyEngine.generate_ilm_policy(
                tier=models.RetentionTier.CRITICAL,
                hot_days=30, warm_days=180, cold_days=365, delete_days=2555
            )
        )
        
        # 2. API Access and Authentication Logs
        api_policy = models.LogRetentionPolicy(
            name="API Access & Authentication", 
            description="API requests, authentication, authorization - Security audit trail",
            service_pattern="axiom.api.*,axiom.mllp-*",
            log_level_filter=None,
            tier=models.RetentionTier.CRITICAL,
            hot_days=30,
            warm_days=90,
            cold_days=365,
            delete_days=2555,  # 7 years for security logs
            max_index_size_gb=5,
            max_index_age_days=7,
            storage_class_hot="fast-ssd",
            storage_class_warm="standard",
            storage_class_cold="cold-storage", 
            compliance_required=True,
            ilm_policy_json=PolicyEngine.generate_ilm_policy(
                tier=models.RetentionTier.CRITICAL,
                hot_days=30, warm_days=90, cold_days=365, delete_days=2555,
                max_index_size_gb=5
            )
        )
        
        # 3. System Health and Operations
        operational_policy = models.LogRetentionPolicy(
            name="System Health & Operations",
            description="Database, workers, coordination services - Operational troubleshooting",
            service_pattern="axiom.worker.*,axiom.beat.*,axiom.db.*,axiom.spanner-*",
            log_level_filter=None,
            tier=models.RetentionTier.OPERATIONAL,
            hot_days=7,
            warm_days=30,
            cold_days=180,
            delete_days=730,  # 2 years
            max_index_size_gb=5,
            max_index_age_days=7,
            storage_class_hot="standard",
            storage_class_warm="standard",
            storage_class_cold="cold-storage",
            compliance_required=False,
            ilm_policy_json=PolicyEngine.generate_ilm_policy(
                tier=models.RetentionTier.OPERATIONAL,
                hot_days=7, warm_days=30, cold_days=180, delete_days=730,
                max_index_size_gb=5
            )
        )
        
        # 4. Debug and Development Logs
        debug_policy = models.LogRetentionPolicy(
            name="Debug & Development",
            description="Verbose application logs, development debugging - Short term retention",
            service_pattern="*",  # Catch-all for unmatched services
            log_level_filter="DEBUG",
            tier=models.RetentionTier.DEBUG,
            hot_days=1,
            warm_days=7,
            cold_days=30,
            delete_days=90,  # 3 months
            max_index_size_gb=2,
            max_index_age_days=1,
            storage_class_hot="standard",
            storage_class_warm="cold-storage",
            storage_class_cold="cold-storage",
            compliance_required=False,
            ilm_policy_json=PolicyEngine.generate_ilm_policy(
                tier=models.RetentionTier.DEBUG,
                hot_days=1, warm_days=7, cold_days=30, delete_days=90,
                max_index_size_gb=2
            )
        )
        
        # Add all policies
        policies = [critical_policy, api_policy, operational_policy, debug_policy]
        for policy in policies:
            db.add(policy)
            print(f"  ✓ Created policy: {policy.name}")
        
        # Example archival rule for old critical logs
        archival_rule = models.LogArchivalRule(
            name="Critical Logs to S3 Archive",
            description="Archive critical DICOM logs older than 1 year to S3 cold storage",
            source_pattern="critical-logs-*",
            archive_after_days=365,
            destination_type="s3",
            destination_config={
                "bucket": "axiom-log-archive",
                "prefix": "critical-logs/",
                "region": "us-west-2",
                "storage_class": "GLACIER"
            },
            compression="gzip",
            format="json",
            k8s_schedule="0 2 * * 0",  # Weekly on Sundays at 2 AM
            k8s_job_template={
                "apiVersion": "batch/v1",
                "kind": "Job", 
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [{
                                "name": "log-archiver",
                                "image": "axiom/log-archiver:latest",
                                "command": ["python", "archive_logs.py"],
                                "env": [
                                    {"name": "SOURCE_PATTERN", "value": "critical-logs-*"},
                                    {"name": "DEST_BUCKET", "value": "axiom-log-archive"}
                                ]
                            }],
                            "restartPolicy": "Never"
                        }
                    }
                }
            }
        )
        db.add(archival_rule)
        print(f"  ✓ Created archival rule: {archival_rule.name}")
        
        db.commit()
        print(f"\n🎉 Successfully seeded {len(policies)} retention policies and 1 archival rule!")
        print("\nPolicies created:")
        for policy in policies:
            print(f"  • {policy.name}: {policy.service_pattern} ({policy.tier.value}, {policy.delete_days} day retention)")
        
    except Exception as e:
        print(f"❌ Error seeding policies: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    seed_default_policies()
