#!/usr/bin/env python3
"""
AE Title Configuration Checker for Axiom Spanner

This script helps you understand and validate the AE title configuration
in your DIMSE spanning system.

Usage: python check_ae_titles.py
"""

import os
import sys
from pathlib import Path

# Add the app directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "app"))

from app.db.session import SessionLocal
from app.db.models.dimse_qr_source import DimseQueryRetrieveSource
from app.db.models.spanner import SpannerSourceMapping
from sqlalchemy.orm import selectinload
from sqlalchemy import select


def check_ae_titles():
    """Check and display AE title configuration."""
    
    print("🔍 AXIOM SPANNER AE TITLE CONFIGURATION CHECKER")
    print("=" * 60)
    
    # 1. Environment Variables
    print("\n📋 ENVIRONMENT VARIABLES:")
    print("-" * 30)
    spanner_scu_ae = os.getenv("SPANNER_SCU_AE_TITLE", "NOT_SET")
    spanner_scp_ae = os.getenv("SPANNER_SCP_AE_TITLE", "NOT_SET")
    
    print(f"SPANNER_SCU_AE_TITLE: {spanner_scu_ae}")
    print(f"SPANNER_SCP_AE_TITLE: {spanner_scp_ae}")
    
    # Validate AE title lengths
    if len(spanner_scu_ae) > 16:
        print(f"❌ WARNING: SPANNER_SCU_AE_TITLE '{spanner_scu_ae}' exceeds 16 characters!")
    if len(spanner_scp_ae) > 16:
        print(f"❌ WARNING: SPANNER_SCP_AE_TITLE '{spanner_scp_ae}' exceeds 16 characters!")
    
    # 2. Database Configuration
    print("\n🗄️  DATABASE CONFIGURATION:")
    print("-" * 30)
    
    try:
        with SessionLocal() as session:
            # Get all DIMSE Q/R sources
            dimse_sources = session.query(DimseQueryRetrieveSource)\
                .filter(DimseQueryRetrieveSource.is_enabled == True)\
                .order_by(DimseQueryRetrieveSource.name).all()
            
            if not dimse_sources:
                print("❌ No enabled DIMSE Q/R sources found in database!")
            else:
                print(f"✅ Found {len(dimse_sources)} enabled DIMSE Q/R sources:")
                for source in dimse_sources:
                    print(f"\n  📡 {source.name}")
                    print(f"     Remote AE Title: {source.remote_ae_title}")
                    print(f"     Local AE Title:  {source.local_ae_title}")
                    print(f"     Remote Host:     {source.remote_host}:{source.remote_port}")
                    print(f"     TLS Enabled:     {source.tls_enabled}")
                    
                    # Validate AE title lengths
                    if len(source.remote_ae_title) > 16:
                        print(f"     ❌ WARNING: Remote AE title exceeds 16 characters!")
                    if len(source.local_ae_title) > 16:
                        print(f"     ❌ WARNING: Local AE title exceeds 16 characters!")
                        
            # Get spanner mappings
            print("\n🔗 SPANNER SOURCE MAPPINGS:")
            print("-" * 30)
            
            mappings = session.query(SpannerSourceMapping)\
                .options(selectinload(SpannerSourceMapping.dimse_qr_source))\
                .filter(SpannerSourceMapping.is_enabled == True)\
                .order_by(SpannerSourceMapping.priority).all()
            
            if not mappings:
                print("❌ No enabled spanner mappings found!")
            else:
                print(f"✅ Found {len(mappings)} enabled spanner mappings:")
                for mapping in mappings:
                    if mapping.source_type == "dimse-qr" and mapping.dimse_qr_source:
                        print(f"\n  🎯 {mapping.dimse_qr_source.name}")
                        print(f"     Source Type:    {mapping.source_type}")
                        print(f"     Priority:       {mapping.priority}")
                        print(f"     Weight:         {mapping.weight}")
                        print(f"     Max Retries:    {mapping.max_retries}")
                        print(f"     Failover:       {mapping.enable_failover}")
                        
    except Exception as e:
        print(f"❌ Database connection error: {e}")
    
    # 3. Summary and Recommendations
    print("\n📝 AE TITLE SUMMARY:")
    print("-" * 30)
    print("\n🔄 DIMSE Query Flow:")
    print("1. External system queries spanner → Uses SPANNER_SCP_AE_TITLE")
    print("2. Spanner queries remote PACS → Uses source.local_ae_title")  
    print("3. Remote PACS responds → Identified by source.remote_ae_title")
    
    print("\n💡 RECOMMENDATIONS:")
    if spanner_scu_ae == "NOT_SET":
        print("- Set SPANNER_SCU_AE_TITLE environment variable (fallback only)")
    if spanner_scp_ae == "NOT_SET":
        print("- Set SPANNER_SCP_AE_TITLE environment variable")
    print("- Ensure all AE titles are ≤16 characters (DICOM standard)")
    print("- Configure local_ae_title per source for proper identification")
    print("- Coordinate with PACS admins on AE title authorization")


if __name__ == "__main__":
    check_ae_titles()
