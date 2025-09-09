#!/usr/bin/env python3
"""
AE Title Configuration Reference for Axiom Spanner

This script demonstrates the proper AE title configuration hierarchy for DICOM spanning.
"""

def show_ae_title_configuration():
    """
    Display the AE title configuration hierarchy and examples.
    """
    
    print("=== AXIOM SPANNER AE TITLE CONFIGURATION ===\n")
    
    print("Three types of AE titles in DIMSE spanning:\n")
    
    print("1. REMOTE_QUERIER - External system querying the spanner")
    print("   └─ Set by the external PACS/workstation making the query")
    print("   └─ Captured in spanning_query.requesting_ae_title")
    print("   └─ Used for logging and audit trails\n")
    
    print("2. SPANNER SCP AE TITLE - Spanner's identity when receiving queries")
    print("   └─ Configured in spanner_configs.scp_ae_title")
    print("   └─ Default: 'AXIOM_SCP'")
    print("   └─ Environment override: SPANNER_SCP_AE_TITLE")
    print("   └─ Used by: DIMSE SCP Listener")
    print("   └─ This is what external systems connect TO\n")
    
    print("3. SPANNER SCU AE TITLE - Spanner's identity when querying sources")
    print("   └─ Priority hierarchy:")
    print("      a) dimse_qr_sources.local_ae_title (per-source override)")
    print("      b) spanner_configs.scu_ae_title (spanner default)")
    print("      c) Environment: SPANNER_SCU_AE_TITLE (fallback)")
    print("      d) Hard-coded: 'AXIOM_SPAN' (final fallback)")
    print("   └─ Used by: DIMSE Query Workers")
    print("   └─ This is what remote PACS see when spanner connects\n")
    
    print("=== CONFIGURATION EXAMPLES ===\n")
    
    print("Example 1: Basic setup with spanner defaults")
    print("spanner_config:")
    print("  scp_ae_title: 'AXIOM_SCP'    # What external systems connect to")
    print("  scu_ae_title: 'AXIOM_SPAN'   # Default for outgoing connections")
    print("")
    print("dimse_qr_source (Orthanc):")
    print("  remote_ae_title: 'ORTHANC'   # Target AE title")
    print("  local_ae_title: 'AXIOM_SPAN' # Uses spanner default")
    print("")
    
    print("Example 2: Per-source AE title customization")
    print("dimse_qr_source (Legacy PACS):")
    print("  remote_ae_title: 'LEGACY_PACS'")
    print("  local_ae_title: 'LEGACY_SCU'  # Custom AE for this specific PACS")
    print("")
    
    print("=== CURRENT IMPLEMENTATION STATUS ===\n")
    print("✅ Database schema updated with scp_ae_title and scu_ae_title")
    print("✅ DIMSE SCP Listener uses spanner config AE title")
    print("✅ DIMSE Query Workers use source-specific local_ae_title")
    print("✅ Fallback hierarchy implemented for SCU AE titles")
    print("✅ Migration created for new AE title fields")
    print("")
    print("⏳ TODO: Apply migration when containers are running")
    print("⏳ TODO: Update existing spanner config with proper AE titles")


if __name__ == "__main__":
    show_ae_title_configuration()
