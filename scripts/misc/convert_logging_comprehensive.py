#!/usr/bin/env python3

import os
import re
import sys

# Files that should NOT be converted (infrastructure/config files)
EXCLUDE_FILES = [
    "app/core/config.py",  # Core configuration - should use standard logging
    "app/core/logging_config.py",  # Logging configuration itself
    "convert_logging.py",  # Our conversion script
]

# Files that SHOULD be converted to structlog
CONVERT_FILES = [
    "app/worker/google_healthcare_poller.py",
    "app/worker/dimse_qr_poller.py", 
    "app/worker/dimse_qr_retriever.py",
    "app/crosswalk/service.py",
    "app/crosswalk/tasks.py",
    "app/cache/rules_cache.py",
    "app/services/google_healthcare_service.py",
    "app/services/dimse/spanner_scp.py",
    "app/services/dimse/cmove_proxy.py", 
    "app/services/dimse/spanner_service_manager.py",
    "app/services/spanner_engine.py",
    "app/services/storage_backends/filesystem.py",
    "app/services/storage_backends/gcs.py",
    "app/services/storage_backends/google_healthcare.py",
    "app/services/dicomweb_client.py",
    "app/services/network/dimse/scu_service.py",
    "app/schemas/data_browser.py",
    "app/schemas/dicomweb.py",
    "inject_admin.py",
    "scripts/test_connectivity.py",
    "scripts/start_spanner_services.py", 
    "scripts/inspect_listeners.py"
]

def convert_file(filepath):
    """Convert a file from logging.getLogger to structlog.get_logger"""
    print(f"🔄 Processing {filepath}...")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    original = content
    
    # Add structlog import if not present
    if 'import structlog' not in content:
        lines = content.split('\n')
        import_added = False
        
        # Try to find a good place to add the import
        for i, line in enumerate(lines):
            # Add after logging import
            if line.strip() == 'import logging' and not import_added:
                lines.insert(i + 1, 'import structlog')
                import_added = True
                break
            # Add after other standard library imports
            elif (line.startswith('from typing') or 
                  line.startswith('from datetime') or
                  line.startswith('from pathlib')) and not import_added:
                # Find the end of the import block
                j = i + 1
                while j < len(lines) and (lines[j].startswith('from ') or lines[j].startswith('import ') or lines[j].strip() == ''):
                    j += 1
                lines.insert(j, 'import structlog')
                import_added = True
                break
        
        # If we couldn't find a good place, add it early
        if not import_added:
            for i, line in enumerate(lines):
                if line.startswith('import ') or line.startswith('from '):
                    lines.insert(i + 1, 'import structlog')
                    import_added = True
                    break
        
        if import_added:
            content = '\n'.join(lines)
    
    # Replace logger = logging.getLogger(__name__) with logger = structlog.get_logger(__name__)
    content = re.sub(
        r'logger = logging\.getLogger\(__name__\)',
        r'logger = structlog.get_logger(__name__)',
        content
    )
    
    # Also handle cases with comments or spacing
    content = re.sub(
        r'logger\s*=\s*logging\.getLogger\(__name__\)\s*#.*',
        r'logger = structlog.get_logger(__name__)',
        content
    )
    
    # For script files, add fallback mechanism
    if filepath.startswith('scripts/') or filepath == 'inject_admin.py':
        content = re.sub(
            r'logger = structlog\.get_logger\(__name__\)',
            '''try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)''',
            content
        )
    
    # If content changed, write it back
    if content != original:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"✅ Converted {filepath}")
        return True
    else:
        print(f"⏭️  Skipped {filepath} (no changes needed)")
        return False

def main():
    converted_count = 0
    
    for filepath in CONVERT_FILES:
        if filepath in EXCLUDE_FILES:
            print(f"🚫 Excluded {filepath} (infrastructure file)")
            continue
            
        if os.path.exists(filepath):
            if convert_file(filepath):
                converted_count += 1
        else:
            print(f"❌ File not found: {filepath}")
    
    print(f"\n🎉 Comprehensive conversion complete! {converted_count} files converted.")

if __name__ == "__main__":
    main()
