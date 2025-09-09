#!/usr/bin/env python3

import os
import re
import sys

# Files to convert (API endpoints that still need conversion)
api_files = [
    "app/api/api_v1/endpoints/config_crosswalk.py",
    "app/api/api_v1/endpoints/config_google_healthcare_sources.py", 
    "app/api/api_v1/endpoints/modalities.py",
    "app/api/api_v1/endpoints/config_schedules.py",
    "app/api/api_v1/endpoints/config_dicomweb.py",
    "app/api/api_v1/endpoints/facilities.py",
    "app/api/api_v1/endpoints/roles.py",
    "app/api/api_v1/endpoints/config_dimse_qr.py",
    "app/api/api_v1/endpoints/users.py",
    "app/api/api_v1/endpoints/api_keys.py",
    "app/api/api_v1/endpoints/spanner_services.py",
    "app/api/api_v1/endpoints/config_spanner.py",
    "app/api/api_v1/endpoints/dicomweb.py",
    "app/api/api_v1/endpoints/ai_assist.py"
]

def convert_file(filepath):
    """Convert a file from logging.getLogger to structlog.get_logger"""
    with open(filepath, 'r') as f:
        content = f.read()
    
    original = content
    
    # Add structlog import if not present
    if 'import structlog' not in content:
        # Find the right place to add it - after other imports but before local imports
        lines = content.split('\n')
        import_line_idx = -1
        
        for i, line in enumerate(lines):
            if line.startswith('from fastapi') or line.startswith('from sqlalchemy'):
                import_line_idx = i
                break
                
        if import_line_idx >= 0:
            lines.insert(import_line_idx + 1, 'import structlog')
            content = '\n'.join(lines)
    
    # Replace logger = logging.getLogger(__name__) with logger = structlog.get_logger(__name__)
    content = re.sub(
        r'logger = logging\.getLogger\(__name__\)',
        r'logger = structlog.get_logger(__name__)',
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
    
    for filepath in api_files:
        if os.path.exists(filepath):
            if convert_file(filepath):
                converted_count += 1
        else:
            print(f"❌ File not found: {filepath}")
    
    print(f"\n🎉 Conversion complete! {converted_count} files converted.")

if __name__ == "__main__":
    main()
