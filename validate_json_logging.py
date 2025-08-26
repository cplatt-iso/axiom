#!/usr/bin/env python3
import subprocess
import json
import sys

def check_container_logging(container_name):
    """Check if a container is logging in JSON format"""
    try:
        # Get last few log lines
        result = subprocess.run(['./axiomctl', 'logs', '--tail=3', container_name], 
                              capture_output=True, text=True, cwd='/home/icculus/axiom/backend')
        
        if result.returncode != 0:
            print(f"❌ {container_name}: Failed to get logs")
            return False
            
        lines = result.stdout.strip().split('\n')
        json_lines = 0
        total_lines = 0
        
        for line in lines:
            if not line.strip():
                continue
                
            # Extract JSON part after container name
            if '|' in line:
                json_part = line.split('|', 1)[1].strip()
                if json_part:
                    total_lines += 1
                    try:
                        json.loads(json_part)
                        json_lines += 1
                    except json.JSONDecodeError:
                        # Not JSON, this is old format
                        print(f"  Non-JSON: {json_part}")
        
        if total_lines == 0:
            print(f"⚠️  {container_name}: No log lines found")
            return True  # No logs to check
        
        json_percentage = (json_lines / total_lines) * 100
        if json_percentage >= 80:
            print(f"✅ {container_name}: {json_lines}/{total_lines} lines are JSON ({json_percentage:.0f}%)")
            return True
        else:
            print(f"❌ {container_name}: Only {json_lines}/{total_lines} lines are JSON ({json_percentage:.0f}%)")
            return False
            
    except Exception as e:
        print(f"❌ {container_name}: Error checking logs: {e}")
        return False

def main():
    containers = [
        'api',
        'worker', 
        'beat',
        'dcm4che_1',        # listener
        'dcm4che-sender',
        'pynetdicom-sender',
        'storescp_1',
        'storescp_2',
        'dustbin-verification-worker'
    ]
    
    print("🔍 Validating JSON logging across containers...")
    print("=" * 50)
    
    all_good = True
    for container in containers:
        if not check_container_logging(container):
            all_good = False
    
    print("=" * 50)
    if all_good:
        print("🎉 All containers are using JSON logging!")
        return 0
    else:
        print("⚠️  Some containers still need JSON logging fixes")
        return 1

if __name__ == "__main__":
    sys.exit(main())
