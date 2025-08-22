#!/usr/bin/env python3
"""
Dustbin Management Command

Administrative tool for managing the medical-grade dustbin system.
Provides commands for monitoring, maintenance, and emergency operations.

Usage:
    python dustbin_manager.py status
    python dustbin_manager.py cleanup --dry-run
    python dustbin_manager.py cleanup --execute
    python dustbin_manager.py verify-pending
    python dustbin_manager.py emergency-recovery
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.dustbin_service import dustbin_service
from app.core.config import settings
import structlog

# Configure logging
log = structlog.get_logger(__name__)

class DustbinManager:
    """Administrative interface for the dustbin system."""
    
    def __init__(self):
        # Use external volume path if running outside Docker, container path if inside Docker
        if os.path.exists("/dicom_data"):
            self.dustbin_path = Path("/dicom_data/dustbin")
        else:
            # Running outside Docker - use external volume path  
            self.dustbin_path = Path("/home/icculus/axiom/axiom-volumes/dicom_dustbin")
    
    def status(self) -> Dict[str, Any]:
        """Get current status of the dustbin system."""
        try:
            stats = {
                'dustbin_path': str(self.dustbin_path),
                'dustbin_exists': self.dustbin_path.exists(),
                'total_files': 0,
                'total_size_bytes': 0,
                'files_by_date': {},
                'oldest_file': None,
                'newest_file': None,
                'pending_verifications': 0,
                'expired_files': 0
            }
            
            if not self.dustbin_path.exists():
                return stats
            
            # Count files and gather statistics
            oldest_date = None
            newest_date = None
            
            for dcm_file in self.dustbin_path.rglob("*.dcm"):
                stats['total_files'] += 1
                file_size = dcm_file.stat().st_size
                stats['total_size_bytes'] += file_size
                
                # Get date from path structure (YYYYMMDD)
                date_parts = dcm_file.parts
                for part in date_parts:
                    if len(part) == 8 and part.isdigit():
                        date_str = part
                        if date_str not in stats['files_by_date']:
                            stats['files_by_date'][date_str] = {'count': 0, 'size_bytes': 0}
                        stats['files_by_date'][date_str]['count'] += 1
                        stats['files_by_date'][date_str]['size_bytes'] += file_size
                        
                        if oldest_date is None or date_str < oldest_date:
                            oldest_date = date_str
                        if newest_date is None or date_str > newest_date:
                            newest_date = date_str
                        break
            
            stats['oldest_file'] = oldest_date
            stats['newest_file'] = newest_date
            
            # Count pending verifications
            confirmations_dir = self.dustbin_path / "confirmations"
            if confirmations_dir.exists():
                stats['pending_verifications'] = len(list(confirmations_dir.glob("*.json")))
            
            # Count expired files
            now = datetime.now()
            retention_cutoff = now - timedelta(days=settings.DUSTBIN_RETENTION_DAYS)
            
            for audit_file in self.dustbin_path.rglob("*.json"):
                if audit_file.parent.name == "confirmations":
                    continue  # Skip confirmation files
                
                try:
                    with open(audit_file, 'r') as f:
                        audit_data = json.load(f)
                    
                    retention_until = datetime.fromisoformat(audit_data.get("retention_until", ""))
                    if now > retention_until:
                        stats['expired_files'] += 1
                        
                except Exception:
                    pass  # Skip invalid audit files
            
            return stats
            
        except Exception as e:
            log.error("Error getting dustbin status", error=str(e), exc_info=True)
            return {'error': str(e)}
    
    def verify_pending(self) -> Dict[str, Any]:
        """Check status of pending verifications."""
        try:
            results = {
                'pending_verifications': [],
                'expired_verifications': [],
                'total_pending': 0,
                'total_expired': 0
            }
            
            confirmations_dir = self.dustbin_path / "confirmations"
            if not confirmations_dir.exists():
                return results
            
            now = datetime.now()
            timeout_hours = settings.DUSTBIN_VERIFICATION_TIMEOUT_HOURS
            timeout_cutoff = now - timedelta(hours=timeout_hours)
            
            # Group confirmations by verification ID
            verifications = {}
            
            for conf_file in confirmations_dir.glob("*.json"):
                try:
                    with open(conf_file, 'r') as f:
                        conf_data = json.load(f)
                    
                    verification_id = conf_data.get('verification_id')
                    if not verification_id:
                        continue
                    
                    if verification_id not in verifications:
                        verifications[verification_id] = {
                            'verification_id': verification_id,
                            'confirmations': [],
                            'oldest_timestamp': None,
                            'is_expired': False
                        }
                    
                    timestamp_str = conf_data.get('confirmation_timestamp')
                    if timestamp_str:
                        timestamp = datetime.fromisoformat(timestamp_str)
                        if (verifications[verification_id]['oldest_timestamp'] is None or 
                            timestamp < verifications[verification_id]['oldest_timestamp']):
                            verifications[verification_id]['oldest_timestamp'] = timestamp
                        
                        if timestamp < timeout_cutoff:
                            verifications[verification_id]['is_expired'] = True
                    
                    verifications[verification_id]['confirmations'].append(conf_data)
                    
                except Exception as e:
                    log.warning(f"Error reading confirmation file {conf_file}: {e}")
            
            # Categorize verifications
            for verification_id, verification_data in verifications.items():
                if verification_data['is_expired']:
                    results['expired_verifications'].append(verification_data)
                    results['total_expired'] += 1
                else:
                    results['pending_verifications'].append(verification_data)
                    results['total_pending'] += 1
            
            return results
            
        except Exception as e:
            log.error("Error checking pending verifications", error=str(e), exc_info=True)
            return {'error': str(e)}
    
    def cleanup(self, dry_run: bool = True) -> Dict[str, Any]:
        """Clean up expired files from dustbin."""
        return dustbin_service.cleanup_expired_dustbin_files(dry_run=dry_run)
    
    def emergency_recovery(self) -> Dict[str, Any]:
        """Emergency recovery operations for dustbin system."""
        try:
            results = {
                'recovery_operations': [],
                'files_recovered': 0,
                'errors': []
            }
            
            # Check for orphaned files (DICOM files without audit files)
            orphaned_files = []
            
            for dcm_file in self.dustbin_path.rglob("*.dcm"):
                audit_file = dcm_file.with_suffix('.json')
                if not audit_file.exists():
                    orphaned_files.append(str(dcm_file))
            
            if orphaned_files:
                results['recovery_operations'].append(f"Found {len(orphaned_files)} orphaned DICOM files without audit metadata")
                results['orphaned_files'] = orphaned_files
            
            # Check for orphaned audit files (audit files without DICOM files)
            orphaned_audits = []
            
            for audit_file in self.dustbin_path.rglob("*.json"):
                if audit_file.parent.name == "confirmations":
                    continue  # Skip confirmation files
                
                dcm_file = audit_file.with_suffix('.dcm')
                if not dcm_file.exists():
                    orphaned_audits.append(str(audit_file))
            
            if orphaned_audits:
                results['recovery_operations'].append(f"Found {len(orphaned_audits)} orphaned audit files without DICOM files")
                results['orphaned_audits'] = orphaned_audits
            
            # Check for corrupted audit files
            corrupted_audits = []
            
            for audit_file in self.dustbin_path.rglob("*.json"):
                if audit_file.parent.name == "confirmations":
                    continue
                
                try:
                    with open(audit_file, 'r') as f:
                        json.load(f)
                except json.JSONDecodeError:
                    corrupted_audits.append(str(audit_file))
            
            if corrupted_audits:
                results['recovery_operations'].append(f"Found {len(corrupted_audits)} corrupted audit files")
                results['corrupted_audits'] = corrupted_audits
            
            return results
            
        except Exception as e:
            log.error("Error during emergency recovery", error=str(e), exc_info=True)
            return {'error': str(e)}


def main():
    parser = argparse.ArgumentParser(description="Dustbin Management Tool")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    subparsers.add_parser('status', help='Show dustbin system status')
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up expired files')
    cleanup_parser.add_argument('--dry-run', action='store_true', default=True,
                               help='Show what would be deleted without actually deleting (default)')
    cleanup_parser.add_argument('--execute', action='store_true',
                               help='Actually delete expired files')
    
    # Verify pending command
    subparsers.add_parser('verify-pending', help='Check pending verifications')
    
    # Emergency recovery command
    subparsers.add_parser('emergency-recovery', help='Emergency recovery operations')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    manager = DustbinManager()
    
    if args.command == 'status':
        result = manager.status()
        print(json.dumps(result, indent=2))
        
    elif args.command == 'cleanup':
        dry_run = not args.execute
        result = manager.cleanup(dry_run=dry_run)
        print(json.dumps(result, indent=2))
        
    elif args.command == 'verify-pending':
        result = manager.verify_pending()
        print(json.dumps(result, indent=2))
        
    elif args.command == 'emergency-recovery':
        result = manager.emergency_recovery()
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
