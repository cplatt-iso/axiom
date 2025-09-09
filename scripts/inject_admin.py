#!/usr/bin/env python3
# inject_admin.py - DEPRECATED
"""
DEPRECATED: This script is no longer needed.

Starting with this version, the first user to log in automatically becomes
the admin user with full privileges. No manual script execution is required.

HOW IT WORKS:
1. When the first user logs in via Google OAuth, they are automatically:
   - Assigned the "Admin" role
   - Set as a superuser (is_superuser=True)
   - Given full administrative privileges

2. All subsequent users get the standard "User" role by default

MIGRATION:
If you're upgrading from a previous version and already have users in the system
but no admin user, you can manually assign admin role using the API or database
directly.

For more information, see the updated documentation.
"""

import sys
import os

# Add color support for terminal output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_deprecation_notice():
    """Print a clear deprecation notice"""
    print(f"{Colors.HEADER}{Colors.BOLD}=" * 70 + f"{Colors.ENDC}")
    print(f"{Colors.WARNING}{Colors.BOLD}⚠️  DEPRECATED SCRIPT ⚠️{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}=" * 70 + f"{Colors.ENDC}")
    print()
    print(f"{Colors.OKBLUE}This script is no longer needed!{Colors.ENDC}")
    print()
    print(f"{Colors.OKGREEN}✨ NEW BEHAVIOR:{Colors.ENDC}")
    print(f"   • The first user to log in automatically becomes the admin")
    print(f"   • No manual script execution required")
    print(f"   • Subsequent users get standard permissions")
    print()
    print(f"{Colors.OKCYAN}📖 HOW TO CREATE YOUR FIRST ADMIN:{Colors.ENDC}")
    print(f"   1. Start your Axiom instance")
    print(f"   2. Navigate to the login page")
    print(f"   3. Log in with Google OAuth")
    print(f"   4. You'll automatically get admin privileges!")
    print()
    print(f"{Colors.WARNING}🔄 MIGRATING FROM OLD VERSION?{Colors.ENDC}")
    print(f"   If you already have users but no admin, you can:")
    print(f"   • Use the API to assign admin role manually")
    print(f"   • Or contact support for assistance")
    print()
    print(f"{Colors.HEADER}For more info: see updated documentation{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}=" * 70 + f"{Colors.ENDC}")

def check_for_force_flag():
    """Check if user wants to run the old script anyway"""
    if "--force-old-behavior" in sys.argv:
        print(f"{Colors.WARNING}⚠️  Force flag detected. Running legacy inject_admin logic...{Colors.ENDC}")
        return True
    return False

def run_legacy_script():
    """Run the original inject_admin script if explicitly requested"""
    try:
        # Import and run the old script
        from scripts.misc.inject_admin_legacy import inject_admin_role
        print(f"{Colors.OKCYAN}Running legacy admin injection...{Colors.ENDC}")
        inject_admin_role()
        print(f"{Colors.OKGREEN}✅ Legacy admin injection completed.{Colors.ENDC}")
        print(f"{Colors.WARNING}Note: Consider migrating to the new automatic behavior.{Colors.ENDC}")
    except ImportError as e:
        print(f"{Colors.FAIL}❌ Error: Could not import legacy script: {e}{Colors.ENDC}")
        print(f"{Colors.WARNING}The old inject_admin.py script may have been moved or removed.{Colors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print(f"{Colors.FAIL}❌ Error running legacy script: {e}{Colors.ENDC}")
        sys.exit(1)

def main():
    """Main entry point"""
    print_deprecation_notice()
    
    if check_for_force_flag():
        run_legacy_script()
    else:
        print(f"{Colors.OKGREEN}✅ No action needed. First user login will automatically create admin.{Colors.ENDC}")
        print()
        print(f"{Colors.OKCYAN}💡 TIP: If you need to force the old behavior, run:{Colors.ENDC}")
        print(f"   python inject_admin.py --force-old-behavior")
        print()

if __name__ == "__main__":
    main()
