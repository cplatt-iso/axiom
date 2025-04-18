# app/crud/__init__.py

# This file makes the 'crud' directory a Python package.

# Import the main CRUD objects from submodules for convenient access
# from other parts of the application. For example, instead of:
#   from app.crud.crud_rule import ruleset
# you can use:
#   from app.crud import ruleset

from .crud_rule import ruleset, rule
from . import crud_user, crud_api_key
from . import crud_role

# Add imports for other CRUD modules as they are created:
# from .crud_user import user, role
# from .crud_audit import audit_log
# etc.
