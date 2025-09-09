#!/usr/bin/env python3
"""Test script to debug structlog JSON output"""

import sys
sys.path.append('/home/icculus/axiom/backend')

from app.core.logging_config import configure_json_logging

# Configure logger
logger = configure_json_logging("test_service")

# Test with various parameters
logger.info("Test message with parameters", 
           test_param1="value1", 
           test_param2=42, 
           test_param3=["item1", "item2"])

print("If you see only basic fields above, there's a structlog configuration issue.")
