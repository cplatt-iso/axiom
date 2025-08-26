#!/usr/bin/env python3
# scripts/start_spanner_services.py
"""
Standalone script to start DICOM query spanning services.

This script can be run independently to start all spanner DIMSE services
without the web API, useful for dedicated spanner instances.
"""

import sys
import os
import signal
import logging
import structlog
import time
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import SessionLocal
from app.services.dimse.spanner_service_manager import SpannerServiceManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/var/log/axiom/spanner_services.log', mode='a')
    ]
)

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

# Global service manager
service_manager = None
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_requested = True
    
    if service_manager:
        service_manager.stop_spanner_services()


def main():
    """Main entry point for spanner services."""
    global service_manager
    
    logger.info("Starting DICOM Query Spanning Services")
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Initialize service manager
        service_manager = SpannerServiceManager(SessionLocal)
        
        # Start all spanner services
        service_manager.start_spanner_services()
        
        logger.info("Spanner services started successfully")
        
        # Main loop - wait for shutdown signal
        while not shutdown_requested:
            time.sleep(1)
        
        logger.info("Shutdown complete")
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Error in spanner services: {e}", exc_info=True)
        return 1
    finally:
        if service_manager:
            service_manager.stop_spanner_services()
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
