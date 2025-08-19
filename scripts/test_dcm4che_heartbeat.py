#!/usr/bin/env python
"""
Test script to verify the dcm4che heartbeat functionality.
This can be run independently to test the heartbeat manager.
"""

import os
import sys
import time
import threading
import logging

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.batching_listener import HeartbeatManager
from app.db.session import SessionLocal
from app.crud import crud_dimse_listener_state

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_heartbeat():
    """Test the heartbeat functionality."""
    listener_id = os.getenv("AXIOM_INSTANCE_ID", "dcm4che_1")
    
    logging.info(f"Testing heartbeat functionality for listener: {listener_id}")
    
    # Create heartbeat manager
    heartbeat_manager = HeartbeatManager(listener_id)
    
    try:
        # Start the heartbeat
        heartbeat_manager.start()
        
        # Monitor for 30 seconds and check status updates
        start_time = time.time()
        while time.time() - start_time < 30:
            db = None
            try:
                # Check current status in database
                db = SessionLocal()
                listener_state = crud_dimse_listener_state.get_listener_state(db, listener_id=listener_id)
                if listener_state:
                    logging.info(f"Current status: {listener_state.status} - {listener_state.status_message}")
                    logging.info(f"Last heartbeat: {listener_state.last_heartbeat}")
                else:
                    logging.warning("No listener state found in database")
                db.close()
                
            except Exception as e:
                logging.error(f"Error checking status: {e}")
                if db:
                    db.close()
            
            time.sleep(10)
            
    except KeyboardInterrupt:
        logging.info("Test interrupted")
    finally:
        heartbeat_manager.stop()
        logging.info("Heartbeat test completed")

if __name__ == "__main__":
    test_heartbeat()
