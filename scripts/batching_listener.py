#!/usr/bin/env python

import os
import sys
import time
import subprocess
import threading
from collections import defaultdict, deque
from pathlib import Path
import logging
import argparse

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Database imports
from sqlalchemy.exc import SQLAlchemyError
from app.db.session import SessionLocal
from app.crud import crud_dimse_listener_state, crud_dimse_listener_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [AssociationBatcher] - %(message)s')

DICOM_DIR = "/dicom_data/incoming"
PROCESS_ASSOCIATION_SCRIPT = "/app/scripts/process_association.py"
# Timeout to wait for more files in a study directory after the last one arrived.
STUDY_INACTIVITY_TIMEOUT = 5.0
# How often to scan for new study directories.
SCAN_INTERVAL = 1.0
HEARTBEAT_INTERVAL = 30  # Send heartbeat every 30 seconds
LISTENER_INSTANCE_ID = os.getenv("AXIOM_INSTANCE_ID", "dcm4che_1")

class HeartbeatManager:
    """Manages heartbeat and status reporting for the dcm4che listener."""
    
    def __init__(self, listener_id: str):
        self.listener_id = listener_id
        self.listener_config = None
        self.last_heartbeat = time.time()
        self.heartbeat_thread = None
        self.running = False
    
    def _load_listener_config(self):
        """Load the listener configuration from the database."""
        db = None
        try:
            db = SessionLocal()
            config = crud_dimse_listener_config.get_by_instance_id(db=db, instance_id=self.listener_id)
            if config:
                self.listener_config = config
                logging.info(f"Loaded config for listener '{config.name}' (AE: {config.ae_title})")
            else:
                logging.error(f"No config found for listener instance: {self.listener_id}")
        except Exception as e:
            logging.error(f"Failed to load listener config: {e}")
        finally:
            if db:
                db.close()
    
    def _update_status(self, status: str, message: str = ""):
        """Update listener status in database."""
        db = None
        try:
            db = SessionLocal()
            crud_dimse_listener_state.update_listener_state(
                db=db,
                listener_id=self.listener_id,
                status=status,
                status_message=message,
                host="0.0.0.0",
                port=11114,
                ae_title="DCM4CHE"
            )
            db.commit()
        except Exception as e:
            logging.error(f"Failed to update status: {e}")
            if db:
                db.rollback()
        finally:
            if db:
                db.close()
    
    def _send_heartbeat(self):
        """Send heartbeat to database."""
        db = None
        try:
            db = SessionLocal()
            crud_dimse_listener_state.update_listener_heartbeat(db=db, listener_id=self.listener_id)
            db.commit()
            logging.debug("Heartbeat sent successfully")
        except Exception as e:
            logging.error(f"Failed to send heartbeat: {e}")
            if db:
                db.rollback()
        finally:
            if db:
                db.close()
    
    def _heartbeat_loop(self):
        """Main heartbeat loop running in background thread."""
        logging.info(f"Starting heartbeat loop (interval: {HEARTBEAT_INTERVAL}s)")
        while self.running:
            try:
                self._send_heartbeat()
                self.last_heartbeat = time.time()
            except Exception as e:
                logging.error(f"Error in heartbeat loop: {e}")
            
            # Sleep in small intervals to allow for quick shutdown
            for _ in range(HEARTBEAT_INTERVAL * 10):  # 0.1s intervals
                if not self.running:
                    break
                time.sleep(0.1)
    
    def start(self):
        """Start the heartbeat manager."""
        self._load_listener_config()
        self._update_status("starting", "DCM4CHE listener starting up")
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
        self._update_status("running", "DCM4CHE listener active and monitoring")
        logging.info("Heartbeat manager started")
    
    def stop(self):
        """Stop the heartbeat manager."""
        logging.info("Stopping heartbeat manager")
        self.running = False
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=2.0)
        self._update_status("stopped", "DCM4CHE listener stopped")

def process_study_directory(study_path: Path, listener_id: str):
    """Processes all DICOM files in a given study directory."""
    files_to_process = [str(f) for f in study_path.glob('*.dcm')]
    if not files_to_process:
        logging.warning(f"Study directory {study_path} is empty, skipping.")
        return

    logging.info(f"Processing {len(files_to_process)} files from {study_path}...")
    db = None
    try:
        # Construct the command to run the processing script
        command = ["python", PROCESS_ASSOCIATION_SCRIPT] + files_to_process
        
        # Execute the script
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        
        logging.info(f"Successfully processed batch for {study_path}.")
        logging.debug(f"Processing script output:\n{result.stdout}")

        # Update processed count in the database
        try:
            db = SessionLocal()
            crud_dimse_listener_state.increment_processed_count(db=db, listener_id=listener_id, count=len(files_to_process))
            db.commit()
        except Exception as e:
            logging.error(f"Failed to update processed count for {study_path}: {e}")
            if db: db.rollback()
        
        # Simple cleanup: remove the processed study directory.
        import shutil
        shutil.rmtree(study_path)
        logging.info(f"Removed processed directory: {study_path}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error processing {study_path}: {e.stderr}")
        # Update failed count in the database
        try:
            db = SessionLocal()
            crud_dimse_listener_state.increment_failed_count(db=db, listener_id=listener_id, count=len(files_to_process))
            db.commit()
        except Exception as db_e:
            logging.error(f"Failed to update failed count for {study_path}: {db_e}")
            if db: db.rollback()

    except Exception as e:
        logging.error(f"An unexpected error occurred during processing of {study_path}: {e}")
    finally:
        if db:
            db.close()


def watch_directories(listener_id: str):
    """
    Watches for new study directories and processes them after a period of inactivity.
    Directory structure: /dicom_data/incoming/{StudyDate}/{StudyInstanceUID}/files.dcm
    """
    active_studies = {}  # {study_path: last_modified_time}
    logging.info(f"Starting to watch for new study directories in {DICOM_DIR}")
    db = None

    while True:
        now = time.time()
        
        # Discover new or updated study directories
        try:
            if not os.path.exists(DICOM_DIR):
                time.sleep(SCAN_INTERVAL)
                continue

            for study_date_dir in Path(DICOM_DIR).iterdir():
                if not study_date_dir.is_dir():
                    continue
                for study_dir in study_date_dir.iterdir():
                    if not study_dir.is_dir():
                        continue
                    
                    # Check if the study has .dcm files and add to tracking if new
                    if any(study_dir.glob('*.dcm')):
                        if study_dir not in active_studies:
                            active_studies[study_dir] = now

        except Exception as e:
            logging.error(f"Error scanning for new files: {e}")

        # Process timed-out studies
        processed_studies = []
        for study_path, last_seen_time in active_studies.items():
            if now - last_seen_time > STUDY_INACTIVITY_TIMEOUT:
                logging.info(f"Study '{study_path.name}' from date '{study_path.parent.name}' is complete (inactivity timeout).")
                process_study_directory(study_path, listener_id)
                processed_studies.append(study_path)

        # Clean up processed studies from the active list
        if processed_studies:
            active_studies = {k: v for k, v in active_studies.items() if k not in processed_studies}
            if active_studies:
                logging.info(f"Remaining active studies to track: {len(active_studies)}")

        time.sleep(SCAN_INTERVAL)


def main():
    """Main entry point for the listener script."""
    parser = argparse.ArgumentParser(description="Axiom DICOM Batching Listener")
    parser.add_argument(
        '--watch-dirs',
        action='store_true',
        help='Enable directory watching mode for structured dcm4che output.'
    )
    args = parser.parse_args()

    heartbeat_manager = HeartbeatManager(LISTENER_INSTANCE_ID)
    try:
        heartbeat_manager.start()
        
        if args.watch_dirs:
            watch_directories(LISTENER_INSTANCE_ID)
        else:
            # Fallback to old logic if needed, or just error out.
            logging.error("This script is now intended to be run with the --watch-dirs flag.")
            # old_main_loop(LISTENER_INSTANCE_ID) # Or whatever the old main loop was called
            sys.exit(1)

    except KeyboardInterrupt:
        logging.info("Shutdown signal received.")
    except Exception as e:
        logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)
    finally:
        heartbeat_manager.stop()
        logging.info("Listener has been shut down.")

if __name__ == "__main__":
    main()
