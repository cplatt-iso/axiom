#!/usr/bin/env python

import os
import sys
import time
import subprocess
import threading
from collections import defaultdict, deque
from pathlib import Path
import logging

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [AssociationBatcher] - %(message)s')

DICOM_DIR = "/dicom_data/incoming"
BATCH_TIMEOUT = 5.0  # Wait 5 seconds after the last file before processing the batch
PROCESS_ASSOCIATION_SCRIPT = "/app/scripts/process_association.py"

class AssociationBatcher:
    def __init__(self):
        self.file_queue = deque()
        self.last_file_time = None
        self.lock = threading.Lock()
        self.running = True
        
    def add_file(self, filepath):
        """Add a file to the current association batch."""
        with self.lock:
            self.file_queue.append(filepath)
            self.last_file_time = time.time()
            logging.info(f"Added file to batch: {filepath} (batch size: {len(self.file_queue)})")
    
    def process_batch_if_ready(self):
        """Process the current batch if enough time has passed since the last file."""
        with self.lock:
            if not self.file_queue:
                return
                
            if self.last_file_time and (time.time() - self.last_file_time) >= BATCH_TIMEOUT:
                files_to_process = list(self.file_queue)
                self.file_queue.clear()
                logging.info(f"Processing batch of {len(files_to_process)} files")
                
                try:
                    # Call our process_association.py script with all the files
                    cmd = [PROCESS_ASSOCIATION_SCRIPT] + files_to_process
                    subprocess.run(cmd, check=True)
                    logging.info(f"Successfully processed batch of {len(files_to_process)} files")
                except subprocess.CalledProcessError as e:
                    logging.error(f"Error processing batch: {e}")
                except Exception as e:
                    logging.error(f"Unexpected error processing batch: {e}")
    
    def monitor_loop(self):
        """Continuously monitor for batches that are ready to process."""
        while self.running:
            try:
                self.process_batch_if_ready()
                time.sleep(1.0)  # Check every second
            except Exception as e:
                logging.error(f"Error in monitor loop: {e}")
                time.sleep(1.0)
    
    def stop(self):
        """Stop the monitoring loop."""
        self.running = False

def monitor_directory():
    """Monitor the DICOM directory for new files."""
    batcher = AssociationBatcher()
    
    # Start the batch processing thread
    monitor_thread = threading.Thread(target=batcher.monitor_loop, daemon=True)
    monitor_thread.start()
    
    # Keep track of files we've already seen
    seen_files = set()
    
    logging.info(f"Starting to monitor directory: {DICOM_DIR}")
    
    try:
        while True:
            try:
                # Scan for new files (DICOM files may not have .dcm extension)
                dicom_path = Path(DICOM_DIR)
                if dicom_path.exists():
                    for file_path in dicom_path.iterdir():
                        # Skip directories and files we've already seen
                        if file_path.is_file() and str(file_path) not in seen_files:
                            # Skip temporary files (dcm4che creates .part files during transfer)
                            if not file_path.name.endswith('.part'):
                                seen_files.add(str(file_path))
                                batcher.add_file(str(file_path))
                
                time.sleep(0.5)  # Check for new files every 500ms
                
            except Exception as e:
                logging.error(f"Error scanning directory: {e}")
                time.sleep(1.0)
                
    except KeyboardInterrupt:
        logging.info("Received interrupt signal, stopping...")
        batcher.stop()
        return

def start_storescp():
    """Start the dcm4che storescp service."""
    ae_title = os.getenv("AXIOM_AETITLE", "DCM4CHE")
    cmd = [
        "/opt/dcm4che/bin/storescp",
        "-b", f"{ae_title}:11114",
        "--directory", DICOM_DIR
    ]
    
    logging.info(f"Starting storescp with command: {' '.join(cmd)}")
    
    try:
        # Start storescp in the background
        process = subprocess.Popen(cmd)
        logging.info(f"storescp started with PID: {process.pid}")
        return process
    except Exception as e:
        logging.error(f"Failed to start storescp: {e}")
        raise

def main():
    """Main entry point."""
    logging.info("Starting dcm4che listener with association batching")
    
    storescp_process = None
    try:
        # Start storescp in the background
        storescp_process = start_storescp()
        
        # Give storescp a moment to start up
        time.sleep(2)
        
        # Start monitoring the directory for incoming files
        monitor_directory()
        
    except KeyboardInterrupt:
        logging.info("Received interrupt, shutting down...")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return 1
    finally:
        # Clean up
        if storescp_process is not None and storescp_process.poll() is None:
            logging.info("Terminating storescp process...")
            storescp_process.terminate()
            storescp_process.wait()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
