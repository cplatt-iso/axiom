#!/usr/bin/env python

import os
import sys
import subprocess
import time
import re
import threading
import structlog
from queue import Queue, Empty

# --- Add app to Python path ---
# This allows us to import from app.db, app.crud, etc.
sys.path.append('/app')

from sqlalchemy import text
from app.db.session import SessionLocal
from app.crud import crud_dimse_listener_config
from app.core.logging_config import configure_json_logging

# --- Setup Structured Logging ---
configure_json_logging(service_name="dcm4che_listener")
logger = structlog.get_logger(__name__)

def get_listener_config(db, instance_id):
    """Fetches the listener configuration from the database."""
    logger.info("Querying database for listener configuration", instance_id=instance_id)
    config = crud_dimse_listener_config.get_by_instance_id(db, instance_id=instance_id)
    if not config:
        logger.error("No configuration found for instance_id", instance_id=instance_id)
        sys.exit(1)
    if not config.is_enabled:
        logger.warning("Listener is disabled - exiting", 
                      listener_name=config.name, 
                      instance_id=instance_id)
        sys.exit(0)
    if config.listener_type != 'dcm4che':
        logger.error("Configuration is not dcm4che type", 
                    listener_name=config.name,
                    listener_type=config.listener_type)
        sys.exit(1)
    logger.info("Found dcm4che listener configuration", 
               listener_name=config.name,
               port=config.port,
               ae_title=config.ae_title)
    return config

def parse_dcm4che_log_line(line):
    """Parse dcm4che log output and convert to structured format."""
    line = line.strip()
    if not line:
        return None
    
    # Parse different dcm4che log patterns
    patterns = [
        # Connection events: "13:28:37.953 INFO  - Accept connection Socket[...]"
        (r'^(\d{2}:\d{2}:\d{2}\.\d+)\s+(INFO|DEBUG|WARN|ERROR)\s+-\s+Accept connection (.+)$', 
         lambda m: {
             'timestamp': m.group(1),
             'level': m.group(2).lower(),
             'event': 'Connection accepted',
             'connection_info': m.group(3)
         }),
        
        # Association events: "DCM4CHE<-YEETER(5) >> A-ASSOCIATE-RQ"  
        (r'^([A-Z_]+)<-([A-Z_]+)\((\d+)\)\s+(>>|<<)\s+(.+)$',
         lambda m: {
             'event': 'DICOM association event',
             'called_ae': m.group(1),
             'calling_ae': m.group(2),
             'association_id': m.group(3),
             'direction': 'received' if m.group(4) == '>>' else 'sent',
             'message_type': m.group(5)
         }),
         
        # State changes: "/172.22.0.23:11114<-/172.22.0.1:41940(5): enter state: Sta2"
        (r'^(.+?)\((\d+)\):\s+enter state:\s+(.+)$',
         lambda m: {
             'event': 'State change',
             'connection': m.group(1),
             'association_id': m.group(2), 
             'new_state': m.group(3)
         }),
    ]
    
    for pattern, extractor in patterns:
        match = re.match(pattern, line)
        if match:
            return extractor(match)
    
    # Default: treat as unstructured log
    return {
        'event': 'dcm4che log',
        'message': line
    }

def log_dcm4che_output(process, config):
    """Monitor dcm4che process output and convert to structured logging."""
    
    def read_stream(stream, stream_name):
        """Read from stdout/stderr stream and log structured output."""
        while True:
            try:
                line = stream.readline()
                if not line:  # EOF
                    break
                
                line = line.decode('utf-8', errors='ignore').rstrip()
                if not line:
                    continue
                
                # Parse and structure the log line
                parsed = parse_dcm4che_log_line(line)
                if parsed:
                    # Add context info
                    parsed['service'] = 'dcm4che_listener'
                    parsed['ae_title'] = config.ae_title
                    parsed['port'] = config.port
                    parsed['stream'] = stream_name
                    
                    # Log at appropriate level
                    level = parsed.get('level', 'info')
                    if level == 'error':
                        logger.error("dcm4che message", **parsed)
                    elif level == 'warn':
                        logger.warning("dcm4che message", **parsed)
                    elif level == 'debug' or 'State change' in parsed.get('event', ''):
                        logger.debug("dcm4che message", **parsed)
                    else:
                        logger.info("dcm4che message", **parsed)
                        
            except Exception as e:
                logger.error("Error processing dcm4che output", 
                           error=str(e), stream=stream_name)
    
    # Start threads to read stdout and stderr
    stdout_thread = threading.Thread(
        target=read_stream, 
        args=(process.stdout, 'stdout'),
        daemon=True
    )
    stderr_thread = threading.Thread(
        target=read_stream,
        args=(process.stderr, 'stderr'), 
        daemon=True
    )
    
    stdout_thread.start()
    stderr_thread.start()
    
    return stdout_thread, stderr_thread

def main():
    """
    Main entry point. Fetches config from DB and starts the dcm4che storescp process.
    """
    instance_id = os.getenv("AXIOM_INSTANCE_ID")
    if not instance_id:
        logger.error("AXIOM_INSTANCE_ID environment variable not set")
        sys.exit(1)

    db = None
    try:
        # Wait for DB to be ready
        max_retries = 10
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                db = SessionLocal()
                # A simple query to check if the DB is responsive
                db.execute(text("SELECT 1"))
                logger.info("Database connection successful")
                break
            except Exception as e:
                logger.warning("Database not ready - retrying", 
                             attempt=attempt + 1, 
                             max_attempts=max_retries,
                             error=str(e),
                             retry_delay=retry_delay)
                if db:
                    db.close()
                time.sleep(retry_delay)
        else:
            logger.error("Could not connect to database after retries")
            sys.exit(1)

        config = get_listener_config(db, instance_id)

        # Start the watchdog notifier in the background
        notifier_command = ['python', '/app/scripts/watch_and_notify.py']
        logger.info("Starting watchdog notifier", command=' '.join(notifier_command))
        notifier_process = subprocess.Popen(notifier_command)

        # Construct the storescp command
        command = [
            'storescp',
            '-b', f'{config.ae_title}:{config.port}',
            '--directory', '/dicom_data/incoming'
        ]

        # Add TLS options if enabled in the config
        if config.tls_enabled:
            logger.warning("TLS enabled in config but not yet implemented for dcm4che auto-configuration")

        logger.info("Starting dcm4che storescp listener", 
                   command=' '.join(command),
                   ae_title=config.ae_title,
                   port=config.port,
                   directory='/dicom_data/incoming')

        # Execute the command with output capture for structured logging
        process = subprocess.Popen(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            bufsize=1,  # Line buffered
            universal_newlines=False  # We'll handle encoding
        )
        
        # Start monitoring the process output
        stdout_thread, stderr_thread = log_dcm4che_output(process, config)
        
        logger.info("dcm4che storescp listener started", 
                   pid=process.pid,
                   ae_title=config.ae_title,
                   port=config.port)

        # Wait for the process to complete
        return_code = process.wait()
        
        logger.info("dcm4che storescp listener stopped", 
                   return_code=return_code,
                   ae_title=config.ae_title)

    except Exception as e:
        logger.error("Unexpected error occurred", error=str(e), exc_info=True)
        sys.exit(1)
    finally:
        if db:
            db.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()
