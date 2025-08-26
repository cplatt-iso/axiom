#!/usr/bin/env python3

import os
import sys
import subprocess
import threading
import re
import structlog
import signal
import time
import json

# Configure direct JSON logging that matches other containers
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO level
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger("dcm4che_storescp")

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
         
        # C-STORE events: "DCM4CHE<-YEETER(5) >> 1:C-STORE-RQ[...]"
        (r'^([A-Z_]+)<-([A-Z_]+)\((\d+)\)\s+(>>|<<)\s+(\d+):C-STORE-(.+)$',
         lambda m: {
             'event': 'DICOM C-STORE',
             'called_ae': m.group(1),
             'calling_ae': m.group(2), 
             'association_id': m.group(3),
             'direction': 'received' if m.group(4) == '>>' else 'sent',
             'message_id': m.group(5),
             'operation': f'C-STORE-{m.group(6)}'
         }),
    ]
    
    for pattern, extractor in patterns:
        match = re.match(pattern, line)
        if match:
            return extractor(match)
    
    # Default: treat as unstructured log  
    return {
        'event': 'dcm4che raw log',
        'event': 'dcm4che raw log',
        'message': line
    }

def read_stream(stream, stream_name):
    """Read from a stream and log structured output."""
    for line in iter(stream.readline, b''):
        if line:
            line_str = line.decode('utf-8', errors='ignore').strip()
            if line_str:
                try:
                    parsed = parse_dcm4che_log_line(line_str)
                    
                    # Check if parsed is None or handle it properly
                    if parsed is None:
                        logger.debug("dcm4che raw output", message=line_str, stream=stream_name)
                        continue
                        
                    # Log based on event type
                    if parsed.get('event') == 'dcm4che raw log':
                        logger.debug("dcm4che raw output", message=line_str, stream=stream_name)
                    else:
                        # Use 'dicom_event' instead of 'event' to avoid parameter conflicts
                        event_data = parsed.copy() if parsed else {}
                        event_type = event_data.pop('event', 'dcm4che structured event')
                        logger.info(event_type, stream=stream_name, **event_data)
                        
                except Exception as e:
                    logger.error("Error parsing dcm4che output", 
                               error=str(e), stream=stream_name, raw_line=line_str)

def log_storescp_output(process, ae_title, port):
    """Start threads to read stdout and stderr."""
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
    """Start dcm4che storescp with structured logging."""
    
    # Get configuration from environment
    ae_title = os.getenv('AXIOM_AETITLE', 'DCM4CHE')
    port = 11114  # Default port, could be made configurable
    directory = '/dicom_data/incoming'
    filepath_pattern = '{00080020}/{0020000d}/{00080018}.dcm'
    
    # Construct storescp command (remove -v as it's not supported)
    command = [
        '/opt/dcm4che/bin/storescp',
        '-b', f'{ae_title}:{port}',
        '--directory', directory,
        '--filepath', filepath_pattern
    ]
    
    logger.info("Starting dcm4che storescp with structured logging", 
               ae_title=ae_title,
               port=port,  
               directory=directory,
               filepath_pattern=filepath_pattern,
               command=' '.join(command))
    
    # Start the storescp process
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, 
        bufsize=1,
        universal_newlines=False
    )
    
    # Start monitoring output
    stdout_thread, stderr_thread = log_storescp_output(process, ae_title, port)
    
    logger.info("dcm4che storescp started", 
               pid=process.pid,
               ae_title=ae_title,
               port=port)
    
    # Handle shutdown gracefully
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal - terminating storescp", signal=signum)
        process.terminate()
        
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Wait for process to complete
        return_code = process.wait()
        logger.info("dcm4che storescp stopped", 
                   return_code=return_code,
                   ae_title=ae_title)
        return return_code
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt - stopping storescp")
        process.terminate()
        return process.wait()

if __name__ == "__main__":
    sys.exit(main())
