
import os
import json
import subprocess
import shlex
import tempfile
import pika
import uuid
from datetime import datetime
from pydantic import BaseModel
from typing import Dict, Any, List, Union, Optional

# Structured logging
import structlog
from app.core.logging_config import configure_json_logging

# Configure logging for this service
logger = configure_json_logging("dcm4che_sender")

# Import dustbin service for confirmations
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.services.dustbin_service import dustbin_service

# Basic configuration from environment variables
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
DCM4CHE_QUEUE = "cstore_dcm4che_jobs"
DCM4CHE_PREFIX = os.environ.get("DCM4CHE_PREFIX", "/opt/dcm4che")

class CStoreJob(BaseModel):
    file_path: Optional[str] = None  # Legacy single file format
    file_paths: Optional[Union[str, List[str]]] = None  # New batch processing format
    destination_config: Dict[str, Any]
    
    def get_file_paths(self) -> List[str]:
        """Get file paths in a normalized list format."""
        if self.file_paths is not None:
            return [self.file_paths] if isinstance(self.file_paths, str) else self.file_paths
        elif self.file_path is not None:
            return [self.file_path]
        else:
            raise ValueError("Either file_path or file_paths must be provided")

def send_with_dcm4che(job: CStoreJob):
    """Uses dcm4che's storescu to send DICOM files in a single association."""
    config = job.destination_config
    
    # Get normalized file paths using the helper method
    file_paths = job.get_file_paths()

    # Get destination name for confirmation tracking
    destination_name = config.get('remote_ae_title', 'UNKNOWN_DESTINATION')
    
    # Extract verification info if present (for dustbin confirmations)
    verification_id = config.get('verification_id')
    task_id = config.get('task_id')

    logger.info("Processing dcm4che job", 
                file_count=len(file_paths), 
                destination_ae_title=config.get('remote_ae_title'),
                destination_host=config.get('remote_host'),
                destination_port=config.get('remote_port'),
                files=[os.path.basename(f) for f in file_paths[:5]],  # Show first 5 filenames
                verification_id=verification_id)

    # Validate all files exist
    for file_path in file_paths:
        if not os.path.exists(file_path):
            logger.error("File does not exist", file_path=file_path)
            raise FileNotFoundError(f"DICOM file not found: {file_path}")

    storescu_path = os.path.join(DCM4CHE_PREFIX, "bin", "storescu")
    
    # Build the command with all files - dcm4che will send them in one association
    command = [
        storescu_path,
        "-c", f"{config['remote_ae_title']}@{config['remote_host']}:{config['remote_port']}"
    ]
    
    # Add all file paths to the command
    command.extend(file_paths)

    if config.get('local_ae_title'):
        command.extend(["--bind", config['local_ae_title']])

    # Add timeouts if configured in settings (using defaults here)
    command.extend(["--connect-timeout", "5000"])
    command.extend(["--accept-timeout", "15000"])

    if config.get('tls_enabled'):
        command.append("--tls")
        logger.warning("TLS is enabled, but secret handling for dcm4che sender is not fully implemented")

    logger.info("Executing dcm4che storescu", 
                file_count=len(file_paths), 
                destination_ae_title=config.get('remote_ae_title'),
                destination_host=config.get('remote_host'),
                destination_port=config.get('remote_port'),
                command_preview=' '.join(command[:5]) + '...' if len(command) > 5 else ' '.join(command))

    try:
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True  # Raise exception on non-zero exit code
        )
        
        # Extract key metrics from dcm4che output instead of logging everything
        output_lines = process.stdout.strip().split('\n') if process.stdout.strip() else []
        
        # Look for the final summary line (e.g., "Sent 10 objects (=5.011MB) in 2.022s (=2.478MB/s)")
        summary_info = {}
        for line in output_lines:
            if "Sent " in line and " objects " in line and " in " in line:
                # Extract key metrics from summary line
                try:
                    # Parse something like: "Sent 10 objects (=5.011MB) in 2.022s (=2.478MB/s)"
                    if "objects" in line and "in" in line:
                        parts = line.split()
                        objects_sent = None
                        total_size = None
                        transfer_time = None
                        transfer_rate = None
                        
                        for i, part in enumerate(parts):
                            if part == "objects" and i > 0:
                                objects_sent = parts[i-1]
                            elif "MB)" in part:
                                total_size = part.replace("(=", "").replace(")", "")
                            elif part.endswith("s") and "." in part:
                                transfer_time = part
                            elif "MB/s)" in part:
                                transfer_rate = part.replace("(=", "").replace(")", "")
                        
                        if objects_sent:
                            summary_info["objects_sent"] = objects_sent
                        if total_size:
                            summary_info["total_size"] = total_size
                        if transfer_time:
                            summary_info["transfer_time"] = transfer_time
                        if transfer_rate:
                            summary_info["transfer_rate"] = transfer_rate
                except Exception:
                    # If parsing fails, just continue
                    pass
                break
        
        # Log success with concise summary
        logger.info("dcm4che storescu completed successfully", 
                    file_count=len(file_paths), 
                    destination_ae_title=config.get('remote_ae_title'),
                    destination_host=config.get('remote_host'),
                    destination_port=config.get('remote_port'),
                    files_sent_count=len(file_paths),  # Just the count, not full list
                    verification_id=verification_id,
                    **summary_info)  # Include parsed summary metrics
        
        # Log detailed file list at debug level only
        if len(file_paths) <= 5:
            # For small batches, include filenames in main log
            logger.debug("Files sent details", 
                        files_sent=[os.path.basename(f) for f in file_paths],
                        destination_ae_title=config.get('remote_ae_title'))
        else:
            # For large batches, just show first few + count
            sample_files = [os.path.basename(f) for f in file_paths[:3]]
            logger.debug("Files sent details (sample)", 
                        files_sent_sample=sample_files,
                        total_files_sent=len(file_paths),
                        destination_ae_title=config.get('remote_ae_title'))
        
        # Optionally log the full output at debug level for troubleshooting
        if process.stdout.strip():
            # Even for debug, truncate extremely long output
            stdout_output = process.stdout.strip()
            if len(stdout_output) > 2000:  # Limit debug output to 2000 chars
                truncated_output = stdout_output[:2000] + f"... [TRUNCATED - full output was {len(stdout_output)} characters]"
                logger.debug("dcm4che full output (truncated)", 
                            destination_ae_title=config.get('remote_ae_title'),
                            dcm4che_output=truncated_output)
            else:
                logger.debug("dcm4che full output", 
                            destination_ae_title=config.get('remote_ae_title'),
                            dcm4che_output=stdout_output)
        
        # Log transaction summary instead of individual files
        successful_files = []
        for file_path in file_paths:
            successful_files.append(file_path)
        
        logger.info("DICOM transmission completed", 
                    transaction_status="SUCCESS",
                    file_count=len(successful_files),
                    destination_ae_title=config.get('remote_ae_title'),
                    files_summary=f"{len(successful_files)} files sent successfully")
        
        # MEDICAL SAFETY: Send confirmation to dustbin verification system
        if verification_id and successful_files:
            try:
                confirmation_details = {
                    'destination_name': destination_name,
                    'transmission_timestamp': datetime.now().isoformat(),
                    'files_transmitted': len(successful_files),
                    'transmission_method': 'dcm4che_storescu',
                    'dcm4che_command': ' '.join([shlex.quote(arg) for arg in command[:5]]) + '...',  # Log command (truncated for security)
                    'files_confirmed': successful_files
                }
                
                success = dustbin_service.verify_destination_receipt(
                    verification_id=verification_id,
                    destination_name=destination_name,
                    success=True,
                    confirmation_details=confirmation_details
                )
                
                if success:
                    logger.info("MEDICAL SAFETY: Transmission confirmed for verification to dustbin system", 
                               verification_id=verification_id)
                else:
                    logger.warning("Failed to confirm transmission to dustbin system for verification", 
                                  verification_id=verification_id)
                    
            except Exception as conf_err:
                logger.error("Failed to send dustbin confirmation", error=str(conf_err))
                # Continue execution - don't fail the transmission due to confirmation failure
            
    except FileNotFoundError:
        logger.critical("storescu command not found", path=storescu_path)
        raise
    except subprocess.CalledProcessError as e:
        # Extract key error information without dumping massive output
        stderr_summary = e.stderr[:500] + "..." if e.stderr and len(e.stderr) > 500 else e.stderr
        stdout_summary = e.stdout[:200] + "..." if e.stdout and len(e.stdout) > 200 else e.stdout
        
        logger.error("dcm4che storescu failed for batch", 
                     file_count=len(file_paths),
                     return_code=e.returncode,
                     destination_ae_title=config.get('remote_ae_title'),
                     error_summary=stderr_summary or stdout_summary or "No error details",
                     files=len(file_paths))  # Just log file count, not full paths
        
        # Log full error details at debug level for troubleshooting
        if e.stderr or e.stdout:
            logger.debug("dcm4che full error output",
                        stderr=e.stderr,
                        stdout=e.stdout,
                        destination_ae_title=config.get('remote_ae_title'))
        
        # MEDICAL SAFETY: Send failure confirmation to dustbin verification system
        if verification_id:
            try:
                failure_details = {
                    'destination_name': destination_name,
                    'failure_timestamp': datetime.now().isoformat(),
                    'files_attempted': len(file_paths),
                    'transmission_method': 'dcm4che_storescu',
                    'error_code': e.returncode,
                    'error_message': str(e.stderr) if e.stderr else 'Unknown error',
                    'files_failed': file_paths
                }
                
                dustbin_service.verify_destination_receipt(
                    verification_id=verification_id,
                    destination_name=destination_name,
                    success=False,
                    confirmation_details=failure_details
                )
                
                logger.info("MEDICAL SAFETY: Transmission failure confirmed for verification to dustbin system", 
                           verification_id=verification_id)
                
            except Exception as conf_err:
                logger.error("Failed to send dustbin failure confirmation", error=str(conf_err))
        
        raise
    except Exception as e:
        logger.error("Unexpected error during dcm4che execution", 
                     error=str(e), 
                     files=file_paths)
        raise

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        job = CStoreJob(**data)
        
        # Log detailed message receipt information
        logger.info("Received DICOM transmission job", 
                    queue=DCM4CHE_QUEUE,
                    file_count=len(job.get_file_paths()),
                    destination_ae_title=job.destination_config.get('remote_ae_title'),
                    destination_host=job.destination_config.get('remote_host'),
                    destination_port=job.destination_config.get('remote_port'),
                    verification_id=job.destination_config.get('verification_id'))
        
        # Execute the job
        send_with_dcm4che(job)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        logger.info("DICOM transmission job completed", 
                    queue=DCM4CHE_QUEUE,
                    destination_ae_title=job.destination_config.get('remote_ae_title'),
                    file_count=len(job.get_file_paths()),
                    status="ACKNOWLEDGED")
    except Exception as e:
        logger.error("Failed to process DICOM transmission job", 
                     error=str(e),
                     queue=DCM4CHE_QUEUE)
        # In a real system, you might want to requeue with a delay or send to a dead-letter queue
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    logger.info("Starting dcm4che sender")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=DCM4CHE_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=DCM4CHE_QUEUE, on_message_callback=callback)

    logger.info("Waiting for messages. To exit press CTRL+C", queue=DCM4CHE_QUEUE)
    channel.start_consuming()

if __name__ == "__main__":
    main()
