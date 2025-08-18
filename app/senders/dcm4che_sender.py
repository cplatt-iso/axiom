
import os
import json
import subprocess
import shlex
import tempfile
import pika
from pydantic import BaseModel
from typing import Dict, Any, List, Union, Optional

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

    print(f"Processing dcm4che job for {len(file_paths)} file(s)")
    print(f"Destination: {config.get('remote_ae_title')}")

    # Validate all files exist
    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"ERROR: File does not exist: {file_path}")
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
        print("WARNING: TLS is enabled, but secret handling for dcm4che sender is not fully implemented.")

    print(f"Executing dcm4che command for {len(file_paths)} files in single association:")
    print(f"Command: {' '.join(shlex.quote(c) for c in command)}")

    try:
        process = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True  # Raise exception on non-zero exit code
        )
        print(f"dcm4che storescu completed successfully for {len(file_paths)} files in single association.")
        print(f"Output: {process.stdout}")
        
        # Log each successful file
        for file_path in file_paths:
            print(f"Successfully sent: {file_path}")
            
    except FileNotFoundError:
        print(f"CRITICAL: storescu command not found at {storescu_path}")
        raise
    except subprocess.CalledProcessError as e:
        print(f"ERROR: dcm4che storescu failed for batch of {len(file_paths)} files.")
        print(f"Return Code: {e.returncode}")
        print(f"Stderr: {e.stderr}")
        print(f"Stdout: {e.stdout}")
        print("Files that were attempted:")
        for file_path in file_paths:
            print(f"  - {file_path}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred during dcm4che execution: {e}")
        print(f"Files involved: {file_paths}")
        raise

def callback(ch, method, properties, body):
    try:
        print(f"Received message from {DCM4CHE_QUEUE}")
        data = json.loads(body)
        job = CStoreJob(**data)
        send_with_dcm4che(job)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("Job completed and acknowledged.")
    except Exception as e:
        print(f"Failed to process message: {e}")
        # In a real system, you might want to requeue with a delay or send to a dead-letter queue
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    print("Starting dcm4che sender...")
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=DCM4CHE_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=DCM4CHE_QUEUE, on_message_callback=callback)

    print(f"[*] Waiting for messages on {DCM4CHE_QUEUE}. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()
