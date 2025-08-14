#!/usr/bin/env python

import os
import sys
import subprocess
import logging
import time

# --- Add app to Python path ---
# This allows us to import from app.db, app.crud, etc.
sys.path.append('/app')

from sqlalchemy import text
from app.db.session import SessionLocal
from app.crud import crud_dimse_listener_config

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_listener_config(db, instance_id):
    """Fetches the listener configuration from the database."""
    logging.info(f"Querying database for listener configuration with instance_id: {instance_id}")
    config = crud_dimse_listener_config.get_by_instance_id(db, instance_id=instance_id)
    if not config:
        logging.error(f"No configuration found for instance_id '{instance_id}'. Cannot start listener.")
        sys.exit(1)
    if not config.is_enabled:
        logging.warning(f"Listener '{config.name}' (instance_id: {instance_id}) is disabled. Exiting.")
        sys.exit(0)
    if config.listener_type != 'dcm4che':
        logging.error(f"Configuration '{config.name}' is not of type 'dcm4che'. This script can only start dcm4che listeners.")
        sys.exit(1)
    logging.info(f"Found configuration: '{config.name}' on port {config.port} for AE Title '{config.ae_title}'")
    return config

def main():
    """
    Main entry point. Fetches config from DB and starts the dcm4che storescp process.
    """
    instance_id = os.getenv("AXIOM_INSTANCE_ID")
    if not instance_id:
        logging.error("AXIOM_INSTANCE_ID environment variable not set. Cannot determine which listener to start.")
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
                logging.info("Database connection successful.")
                break
            except Exception as e:
                logging.warning(f"Database not ready (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay} seconds...")
                if db:
                    db.close()
                time.sleep(retry_delay)
        else:
            logging.error("Could not connect to the database after several retries. Exiting.")
            sys.exit(1)

        config = get_listener_config(db, instance_id)

        # Start the watchdog notifier in the background
        notifier_command = ['python', '/app/scripts/watch_and_notify.py']
        logging.info(f"Starting watchdog notifier with command: {' '.join(notifier_command)}")
        notifier_process = subprocess.Popen(notifier_command)

        # Construct the storescp command
        # The dcm4che user runs this, so paths should be accessible by it.
        command = [
            'storescp',
            '-b', f'{config.ae_title}:{config.port}',
            '--directory', '/dicom_data/incoming'
        ]

        # Add TLS options if enabled in the config
        # NOTE: This part is a placeholder. DCM4CHE requires a truststore and keystore.
        # You would need a mechanism to generate these from your secrets and place them
        # in the container for dcm4che to use. This is a non-trivial step.
        if config.tls_enabled:
            logging.warning("TLS is enabled in config, but this script does not yet support auto-generating keystores/truststores for dcm4che. Starting without TLS.")
            # Example of what would be needed:
            # command.extend([
            #     '--tls-need-client-auth',
            #     '--tls-keystore', '/path/to/keystore.p12',
            #     '--tls-keystore-pass', 'your_password',
            #     '--tls-truststore', '/path/to/truststore.p12',
            #     '--tls-truststore-pass', 'your_password'
            # ])

        logging.info(f"Starting dcm4che storescp with command: {' '.join(command)}")

        # Execute the command
        process = subprocess.Popen(command, stdout=sys.stdout, stderr=sys.stderr)
        process.wait()

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if db:
            db.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
