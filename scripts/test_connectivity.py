import socket
import sys
import logging
import os

# Add the project root to the Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import structlog
    from app.core.logging_config import configure_json_logging
    configure_json_logging("test_connectivity")
    try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)

def check_connectivity(host, port):
    """
    Attempts to establish a socket connection to a given host and port.
    """
    logger.info(f"Testing connectivity to {host}:{port}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)  # Set a timeout of 5 seconds
            s.connect((host, port))
        logger.info(f"Successfully connected to {host}:{port}. Connection is open.")
        return True
    except socket.timeout:
        logger.error(f"Connection to {host}:{port} timed out.")
        return False
    except ConnectionRefusedError:
        logger.error(f"Connection to {host}:{port} was refused. The service may not be running or is blocked.")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while connecting to {host}:{port}: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python test_connectivity.py <host> <port>")
        sys.exit(1)

    target_host = sys.argv[1]
    try:
        target_port = int(sys.argv[2])
    except ValueError:
        print("Error: Port must be an integer.")
        sys.exit(1)

    if check_connectivity(target_host, target_port):
        sys.exit(0)
    else:
        sys.exit(1)
