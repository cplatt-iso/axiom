import socket
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_connectivity(host, port):
    """
    Attempts to establish a socket connection to a given host and port.
    """
    logging.info(f"Attempting to connect to {host} on port {port}...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)  # Set a timeout of 5 seconds
            s.connect((host, port))
        logging.info(f"Successfully connected to {host}:{port}. Connection is open.")
        return True
    except socket.timeout:
        logging.error(f"Connection to {host}:{port} timed out.")
        return False
    except ConnectionRefusedError:
        logging.error(f"Connection to {host}:{port} was refused. The service may not be running or is blocked.")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while connecting to {host}:{port}: {e}", exc_info=True)
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
