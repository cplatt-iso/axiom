"""
Service naming standardization for Axiom Flow
"""
import os
from typing import Dict


def get_axiom_service_name() -> str:
    """
    Get standardized service name based on container or component.
    
    Maps container names and components to clean service identifiers for logging.
    """
    
    # Try to get from environment first (allows override)
    env_service = os.getenv('AXIOM_SERVICE_NAME')
    if env_service:
        return env_service
        
    # Try to get from container name
    container_name = os.getenv('HOSTNAME') or os.getenv('CONTAINER_NAME', '')
    
    # Service mapping from container names to clean service names
    service_mapping: Dict[str, str] = {
        # Core services
        'axiom-api': 'axiom-api',
        'axiom-worker': 'axiom-worker', 
        'axiom-beat': 'axiom-beat',
        'axiom-db': 'axiom-db',
        'axiom-rabbitmq': 'axiom-rabbitmq',
        'axiom-redis': 'axiom-redis',
        
        # Listeners
        'axiom-storescp-1': 'axiom-storescp',
        'axiom-storescp-2': 'axiom-storescp',
        'axiom-mllp-1': 'axiom-mllp',
        'axiom-dcm4che-1': 'axiom-dcm4che-listener',
        
        # Senders  
        'axiom-dcm4che-sender': 'axiom-dcm4che-sender',
        'axiom-pynetdicom-sender': 'axiom-pynetdicom-sender',
        
        # Infrastructure
        'axiom-elasticsearch': 'axiom-elasticsearch',
        'axiom-fluent-bit': 'axiom-fluent-bit',
        'axiom-docs': 'axiom-docs',
        'axiom-dustbin-verification': 'axiom-dustbin-verification',
        
        # Legacy fallbacks (during transition)
        'dicom_processor_api': 'axiom-api',
        'dicom_processor_worker': 'axiom-worker',
        'dicom_processor_beat': 'axiom-beat',
    }
    
    # Clean up container name (remove leading slash if present)
    clean_container = container_name.lstrip('/')
    
    # Look up in mapping
    service_name = service_mapping.get(clean_container)
    
    if service_name:
        return service_name
        
    # Fallback: use clean container name or default
    if clean_container:
        return clean_container
        
    return 'axiom-unknown'


def get_axiom_instance_id() -> str:
    """Get instance identifier for multi-instance services."""
    instance_id = os.getenv('AXIOM_INSTANCE_ID')
    if instance_id:
        return instance_id
        
    # Extract instance from container name if available
    container_name = os.getenv('HOSTNAME') or os.getenv('CONTAINER_NAME', '')
    if '-' in container_name and container_name.endswith(('-1', '-2', '-3')):
        return container_name.split('-')[-1]
        
    return '1'
