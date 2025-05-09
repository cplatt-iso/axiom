# app/worker/destination_handler.py

import structlog
from typing import List, Dict, Any, Optional, Set

from app.db.models import Rule
# Assuming StorageBackendConfig is the base model for all destination configs
from app.db.models.storage_backend_config import StorageBackendConfig 
# If you have an enum for backend types that's used in the model, import it too.
# from app.services.storage_backends.base_backend import StorageBackendType # Example

logger = structlog.get_logger(__name__)

def collect_destinations_for_rule(rule: Rule) -> List[Dict[str, Any]]:
    """
    Collects and formats destination information from a single rule.
    Only includes destinations that are enabled.

    Args:
        rule: The Rule object, expected to have its 'destinations' relationship loaded.

    Returns:
        A list of dictionaries, where each dictionary represents an enabled destination
        with 'id', 'name', and 'backend_type'.
    """
    collected_destinations: List[Dict[str, Any]] = []
    
    # The 'rule.destinations' relationship should provide a list of StorageBackendConfig objects
    rule_destinations: List[StorageBackendConfig] = getattr(rule, 'destinations', [])

    if not rule_destinations:
        logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) has no destinations configured.")
        return []

    logger.debug(f"Collecting destinations for rule '{rule.name}' (ID: {rule.id}). Found {len(rule_destinations)} potential destinations.")
    
    for dest_config_obj in rule_destinations:
        if not isinstance(dest_config_obj, StorageBackendConfig): # Basic type check
            logger.warning(f"Rule '{rule.name}': Encountered an item in 'destinations' that is not a StorageBackendConfig. Skipping.")
            continue

        if dest_config_obj.is_enabled:
            try:
                # Resolve backend_type to a string (handling direct string or enum.value)
                backend_type_value = dest_config_obj.backend_type
                resolved_backend_type_str: Optional[str] = None

                if isinstance(backend_type_value, str):
                    resolved_backend_type_str = backend_type_value
                # Example if backend_type is an enum like StorageBackendType:
                # elif isinstance(backend_type_value, StorageBackendType):
                #     resolved_backend_type_str = backend_type_value.value
                elif hasattr(backend_type_value, 'value') and isinstance(getattr(backend_type_value, 'value', None), str):
                    # Generic fallback for enum-like objects with a .value attribute
                    resolved_backend_type_str = backend_type_value.value
                else:
                    logger.error(f"Destination ID {dest_config_obj.id} ('{dest_config_obj.name}') has an "
                                 f"unresolvable backend_type (type: {type(backend_type_value)}). Skipping.")
                    continue
                
                if resolved_backend_type_str is None: # Should be caught by the else above
                    logger.error(f"Destination ID {dest_config_obj.id} ('{dest_config_obj.name}') failed to resolve backend_type to string. Skipping.")
                    continue

                dest_info = {
                    "id": dest_config_obj.id,
                    "name": dest_config_obj.name,
                    "backend_type": resolved_backend_type_str 
                }
                collected_destinations.append(dest_info)
                logger.debug(f"Added enabled destination: ID {dest_config_obj.id}, Name '{dest_config_obj.name}', Type '{resolved_backend_type_str}' from rule '{rule.name}'.")
            
            except AttributeError as attr_err:
                dest_id_log = getattr(dest_config_obj, 'id', 'Unknown ID')
                logger.error(f"Error accessing attributes for destination object (ID: {dest_id_log}) "
                             f"linked to rule '{rule.name}': {attr_err}.", exc_info=True)
            except Exception as e:
                dest_name_log = getattr(dest_config_obj, 'name', 'Unknown Name')
                logger.error(f"Unexpected error processing destination '{dest_name_log}' from rule '{rule.name}': {e}", exc_info=True)
        else:
            logger.debug(f"Skipping disabled destination: ID {dest_config_obj.id}, Name '{dest_config_obj.name}' from rule '{rule.name}'.")
            
    return collected_destinations


def finalize_and_deduplicate_destinations(
    all_collected_destinations: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Deduplicates a list of destination information dictionaries based on 'id'.

    Args:
        all_collected_destinations: A list of destination dictionaries, potentially with duplicates.

    Returns:
        A list of unique destination dictionaries.
    """
    unique_destinations: List[Dict[str, Any]] = []
    seen_dest_ids: Set[int] = set()

    if not all_collected_destinations:
        return []

    for dest_info in all_collected_destinations:
        dest_id = dest_info.get("id")
        
        # Ensure dest_id is an int and that the dict is properly formed
        if not isinstance(dest_id, int):
            logger.warning(f"Found destination info with missing or invalid ID: {dest_info}. Skipping.")
            continue
        
        if dest_id not in seen_dest_ids:
            unique_destinations.append(dest_info)
            seen_dest_ids.add(dest_id)
        else:
            logger.debug(f"Duplicate destination ID {dest_id} ('{dest_info.get('name', 'N/A')}') encountered. Keeping first instance.")
            
    logger.info(f"Finalized destinations: {len(unique_destinations)} unique destinations from {len(all_collected_destinations)} collected.")
    return unique_destinations
