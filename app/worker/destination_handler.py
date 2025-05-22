# app/worker/destination_handler.py

import structlog
from typing import List, Dict, Any, Optional, Set

from app.schemas.rule import Rule as RuleSchema
# --- CHANGE THIS IMPORT ---
# We need the common Pydantic base class for instance checking
from app.schemas.storage_backend_config import StorageBackendConfigBase # Import the Pydantic base schema

logger = structlog.get_logger(__name__)

def collect_destinations_for_rule(rule: RuleSchema) -> List[Dict[str, Any]]:
    """
    Collects and formats destination information from a single Pydantic Rule schema.
    Only includes destinations that are enabled.

    Args:
        rule: The RuleSchema object (Pydantic model), which contains a list of
              Pydantic storage backend config schemas in its 'destinations' field.

    Returns:
        A list of dictionaries, where each dictionary represents an enabled destination
        with 'id', 'name', and 'backend_type'.
    """
    collected_destinations: List[Dict[str, Any]] = []

    # rule.destinations will be a list of objects that are instances of one of the
    # concrete types in the StorageBackendConfigRead Union, all of which
    # should inherit from StorageBackendConfigBase.
    rule_destinations_schemas: List[Any] = rule.destinations # Type as List[Any] for loop, or use the Union if preferred for stricter typing before check

    if not rule_destinations_schemas:
        logger.debug(f"Rule '{rule.name}' (ID: {rule.id}) has no destinations configured in its Pydantic schema.")
        return []

    logger.debug(f"Collecting destinations for Pydantic rule '{rule.name}' (ID: {rule.id}). Found {len(rule_destinations_schemas)} potential Pydantic destination schemas.")

    for i, dest_schema_obj in enumerate(rule_destinations_schemas):
        log_ctx = logger.bind(dest_index=i, rule_id=rule.id, rule_name=rule.name, dest_id_from_schema=getattr(dest_schema_obj, 'id', 'N/A'))

        # Your diagnostic logs from previous iteration confirmed the object type
        # was e.g., <class 'app.schemas.storage_backend_config.StorageBackendConfigRead_CStore'>
        # which IS a subclass of StorageBackendConfigBase.

        if dest_schema_obj is None:
            log_ctx.warning("Encountered a 'None' item in the 'destinations' list of the Pydantic Rule schema. Skipping.")
            continue

        # --- THIS IS THE CORRECTED isinstance CHECK ---
        if not isinstance(dest_schema_obj, StorageBackendConfigBase):
            log_ctx.warning(
                "Item in Pydantic rule's 'destinations' list is not an instance of Pydantic StorageBackendConfigBase. Skipping.",
                obj_type_repr_on_fail=str(type(dest_schema_obj))
            )
            continue
        # --- END OF CORRECTION ---

        # Now, dest_schema_obj is confirmed to be an instance of StorageBackendConfigBase (or one of its subclasses)
        if dest_schema_obj.is_enabled:
            try:
                # backend_type may not be present on StorageBackendConfigBase, so use getattr to safely access it.
                backend_type_value = getattr(dest_schema_obj, "backend_type", None)
                resolved_backend_type_str: Optional[str] = None

                if isinstance(backend_type_value, str):
                    resolved_backend_type_str = backend_type_value
                else:
                    log_ctx.error(
                        f"Pydantic Destination Schema ID {getattr(dest_schema_obj, 'id', 'Unknown ID')} ('{getattr(dest_schema_obj, 'name', 'Unknown Name')}') has an "
                        f"unresolvable backend_type (expected str, got type: {type(backend_type_value).__name__}). Skipping."
                    )
                    continue

                if resolved_backend_type_str is None: # Should be caught by the above
                    log_ctx.error(f"Pydantic Destination Schema ID {getattr(dest_schema_obj, 'id', 'Unknown ID')} ('{getattr(dest_schema_obj, 'name', 'Unknown Name')}') failed to resolve backend_type to string. Skipping.")
                    continue

                dest_info = {
                    "id": getattr(dest_schema_obj, "id", None),
                    "name": getattr(dest_schema_obj, "name", None),
                    "backend_type": resolved_backend_type_str
                }
                collected_destinations.append(dest_info)
                log_ctx.debug(f"Added enabled Pydantic destination: ID {getattr(dest_schema_obj, 'id', 'Unknown ID')}, Name '{getattr(dest_schema_obj, 'name', 'Unknown Name')}', Type '{resolved_backend_type_str}'.")

            except AttributeError as attr_err:
                log_ctx.error(
                    f"AttributeError accessing attributes for Pydantic destination schema (ID: {getattr(dest_schema_obj, 'id', 'Unknown ID')}) "
                    f": {attr_err}.", exc_info=True
                )
            except Exception as e:
                log_ctx.error(
                    f"Unexpected error processing Pydantic destination schema (Name: '{getattr(dest_schema_obj, 'name', 'Unknown Name')}')"
                    f": {e}", exc_info=True
                )
        else:
            log_ctx.debug(
                f"Skipping disabled Pydantic destination schema: ID {getattr(dest_schema_obj, 'id', 'Unknown ID')}, Name '{getattr(dest_schema_obj, 'name', 'Unknown Name')}'."
            )

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