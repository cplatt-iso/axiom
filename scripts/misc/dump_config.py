import requests
import json
import os
import logging
from typing import List, Dict, Any, Optional, Tuple

# --- Configuration ---
AXIOM_API_BASE_URL = os.environ.get("AXIOM_API_BASE_URL", "http://localhost:8001/api/v1")
AXIOM_API_KEY = os.environ.get("AXIOM_API_KEY")
OUTPUT_FILE = "axiom_config_seed.json"
REQUEST_TIMEOUT = 20 # Increased timeout for potentially larger lists
PAGE_LIMIT = 200 # How many items to fetch per page

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("requests").setLevel(logging.WARNING) # Quieten requests library logging
logging.getLogger("urllib3").setLevel(logging.WARNING)

if not AXIOM_API_KEY:
    logging.error("Error: AXIOM_API_KEY environment variable not set.")
    exit(1)

HEADERS = {
    "Authorization": f"Api-Key {AXIOM_API_KEY}",
    "Accept": "application/json",
}

session = requests.Session() # Use a session for potential connection reuse

def fetch_paginated_data(endpoint: str, resource_name: str) -> List[Dict[str, Any]]:
    """Fetches all items from a paginated API endpoint."""
    all_items = []
    url = f"{AXIOM_API_BASE_URL}{endpoint}"
    offset = 0
    logging.info(f"Fetching {resource_name} from {url}...")
    while True:
        params = {"skip": offset, "limit": PAGE_LIMIT}
        try:
            response = session.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            page_data = response.json()
            if not page_data:
                break # No more data
            all_items.extend(page_data)
            logging.debug(f"Fetched {len(page_data)} {resource_name} (total: {len(all_items)}).")
            if len(page_data) < PAGE_LIMIT:
                break # Last page
            offset += len(page_data) # Increment by actual number received
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP Error fetching {resource_name}: {e.response.status_code} - {e.response.text}")
            # Decide whether to proceed or halt on error
            if e.response.status_code >= 500: raise # Halt on server errors
            else: break # Stop fetching this resource on client errors (4xx)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {resource_name}: {e}")
            raise # Halt on connection errors etc.
    logging.info(f"Finished fetching {len(all_items)} {resource_name}.")
    return all_items

def transform_for_create(item_data: Dict[str, Any], schema_type: str) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """Transforms a Read schema dict into a Create schema dict, extracting original ID."""
    original_id = item_data.get("id")
    if original_id is None:
        logging.warning(f"Item of type '{schema_type}' missing 'id': {item_data.get('name', 'N/A')}")
        return None, None

    create_payload = {}
    # Define fields generally needed for creation (excluding id, timestamps)
    # This needs specific logic per type
    if schema_type == "schedule":
        keep_fields = ["name", "description", "cron_string", "timezone", "task_name", "task_args", "is_enabled"]
    elif schema_type == "storage_backend":
        # Keep all fields except id, created_at, updated_at
        # The structure is already correct due to discriminated union
        keep_fields = [k for k in item_data.keys() if k not in ["id", "created_at", "updated_at"]]
    elif schema_type == "dimse_listener":
        keep_fields = ["name", "description", "ae_title", "port", "is_enabled", "instance_id", "tls_enabled", "tls_cert_secret_name", "tls_key_secret_name", "tls_ca_cert_secret_name"]
    elif schema_type == "dicomweb_source":
        keep_fields = ["name", "description", "qido_url", "wado_url", "stow_url", "auth_type", "auth_config", "is_enabled", "polling_interval_seconds", "query_level", "query_filters"]
    elif schema_type == "dimse_qr_source":
        keep_fields = ["name", "description", "remote_ae_title", "remote_host", "remote_port", "local_ae_title", "polling_interval_seconds", "is_enabled", "is_active", "query_level", "query_filters", "move_destination_ae_title", "tls_enabled", "tls_ca_cert_secret_name", "tls_client_cert_secret_name", "tls_client_key_secret_name"]
    elif schema_type == "crosswalk_data_source":
        keep_fields = ["name", "description", "db_type", "host", "port", "database", "username", "password_secret_name", "query_table", "query_column_map", "query_filter", "sync_interval_minutes", "is_enabled"]
    elif schema_type == "crosswalk_mapping":
        keep_fields = ["name", "description", "data_source_id", "dicom_tag", "lookup_column", "fallback_action", "is_enabled"] # data_source_id needs mapping later
    elif schema_type == "ruleset":
        keep_fields = ["name", "description", "is_active", "priority", "execution_mode"]
    elif schema_type == "rule":
        # Handle rules separately as they need multiple original IDs
        return original_id, item_data # Return full data for special handling
    else:
        logging.warning(f"Unknown schema_type for transformation: {schema_type}")
        return original_id, item_data # Return full data as fallback

    # Create the payload
    for field in keep_fields:
        if field in item_data:
            create_payload[field] = item_data[field]

    # Special case for storage backend: already transformed by GET endpoint returning union
    if schema_type == "storage_backend":
         create_payload = {k: v for k, v in item_data.items() if k not in ["id", "created_at", "updated_at"]}

    return original_id, create_payload

def transform_rule(rule_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Special transformation for rules to prepare for seeding."""
    original_id = rule_data.get("id")
    original_ruleset_id = rule_data.get("ruleset_id")
    if original_id is None or original_ruleset_id is None:
         logging.warning(f"Rule missing ID or RuleSet ID: {rule_data.get('name', 'N/A')}")
         return None

    create_payload = {}
    keep_fields = [
        "name", "description", "is_active", "priority",
        "match_criteria", "association_criteria", "tag_modifications",
        "applicable_sources" # These are strings, keep as is
        # We will map schedule_id and destination_ids during seeding
    ]
    for field in keep_fields:
        if field in rule_data and rule_data[field] is not None:
            create_payload[field] = rule_data[field]

    # Store original IDs needed for mapping during seeding
    seed_entry = {
        "original_id": original_id, # Keep rule's own original ID for logging
        "original_ruleset_id": original_ruleset_id,
        "original_schedule_id": rule_data.get("schedule", {}).get("id") if rule_data.get("schedule") else None,
        "original_destination_ids": [dest.get("id") for dest in rule_data.get("destinations", []) if dest and "id" in dest],
        "create_payload": create_payload
    }
    return seed_entry


def main():
    logging.info("Starting comprehensive configuration dump...")
    output_data = {
        "schedules": [], "storage_backends": [], "dimse_listeners": [],
        "dicomweb_sources": [], "dimse_qr_sources": [],
        "crosswalk_data_sources": [], "crosswalk_mappings": [],
        "rulesets": [], "rules": []
    }
    success = True

    config_endpoints = {
        "schedules": ("/config/schedules", "schedule"),
        "storage_backends": ("/config/storage-backends", "storage_backend"),
        "dimse_listeners": ("/config/dimse-listeners", "dimse_listener"),
        "dicomweb_sources": ("/config/dicomweb-sources", "dicomweb_source"),
        "dimse_qr_sources": ("/config/dimse-qr-sources", "dimse_qr_source"),
        "crosswalk_data_sources": ("/config/crosswalk/data-sources", "crosswalk_data_source"),
        "crosswalk_mappings": ("/config/crosswalk/mappings", "crosswalk_mapping"),
        "rulesets": ("/rulesets", "ruleset"),
    }

    try:
        # Fetch simple config types
        for key, (endpoint, schema_type) in config_endpoints.items():
            items_data = fetch_paginated_data(endpoint, key)
            for item_data in items_data:
                original_id, create_payload = transform_for_create(item_data, schema_type)
                if original_id is not None and create_payload is not None:
                    output_data[key].append({
                        "original_id": original_id,
                        "create_payload": create_payload
                    })
                else:
                     logging.warning(f"Skipping item during transformation for {key}: {item_data.get('name', 'N/A')}")

        # Fetch rules (depend on rulesets being fetched first)
        if output_data["rulesets"]:
            logging.info("Fetching rules for each ruleset...")
            all_rules_data = []
            for ruleset_entry in output_data["rulesets"]:
                 original_ruleset_id = ruleset_entry["original_id"]
                 rules_for_set = fetch_paginated_data(f"/rules?ruleset_id={original_ruleset_id}", f"rules for ruleset {original_ruleset_id}")
                 all_rules_data.extend(rules_for_set)

            logging.info(f"Transforming {len(all_rules_data)} fetched rules...")
            for rule_data in all_rules_data:
                 transformed_rule_entry = transform_rule(rule_data)
                 if transformed_rule_entry:
                     output_data["rules"].append(transformed_rule_entry)
            logging.info(f"Finished transforming {len(output_data['rules'])} rules.")
        else:
             logging.warning("No rulesets found, skipping rule fetching.")

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed during dump: {e}")
        success = False
    except Exception as e:
        logging.error(f"An unexpected error occurred during dump: {e}", exc_info=True)
        success = False

    # Save the combined data
    if success:
        try:
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(output_data, f, indent=2)
            logging.info(f"--- Dump Summary ---")
            for key, items in output_data.items():
                logging.info(f"- {key.replace('_', ' ').title()}: {len(items)}")
            logging.info(f"Successfully dumped configuration to {OUTPUT_FILE}")
        except IOError as e:
            logging.error(f"Failed to write output file {OUTPUT_FILE}: {e}")
            exit(1)
    else:
        logging.error("Configuration dump failed due to errors.")
        exit(1)

if __name__ == "__main__":
    main()
