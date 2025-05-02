import requests
import json
import os
import logging
import time
from typing import List, Dict, Any, Optional, Callable

# --- Configuration ---
AXIOM_API_BASE_URL = os.environ.get("AXIOM_API_BASE_URL", "http://localhost:8001/api/v1")
AXIOM_API_KEY = os.environ.get("AXIOM_API_KEY")
INPUT_FILE = "axiom_config_seed.json"
REQUEST_TIMEOUT = 20
DELAY_BETWEEN_REQUESTS = 0.1 # Optional delay

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

if not AXIOM_API_KEY:
    logging.error("Error: AXIOM_API_KEY environment variable not set.")
    exit(1)

HEADERS = {
    "Authorization": f"Api-Key {AXIOM_API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

session = requests.Session()

# --- Generic Creator Function ---
def create_item(endpoint: str, item_name: str, payload: Dict[str, Any]) -> Optional[int]:
    """Attempts to create a single item via the API. Returns new ID on success, None on failure."""
    url = f"{AXIOM_API_BASE_URL}{endpoint}"
    display_name = payload.get('name', 'Unknown Item')
    logging.info(f"Attempting to create {item_name} '{display_name}'...")
    try:
        response = session.post(url, headers=HEADERS, json=payload, timeout=REQUEST_TIMEOUT)
        if response.status_code == 201: # Standard CREATED status
            new_id = response.json().get('id')
            if new_id is not None:
                 logging.info(f"Successfully created {item_name} '{display_name}' (New ID: {new_id}).")
                 return new_id
            else:
                 logging.error(f"Created {item_name} '{display_name}' but API did not return an ID.")
                 return None
        else:
            error_detail = "Unknown error"
            try: error_detail = response.json().get('detail', response.text)
            except json.JSONDecodeError: error_detail = response.text
            logging.error(f"Failed to create {item_name} '{display_name}'. Status: {response.status_code}, Detail: {error_detail}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending request to create {item_name} '{display_name}': {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error creating {item_name} '{display_name}': {e}")
        return None

# --- Main Seeding Logic ---
def main():
    logging.info(f"Starting comprehensive configuration seeding from {INPUT_FILE}...")

    # --- Load Data ---
    try:
        with open(INPUT_FILE, 'r') as f: seed_data = json.load(f)
    except FileNotFoundError: logging.error(f"{INPUT_FILE} not found."); exit(1)
    except json.JSONDecodeError as e: logging.error(f"Failed to parse JSON: {e}"); exit(1)
    except IOError as e: logging.error(f"Failed to read file: {e}"); exit(1)

    # --- Check API Reachability ---
    try:
         logging.info(f"Checking API availability at {AXIOM_API_BASE_URL}...")
         ping_url = f"{AXIOM_API_BASE_URL}/system/health"
         response = session.get(ping_url, timeout=5)
         response.raise_for_status()
         logging.info("API seems reachable.")
    except requests.exceptions.RequestException as e:
         logging.error(f"API at {AXIOM_API_BASE_URL} is not reachable: {e}"); exit(1)

    # --- ID Mapping Dictionaries ---
    id_maps = {
        "schedules": {}, "storage_backends": {}, "dimse_listeners": {},
        "dicomweb_sources": {}, "dimse_qr_sources": {},
        "crosswalk_data_sources": {}, "crosswalk_mappings": {},
        "rulesets": {}, "rules": {} # Rules map original to new ID for logging mostly
    }
    success_counts = {k: 0 for k in id_maps}
    fail_counts = {k: 0 for k in id_maps}

    # --- Define Seeding Order and Endpoints ---
    # Ordered list of tuples: (key_in_json, api_endpoint, item_name_singular)
    seed_order = [
        ("schedules", "/config/schedules", "schedule"),
        ("storage_backends", "/config/storage-backends", "storage backend"),
        ("dimse_listeners", "/config/dimse-listeners", "dimse listener"),
        ("dicomweb_sources", "/config/dicomweb-sources", "dicomweb source"),
        ("dimse_qr_sources", "/config/dimse-qr-sources", "dimse qr source"),
        ("crosswalk_data_sources", "/config/crosswalk/data-sources", "crosswalk data source"),
        ("crosswalk_mappings", "/config/crosswalk/mappings", "crosswalk mapping"), # Mappings after sources
        ("rulesets", "/rulesets", "ruleset"),
        # Rules are handled separately due to complex dependencies
    ]

    # --- Seed Items with Simple Dependencies ---
    for key, endpoint, item_name in seed_order:
        logging.info(f"--- Phase: Creating {item_name.replace('_', ' ').title()}s ---")
        items_to_create = seed_data.get(key, [])
        if not items_to_create:
            logging.info(f"No {item_name}s found in seed file.")
            continue

        for item_entry in items_to_create:
            original_id = item_entry.get("original_id")
            payload = item_entry.get("create_payload")

            if not isinstance(original_id, int) or not isinstance(payload, dict):
                logging.warning(f"Skipping invalid {item_name} entry: {item_entry}")
                fail_counts[key] += 1
                continue

            # --- Dependency Mapping (Example for Crosswalk Mappings) ---
            if key == "crosswalk_mappings":
                original_ds_id = payload.get("data_source_id")
                if original_ds_id is not None:
                    new_ds_id = id_maps["crosswalk_data_sources"].get(original_ds_id)
                    if new_ds_id is None:
                        logging.warning(f"Skipping crosswalk mapping '{payload.get('name')}' because original data source ID {original_ds_id} was not mapped.")
                        fail_counts[key] += 1
                        continue
                    payload["data_source_id"] = new_ds_id # Update payload with new ID
                else:
                     logging.warning(f"Crosswalk mapping '{payload.get('name')}' missing data_source_id. Skipping.")
                     fail_counts[key] += 1
                     continue
            # --- Add other dependency mappings here if needed ---

            new_id = create_item(endpoint, item_name, payload)
            if new_id is not None:
                id_maps[key][original_id] = new_id
                success_counts[key] += 1
            else:
                fail_counts[key] += 1
            time.sleep(DELAY_BETWEEN_REQUESTS)

        logging.info(f"--- {item_name.replace('_', ' ').title()} Creation Summary ---")
        logging.info(f"Success: {success_counts[key]}, Failed: {fail_counts[key]}")
        if fail_counts[key] > 0:
            logging.warning(f"Some {item_name}s failed to create. Dependent items might be skipped.")

    # --- Seed Rules (Complex Dependencies) ---
    logging.info("--- Phase: Creating Rules ---")
    rules_to_create = seed_data.get("rules", [])
    if not rules_to_create:
        logging.info("No rules found in seed file.")
    else:
        for rule_entry in rules_to_create:
            original_id = rule_entry.get("original_id")
            original_ruleset_id = rule_entry.get("original_ruleset_id")
            original_schedule_id = rule_entry.get("original_schedule_id")
            original_destination_ids = rule_entry.get("original_destination_ids", [])
            payload = rule_entry.get("create_payload")
            rule_name = payload.get("name", "Unknown Rule") if isinstance(payload, dict) else "Invalid Payload"

            if not isinstance(original_id, int) or not isinstance(original_ruleset_id, int) or not isinstance(payload, dict):
                logging.warning(f"Skipping invalid rule entry: {rule_entry}")
                fail_counts["rules"] += 1
                continue

            # Map RuleSet ID (Mandatory)
            new_ruleset_id = id_maps["rulesets"].get(original_ruleset_id)
            if new_ruleset_id is None:
                logging.warning(f"Skipping rule '{rule_name}' (Original ID: {original_id}) because original ruleset ID {original_ruleset_id} was not mapped.")
                fail_counts["rules"] += 1
                continue
            payload["ruleset_id"] = new_ruleset_id

            # Map Schedule ID (Optional)
            if original_schedule_id is not None:
                new_schedule_id = id_maps["schedules"].get(original_schedule_id)
                if new_schedule_id is None:
                    logging.warning(f"Rule '{rule_name}' (Original ID: {original_id}): Original schedule ID {original_schedule_id} not mapped. Schedule will not be linked.")
                    # Decide: skip rule or create without schedule? Create without for now.
                else:
                    payload["schedule_id"] = new_schedule_id

            # Map Destination IDs (Optional)
            if original_destination_ids:
                new_destination_ids = []
                mapping_successful = True
                for orig_dest_id in original_destination_ids:
                    new_dest_id = id_maps["storage_backends"].get(orig_dest_id)
                    if new_dest_id is None:
                        logging.warning(f"Rule '{rule_name}' (Original ID: {original_id}): Original destination ID {orig_dest_id} not mapped. Destination will be skipped for this rule.")
                        mapping_successful = False # Mark partial failure, but still try to add others
                    else:
                        new_destination_ids.append(new_dest_id)

                if new_destination_ids: # Only add if we mapped at least one
                    payload["destination_ids"] = new_destination_ids
                elif original_destination_ids: # Log if none could be mapped but some were expected
                     logging.warning(f"Rule '{rule_name}' (Original ID: {original_id}): None of the original destination IDs could be mapped. Rule will have no destinations.")


            # Create the rule
            new_rule_id = create_item("/rules", "rule", payload)
            if new_rule_id is not None:
                id_maps["rules"][original_id] = new_rule_id # Store mapping for logging/reference if needed
                success_counts["rules"] += 1
            else:
                fail_counts["rules"] += 1
            time.sleep(DELAY_BETWEEN_REQUESTS)

        logging.info(f"--- Rule Creation Summary ---")
        logging.info(f"Success: {success_counts['rules']}, Failed: {fail_counts['rules']}")

    # --- Final Summary ---
    logging.info("--- Seeding Complete: Overall Summary ---")
    total_success = 0
    total_fail = 0
    for key in id_maps.keys():
        logging.info(f"- {key.replace('_', ' ').title()}: Success={success_counts[key]}, Failed={fail_counts[key]}")
        total_success += success_counts[key]
        total_fail += fail_counts[key]

    logging.info(f"Total items processed: {total_success + total_fail}. Total failures: {total_fail}.")
    if total_fail > 0:
        logging.warning("Some items failed to create. Check logs above for details.")
        exit(1)
    else:
         logging.info("Seeding completed successfully.")

if __name__ == "__main__":
    main()
