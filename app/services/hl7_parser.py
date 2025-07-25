# backend/app/services/hl7_parser.py

import structlog
from datetime import datetime, date
from typing import Optional

from hl7apy.parser import parse_message, ParserError
from hl7apy.core import Message as HL7apyMessage, Segment, Field

from app.schemas.imaging_order import ImagingOrderCreate, OrderStatus

logger = structlog.get_logger(__name__)

# --- HELPER FUNCTIONS ---
# These were fine, so they remain.

def _parse_hl7_date(hl7_date: str) -> Optional[date]:
    if not hl7_date or len(hl7_date) < 8: return None
    try: return datetime.strptime(hl7_date[:8], '%Y%m%d').date()
    except (ValueError, TypeError): return None

def _parse_hl7_datetime(hl7_datetime: str) -> Optional[datetime]:
    if not hl7_datetime: return None
    try: return datetime.strptime(hl7_datetime[:14], '%Y%m%d%H%M%S')
    except (ValueError, TypeError): return None

def _get_component(field_string: str, index: int = 1, default: str = "") -> str:
    """Safely gets a component from a raw field string."""
    if not field_string: return default
    try:
        components = field_string.split('^')
        if 1 <= index <= len(components):
            return components[index - 1]
        return default
    except Exception:
        return default

def _parse_person_name(field_string: str) -> str:
    """Cleans up a person name field string into 'Last, First' format."""
    last_name = _get_component(field_string, 1)
    first_name = _get_component(field_string, 2)
    name_parts = [name for name in [last_name, first_name] if name]
    return ", ".join(name_parts)

def _parse_hl7_address(field: str) -> Optional[str]:
    """Parses an HL7 XAD address field into a single-line string."""
    if not field:
        return None
    # Street^Other^City^State^Zip^Country -> Street, Other, City, State Zip, Country
    parts = [_get_component(field, i) for i in [1, 2, 3, 4, 5, 6]]
    # A more readable address format
    street = f"{parts[0]} {parts[1]}".strip()
    city_state_zip = f"{parts[2]}, {parts[3]} {parts[4]}".strip(", ")
    country = parts[5]
    
    full_address = ", ".join(filter(None, [street, city_state_zip, country]))
    return full_address if full_address else None

# --- THE NEW, ROBUST HELPER FUNCTION ---
def _map_order_control_to_status(order_control: str) -> OrderStatus:
    """
    Maps an HL7 ORC-1 Order Control code to our internal OrderStatus enum.
    This is the comprehensive version you demanded, and you were right to do so.
    """
    mapping = {
        # --- Common Codes We Will Handle ---
        'NW': OrderStatus.SCHEDULED,      # New order
        'XO': OrderStatus.SCHEDULED,      # Change order (treat as an update to a scheduled order)
        'CA': OrderStatus.CANCELED,       # Cancel order request
        'OC': OrderStatus.CANCELED,       # Order canceled (synonym for CA)
        'DC': OrderStatus.DISCONTINUED,   # Discontinue order request
        'SC': OrderStatus.IN_PROGRESS,    # Status change (e.g., patient arrived, in progress)
        'IP': OrderStatus.IN_PROGRESS,    # In process
        'CM': OrderStatus.COMPLETED,      # Order is completed

        # --- Less Common Codes We Will Acknowledge But May Not Fully Implement Yet ---
        'RO': OrderStatus.SCHEDULED,      # Replacement order (comment: similar to XO)
        'RP': OrderStatus.SCHEDULED,      # Order replace request (comment: similar to XO)
    }
    # If the code isn't in our map, we don't know what the fuck it is.
    return mapping.get(str(order_control).upper(), OrderStatus.UNKNOWN)


# --- THE FULL, UNABRIDGED FUNCTION YOU ASKED FOR ---
def parse_orm_o01(hl7_message_str: str) -> ImagingOrderCreate:
    """
    Parses an ORM^O01 HL7 message string using a manual method to ensure
    maximum robustness against malformed messages from shitty systems.
    """
    try:
        # Normalize the message for consistent parsing
        normalized_hl7_str = hl7_message_str.replace('\n', '\r').strip()
        if not normalized_hl7_str.endswith('\r'):
            normalized_hl7_str += '\r'
        
        # We still use hl7apy's initial parse to validate the basic structure.
        # If this fails, the message is truly hopeless.
        parse_message(normalized_hl7_str, find_groups=False)
    except ParserError as e:
        logger.error("HL7APY_PARSER_FAILED", error=str(e), raw_message=hl7_message_str, exc_info=True)
        raise ValueError(f"Invalid HL7 message format: {e}")

    # Manual parsing begins here. We trust nothing.
    # Create a dictionary of segments for easy access.
    raw_segments_dict = {
        seg.split('|', 1)[0].upper(): seg
        for seg in normalized_hl7_str.strip().split('\r')
        if seg and '|' in seg
    }

    # Split each segment string into a list of its fields.
    raw_segments = {key: value.split('|') for key, value in raw_segments_dict.items()}

    # Helper to safely access a field from a list of fields.
    def safe_get(fields: list, index: int, is_msh: bool = False) -> str:
        # MSH is 1-based because the field separator | is the first character.
        # All other segments are 0-based for the segment name, so the first
        # real field is at index 1. This is confusing as hell.
        list_index = index if not is_msh else index - 1
        if list_index < len(fields):
            return fields[list_index]
        return ""

    # Get the field lists for the segments we care about.
    pid_fields = raw_segments.get("PID", [])
    orc_fields = raw_segments.get("ORC", [])
    obr_fields = raw_segments.get("OBR", [])
    msh_fields = raw_segments.get("MSH", [])
    pv1_fields = raw_segments.get("PV1", [])
    zds_fields = raw_segments.get("ZDS", [])

    if not pid_fields or not obr_fields or not orc_fields:
        raise ValueError("Missing required PID, ORC, or OBR segment in HL7 message")

    # --- Extract data with extreme prejudice ---

    patient_id = _get_component(safe_get(pid_fields, 3), 1)
    if not patient_id: raise ValueError("Missing required field: Patient ID (PID-3)")

    # Accession can be in OBR-3 (preferred) or ORC-3 (fallback).
    accession_number = _get_component(safe_get(obr_fields, 3)) or _get_component(safe_get(orc_fields, 3))
    if not accession_number: raise ValueError("Missing required field: Accession Number (OBR-3 or ORC-3)")

    # Procedure description can have components, we usually want the second (text). Fallback to first.
    proc_desc = _get_component(safe_get(obr_fields, 4), 2, default=_get_component(safe_get(obr_fields, 4), 1))
    
    modality_value = safe_get(obr_fields, 24)

    # THIS IS THE CRITICAL CHANGE: We get the ORC-1 code...
    order_control_code = safe_get(orc_fields, 1)
    # ...and we use our new, robust function to map it to a status.
    order_status = _map_order_control_to_status(order_control_code)

    # --- NEW: Extract Study Instance UID, Scheduled AE Title, and Scheduled Station Name ---
    # ZDS-2 for Study Instance UID
    study_instance_uid = safe_get(zds_fields, 2)

    # ZDS-3 has Scheduled AE Title as the first component
    scheduled_station_ae_title = _get_component(safe_get(zds_fields, 3), 1)

    # PV1-3 has Assigned Patient Location, e.g., "RAD^R101^1^RADIOLOGY"
    # We want the 4th component for the human-readable name.
    scheduled_station_name = _get_component(safe_get(pv1_fields, 3), 4)

    # Assemble the final data payload for our database schema.
    order_data = {
        "patient_id": patient_id,
        "accession_number": accession_number,
        "patient_name": _parse_person_name(safe_get(pid_fields, 5)),
        "patient_dob": _parse_hl7_date(safe_get(pid_fields, 7)),
        "patient_sex": safe_get(pid_fields, 8),
        "patient_address": _parse_hl7_address(safe_get(pid_fields, 11)),
        "patient_phone_number": safe_get(pid_fields, 13),
        "patient_class": _get_component(safe_get(pv1_fields, 2)),
        "visit_number": _get_component(safe_get(pv1_fields, 19)),
        "placer_order_number": safe_get(orc_fields, 2),
        "filler_order_number": accession_number, # Often the same as accession
        "placer_group_number": _get_component(safe_get(orc_fields, 4)),
        "requested_procedure_description": proc_desc,
        "modality": modality_value,
        "scheduled_procedure_step_start_datetime": _parse_hl7_datetime(safe_get(obr_fields, 7)),
        "requesting_physician": _parse_person_name(safe_get(obr_fields, 16)),
        "referring_physician": _parse_person_name(safe_get(pv1_fields, 8)),
        "attending_physician": _parse_person_name(safe_get(pv1_fields, 7)),
        "order_status": order_status, # The glorious result of our new logic
        "study_instance_uid": study_instance_uid, 
        "scheduled_station_ae_title": scheduled_station_ae_title, 
        "scheduled_station_name": scheduled_station_name,
        "source": f"hl7_mllp:{safe_get(msh_fields, 4, is_msh=True)}",
        "source_sending_application": safe_get(msh_fields, 3, is_msh=True),
        "source_sending_facility": safe_get(msh_fields, 4, is_msh=True),
        "source_receiving_application": safe_get(msh_fields, 5, is_msh=True),
        "source_receiving_facility": safe_get(msh_fields, 6, is_msh=True),
        "source_message_control_id": safe_get(msh_fields, 10, is_msh=True),
        "raw_hl7_message": hl7_message_str
    }

    logger.info("PARSED_HL7_DATA_DICTIONARY", parsed_data=order_data, order_control=order_control_code)
    
    # Create the Pydantic object to be returned. This will also validate the data types.
    return ImagingOrderCreate(**order_data)