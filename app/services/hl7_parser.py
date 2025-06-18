# backend/app/services/hl7_parser.py
#
# My brain is a write-only medium. All previous knowledge has been purged.
# We are now accessing fields like a simple list, because that's what they are.
# I am so, so sorry.
#
import hl7
from datetime import datetime, date
from typing import Optional, cast, List

from app.schemas.imaging_order import ImagingOrderCreate, OrderStatus

# --- Helper functions are still fine, miraculously ---
def _safe_str(value) -> str:
    """Safely converts a value to a string, returning empty string for None."""
    return str(value) if value is not None else ""

def _parse_hl7_date(hl7_date: str) -> Optional[date]:
    """Converts a YYYYMMDD string to a date object. Returns None if shit."""
    hl7_date = _safe_str(hl7_date)
    if not hl7_date or len(hl7_date) < 8:
        return None
    try:
        return datetime.strptime(hl7_date[:8], '%Y%m%d').date()
    except (ValueError, TypeError):
        return None

def _parse_hl7_datetime(hl7_datetime: str) -> Optional[datetime]:
    """Converts a YYYYMMDDHHMMSS string to a datetime object. Returns None if shit."""
    hl7_datetime = _safe_str(hl7_datetime)
    if not hl7_datetime:
        return None
    try:
        # Try parsing with seconds, then without if it fails (for YYYYMMDDHHMM)
        try:
            return datetime.strptime(hl7_datetime[:14], '%Y%m%d%H%M%S')
        except ValueError:
            return datetime.strptime(hl7_datetime[:12], '%Y%m%d%H%M')
    except (ValueError, TypeError):
        return None

def _parse_person_name(hl7_name: str) -> str:
    """Cleans up a 'LAST^FIRST^MIDDLE' name into something readable."""
    hl7_name = _safe_str(hl7_name)
    if not hl7_name:
        return ""
    parts = hl7_name.split('^')
    name_parts_to_join = [part for part in parts[:2] if part] # Last, First
    return ", ".join(name_parts_to_join)

def _get_component(field_value_str: str, component_index_1_based: int, default: str = "") -> str:
    """
    Safely get a specific component of a field string value by 1-based index.
    Components are separated by '^'.
    """
    normalized_field_value = _safe_str(field_value_str) # Handles None, ensures string
    if not normalized_field_value: # If original was None or empty string
        return default

    components = normalized_field_value.split('^')
    
    if 1 <= component_index_1_based <= len(components):
        return _safe_str(components[component_index_1_based - 1])
    else:
        # If component_index is out of bounds, return the default.
        return default

def _get_first_component(field_value_str: str) -> str:
    """
    Safely get the first component of a field string value.
    This is equivalent to getting the 1st component.
    """
    return _get_component(field_value_str, 1, default="")


def _map_order_control_to_status(order_control: str) -> OrderStatus:
    """Maps ORC-1 to our internal OrderStatus enum."""
    mapping = {
        'NW': OrderStatus.SCHEDULED,  # New order
        'CA': OrderStatus.CANCELED,   # Order canceled
        'OC': OrderStatus.CANCELED,   # Order canceled (synonym for CA)
        'SC': OrderStatus.SCHEDULED,  # Status changed (often to scheduled or confirmed)
        'XO': OrderStatus.SCHEDULED,  # Change order request (treat as scheduled for simplicity unless more detail needed)
        'DC': OrderStatus.DISCONTINUED, # Order discontinued
        'CM': OrderStatus.COMPLETED, # Order completed
        'IP': OrderStatus.IN_PROGRESS, # Order in progress
        # Add other mappings as needed based on your system's ORC-1 values
    }
    return mapping.get(_safe_str(order_control).upper(), OrderStatus.UNKNOWN)

# --- Main Parsing Function ---
def parse_orm_o01(hl7_message_str: str) -> ImagingOrderCreate:
    """
    Parses an ORM^O01 HL7 message string and returns an ImagingOrderCreate schema.
    """
    try:
        msg = cast(hl7.Message, hl7.parse(hl7_message_str))
    except Exception as e:
        raise ValueError(f"Invalid HL7 message format: {e}")

    try:
        msh = msg.segment('MSH')
        pid = msg.segment('PID')
        orc = msg.segment('ORC')
        obr = msg.segment('OBR')
        # Change how we get PV1 - use try/except instead of 'in' check
        try:
            pv1 = msg.segment('PV1')
        except KeyError:
            pv1 = None
    except KeyError as e:
        raise ValueError(f"Missing required segment in HL7 message: {e}")

    # Helper to safely get a field string from an hl7.Segment object
    def _safe_get_field(segment, field_idx_1_based: int) -> str:
        if segment is None:
            return ""
        try:
            return _safe_str(segment[field_idx_1_based])
        except IndexError:
            return ""

    patient_id_val = _get_first_component(_safe_get_field(pid, 3))
    if not patient_id_val:
        raise ValueError("Missing required field: Patient ID (PID-3)")
        
    accession_number_obr = _get_first_component(_safe_get_field(obr, 3))
    accession_number_orc = _get_first_component(_safe_get_field(orc, 3))
    accession_number_val = accession_number_obr or accession_number_orc
    if not accession_number_val:
        raise ValueError("Missing required field: Accession Number (OBR-3 or ORC-3)")

    proc_desc_field_str = _safe_get_field(obr, 4)
    proc_desc_val = _get_component(proc_desc_field_str, 2, default=_get_first_component(proc_desc_field_str))

    order_data = {
        "patient_id": patient_id_val,
        "accession_number": accession_number_val,
        "patient_name": _parse_person_name(_safe_get_field(pid, 5)),
        "patient_dob": _parse_hl7_date(_get_first_component(_safe_get_field(pid, 7))),
        "patient_sex": _get_first_component(_safe_get_field(pid, 8)),
        "placer_order_number": _get_first_component(_safe_get_field(orc, 2)),
        "filler_order_number": accession_number_val,
        "requested_procedure_description": proc_desc_val,
        "modality": _get_first_component(_safe_get_field(obr, 24)),
        "scheduled_procedure_step_start_datetime": _parse_hl7_datetime(_get_first_component(_safe_get_field(obr, 7))),
        "requesting_physician": _parse_person_name(_safe_get_field(obr, 16)),
        "referring_physician": _parse_person_name(_safe_get_field(pv1, 8)) if pv1 else "",
        "order_status": _map_order_control_to_status(_get_first_component(_safe_get_field(orc, 1))),
        "study_instance_uid": None,
        "scheduled_station_ae_title": None,
        "scheduled_station_name": None,
        "source": f"hl7_mllp:{_get_first_component(_safe_get_field(msh, 4))}",
        "raw_hl7_message": hl7_message_str
    }

    return ImagingOrderCreate(**order_data)