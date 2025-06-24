# backend/app/services/hl7_parser.py
#
# The final form. The parser is now a hardened, paranoid veteran that trusts nothing.
#
import structlog
from datetime import datetime, date
from typing import Optional

from hl7apy.parser import parse_message, ParserError
from hl7apy.core import Message as HL7apyMessage, Segment, Field

from app.schemas.imaging_order import ImagingOrderCreate, OrderStatus

logger = structlog.get_logger(__name__)

# --- Helper functions are fine, the core logic was the problem ---
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

def _map_order_control_to_status(order_control: str) -> OrderStatus:
    mapping = {
        'NW': OrderStatus.SCHEDULED, 'CA': OrderStatus.CANCELED, 'OC': OrderStatus.CANCELED,
        'SC': OrderStatus.SCHEDULED, 'XO': OrderStatus.SCHEDULED, 'DC': OrderStatus.DISCONTINUED,
        'CM': OrderStatus.COMPLETED, 'IP': OrderStatus.IN_PROGRESS,
    }
    return mapping.get(str(order_control).upper(), OrderStatus.UNKNOWN)


def parse_orm_o01(hl7_message_str: str) -> ImagingOrderCreate:
    """
    Parses an ORM^O01 HL7 message string using a brutally manual method
    and an excessive amount of logging to hunt down a demon.
    """
    # ... (the try/except block for the initial parse_message can stay the same)
    try:
        normalized_hl7_str = hl7_message_str.replace('\n', '\r').strip()
        if not normalized_hl7_str.endswith('\r'): normalized_hl7_str += '\r'
        parse_message(normalized_hl7_str, find_groups=False)
    except ParserError as e:
        logger.error("HL7APY_PARSER_FAILED", error=str(e), raw_message=hl7_message_str, exc_info=True)
        raise ValueError(f"Invalid HL7 message format: {e}")

    raw_segments_dict = {
        seg.split('|', 1)[0].upper(): seg
        for seg in normalized_hl7_str.strip().split('\r')
        if seg and '|' in seg
    }

    obr_segment_string = raw_segments_dict.get("OBR", "OBR SEGMENT NOT FOUND")
    
    # --- HERE IS THE MICROSCOPE ---
    logger.info(
        "HL7_PARSER_OBR_INSPECTION",
        obr_raw_segment=obr_segment_string
    )

    obr_fields = obr_segment_string.split('|')
    obr_fields_len = len(obr_fields)

    logger.info(
        "HL7_PARSER_OBR_SPLIT_RESULT",
        obr_fields_list=obr_fields,
        obr_fields_count=obr_fields_len
    )
    # --- END OF MICROSCOPE ---

    raw_segments = {key: value.split('|') for key, value in raw_segments_dict.items()}

    def safe_get(fields: list, index: int, is_msh: bool = False) -> str:
        list_index = index - 1 if is_msh else index
        if list_index < len(fields):
            return fields[list_index]
        return ""

    pid_fields = raw_segments.get("PID", [])
    orc_fields = raw_segments.get("ORC", [])
    # We already split obr_fields above for logging
    msh_fields = raw_segments.get("MSH", [])
    pv1_fields = raw_segments.get("PV1", [])

    if not pid_fields or not obr_fields or not orc_fields:
        raise ValueError("Missing required PID, ORC, or OBR segment in HL7 message")

    patient_id = _get_component(safe_get(pid_fields, 3), 1)
    if not patient_id: raise ValueError("Missing required field: Patient ID (PID-3)")

    accession_number = _get_component(safe_get(obr_fields, 3)) or _get_component(safe_get(orc_fields, 3))
    if not accession_number: raise ValueError("Missing required field: Accession Number (OBR-3 or ORC-3)")

    proc_desc = _get_component(safe_get(obr_fields, 4), 2, default=_get_component(safe_get(obr_fields, 4), 1))
    
    # Let's get the value and log it immediately
    modality_value = safe_get(obr_fields, 24)
    logger.info("HL7_PARSER_MODALITY_EXTRACTION", extracted_value=modality_value)

    order_data = {
        "patient_id": patient_id,
        "accession_number": accession_number,
        "patient_name": _parse_person_name(safe_get(pid_fields, 5)),
        "patient_dob": _parse_hl7_date(safe_get(pid_fields, 7)),
        "patient_sex": safe_get(pid_fields, 8),
        "placer_order_number": safe_get(orc_fields, 2),
        "filler_order_number": accession_number,
        "requested_procedure_description": proc_desc,
        "modality": modality_value,
        "scheduled_procedure_step_start_datetime": _parse_hl7_datetime(safe_get(obr_fields, 7)),
        "requesting_physician": _parse_person_name(safe_get(obr_fields, 16)),
        "referring_physician": _parse_person_name(safe_get(pv1_fields, 8)),
        "order_status": _map_order_control_to_status(safe_get(orc_fields, 1)),
        "study_instance_uid": None, "scheduled_station_ae_title": None, "scheduled_station_name": None,
        "source": f"hl7_mllp:{safe_get(msh_fields, 4, is_msh=True)}",
        "raw_hl7_message": hl7_message_str
    }

    logger.info("PARSED_HL7_DATA_DICTIONARY", parsed_data=order_data)
    return ImagingOrderCreate(**order_data)