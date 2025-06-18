# backend/app/tests/services/test_hl7_parser.py

import pytest
from datetime import date

from app.services.hl7_parser import parse_orm_o01
from app.schemas.imaging_order import OrderStatus

# This is a sample, real-world-ish HL7 message. You'll add more.
# Put this here, or even better, in a conftest.py file later.
SAMPLE_ORM_O01_HAPPY_PATH = (
    "MSH|^~\\&|SENDING_APP|SENDING_FACILITY|AXIOM_DMWL|AXIOM_FACILITY|20250618120000||ORM^O01|MSG_CONTROL_12345|P|2.5.1\r"
    "PID|||PATID12345^^^MRN|ALT_PATID|DOE^JANE^^^|MAIDEN^NAME|19800101|F|||123 MAIN ST^^ANYTOWN^ST^12345||(555)555-5555|||S\r"
    "PV1||O|AMB^AMBULATORY|||||REF_DOC^REFERRING^A^^^DR||||||||||ADM123\r"
    "ORC|NW|PLACER123|FILLER456||SC||||20250618120500|||ORDERING_PROV^PHYSICIAN^B\r"
    "OBR|1|PLACER123|FILLER456|PROC_CODE^CT CHEST W/CONTRAST^CUSTOM_CODES|||20250619093000|||||||ORDERING_PROV^PHYSICIAN^B||||||||||CT|||SCHEDULED"
)

SAMPLE_ORM_O01_CANCEL = (
    "MSH|^~\\&|SENDING_APP|SENDING_FACILITY|AXIOM_DMWL|AXIOM_FACILITY|20250618121000||ORM^O01|MSG_CONTROL_12346|P|2.5.1\r"
    "PID|||PATID12345^^^MRN\r"
    "ORC|CA|PLACER123|FILLER456\r"
    "OBR|||FILLER456"
)


def test_parse_orm_o01_happy_path():
    """
    Tests that a standard, well-formed ORM message is parsed correctly.
    """
    parsed_order = parse_orm_o01(SAMPLE_ORM_O01_HAPPY_PATH)

    assert parsed_order.patient_id == "PATID12345"
    assert parsed_order.patient_name == "DOE, JANE"
    assert parsed_order.patient_dob == date(1980, 1, 1)
    assert parsed_order.patient_sex == "F"
    assert parsed_order.accession_number == "FILLER456"
    assert parsed_order.placer_order_number == "PLACER123"
    assert parsed_order.requested_procedure_description == "CT CHEST W/CONTRAST"
    assert parsed_order.modality == "CT"
    assert parsed_order.order_status == OrderStatus.SCHEDULED
    assert parsed_order.referring_physician == "REF_DOC, REFERRING"
    assert "raw_hl7_message" in parsed_order.model_dump()

def test_parse_orm_o01_cancel_order():
    """
    Tests that a cancellation message (ORC-1 = 'CA') is handled correctly.
    """
    parsed_order = parse_orm_o01(SAMPLE_ORM_O01_CANCEL)
    assert parsed_order.accession_number == "FILLER456"
    assert parsed_order.order_status == OrderStatus.CANCELED

def test_parse_orm_o01_missing_patient_id_raises_error():
    """
    Tests that a message missing a critical field raises a ValueError.
    """
    bad_message = SAMPLE_ORM_O01_HAPPY_PATH.replace("PATID12345", "")
    with pytest.raises(ValueError, match="Missing required field: Patient ID"):
        parse_orm_o01(bad_message)
