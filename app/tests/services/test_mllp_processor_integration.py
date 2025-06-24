# backend/app/tests/services/test_mllp_processor_integration.py

import pytest
from sqlalchemy.orm import Session

from app.services.mllp_listener import process_and_store_order
from app.crud.crud_imaging_order import imaging_order
from app.schemas.enums import OrderStatus
from app.db.models.imaging_order import ImagingOrder

# Re-use our trusty sample messages
SAMPLE_ORM_O01_HAPPY_PATH = (
    "MSH|^~\\&|SENDING_APP|SENDING_FACILITY|AXIOM_DMWL|AXIOM_FACILITY|20250618120000||ORM^O01|MSG_CONTROL_12345|P|2.5.1\r"
    "PID|||PATID12345^^^MRN||DOE^JANE^^^||19800101|F\r"
    "ORC|NW|PLACER123|FILLER456\r"
    "OBR|||FILLER456|PROC_CODE^CT CHEST W/CONTRAST^CUSTOM_CODES|||20250619093000"
)

SAMPLE_ORM_O01_CANCEL = (
    "MSH|^~\\&|SENDING_APP|SENDING_FACILITY|AXIOM_DMWL|AXIOM_FACILITY|20250618121000||ORM^O01|MSG_CONTROL_12346|P|2.5.1\r"
    "PID|||PATID12345^^^MRN\r"
    "ORC|CA|PLACER123|FILLER456\r"
    "OBR|||FILLER456"
)

# Mark the tests as async and tell pytest to use our db fixture
@pytest.mark.asyncio
async def test_process_and_store_order_creates_new_order(db: Session):
    """
    Tests that a new order message is correctly parsed and stored in the database.
    """
    # Act
    await process_and_store_order(db, SAMPLE_ORM_O01_HAPPY_PATH, peername="test-peer")

    # Assert
    db_order = imaging_order.get_by_accession_number(db, accession_number="FILLER456")
    assert db_order is not None
    assert db_order.patient_id == "PATID12345"
    assert db_order.patient_name == "DOE, JANE"
    assert db_order.order_status == OrderStatus.SCHEDULED


@pytest.mark.asyncio
async def test_process_and_store_order_updates_existing_order(db: Session):
    """
    Tests that a message for an existing accession number updates the record.
    """
    # Arrange: Create an initial order
    await process_and_store_order(db, SAMPLE_ORM_O01_HAPPY_PATH, peername="test-peer")
    
    # Create an update message (e.g., changing the procedure)
    update_message = SAMPLE_ORM_O01_HAPPY_PATH.replace("CT CHEST W/CONTRAST", "CT ABDOMEN W/O CONTRAST")

    # Act
    await process_and_store_order(db, update_message, peername="test-peer")

    # Assert
    all_orders = db.query(ImagingOrder).all()
    assert len(all_orders) == 1  # Should not create a new order

    updated_order = all_orders[0]
    assert updated_order.accession_number == "FILLER456"
    assert updated_order.requested_procedure_description == "CT ABDOMEN W/O CONTRAST"


@pytest.mark.asyncio
async def test_process_and_store_order_cancels_order(db: Session):
    """
    Tests that a cancellation message correctly updates an existing order's status.
    """
    # Arrange: Create the order to be cancelled
    await process_and_store_order(db, SAMPLE_ORM_O01_HAPPY_PATH, peername="test-peer")

    # Act: Send the cancellation message
    await process_and_store_order(db, SAMPLE_ORM_O01_CANCEL, peername="test-peer")

    # Assert
    cancelled_order = imaging_order.get_by_accession_number(db, accession_number="FILLER456")
    assert cancelled_order is not None
    assert cancelled_order.order_status == OrderStatus.CANCELED

@pytest.mark.asyncio
async def test_process_and_store_order_handles_parsing_error(db: Session):
    """
    Tests that if the parser fails, no data is committed to the DB.
    """
    # Arrange
    bad_message = "MSH|^~\\&|THIS IS NOT VALID HL7"
    
    # Act: This will raise an exception inside, which should be caught and logged.
    # The function itself shouldn't raise the exception out, but handle it.
    await process_and_store_order(db, bad_message, peername="test-peer")
    
    # Assert
    count = db.query(ImagingOrder).count()
    assert count == 0 # The rollback should have worked.