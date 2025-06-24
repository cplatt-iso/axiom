# backend/app/tests/services/test_dmwl_c_find_handler.py

import pytest
from datetime import datetime
from pydicom.dataset import Dataset
from sqlalchemy.orm import Session
from unittest.mock import Mock

from app.services.network.dimse import handlers
from app.crud.crud_imaging_order import imaging_order
from app.schemas.imaging_order import ImagingOrderCreate
from app.schemas.enums import OrderStatus

def create_test_order(db: Session, **kwargs):
    # ... (this helper function is fine)
    defaults = {
        "patient_name": "DOE^JANE", "patient_id": "TEST001",
        "accession_number": f"ACCN_{datetime.now().isoformat()}", "modality": "CT",
        "requested_procedure_description": "CT CHEST",
        "scheduled_procedure_step_start_datetime": datetime(2025, 6, 20, 10, 0, 0),
        "order_status": OrderStatus.SCHEDULED, "source": "test_suite",
    }
    defaults.update(kwargs)
    imaging_order.create(db, obj_in=ImagingOrderCreate(**defaults))
    db.commit()


# We add `monkeypatch` to the test signature
def test_dmwl_find_by_modality(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that a simple query for a modality returns the correct worklist items."""
    # Arrange
    create_test_order(db, modality="CT", accession_number="CT_STUDY")
    create_test_order(db, modality="MR", accession_number="MR_STUDY")
    
    # --- THE SURGERY ---
    # We replace the SessionLocal used by the handler with our test session.
    # The lambda ensures that when SessionLocal() is called, it just returns our db fixture.
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    # Create the query dataset
    query_ds = Dataset()
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    query_ds.ScheduledProcedureStepSequence[0].Modality = "CT"
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 2  # One pending result + one final success
    status, result_ds = responses[0]
    assert status == 0xFF00  # Pending
    assert result_ds.AccessionNumber == "CT_STUDY" # type: ignore
    final_status, _ = responses[1]
    assert final_status == 0x0000  # Success


def test_dmwl_find_no_results(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that a query with no matches returns only a success status."""
    # Arrange
    create_test_order(db, modality="CT")
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    query_ds = Dataset()
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    query_ds.ScheduledProcedureStepSequence[0].Modality = "XA"
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 1
    assert responses[0][0] == 0x0000


def test_dmwl_find_by_date_range(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests filtering by a specific date."""
    # Arrange
    create_test_order(db, scheduled_procedure_step_start_datetime=datetime(2025, 6, 20, 9, 0), accession_number="TODAY")
    create_test_order(db, scheduled_procedure_step_start_datetime=datetime(2025, 6, 21, 9, 0), accession_number="TOMORROW")
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    query_ds = Dataset()
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    query_ds.ScheduledProcedureStepSequence[0].ScheduledProcedureStepStartDate = "20250620"
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 2
    assert responses[0][1].AccessionNumber == "TODAY" # type: ignore

# ... (the unsupported sop class test doesn't need the DB, so it's fine) ...
def test_c_find_unsupported_sop_class(mock_c_find_event: Mock):
    from pydicom.uid import UID

    # The pynetdicom.sop_class module dynamically creates its attributes, which can
    # confuse some static analysis tools. We use the UID directly to avoid this.
    mock_c_find_event.AffectedSOPClassUID = UID("1.2.840.10008.5.1.4.1.1.2")  # CTImageStorage
    responses = list(handlers.handle_c_find(mock_c_find_event))
    assert len(responses) == 1
    assert responses[0] == (0x0000, None)