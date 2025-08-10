# backend/app/tests/services/test_dmwl_c_find_handler.py

import pytest
from datetime import datetime
from pydicom.dataset import Dataset
from sqlalchemy.orm import Session
from unittest.mock import Mock, patch

from app.services.network.dimse import handlers
from app.crud.crud_imaging_order import imaging_order
from app.schemas.imaging_order import ImagingOrderCreate
from app.schemas.enums import OrderStatus
from app.crud import crud_modality, crud_facility
from app.schemas.modality import ModalityCreate
from app.schemas.facility import FacilityCreate

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

def create_test_facility(db: Session, **kwargs) -> int:
    """Create a test facility and return its ID."""
    defaults = {
        "name": "Test Hospital",
        "description": "Test facility for DMWL testing",
        "is_active": True
    }
    defaults.update(kwargs)
    facility = crud_facility.create(db, obj_in=FacilityCreate(**defaults))
    db.commit()
    return facility.id

def create_test_modality(db: Session, facility_id: int, **kwargs) -> int:
    """Create a test modality and return its ID."""
    defaults = {
        "name": "Test CT Scanner",
        "description": "Test CT modality",
        "ae_title": "TEST_CT",
        "ip_address": "192.168.1.100",
        "port": 104,
        "modality_type": "CT",
        "is_active": True,
        "is_dmwl_enabled": True,
        "facility_id": facility_id,
        "manufacturer": "Test Manufacturer",
        "model": "Test Model"
    }
    defaults.update(kwargs)
    modality = crud_modality.create(db, obj_in=ModalityCreate(**defaults))
    db.commit()
    return modality.id


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


def test_dmwl_find_by_accession_number(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests filtering by a specific accession number."""
    # Arrange
    create_test_order(db, accession_number="ACC12345", patient_name="SMITH^JOHN")
    create_test_order(db, accession_number="ACC67890", patient_name="DOE^JANE") 
    create_test_order(db, accession_number="ACC99999", patient_name="JONES^BOB")
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    query_ds = Dataset()
    query_ds.AccessionNumber = "ACC12345"  # Query for specific accession number
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 2  # One pending result + one final success
    status, result_ds = responses[0]
    assert status == 0xFF00  # Pending
    assert result_ds.AccessionNumber == "ACC12345" # type: ignore
    assert result_ds.PatientName == "SMITH^JOHN" # type: ignore
    final_status, _ = responses[1]
    assert final_status == 0x0000  # Success


def test_dmwl_find_by_nonexistent_accession_number(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that querying for a non-existent accession number returns no results."""
    # Arrange
    create_test_order(db, accession_number="ACC12345")
    create_test_order(db, accession_number="ACC67890")
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    query_ds = Dataset()
    query_ds.AccessionNumber = "ACC18744718"  # Query for non-existent accession number
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 1  # Only the final success response, no pending responses
    assert responses[0][0] == 0x0000  # Success with no results


# ... (the unsupported sop class test doesn't need the DB, so it's fine) ...
def test_c_find_unsupported_sop_class(mock_c_find_event: Mock):
    from pydicom.uid import UID

    # The pynetdicom.sop_class module dynamically creates its attributes, which can
    # confuse some static analysis tools. We use the UID directly to avoid this.
    mock_c_find_event.AffectedSOPClassUID = UID("1.2.840.10008.5.1.4.1.1.2")  # CTImageStorage
    responses = list(handlers.handle_c_find(mock_c_find_event))
    assert len(responses) == 1
    assert responses[0] == (0x0000, None)

# --- Modality Access Control Tests ---

def test_dmwl_find_authorized_modality(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that an authorized modality can query DMWL and gets results."""
    # Arrange
    facility_id = create_test_facility(db)
    modality_id = create_test_modality(db, facility_id, ae_title="AUTHORIZED_CT", ip_address="192.168.1.100")
    create_test_order(db, modality="CT", accession_number="CT_STUDY")
    
    # Mock the association to simulate authorized modality
    mock_c_find_event.assoc.requestor.ae_title = "AUTHORIZED_CT"
    mock_c_find_event.assoc.requestor.address = "192.168.1.100"
    
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
    assert result_ds is not None
    assert result_ds.AccessionNumber == "CT_STUDY"
    final_status, _ = responses[1]
    assert final_status == 0x0000  # Success

def test_dmwl_find_unknown_modality(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that an unknown modality gets 0 results."""
    # Arrange
    create_test_order(db, modality="CT", accession_number="CT_STUDY")
    
    # Mock the association to simulate unknown modality
    mock_c_find_event.assoc.requestor.ae_title = "UNKNOWN_CT"
    mock_c_find_event.assoc.requestor.address = "192.168.1.999"
    
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    # Create the query dataset
    query_ds = Dataset()
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    query_ds.ScheduledProcedureStepSequence[0].Modality = "CT"
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 1  # Only success response, no results
    assert responses[0][0] == 0x0000  # Success with no data

def test_dmwl_find_inactive_modality(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that an inactive modality gets 0 results."""
    # Arrange
    facility_id = create_test_facility(db)
    modality_id = create_test_modality(
        db, facility_id, 
        ae_title="INACTIVE_CT", 
        ip_address="192.168.1.101",
        is_active=False  # Inactive modality
    )
    create_test_order(db, modality="CT", accession_number="CT_STUDY")
    
    # Mock the association to simulate inactive modality
    mock_c_find_event.assoc.requestor.ae_title = "INACTIVE_CT"
    mock_c_find_event.assoc.requestor.address = "192.168.1.101"
    
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    # Create the query dataset
    query_ds = Dataset()
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    query_ds.ScheduledProcedureStepSequence[0].Modality = "CT"
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 1  # Only success response, no results
    assert responses[0][0] == 0x0000  # Success with no data

def test_dmwl_find_dmwl_disabled_modality(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that a modality with DMWL disabled gets 0 results."""
    # Arrange
    facility_id = create_test_facility(db)
    modality_id = create_test_modality(
        db, facility_id, 
        ae_title="DMWL_DISABLED_CT", 
        ip_address="192.168.1.102",
        is_dmwl_enabled=False  # DMWL disabled
    )
    create_test_order(db, modality="CT", accession_number="CT_STUDY")
    
    # Mock the association to simulate DMWL disabled modality
    mock_c_find_event.assoc.requestor.ae_title = "DMWL_DISABLED_CT"
    mock_c_find_event.assoc.requestor.address = "192.168.1.102"
    
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    # Create the query dataset
    query_ds = Dataset()
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    query_ds.ScheduledProcedureStepSequence[0].Modality = "CT"
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 1  # Only success response, no results
    assert responses[0][0] == 0x0000  # Success with no data

def test_dmwl_find_ip_mismatch(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that a modality querying from wrong IP gets 0 results."""
    # Arrange
    facility_id = create_test_facility(db)
    modality_id = create_test_modality(
        db, facility_id, 
        ae_title="IP_MISMATCH_CT", 
        ip_address="192.168.1.103"  # Configured IP
    )
    create_test_order(db, modality="CT", accession_number="CT_STUDY")
    
    # Mock the association to simulate different IP
    mock_c_find_event.assoc.requestor.ae_title = "IP_MISMATCH_CT"
    mock_c_find_event.assoc.requestor.address = "192.168.1.999"  # Different IP
    
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    # Create the query dataset
    query_ds = Dataset()
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    query_ds.ScheduledProcedureStepSequence[0].Modality = "CT"
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 1  # Only success response, no results
    assert responses[0][0] == 0x0000  # Success with no data

def test_dmwl_find_inactive_facility(db: Session, mock_c_find_event: Mock, monkeypatch):
    """Tests that a modality from an inactive facility gets 0 results."""
    # Arrange
    facility_id = create_test_facility(db, is_active=False)  # Inactive facility
    modality_id = create_test_modality(
        db, facility_id, 
        ae_title="INACTIVE_FACILITY_CT", 
        ip_address="192.168.1.104"
    )
    create_test_order(db, modality="CT", accession_number="CT_STUDY")
    
    # Mock the association
    mock_c_find_event.assoc.requestor.ae_title = "INACTIVE_FACILITY_CT"
    mock_c_find_event.assoc.requestor.address = "192.168.1.104"
    
    monkeypatch.setattr(handlers, "SessionLocal", lambda: db)
    
    # Create the query dataset
    query_ds = Dataset()
    query_ds.ScheduledProcedureStepSequence = [Dataset()]
    query_ds.ScheduledProcedureStepSequence[0].Modality = "CT"
    mock_c_find_event.identifier = query_ds
    
    # Act
    responses = list(handlers.handle_c_find(mock_c_find_event))
    
    # Assert
    assert len(responses) == 1  # Only success response, no results
    assert responses[0][0] == 0x0000  # Success with no data