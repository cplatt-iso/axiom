# backend/app/tests/conftest.py

import pytest
import os
import pydicom
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
from unittest.mock import Mock, MagicMock
import importlib

from app.db.base import Base
from app.core.config import settings

# This fixture provides the test DB session.
@pytest.fixture(scope="function")
def db() -> Generator[Session, None, None]:
    """
    Pytest fixture that provides a test DB session to a dedicated test database,
    and REFUSES to run against a non-test database.
    """
    # --- THE SAFETY CATCH ---
    # We derive the test DB name and explicitly check it.
    base_db_name = settings.POSTGRES_DB
    test_db_name = f"{base_db_name}_test"
    
    if "_test" not in test_db_name:
        pytest.fail(
            "FATAL: Attempting to run tests on a database that does not end in '_test'. "
            f"Cowardly refusing to drop tables on '{test_db_name}'. "
            "Check your POSTGRES_DB environment variable."
        )

    db_host = "localhost" # For local testing
    user = settings.POSTGRES_USER
    password = settings.POSTGRES_PASSWORD.get_secret_value()
    
    SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg://{user}:{password}@{db_host}:5432/{test_db_name}"
    
    try:
        engine = create_engine(SQLALCHEMY_DATABASE_URL)
        Base.metadata.create_all(bind=engine)
        connection = engine.connect()
    except Exception as e:
        pytest.fail(
            f"FATAL: Could not connect to the test database '{test_db_name}'. "
            f"Please ensure it exists and is accessible. Original error: {e}"
        )

    transaction = connection.begin()
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=connection)
    db_session = TestingSessionLocal()
    
    yield db_session
    
    db_session.close()
    transaction.rollback()
    connection.close()
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def mock_c_find_event() -> Mock:
    """
    Provides a mock pynetdicom C-FIND event object for testing handlers.
    """
    mock_assoc = MagicMock()
    mock_assoc.native_id = 123
    mock_assoc.requestor.ae_title = "TEST_MODALITY"
    mock_assoc.requestor.address = "127.0.0.1"
    mock_assoc.acceptor.ae_title = "TEST_SCP"
    event = Mock()
    event.assoc = mock_assoc
    event.AffectedSOPClassUID = "1.2.840.10008.5.1.4.31"
    event.identifier = pydicom.Dataset()
    return event