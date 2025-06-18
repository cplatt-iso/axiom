# backend/app/tests/conftest.py

import pytest
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from app.db.base import Base
from app.core.config import settings
from typing import Generator

# --- THE ONE FIX TO RULE THEM ALL ---
# Check an environment variable to determine the database host.
# This makes the test suite work both inside and outside Docker.
DB_HOST = "db" if os.getenv("RUNNING_IN_DOCKER") else "localhost"

# Use a separate test database to be safe.
# We append '_test' to the database name.
user = settings.POSTGRES_USER
password = settings.POSTGRES_PASSWORD.get_secret_value()
db_name = f"{settings.POSTGRES_DB}_test"

SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg://{user}:{password}@{DB_HOST}:5432/{db_name}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function")
def db() -> Generator[Session, None, None]:
    """
    Pytest fixture to provide a transactional scope around a test.
    This fixture now connects to a separate test database and ensures all tables are created.
    """
    # Create all tables
    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        # If the test DB doesn't exist, this might fail.
        # A proper setup would involve a script to run `CREATE DATABASE ..._test`
        print(f"Error creating tables, you might need to manually create the test database '{db_name}'. Error: {e}")
        raise

    connection = engine.connect()
    transaction = connection.begin()
    db_session = TestingSessionLocal(bind=connection)

    yield db_session

    # After the test, rollback and drop all tables for a clean slate.
    db_session.close()
    transaction.rollback()
    connection.close()
    Base.metadata.drop_all(bind=engine)