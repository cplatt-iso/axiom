# app/db/session.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator

from app.core.config import settings

# Create the SQLAlchemy engine
# connect_args is optional, used here to set command timeout for psycopg driver
# 'check_same_thread': False is ONLY needed for SQLite, not PostgreSQL
# Add pool_pre_ping=True for resilience against db connection drops
engine_args = {"pool_pre_ping": True}
# if "sqlite" in settings.SQLALCHEMY_DATABASE_URI:
#     engine_args["connect_args"] = {"check_same_thread": False}
# else:
#     engine_args["connect_args"] = {"options": "-c statement_timeout=30000"} # 30 seconds in ms

engine = create_engine(
    str(settings.SQLALCHEMY_DATABASE_URI), # Use the URI from settings
    **engine_args
)

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get DB session in FastAPI routes
def get_db() -> Generator[Session, None, None]:
    """
    FastAPI dependency that provides a SQLAlchemy database session.

    Ensures the session is closed after the request is finished.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Optional: Function to initialize DB (create tables)
# You might call this from a script or Alembic migrations later
def init_db():
    from app.db.base import Base
    # Import all models here so they register with Base.metadata
    from app.db.models import User, Role, RuleSet, Rule # noqa F401
    print("Creating database tables...")
    try:
        Base.metadata.create_all(bind=engine)
        print("Database tables created successfully.")
    except Exception as e:
        print(f"Error creating database tables: {e}")

def try_connect_db():
    """Attempts to connect to the database to verify connection."""
    try:
        connection = engine.connect()
        connection.close()
        print("Database connection successful.")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False

if __name__ == "__main__":
    # Simple command-line execution for testing connection or initializing DB
    import argparse
    parser = argparse.ArgumentParser(description="Database Utils")
    parser.add_argument(
        '--init',
        action='store_true',
        help='Initialize database (create tables)'
        )
    parser.add_argument(
        '--connect-test',
        action='store_true',
        help='Test database connection'
        )
    args = parser.parse_args()

    if args.connect_test:
        try_connect_db()
    if args.init:
        # Maybe add a prompt here asking for confirmation in a real scenario
        print("WARNING: This will create tables based on current models.")
        if try_connect_db():
             init_db()
        else:
             print("Skipping table creation due to connection failure.")
