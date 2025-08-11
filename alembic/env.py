# alembic/env.py
import os # Added import
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# --- ADDED: Import Settings ---
from app.core.config import settings
# --- END ADDED ---

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# --- ADDED: Import Base and models ---
from app.db.base import Base
from app.db.models import user
from app.db.models import rule
from app.db.models import api_key
from app.db.models import dicomweb_source_state
from app.db.models import dimse_listener_state
from app.db.models import dimse_listener_config  # <--- Ensure this one is present
from app.db.models import dimse_qr_source        # <--- Ensure this one is present
from app.db.models import processed_study_log
from app.db.models import storage_backend_config # <--- Ensure this one is present
from app.db.models import crosswalk
from app.db.models import schedule
from app.db.models import ai_prompt_config
from app.db.models import spanner               # <--- Added for query spanning
# Import your models here so Alembic can see them
# This relies on app/db/models/__init__.py importing all model classes
# from app.db import models # noqa F401 - Imports __init__, which imports all models
target_metadata = Base.metadata
# --- END ADDED ---

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.

def get_url():
    """Builds the database URL from settings."""
    # Ensure settings are loaded
    db_uri = settings.SQLALCHEMY_DATABASE_URI
    if db_uri:
        return str(db_uri) # Convert Pydantic DSN type to string
    else:
         # Fallback or error if URI isn't constructed in settings
         # This part should ideally not be reached if settings are correct
         user = os.getenv("POSTGRES_USER", "dicom_processor_user")
         password = os.getenv("POSTGRES_PASSWORD", "changeme") # Fallback password
         server = os.getenv("POSTGRES_SERVER", "db")
         port = os.getenv("POSTGRES_PORT", "5432")
         db = os.getenv("POSTGRES_DB", "dicom_processor_db")
         return f"postgresql+psycopg://{user}:{password}@{server}:{port}/{db}"

def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # --- UPDATED: Use get_url() ---
    # url = config.get_main_option("sqlalchemy.url")
    url = get_url()
    # --- END UPDATED ---
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
         # Add naming convention for offline mode too
        compare_type=True, # Compare column types
        render_as_batch=True, # Enable batch mode for SQLite compatibility if needed, generally safe
        include_schemas=True, # Ensure schemas are considered if using PostgreSQL schemas
        # Naming convention for constraints
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        },
    )

    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # --- MODIFIED: Build config from settings ---
    # Use the get_url() function to build the URL consistently
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url() # Set the URL from our function

    # Example of setting other options from settings if needed, ensuring they are strings
    # password_value = settings.POSTGRES_PASSWORD.get_secret_value() if hasattr(settings.POSTGRES_PASSWORD, 'get_secret_value') else str(settings.POSTGRES_PASSWORD)
    # configuration['POSTGRES_PASSWORD'] = password_value # Example if needed directly in config

    connectable = engine_from_config(
        configuration, # Use the modified configuration dict
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    # --- END MODIFIED ---

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True, # Compare column types
            render_as_batch=True, # Enable batch mode for SQLite compatibility if needed, generally safe
            include_schemas=True, # Ensure schemas are considered if using PostgreSQL schemas
             # Naming convention for constraints
            naming_convention={
                "ix": "ix_%(column_0_label)s",
                "uq": "uq_%(table_name)s_%(column_0_name)s",
                "ck": "ck_%(table_name)s_%(constraint_name)s",
                "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
                "pk": "pk_%(table_name)s",
            },
        )

        with context.begin_transaction():
            context.run_migrations()

# --- REMOVED problematic config.set_main_option calls ---
# config.set_main_option('POSTGRES_USER', settings.POSTGRES_USER)
# password_value = settings.POSTGRES_PASSWORD.get_secret_value() if hasattr(settings.POSTGRES_PASSWORD, 'get_secret_value') else str(settings.POSTGRES_PASSWORD)
# config.set_main_option('POSTGRES_PASSWORD', password_value) # Pass the string value
# config.set_main_option('POSTGRES_SERVER', settings.POSTGRES_SERVER)
# config.set_main_option('POSTGRES_PORT', str(settings.POSTGRES_PORT)) # Ensure port is string
# config.set_main_option('POSTGRES_DB', settings.POSTGRES_DB)
# config.set_main_option("sqlalchemy.url", get_url())
# --- END REMOVED ---


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
