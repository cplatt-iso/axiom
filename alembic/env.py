# alembic/env.py
import os
import sys
from pathlib import Path # Use pathlib for robust path handling

from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool
# from sqlalchemy import create_engine # Only needed if setting URL directly from settings

from alembic import context

# --- START Project Specific Setup ---

# Add project root directory to Python path (assuming env.py is in project_root/alembic/)
# The root is the parent directory of the 'alembic' directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import Base from your application's model base
# Ensure this path matches your project structure
from app.db.base import Base

# Import all your models here so Base.metadata knows about them
# This relies on your models/__init__.py importing all necessary model files
from app.db import models

# Import settings to get DB URL details for interpolation in alembic.ini
from app.core.config import settings

# --- END Project Specific Setup ---


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# --- START Set .ini variables from Settings ---
# Make database connection details from your .env/settings available
# for interpolation in alembic.ini's sqlalchemy.url. This allows
# alembic.ini to use generic placeholders like %(POSTGRES_USER)s
# while the actual values come securely from your Settings object (loaded from .env).
if settings.POSTGRES_USER:
    config.set_main_option('POSTGRES_USER', settings.POSTGRES_USER)
if settings.POSTGRES_PASSWORD:
    # Be cautious committing this if the password isn't set via ENV VAR in production
    config.set_main_option('POSTGRES_PASSWORD', settings.POSTGRES_PASSWORD)
if settings.POSTGRES_DB:
    config.set_main_option('POSTGRES_DB', settings.POSTGRES_DB)

# Ensure the host is 'db' (the service name) when running inside docker
# This overrides any host setting (like 'localhost') that might be in settings
# specifically for Alembic running within the docker network.
config.set_main_option('POSTGRES_HOST', 'db')

# The port should be the internal PostgreSQL port (5432) used within the Docker network
config.set_main_option('POSTGRES_PORT', str(settings.POSTGRES_PORT)) # Usually 5432

# Optional: Set the sqlalchemy.url directly from settings if needed
# if settings.SQLALCHEMY_DATABASE_URI:
#     config.set_main_option('sqlalchemy.url', settings.SQLALCHEMY_DATABASE_URI)
# else:
#     print("Warning: SQLALCHEMY_DATABASE_URI not found in settings. Relying solely on alembic.ini interpolation.")

# --- END Set .ini variables ---


# Interpret the config file for Python logging.
# Ensure this is done AFTER setting up sys.path if your logging config relies on app modules
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# --- START Target Metadata Setup ---
target_metadata = Base.metadata # Point Alembic to your Base's metadata
# --- END Target Metadata Setup ---

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # Get the URL interpolated from alembic.ini using the options we set above
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata, # Use the imported Base.metadata
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # This reads connection info from alembic.ini, using the interpolated values
    # config.get_section(config.config_main_option name) returns the [alembic] section
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}), # Use config_ini_section
        prefix="sqlalchemy.", # Look for keys starting with 'sqlalchemy.'
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata # Use the imported Base.metadata
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
