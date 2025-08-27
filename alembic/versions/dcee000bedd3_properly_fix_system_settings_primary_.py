"""Properly fix system_settings primary key to use auto-increment id

Revision ID: dcee000bedd3
Revises: 9e323e9873e2
Create Date: 2025-08-26 22:01:07.400459

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'dcee000bedd3'
down_revision: Union[str, None] = '9e323e9873e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # First, drop the compound primary key constraint
    op.drop_constraint('pk_system_settings', 'system_settings', type_='primary')
    
    # Create a sequence for the id column 
    op.execute("CREATE SEQUENCE IF NOT EXISTS system_settings_id_seq")
    
    # Set the default for id column to use the sequence
    op.execute("ALTER TABLE system_settings ALTER COLUMN id SET DEFAULT nextval('system_settings_id_seq')")
    
    # Set the sequence ownership to the id column
    op.execute("ALTER SEQUENCE system_settings_id_seq OWNED BY system_settings.id")
    
    # Set the current sequence value based on existing data (if any)
    op.execute("SELECT setval('system_settings_id_seq', COALESCE(MAX(id), 1)) FROM system_settings")
    
    # Now make id the primary key
    op.create_primary_key('pk_system_settings', 'system_settings', ['id'])


def downgrade() -> None:
    # Drop the single-column primary key
    op.drop_constraint('pk_system_settings', 'system_settings', type_='primary')
    
    # Remove the sequence default
    op.execute("ALTER TABLE system_settings ALTER COLUMN id DROP DEFAULT")
    
    # Drop the sequence
    op.execute("DROP SEQUENCE IF EXISTS system_settings_id_seq")
    
    # Recreate the compound primary key
    op.create_primary_key('pk_system_settings', 'system_settings', ['key', 'id'])

