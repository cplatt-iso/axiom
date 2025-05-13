"""add_is_enabled_to_ai_prompt_configs

Revision ID: c7ad173ca9fd
Revises: ed47be840842
Create Date: 2025-05-13 16:39:03.727706 
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
# from sqlalchemy import text # If you were to use text('TRUE') for server_default

# revision identifiers, used by Alembic.
revision: str = 'c7ad173ca9fd'
down_revision: Union[str, None] = 'ed47be840842'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Using op.batch_alter_table for compatibility, common with SQLite, but works for PG too.
    # Ensure this matches how other alterations are done in your project for this table.
    with op.batch_alter_table('a_i_prompt_configs', schema=None) as batch_op: # <<< CORRECTED TABLE NAME
        batch_op.add_column(sa.Column('is_enabled', 
                                      sa.Boolean(), 
                                      nullable=False, 
                                      server_default=sa.true(), # <<< CRITICAL SERVER DEFAULT
                                      comment='Whether this AI prompt configuraiton is active and can be used.'))
        # The batch_op.f() helper for index names should correctly use the table name from batch_alter_table.
        batch_op.create_index(batch_op.f('ix_a_i_prompt_configs_is_enabled'), ['is_enabled'], unique=False)


def downgrade() -> None:
    with op.batch_alter_table('a_i_prompt_configs', schema=None) as batch_op: # <<< CORRECTED TABLE NAME
        # The batch_op.f() helper for index names should correctly use the table name from batch_alter_table.
        batch_op.drop_index(batch_op.f('ix_a_i_prompt_configs_is_enabled')) # No need for explicit table_name here if using batch_op.f
        batch_op.drop_column('is_enabled')
