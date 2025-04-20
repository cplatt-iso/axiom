# alembic/versions/7aa6e752efad_....py

"""Correct DicomWebSourceState schema: remove id column, set source_name as PK

Revision ID: 7aa6e752efad
Revises: b1a7967d4509 # Make sure 'Revises' points to your previous migration ID
Create Date: 2025-04-18 19:42:51.303388

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
# Import TIMESTAMP if needed for downgrade type hint
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '7aa6e752efad'
down_revision: Union[str, None] = 'b1a7967d4509' # Adjust if your previous revision was different
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands manually edited based on schema diff ###

    # 1. Drop the incorrect composite primary key constraint
    # Constraint name found from \d output
    op.drop_constraint('pk_dicomweb_source_state', 'dicomweb_source_state', type_='primary')

    # 2. Drop the index on the 'id' column (optional but good practice)
    # Index name found from \d output
    op.drop_index('ix_dicomweb_source_state_id', table_name='dicomweb_source_state')

    # 3. Drop the unnecessary 'id' column
    op.drop_column('dicomweb_source_state', 'id')

    # 4. Create the correct primary key constraint on 'source_name' only
    # Using a standard naming convention here, adjust if needed
    op.create_primary_key(
        'pk_dicomweb_source_state', # Standard name for primary key constraints
        'dicomweb_source_state',
        ['source_name']
    )

    # Note: We are *keeping* created_at and updated_at as they might be useful,
    # even if not explicitly in the basic model. If you want them removed,
    # add op.drop_column(...) calls for them here.

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands manually edited to reverse the upgrade ###

    # 1. Drop the correct primary key constraint
    op.drop_constraint('pk_dicomweb_source_state', 'dicomweb_source_state', type_='primary')

    # 2. Add the 'id' column back
    # Must match the original definition (INTEGER, NOT NULL)
    op.add_column('dicomweb_source_state', sa.Column('id', sa.INTEGER(), autoincrement=False, nullable=False))

    # 3. Recreate the index on the 'id' column
    op.create_index('ix_dicomweb_source_state_id', 'dicomweb_source_state', ['id'], unique=False)

    # 4. Recreate the incorrect composite primary key constraint
    op.create_primary_key(
        'pk_dicomweb_source_state',
        'dicomweb_source_state',
         ['source_name', 'id'] # Recreate the composite key
    )

    # Note: If you removed created_at/updated_at in upgrade, add them back here.
    # op.add_column('dicomweb_source_state', sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('now()'), autoincrement=False, nullable=False))
    # op.add_column('dicomweb_source_state', sa.Column('updated_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('now()'), autoincrement=False, nullable=False))


    # ### end Alembic commands ###
