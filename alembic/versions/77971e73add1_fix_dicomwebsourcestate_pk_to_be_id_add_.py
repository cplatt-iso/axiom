"""Fix DicomWebSourceState PK to be id, add sequence

Revision ID: 77971e73add1
Revises: dd336697353e
Create Date: 2025-04-18 21:28:01.390892

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '77971e73add1'
down_revision: Union[str, None] = 'dd336697353e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE_NAME = 'dicomweb_source_state'
SEQUENCE_NAME = f'{TABLE_NAME}_id_seq'
PK_CONSTRAINT_NAME = f'pk_{TABLE_NAME}' # Standard PK name from convention
OLD_PK_CONSTRAINT_NAME = 'pk_dicomweb_source_state' # The current incorrect one

def upgrade() -> None:
    # ### Manually define operations to fix PK and add sequence ###

    # 1. Drop the existing incorrect primary key constraint on source_name
    # Use the actual name shown by \d if different
    op.drop_constraint(OLD_PK_CONSTRAINT_NAME, TABLE_NAME, type_='primary')

    # 2. Create a sequence for the id column
    op.execute(sa.schema.CreateSequence(sa.schema.Sequence(SEQUENCE_NAME)))

    # 3. Alter the id column to use the sequence as default and set NOT NULL (already not null)
    # Ensure the column type is correct (INTEGER)
    op.alter_column(
        TABLE_NAME,
        'id',
        nullable=False,
        server_default=sa.text(f"nextval('{SEQUENCE_NAME}'::regclass)")
    )

    # 4. Create the correct primary key constraint on the 'id' column
    op.create_primary_key(PK_CONSTRAINT_NAME, TABLE_NAME, ['id'])

    # ### end Alembic commands ###


def downgrade() -> None:
    # ### Manually define operations to reverse the fix ###

    # 1. Drop the correct primary key constraint on 'id'
    op.drop_constraint(PK_CONSTRAINT_NAME, TABLE_NAME, type_='primary')

    # 2. Alter the id column to remove the sequence default
    op.alter_column(TABLE_NAME, 'id', server_default=None)

    # 3. Drop the sequence
    op.execute(sa.schema.DropSequence(sa.schema.Sequence(SEQUENCE_NAME)))

    # 4. Recreate the incorrect primary key constraint on 'source_name'
    # This assumes 'id' still exists from the previous downgrade state
    op.create_primary_key(OLD_PK_CONSTRAINT_NAME, TABLE_NAME, ['source_name'])
