"""add_health_status_fields_to_sources

Revision ID: 4dc50b2cbec1
Revises: 123456789abc
Create Date: 2025-08-11 05:43:10.427988

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4dc50b2cbec1'
down_revision: Union[str, None] = '123456789abc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add health status fields to dicomweb_source_state table
    with op.batch_alter_table('dicomweb_source_state', schema=None) as batch_op:
        batch_op.add_column(sa.Column('health_status', sa.String(20), nullable=False, server_default='UNKNOWN'))
        batch_op.add_column(sa.Column('last_health_check', sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column('last_health_error', sa.Text(), nullable=True))
        batch_op.create_index('idx_dicomweb_source_health_status', ['health_status'])

    # Add health status fields to dimse_qr_sources table
    with op.batch_alter_table('dimse_qr_sources', schema=None) as batch_op:
        batch_op.add_column(sa.Column('health_status', sa.String(20), nullable=False, server_default='UNKNOWN'))
        batch_op.add_column(sa.Column('last_health_check', sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column('last_health_error', sa.Text(), nullable=True))
        batch_op.create_index('idx_dimse_qr_source_health_status', ['health_status'])

    # Add health status fields to google_healthcare_sources table
    with op.batch_alter_table('google_healthcare_sources', schema=None) as batch_op:
        batch_op.add_column(sa.Column('health_status', sa.String(20), nullable=False, server_default='UNKNOWN'))
        batch_op.add_column(sa.Column('last_health_check', sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column('last_health_error', sa.Text(), nullable=True))
        batch_op.create_index('idx_google_healthcare_source_health_status', ['health_status'])


def downgrade() -> None:
    # Remove health status fields from google_healthcare_sources table
    with op.batch_alter_table('google_healthcare_sources', schema=None) as batch_op:
        batch_op.drop_index('idx_google_healthcare_source_health_status')
        batch_op.drop_column('health_status')
        batch_op.drop_column('last_health_check')
        batch_op.drop_column('last_health_error')

    # Remove health status fields from dimse_qr_sources table
    with op.batch_alter_table('dimse_qr_sources', schema=None) as batch_op:
        batch_op.drop_index('idx_dimse_qr_source_health_status')
        batch_op.drop_column('health_status')
        batch_op.drop_column('last_health_check')
        batch_op.drop_column('last_health_error')

    # Remove health status fields from dicomweb_source_state table
    with op.batch_alter_table('dicomweb_source_state', schema=None) as batch_op:
        batch_op.drop_index('idx_dicomweb_source_health_status')
        batch_op.drop_column('health_status')
        batch_op.drop_column('last_health_check')
        batch_op.drop_column('last_health_error')
