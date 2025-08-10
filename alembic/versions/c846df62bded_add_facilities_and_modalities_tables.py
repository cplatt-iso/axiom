"""add_facilities_and_modalities_tables

Revision ID: c846df62bded
Revises: 97b137f49c85
Create Date: 2025-08-10 03:13:00.756556

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c846df62bded'
down_revision: Union[str, None] = '97b137f49c85'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade():
    # Create facilities table
    op.create_table('facilities',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('address_line_1', sa.String(length=255), nullable=True),
        sa.Column('address_line_2', sa.String(length=255), nullable=True),
        sa.Column('city', sa.String(length=100), nullable=True),
        sa.Column('state_province', sa.String(length=100), nullable=True),
        sa.Column('postal_code', sa.String(length=20), nullable=True),
        sa.Column('country', sa.String(length=100), nullable=True),
        sa.Column('phone', sa.String(length=50), nullable=True),
        sa.Column('email', sa.String(length=255), nullable=True),
        sa.Column('contact_person', sa.String(length=255), nullable=True),
        sa.Column('admin_contact', sa.String(length=255), nullable=True),
        sa.Column('timezone', sa.String(length=50), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_facilities_id'), 'facilities', ['id'], unique=False)
    op.create_index(op.f('ix_facilities_name'), 'facilities', ['name'], unique=True)
    op.create_index(op.f('ix_facilities_is_active'), 'facilities', ['is_active'], unique=False)

    # Create modalities table
    op.create_table('modalities',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('facility_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('ae_title', sa.String(length=16), nullable=False),
        sa.Column('ip_address', sa.String(length=45), nullable=False),
        sa.Column('port', sa.Integer(), nullable=False, default=104),
        sa.Column('modality_type', sa.String(length=10), nullable=False),
        sa.Column('manufacturer', sa.String(length=255), nullable=True),
        sa.Column('model', sa.String(length=255), nullable=True),
        sa.Column('software_version', sa.String(length=255), nullable=True),
        sa.Column('department', sa.String(length=100), nullable=True),
        sa.Column('location', sa.String(length=255), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=False, default=True),
        sa.Column('is_dmwl_enabled', sa.Boolean(), nullable=False, default=False),
        sa.Column('dmwl_security_level', sa.String(length=20), nullable=False, default='BASIC'),
        sa.Column('supported_sop_classes', postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['facility_id'], ['facilities.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_modalities_id'), 'modalities', ['id'], unique=False)
    op.create_index(op.f('ix_modalities_ae_title'), 'modalities', ['ae_title'], unique=True)
    op.create_index(op.f('ix_modalities_facility_id'), 'modalities', ['facility_id'], unique=False)
    op.create_index(op.f('ix_modalities_is_active'), 'modalities', ['is_active'], unique=False)
    op.create_index(op.f('ix_modalities_is_dmwl_enabled'), 'modalities', ['is_dmwl_enabled'], unique=False)
    op.create_index(op.f('ix_modalities_modality_type'), 'modalities', ['modality_type'], unique=False)


def downgrade():
    # Drop modalities table
    op.drop_index(op.f('ix_modalities_modality_type'), table_name='modalities')
    op.drop_index(op.f('ix_modalities_is_dmwl_enabled'), table_name='modalities')
    op.drop_index(op.f('ix_modalities_is_active'), table_name='modalities')
    op.drop_index(op.f('ix_modalities_facility_id'), table_name='modalities')
    op.drop_index(op.f('ix_modalities_ae_title'), table_name='modalities')
    op.drop_index(op.f('ix_modalities_id'), table_name='modalities')
    op.drop_table('modalities')
    
    # Drop facilities table
    op.drop_index(op.f('ix_facilities_is_active'), table_name='facilities')
    op.drop_index(op.f('ix_facilities_name'), table_name='facilities')
    op.drop_index(op.f('ix_facilities_id'), table_name='facilities')
    op.drop_table('facilities')

