"""add_multi_source_support_to_spanner_mappings

Revision ID: a11c63999d7d
Revises: 20a5e96f8a5a
Create Date: 2025-08-12 21:47:12.810709

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a11c63999d7d'
down_revision: Union[str, None] = '20a5e96f8a5a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add source_type column with default value
    op.add_column('spanner_source_mappings', sa.Column('source_type', sa.String(20), nullable=False, server_default='dimse-qr'))
    
    # Add new source ID columns (nullable)
    op.add_column('spanner_source_mappings', sa.Column('dicomweb_source_id', sa.Integer(), nullable=True))
    op.add_column('spanner_source_mappings', sa.Column('google_healthcare_source_id', sa.Integer(), nullable=True))
    
    # Make dimse_qr_source_id nullable since other source types won't use it
    op.alter_column('spanner_source_mappings', 'dimse_qr_source_id', nullable=True)
    
    # Add foreign key constraints for new source types
    op.create_foreign_key(
        'fk_spanner_source_mappings_dicomweb_source_id',
        'spanner_source_mappings', 'dicomweb_source_state',
        ['dicomweb_source_id'], ['id'],
        ondelete='CASCADE'
    )
    op.create_foreign_key(
        'fk_spanner_source_mappings_google_healthcare_source_id', 
        'spanner_source_mappings', 'google_healthcare_sources',
        ['google_healthcare_source_id'], ['id'],
        ondelete='CASCADE'
    )
    
    
    # Drop the old unique constraint
    op.drop_constraint('uq_spanner_source_mapping', 'spanner_source_mappings', type_='unique')
    
    # Add new unique constraints for each source type (simplified without WHERE clauses)
    # Note: Application-level validation will ensure only one source ID is provided
    op.create_unique_constraint(
        'uq_spanner_dimse_mapping',
        'spanner_source_mappings',
        ['spanner_config_id', 'dimse_qr_source_id']
    )
    op.create_unique_constraint(
        'uq_spanner_dicomweb_mapping',
        'spanner_source_mappings', 
        ['spanner_config_id', 'dicomweb_source_id']
    )
    op.create_unique_constraint(
        'uq_spanner_healthcare_mapping',
        'spanner_source_mappings',
        ['spanner_config_id', 'google_healthcare_source_id']
    )
    
    # Add check constraint to ensure exactly one source ID is set
    op.create_check_constraint(
        'chk_one_source_type',
        'spanner_source_mappings',
        sa.text("""
            (dimse_qr_source_id IS NOT NULL AND dicomweb_source_id IS NULL AND google_healthcare_source_id IS NULL) OR
            (dimse_qr_source_id IS NULL AND dicomweb_source_id IS NOT NULL AND google_healthcare_source_id IS NULL) OR  
            (dimse_qr_source_id IS NULL AND dicomweb_source_id IS NULL AND google_healthcare_source_id IS NOT NULL)
        """)
    )
    
    # Create indexes for better performance
    op.create_index('ix_spanner_source_mappings_source_type', 'spanner_source_mappings', ['source_type'])
    op.create_index('ix_spanner_source_mappings_dicomweb_source_id', 'spanner_source_mappings', ['dicomweb_source_id'])
    op.create_index('ix_spanner_source_mappings_google_healthcare_source_id', 'spanner_source_mappings', ['google_healthcare_source_id'])


def downgrade() -> None:
    # Drop indexes
    op.drop_index('ix_spanner_source_mappings_google_healthcare_source_id', 'spanner_source_mappings')
    op.drop_index('ix_spanner_source_mappings_dicomweb_source_id', 'spanner_source_mappings')
    op.drop_index('ix_spanner_source_mappings_source_type', 'spanner_source_mappings')
    
    # Drop check constraint
    op.drop_constraint('chk_one_source_type', 'spanner_source_mappings', type_='check')
    
    # Drop unique constraints
    op.drop_constraint('uq_spanner_healthcare_mapping', 'spanner_source_mappings', type_='unique')
    op.drop_constraint('uq_spanner_dicomweb_mapping', 'spanner_source_mappings', type_='unique') 
    op.drop_constraint('uq_spanner_dimse_mapping', 'spanner_source_mappings', type_='unique')
    
    # Recreate old unique constraint
    op.create_unique_constraint('uq_spanner_source_mapping', 'spanner_source_mappings', ['spanner_config_id', 'dimse_qr_source_id'])
    
    # Drop foreign key constraints
    op.drop_constraint('fk_spanner_source_mappings_google_healthcare_source_id', 'spanner_source_mappings', type_='foreignkey')
    op.drop_constraint('fk_spanner_source_mappings_dicomweb_source_id', 'spanner_source_mappings', type_='foreignkey')
    
    # Make dimse_qr_source_id non-nullable again
    op.alter_column('spanner_source_mappings', 'dimse_qr_source_id', nullable=False)
    
    # Drop new columns
    op.drop_column('spanner_source_mappings', 'google_healthcare_source_id')
    op.drop_column('spanner_source_mappings', 'dicomweb_source_id')
    op.drop_column('spanner_source_mappings', 'source_type')
