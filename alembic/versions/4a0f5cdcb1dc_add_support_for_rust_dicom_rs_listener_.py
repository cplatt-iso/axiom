"""Add support for rust dicom-rs listener and sender types

Revision ID: 4a0f5cdcb1dc
Revises: 20ccf26766b4
Create Date: 2025-09-09 22:24:50.143610

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4a0f5cdcb1dc'
down_revision: Union[str, None] = '20ccf26766b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Update column comments to include the new dicom-rs type
    op.alter_column(
        'dimse_listener_configs',
        'listener_type',
        comment="The type of listener implementation to use ('pynetdicom', 'dcm4che', or 'dicom-rs')."
    )
    
    op.alter_column(
        'storage_backend_configs',
        'sender_type',
        comment="The type of sender to use ('pynetdicom', 'dcm4che', or 'dicom-rs')."
    )


def downgrade() -> None:
    # Revert column comments to original values
    op.alter_column(
        'dimse_listener_configs',
        'listener_type',
        comment="The type of listener implementation to use ('pynetdicom' or 'dcm4che')."
    )
    
    op.alter_column(
        'storage_backend_configs',
        'sender_type',
        comment="The type of sender to use ('pynetdicom' or 'dcm4che')."
    )


