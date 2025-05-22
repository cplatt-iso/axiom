"""add_auth_and_tls_ca_to_stowrs_backend_config

Revision ID: 9519b9cec0d6
Revises: c7ad173ca9fd
Create Date: 2025-05-20 20:47:35.778661

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9519b9cec0d6'
down_revision: Union[str, None] = 'c7ad173ca9fd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.add_column('storage_backend_configs', sa.Column('username_secret_name', sa.String(length=512), nullable=True, comment="Optional: Secret Manager resource name for the STOW-RS username."))
    op.add_column('storage_backend_configs', sa.Column('password_secret_name', sa.String(length=512), nullable=True, comment="Optional: Secret Manager resource name for the STOW-RS password."))
    # tls_ca_cert_secret_name was likely added by migration b179fd2df5a9
# ...
def downgrade() -> None:
    # tls_ca_cert_secret_name was likely added by migration b179fd2df5a9
    op.drop_column('storage_backend_configs', 'password_secret_name')
    op.drop_column('storage_backend_configs', 'username_secret_name')
