# app/db/base.py
import re
from typing import Optional
from datetime import datetime

from sqlalchemy import MetaData, String, func, DateTime, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, declared_attr

# Define naming convention for constraints
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=convention)

class Base(DeclarativeBase):
    """
    Base class for all SQLAlchemy models.

    Includes an auto-generated __tablename__, a primary key `id`,
    and standard created_at/updated_at timestamps.
    """
    metadata = metadata

    id: Mapped[int] = mapped_column(primary_key=True, index=True)

    # --- Corrected Timestamp Definitions ---
    created_at: Mapped[datetime] = mapped_column( # Can remove Optional if always set by DB
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )
    updated_at: Mapped[datetime] = mapped_column( # Can remove Optional if always set by DB
        DateTime(timezone=True),
        server_default=func.now(), # <-- ADD THIS for initial creation
        onupdate=func.now(),
        nullable=False, # Make non-nullable as it should always be set
        index=True
    )
    # --- End Corrected Timestamp Definitions ---


    # Generate __tablename__ automatically based on class name
    @declared_attr.directive
    def __tablename__(cls) -> str:
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower()
        if not name.endswith('s'):
            name += 's'
        return name
