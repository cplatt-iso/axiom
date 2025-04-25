# app/db/base.py
import re
from typing import Optional
from datetime import datetime

from sqlalchemy import MetaData, String, func, DateTime, Integer, Table, Column, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, declared_attr

convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=convention)

class Base(DeclarativeBase):
    metadata = metadata

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        index=True
    )

    @declared_attr.directive
    def __tablename__(cls) -> str:
        name = re.sub(r'(?<!^)(?=[A-Z])', '_', cls.__name__).lower()
        if not name.endswith('s'):
            name += 's'
        return name

rule_destination_association = Table(
    'rule_destination_association',
    Base.metadata, # Use the metadata from Base
    Column('rule_id', Integer, ForeignKey('rules.id', ondelete="CASCADE"), primary_key=True),
    Column('storage_backend_config_id', Integer, ForeignKey('storage_backend_configs.id', ondelete="CASCADE"), primary_key=True)
)
