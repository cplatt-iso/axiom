from sqlalchemy import String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base

class SystemSetting(Base):
    __tablename__ = "system_settings" # type: ignore
    
    # The key should be unique but let the base id be the primary key
    key: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True, comment="The unique key for the setting.")
    value: Mapped[str] = mapped_column(Text, nullable=False, comment="The value of the setting.")
    
    # Add a unique constraint on key for extra safety
    __table_args__ = (
        UniqueConstraint('key', name='uq_system_settings_key'),
    )