# app/db/models/sender_config.py
from typing import Optional

from sqlalchemy import String, Boolean, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base

class SenderConfig(Base):
    __tablename__ = "senders"  # type: ignore

    name: Mapped[str] = mapped_column(String(100), unique=True, index=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sender_type: Mapped[str] = mapped_column(String(50), nullable=False)
    local_ae_title: Mapped[str] = mapped_column(String(16), nullable=False, default="AXIOM_SCU")
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
