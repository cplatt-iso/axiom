# app/db/models/dimse_listener_state.py
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, mapped_column
from app.db.base import Base
from datetime import datetime

class DimseListenerState(Base):
    __tablename__ = "dimse_listener_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True) # Use Mapped
    listener_id: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="unknown")
    status_message: Mapped[str | None] = mapped_column(Text, nullable=True) # Use Mapped + union type
    host: Mapped[str | None] = mapped_column(String, nullable=True)
    port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ae_title: Mapped[str | None] = mapped_column(String, nullable=True)
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now()) # Use Mapped
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now()) # Use Mapped

    received_instance_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total count of instances received by this listener."
    )
    processed_instance_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default='0',
        comment="Total count of instances successfully processed after reception."
    )

    # updated_at is inherited from Base

    def __repr__(self):
        return (f"<DimseListenerState(id={self.id}, listener_id='{self.listener_id}', "
                f"status='{self.status}', received={self.received_instance_count})>") # Added received count
