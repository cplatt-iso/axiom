# app/db/models/dimse_listener_state.py
from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func
from app.db.base import Base

# Rename class and table
class DimseListenerState(Base):
    __tablename__ = "dimse_listener_state" # Rename table

    id = Column(Integer, primary_key=True, index=True)
    listener_id = Column(String, unique=True, index=True, nullable=False) # Instance ID (e.g., hostname)
    status = Column(String, nullable=False, default="unknown")
    status_message = Column(Text, nullable=True)
    host = Column(String, nullable=True)
    port = Column(Integer, nullable=True)
    ae_title = Column(String, nullable=True)
    last_heartbeat = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        # Update class name in repr
        return (f"<DimseListenerState(id={self.id}, listener_id='{self.listener_id}', "
                f"status='{self.status}', last_heartbeat='{self.last_heartbeat}')>")
