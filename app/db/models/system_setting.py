from sqlalchemy import Column, String, Text
from app.db.base import Base

class SystemSetting(Base):
    __tablename__ = "system_settings" # type: ignore
    key = Column(String(255), primary_key=True, index=True, comment="The unique key for the setting.")
    value = Column(Text, nullable=False, comment="The value of the setting.")