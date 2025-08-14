# app/crud/crud_sender_config.py
from typing import List, Optional
from sqlalchemy.orm import Session

from .base import CRUDBase
from app.db.models.sender_config import SenderConfig
from app.schemas.sender_config import SenderConfigCreate, SenderConfigUpdate

class CRUDSenderConfig(CRUDBase[SenderConfig, SenderConfigCreate, SenderConfigUpdate]):
    def get_by_name(self, db: Session, *, name: str) -> Optional[SenderConfig]:
        return db.query(SenderConfig).filter(SenderConfig.name == name).first()

    def get_multi_enabled(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[SenderConfig]:
        return (
            db.query(self.model)
            .filter(SenderConfig.is_enabled == True)
            .offset(skip)
            .limit(limit)
            .all()
        )

    def get_by_sender_type(self, db: Session, *, sender_type: str) -> List[SenderConfig]:
        return db.query(SenderConfig).filter(SenderConfig.sender_type == sender_type).all()

crud_sender_config = CRUDSenderConfig(SenderConfig)
