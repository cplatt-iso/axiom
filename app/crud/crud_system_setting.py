from app.crud.base import CRUDBase
from app.db.models.system_setting import SystemSetting
from app.schemas.system_setting import SystemSettingCreate, SystemSettingUpdate
from sqlalchemy.orm import Session
from typing import Optional

class CRUDSystemSetting(CRUDBase[SystemSetting, SystemSettingCreate, SystemSettingUpdate]):
    def get_by_key(self, db: Session, key: str) -> Optional[SystemSetting]:
        return db.query(self.model).filter(self.model.key == key).first()

system_setting = CRUDSystemSetting(SystemSetting)
