# /app/crud/crud_mpps.py
from sqlalchemy.orm import Session
from typing import Optional

from app.crud.base import CRUDBase
from app.db.models.mpps import Mpps
from app.schemas.mpps import MppsCreate, MppsUpdate

class CRUDMpps(CRUDBase[Mpps, MppsCreate, MppsUpdate]):
    def get_by_sop_instance_uid(self, db: Session, *, sop_instance_uid: str) -> Optional[Mpps]:
        return db.query(Mpps).filter(Mpps.sop_instance_uid == sop_instance_uid).first()

mpps = CRUDMpps(Mpps)
