from sqlalchemy.orm import Session
import structlog

from app import crud, schemas
from app.core.config import settings

# make sure all SQL Alchemy models are imported before initializing DB
# otherwise, SQL Alchemy might fail to initialize relationships properly
from app.db import base  # noqa: F401

log = structlog.get_logger(__name__)

def init_db(db: Session) -> None:
    """
    Initialize the database with default system settings.
    """
    initial_settings = {
        "DICOM_OID_ROOT": "2.16.840.1.114107.1.1.50",
        "GENERATE_STUDY_INSTANCE_UID": "false",
    }

    for key, value in initial_settings.items():
        setting = crud.system_setting.get_by_key(db, key=key)
        if not setting:
            setting_in = schemas.SystemSettingCreate(key=key, value=value)
            crud.system_setting.create(db, obj_in=setting_in)
            log.info("Created system setting", key=key, value=value)
