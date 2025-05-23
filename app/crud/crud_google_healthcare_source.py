# backend/app/crud/crud_google_healthcare_source.py
from typing import Any, Dict, Optional, Union, List

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError 

from app.crud.base import CRUDBase
from app.db.models.google_healthcare_source import GoogleHealthcareSource
from app.schemas.google_healthcare_source import GoogleHealthcareSourceCreate, GoogleHealthcareSourceUpdate

from app.db.models.processed_study_log import ProcessedStudyLog
from app.schemas.enums import ProcessedStudySourceType

import structlog # For logging within CRUD
logger = structlog.get_logger(__name__)

class CRUDGoogleHealthcareSource(CRUDBase[GoogleHealthcareSource, GoogleHealthcareSourceCreate, GoogleHealthcareSourceUpdate]):
    def get_by_name(self, db: Session, *, name: str) -> Optional[GoogleHealthcareSource]:
        return db.query(GoogleHealthcareSource).filter(GoogleHealthcareSource.name == name).first()

    def create(self, db: Session, *, obj_in: GoogleHealthcareSourceCreate) -> GoogleHealthcareSource:
        existing = self.get_by_name(db, name=obj_in.name)
        if existing:
            raise ValueError(f"Google Healthcare Source with name '{obj_in.name}' already exists.")
        return super().create(db, obj_in=obj_in)

    def update(
        self,
        db: Session,
        *,
        db_obj: GoogleHealthcareSource,
        obj_in: Union[GoogleHealthcareSourceUpdate, Dict[str, Any]]
    ) -> GoogleHealthcareSource:
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)

        if "name" in update_data and update_data["name"] != db_obj.name:
            existing = self.get_by_name(db, name=update_data["name"])
            if existing and existing.id != db_obj.id:
                raise ValueError(f"Another Google Healthcare Source with name '{update_data['name']}' already exists.")
        if update_data.get("is_active") is True:
            if db_obj.is_enabled is False and update_data.get("is_enabled") is not True:
                 raise ValueError("Cannot activate a source that is not enabled. Set 'is_enabled' to true as well.")
        elif update_data.get("is_enabled") is False:
            if db_obj.is_active is True and update_data.get("is_active") is not False:
                 raise ValueError("Cannot disable a source that is still active. Set 'is_active' to false as well.")
        return super().update(db, db_obj=db_obj, obj_in=update_data)

    def get_multi_enabled(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[GoogleHealthcareSource]:
        return (
            db.query(self.model)
            .filter(GoogleHealthcareSource.is_enabled == True)
            .offset(skip)
            .limit(limit)
            .all()
        )

    def get_multi_active(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[GoogleHealthcareSource]:
         return (
             db.query(self.model)
             .filter(GoogleHealthcareSource.is_active == True, GoogleHealthcareSource.is_enabled == True)
             .offset(skip)
             .limit(limit)
             .all()
         )

    # --- NEW METHOD ---
    def log_processed_study(
        self, db: Session, *, source_id: int, study_uid: str
    ) -> bool:
        """
        Logs a study as processed for a given Google Healthcare source by creating
        an entry in ProcessedStudyLog. Prevents duplicate processing.

        Args:
            db: SQLAlchemy session.
            source_id: The ID of the GoogleHealthcareSource.
            study_uid: The StudyInstanceUID that was processed.

        Returns:
            True if a new log entry was created (implying the study was not previously logged
            for this source and can be considered "counted" as processed for the first time).
            False if the study was already logged for this source or if an error occurred.
        """
        log = logger.bind(ghc_source_id=source_id, study_instance_uid=study_uid)
        try:
            # ProcessedStudyLog.source_id is a string.
            # ProcessedStudyLog.source_type is ProcessedStudySourceType enum.
            existing_log = db.query(ProcessedStudyLog).filter(
                ProcessedStudyLog.source_type == ProcessedStudySourceType.GOOGLE_HEALTHCARE,
                ProcessedStudyLog.source_id == str(source_id), # Compare with string version of source_id
                ProcessedStudyLog.study_instance_uid == study_uid
            ).first()

            if existing_log:
                log.debug("Study already logged as processed for this GHC source.")
                return False # Already processed, no new "count"

            new_log_entry = ProcessedStudyLog(
                source_type=ProcessedStudySourceType.GOOGLE_HEALTHCARE,
                source_id=str(source_id), # Store source_id as string
                study_instance_uid=study_uid
                # first_seen_at and retrieval_queued_at will use server_default
            )
            db.add(new_log_entry)
            # The actual commit will be handled by the calling task in task_executors.py
            log.info("New ProcessedStudyLog entry added to session for GHC source.")
            return True # New log created
        except IntegrityError: # Should be caught by the .first() check, but defensive
            db.rollback()
            log.warning("IntegrityError, likely concurrent attempt to log the same GHC study. Rolled back.")
            return False
        except Exception as e:
            db.rollback()
            log.error("Failed to log processed study for GHC source.", error=str(e), exc_info=True)
            return False

google_healthcare_source = CRUDGoogleHealthcareSource(GoogleHealthcareSource)
