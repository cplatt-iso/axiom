# app/crud/crud_processed_study_log.py
import logging
from datetime import datetime, timezone
from typing import Optional, List # ADDED List

from sqlalchemy.orm import Session
from sqlalchemy import select, exists

# Import the model and Enum
from app.db.models import ProcessedStudyLog
from app.schemas.enums import ProcessedStudySourceType

logger = logging.getLogger(__name__)

class CRUDProcessedStudyLog:
    """
    CRUD operations for the ProcessedStudyLog table.
    Primarily used to check for existing records and add new ones.
    """

    def check_exists(
        self,
        db: Session,
        *,
        source_type: ProcessedStudySourceType,
        source_id: str, # MODIFIED: int to str
        study_instance_uid: str
    ) -> bool:
        """
        Checks if a log entry already exists for a given source/study UID combination.
        """
        logger.debug(f"Checking ProcessedStudyLog for {source_type.value} source ID {source_id}, Study UID {study_instance_uid[:10]}...")
        stmt = select(exists().where(
            ProcessedStudyLog.source_type == source_type,
            ProcessedStudyLog.source_id == source_id, # type: ignore[arg-type] # SQLAlchemy handles type adaptation if DB col is different; ensure model matches
            ProcessedStudyLog.study_instance_uid == study_instance_uid
        ))
        result = db.execute(stmt).scalar()
        logger.debug(f"Log entry exists: {result}")
        return result or False # Ensure boolean return

    def create_log_entry(
        self,
        db: Session,
        *,
        source_type: ProcessedStudySourceType,
        source_id: str, # MODIFIED: int to str
        study_instance_uid: str,
        commit: bool = True # Allow controlling commit for background tasks
    ) -> Optional[ProcessedStudyLog]:
        """
        Creates a new log entry. Assumes existence check was done previously.
        Handles commit internally if requested (useful for tasks).
        """
        logger.info(f"Creating ProcessedStudyLog entry for {source_type.value} source ID {source_id}, Study UID {study_instance_uid[:10]}...")
        db_obj = ProcessedStudyLog(
            source_type=source_type,
            source_id=source_id, # type: ignore[arg-type] # SQLAlchemy handles type adaptation
            study_instance_uid=study_instance_uid
            # first_seen_at and retrieval_queued_at will use server_default
        )
        db.add(db_obj)
        try:
            if commit:
                db.commit()
                db.refresh(db_obj) # Get DB defaults like timestamps
                logger.info(f"Successfully created and committed log entry ID {db_obj.id}")
            else:
                # If not committing here, the calling function is responsible.
                # Refresh won't work until commit.
                db.flush() # Make the object available in the session if needed immediately
                logger.info(f"Added log entry for {study_instance_uid} to session (commit pending).")
                # Cannot refresh without commit, return potentially transient object
            return db_obj
        except Exception as e:
             logger.error(f"Error creating processed study log entry: {e}", exc_info=True)
             if commit: # Only rollback if we were supposed to commit here
                 db.rollback()
             # If flush failed, it might implicitly rollback depending on session state
             return None

    def get_processed_study_uids_for_source(
        self,
        db: Session,
        *,
        source_type: ProcessedStudySourceType,
        source_id: str, # MODIFIED: Consistent with model
        study_uids_to_check: List[str]
    ) -> List[str]:
        """
        Given a list of study UIDs, returns those that already exist in the log
        for the specified source type and source ID.
        """
        if not study_uids_to_check:
            logger.debug(f"No study UIDs provided to check for {source_type.value} source ID {source_id}.")
            return []

        logger.debug(f"Querying ProcessedStudyLog for {source_type.value} source ID {source_id}, checking {len(study_uids_to_check)} UIDs.")
        stmt = (
            select(ProcessedStudyLog.study_instance_uid)
            .where(
                ProcessedStudyLog.source_type == source_type,
                ProcessedStudyLog.source_id == source_id, # type: ignore[arg-type] # SQLAlchemy handles type adaptation
                ProcessedStudyLog.study_instance_uid.in_(study_uids_to_check)
            )
        )
        result = db.execute(stmt).scalars().all()
        logger.debug(f"Found {len(result)} matching processed study UIDs in log.")
        return list(result)


# Create a singleton instance
crud_processed_study_log = CRUDProcessedStudyLog()
