# app/crud/crud_exam_batch.py
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.crud.base import CRUDBase
from app.db.models.exam_batch import ExamBatch, ExamBatchInstance
from app.schemas.exam_batch import ExamBatchCreate, ExamBatchUpdate, ExamBatchInstanceCreate, ExamBatchInstanceUpdate
from fastapi.encoders import jsonable_encoder


class CRUDExamBatch(CRUDBase[ExamBatch, ExamBatchCreate, ExamBatchUpdate]):
    def get_by_study_and_destination(self, db: Session, *, study_instance_uid: str, destination_id: int) -> ExamBatch | None:
        return db.query(ExamBatch).filter(ExamBatch.study_instance_uid == study_instance_uid, ExamBatch.destination_id == destination_id).first()

    def get_ready_for_sending(self, db: Session, *, limit: int = 100) -> list[ExamBatch]:
        return db.query(ExamBatch).filter(ExamBatch.status == "READY").limit(limit).all()

    def update_status(self, db: Session, *, db_obj: ExamBatch, status: str) -> ExamBatch:
        from app.schemas.exam_batch import ExamBatchUpdate
        return super().update(db, db_obj=db_obj, obj_in=ExamBatchUpdate(status=status))


crud_exam_batch = CRUDExamBatch(ExamBatch)


class CRUDExamBatchInstance(CRUDBase[ExamBatchInstance, ExamBatchInstanceCreate, ExamBatchInstanceUpdate]):
    def create(self, db: Session, *, obj_in: ExamBatchInstanceCreate) -> ExamBatchInstance:
        """
        Create an ExamBatchInstance, handling duplicate SOP Instance UIDs gracefully.
        If a duplicate exists, update the existing record instead of failing.
        """
        obj_in_data = jsonable_encoder(obj_in)
        
        # First, try to find existing record with same SOP Instance UID and batch_id
        existing_instance = db.query(self.model).filter(
            self.model.sop_instance_uid == obj_in_data["sop_instance_uid"],
            self.model.batch_id == obj_in_data["batch_id"]
        ).first()
        
        if existing_instance:
            # Update existing record with new data
            for field, value in obj_in_data.items():
                if hasattr(existing_instance, field):
                    setattr(existing_instance, field, value)
            db.add(existing_instance)
            try:
                db.commit()
                db.refresh(existing_instance)
                return existing_instance
            except IntegrityError as e:
                db.rollback()
                raise ValueError(f"Database integrity error during update: {e.orig}") from e
        else:
            # Create new record
            db_obj = self.model(**obj_in_data)  # type: ignore
            db.add(db_obj)
            try:
                db.commit()
                db.refresh(db_obj)
                return db_obj
            except IntegrityError as e:
                db.rollback()
                # If we still get an integrity error, it might be a race condition
                # Try to find the record that was created by another process
                existing_instance = db.query(self.model).filter(
                    self.model.sop_instance_uid == obj_in_data["sop_instance_uid"],
                    self.model.batch_id == obj_in_data["batch_id"]
                ).first()
                if existing_instance:
                    return existing_instance
                raise ValueError(f"Database integrity error: {e.orig}") from e


crud_exam_batch_instance = CRUDExamBatchInstance(ExamBatchInstance)
