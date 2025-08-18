# app/db/models/exam_batch.py
from sqlalchemy import String, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from datetime import datetime
from app.db.base import Base


class ExamBatch(Base):
    __tablename__ = "exam_batches"  # type: ignore

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    study_instance_uid: Mapped[str] = mapped_column(String, nullable=False, index=True)
    destination_id: Mapped[int] = mapped_column(ForeignKey("storage_backend_configs.id"), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="PENDING",
                                        index=True)  # PENDING, READY, SENDING, SENT, FAILED
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    destination: Mapped["StorageBackendConfig"] = relationship()  # type: ignore # noqa: F821
    instances: Mapped[list["ExamBatchInstance"]] = relationship(back_populates="batch", cascade="all, delete-orphan")


class ExamBatchInstance(Base):
    __tablename__ = "exam_batch_instances"  # type: ignore

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("exam_batches.id"), nullable=False)
    processed_filepath: Mapped[str] = mapped_column(String, nullable=False)
    sop_instance_uid: Mapped[str] = mapped_column(String, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    batch: Mapped["ExamBatch"] = relationship(back_populates="instances")
