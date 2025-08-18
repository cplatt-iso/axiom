# app/schemas/exam_batch.py
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class ExamBatchInstanceBase(BaseModel):
    processed_filepath: str
    sop_instance_uid: str


class ExamBatchInstanceCreate(ExamBatchInstanceBase):
    batch_id: int


class ExamBatchInstanceUpdate(BaseModel):
    processed_filepath: Optional[str] = None
    sop_instance_uid: Optional[str] = None


class ExamBatchInstance(ExamBatchInstanceBase):
    id: int
    batch_id: int
    created_at: datetime

    class Config:
        from_attributes = True


class ExamBatchBase(BaseModel):
    study_instance_uid: str
    destination_id: int
    status: str = "PENDING"


class ExamBatchCreate(ExamBatchBase):
    pass


class ExamBatchUpdate(BaseModel):
    status: Optional[str] = None


class ExamBatch(ExamBatchBase):
    id: int
    created_at: datetime
    updated_at: datetime
    instances: List[ExamBatchInstance] = []

    class Config:
        from_attributes = True
