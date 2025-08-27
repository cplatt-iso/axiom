from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, Literal
from enum import Enum

class SystemConfigCategory(str, Enum):
    PROCESSING = "processing"
    DUSTBIN = "dustbin"
    BATCH_PROCESSING = "batch_processing"
    CLEANUP = "cleanup"
    CELERY = "celery"
    DICOMWEB = "dicomweb"
    AI = "ai"

class SystemConfigRead(BaseModel):
    key: str
    category: str
    value: Any
    type: Literal["boolean", "integer", "string"]
    description: str
    default: Any
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    is_modified: bool = Field(default=False, description="Whether this setting has been modified from default")

    class Config:
        from_attributes = True

class SystemConfigUpdate(BaseModel):
    value: Any

class SystemConfigBulkUpdate(BaseModel):
    settings: Dict[str, Any]
    ignore_errors: bool = Field(default=False, description="Continue processing even if some settings fail")
