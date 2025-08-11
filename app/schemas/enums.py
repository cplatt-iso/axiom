# /app/schemas/enums.py

import enum

class ProcessedStudySourceType(str, enum.Enum):
    """
    Defines the types of sources from which a DICOM study can be processed.
    Moved from app/db/models/processed_study_log.py for centralization.
    """
    DICOMWEB = "DICOMWEB"
    DIMSE_QR = "DIMSE_QR"
    DIMSE_LISTENER = "DIMSE_LISTENER"
    STOW_RS = "STOW_RS"
    GOOGLE_HEALTHCARE = "GOOGLE_HEALTHCARE"
    FILE_UPLOAD = "FILE_UPLOAD"
    UNKNOWN = "UNKNOWN" # Good to have a fallback

class ExceptionProcessingStage(str, enum.Enum):
    """
    Defines the specific stage in the processing pipeline where an exception occurred.
    """
    INGESTION = "INGESTION"                 # Initial reception, file reading, basic parsing
    RULE_EVALUATION = "RULE_EVALUATION"     # During matching criteria checks
    TAG_MORPHING = "TAG_MORPHING"           # Applying standard tag modifications
    AI_STANDARDIZATION = "AI_STANDARDIZATION" # During AI-based processing
    DESTINATION_SEND = "DESTINATION_SEND"   # Sending to a configured storage backend
    DATABASE_INTERACTION = "DATABASE_INTERACTION" # Errors during core DB ops within processing
    POST_PROCESSING = "POST_PROCESSING"     # Errors in final steps like file disposition
    UNKNOWN = "UNKNOWN"                     # Fallback for unclassified errors

class ExceptionStatus(str, enum.Enum):
    """
    Defines the lifecycle status of a logged DICOM processing exception.
    """
    NEW = "NEW"  # Exception just logged, pending review or automatic retry
    RETRY_PENDING = "RETRY_PENDING"          # Marked for an automatic retry attempt
    RETRY_IN_PROGRESS = "RETRY_IN_PROGRESS"  # An automatic retry attempt is currently active
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED" # Needs human intervention
    RESOLVED_BY_RETRY = "RESOLVED_BY_RETRY"  # Successfully processed after one or more retries
    RESOLVED_MANUALLY = "RESOLVED_MANUALLY"  # Resolved by manual action (e.g., UI intervention, data correction)
    FAILED_PERMANENTLY = "FAILED_PERMANENTLY" # All retry attempts exhausted, or deemed unresolvable
    ARCHIVED = "ARCHIVED"                    # Exception record is archived, no longer active

class OrderStatus(str, enum.Enum):
    SCHEDULED = "SCHEDULED"
    IN_PROGRESS = "IN_PROGRESS"
    CANCELED = "CANCELED"
    COMPLETED = "COMPLETED"
    DISCONTINUED = "DISCONTINUED" # Different from canceled
    UNKNOWN = "UNKNOWN"
    FAILED = "FAILED"

class Modality(str, enum.Enum):
    CR = "CR"
    CT = "CT"
    MR = "MR"
    US = "US"
    XA = "XA"
    OT = "OT" # Other

class MppsStatus(str, enum.Enum):
    """
    Represents the status of a Modality Performed Procedure Step (MPPS).
    """
    IN_PROGRESS = "IN PROGRESS"
    COMPLETED = "COMPLETED"
    DISCONTINUED = "DISCONTINUED"

class HealthStatus(str, enum.Enum):
    """Health status of a scraper source."""
    UNKNOWN = "UNKNOWN"
    OK = "OK"
    DOWN = "DOWN"
    ERROR = "ERROR"