# --- MPPS Schemas ---
from .mpps import (
    MppsBase,
    MppsCreate,
    MppsUpdate,
    MppsRead,
)

# --- Maintenance Schemas ---
from .maintenance import (
    MaintenanceConfigBase,
    MaintenanceConfigCreate,
    MaintenanceConfigUpdate,
)

# --- System Config Schemas ---
from .system_config import (
    SystemConfigCategory,
    SystemConfigRead,
    SystemConfigUpdate,
    SystemConfigBulkUpdate,
)

# --- Logging Config Schemas ---
from .logging_config import (
    ElasticsearchConfig,
    FluentdConfig,
    LoggingConfig,
    LoggingConfigResponse,
    ConnectionTestResult,
)

# --- Maintenance Additional Schemas ---
from .maintenance import (
    MaintenanceConfigRead,
    MaintenanceConfigWithDataCleaner,
    MaintenanceTaskBase,
    MaintenanceTaskCreate,
    MaintenanceTaskUpdate,
    MaintenanceTaskRead,
    MaintenanceTaskWithDetails,
    DataCleanerConfigBase,
    DataCleanerConfigCreate,
    DataCleanerConfigUpdate,
    DataCleanerConfigRead,
    MaintenanceStatusResponse,
    TaskExecutionRequest,
    TaskExecutionResponse,
)

# --- Facility and Modality Schemas ---
from .facility import (
    FacilityBase,
    FacilityCreate,
    FacilityUpdate,
    Facility,
    FacilityModalityInfo,
)
from .modality import (
    ModalityBase,
    ModalityCreate,
    ModalityUpdate,
    Modality,
)

# --- Rule Schemas ---
from .rule import (
    # Base/Common
    MatchOperation,             # Enum
    ModifyAction,               # Enum
    RuleSetExecutionMode,       # Enum
    MatchCriterion,
    AssociationMatchCriterion,  # <--- ADDED THIS
    # --- Specific Tag Modification Schemas (assuming they are defined in rule.py) ---
    # Import the base/union type first
    TagModification,
    # Import specific subtypes if needed elsewhere (often not needed directly)
    # TagSetModification,
    # TagDeleteModification,
    # TagPrependModification,
    # TagSuffixModification,
    # TagRegexReplaceModification,
    # TagCopyModification,
    # TagMoveModification,
    # TagCrosswalkModification,
    # --- End Specific Tag Modification Schemas ---
    # Rule
    RuleBase,
    RuleCreate,
    RuleUpdate,
    Rule,                       # Read model (likely includes destinations)
    # RuleSet
    RuleSetBase,
    RuleSetCreate,
    RuleSetUpdate,
    RuleSet,                    # Read model (likely includes rules)
    RuleSetSummary,             # Add if defined and needed
)

# --- User & Auth Schemas ---
from .user import (
    UserBase,
    # UserCreate, # Using Google Auth primarily
    UserUpdate,
    User,                       # Read model
    RoleCreate,
    Role,                       # Read model
)
from .token import (
    GoogleToken,
    TokenResponse,
    TokenPayload,
)
from .api_key import (
    ApiKeyBase,
    ApiKeyCreate,
    ApiKeyCreateResponse,
    ApiKeyUpdate,
    ApiKey,                     # Read model
)

# --- Health & System Status Schemas ---
from .health import (
    ComponentStatus,
    HealthCheckResponse,
)
from .system import ( # Renamed for clarity from your example
    DicomWebSourceStatus,
    DicomWebPollersStatusResponse,
    DimseListenerStatus,
    DimseListenersStatusResponse,
    DimseListenerFullStatus,
    DimseListenersFullStatusResponse,
    DimseQrSourceStatus,
    DimseQrSourcesStatusResponse,
    DiskUsageStats, # ADDED
    DirectoryUsageStats, # Already present in some contexts, ensure it's here
    SystemInfo, # ADDED
)

# --- DICOMweb & DIMSE Config Schemas ---
from .dicomweb import (
    AuthType,                   # Enum
    DicomWebSourceConfigBase,
    DicomWebSourceConfigCreate,
    DicomWebSourceConfigUpdate,
    DicomWebSourceConfigRead,
    # --- Add STOWResponse related schemas if used directly ---
    # ReferencedSOP,
    # FailedSOP,
    # STOWResponse,
    # FailureReasonCode, # Enum
)
from .dimse_listener_config import (
    DimseListenerConfigBase,
    DimseListenerConfigCreate,
    DimseListenerConfigUpdate,
    DimseListenerConfigRead,
)
# Add DimseListenerState if needed for API responses
# from .dimse_listener_state import DimseListenerState, DimseListenerStateCreate, DimseListenerStateUpdate
from .dimse_qr_source import (
    DimseQueryRetrieveSourceBase,
    DimseQueryRetrieveSourceCreate,
    DimseQueryRetrieveSourceUpdate,
    DimseQueryRetrieveSourceRead,
    DimseQueryRetrieveSourceCreatePayload,
    DimseQueryRetrieveSourceUpdatePayload,
)

# --- Storage & Processing Schemas ---
from .storage_backend_config import (
    StorageBackendConfigBase,
    StorageBackendConfigCreate,
    StorageBackendConfigUpdate,
    StorageBackendConfigRead,
)
from .processing import (
    JsonProcessRequest,
    JsonProcessResponse,
    # --- Add ProcessedStudyLog schemas if used directly by API ---
    # ProcessedStudyLog,
    # ProcessedStudyLogCreate,
)

# --- Crosswalk Schemas ---
from .crosswalk import (
    CrosswalkDbType,            # Enum
    CrosswalkSyncStatus,        # Enum
    CrosswalkDataSourceBase,
    CrosswalkDataSourceCreate,
    CrosswalkDataSourceUpdate,
    CrosswalkDataSourceRead,
    CrosswalkMapBase,
    CrosswalkMapCreate,
    CrosswalkMapUpdate,
    CrosswalkMapRead,
)

# --- Schedule Schemas ---
from .schedule import (
    TimeRange,                  # Schema for time ranges
    ScheduleBase,
    ScheduleCreate,
    ScheduleUpdate,
    ScheduleRead
)

# --- AI Assist Schemas ---
from .ai_assist import (
    RuleGenRequest,
    RuleGenSuggestion,
    RuleGenResponse,
)

from .google_healthcare_source import (
    GoogleHealthcareSourceBase,
    GoogleHealthcareSourceCreate,
    GoogleHealthcareSourceUpdate,
    GoogleHealthcareSourceRead, # Assuming this is correct and Pylance might be having a moment
)

from .ai_prompt_config import (
    AIPromptConfigBase,
    AIPromptConfigCreate,
    AIPromptConfigUpdate,
    AIPromptConfigRead,
    AIPromptConfigSummary,
    AIPromptConfigInDBBase 
)

from .imaging_order import (
    ImagingOrderBase,               # Read model
    ImagingOrderCreate,        # Create model
    ImagingOrderUpdate,        # Update model
    OrderStatus,               # Enum for order status
    ImagingOrderRead,
    ImagingOrderReadResponse
)
from .order_dicom_evidence import (
    OrderDicomEvidenceBase,
    OrderDicomEvidenceCreate,
    OrderDicomEvidenceUpdate,
    OrderDicomEvidenceRead,
    OrderDicomEvidenceSummary
)
from .system_setting import (
    SystemSettingCreate,
    SystemSettingUpdate,
    SystemSettingRead
)
from .system_config import (
    SystemConfigCategory,
    SystemConfigRead,
    SystemConfigUpdate,
    SystemConfigBulkUpdate
)
from .spanner import (
    SpannerConfigCreate,
    SpannerConfigUpdate, 
    SpannerConfigRead,
    SpannerConfigListResponse,
    SpannerSourceMappingCreate,
    SpannerSourceMappingUpdate,
    SpannerSourceMappingRead,
    SpannerSourceMappingListResponse,
    SpannerQueryLogRead,
    SpannerQueryLogListResponse,
    SpannerTestRequest,
    SpannerTestResult,
    FailureStrategy,
    DeduplicationStrategy,
    CMoveStrategy,
    QueryStatus
)
# --- Data Browser Schemas (if defined) ---
# from .data_browser import QueryLevel, DataBrowserQuery, DataBrowserResultItem, DataBrowserResult


# Optional: Define __all__ to control 'from app.schemas import *' behavior
# This explicitly lists what gets imported with *. It's good practice but verbose.
__all__ = [
    # Rule Schemas
    "MatchOperation", "ModifyAction", "RuleSetExecutionMode", "MatchCriterion",
    "AssociationMatchCriterion", "TagModification", "RuleBase", "RuleCreate",
    "RuleUpdate", "Rule", "RuleSetBase", "RuleSetCreate", "RuleSetUpdate",
    "RuleSet", "RuleSetSummary",
    # User & Auth
    "UserBase", "UserUpdate", "User", "RoleCreate", "Role",
    "GoogleToken", "TokenResponse", "TokenPayload",
    "ApiKeyBase", "ApiKeyCreate", "ApiKeyCreateResponse", "ApiKeyUpdate", "ApiKey",
    # Health & System
    "ComponentStatus", "HealthCheckResponse", "SystemInfo", "DiskUsageStats", # MODIFIED DiskUsage to DiskUsageStats
    "DicomWebSourceStatus", "DicomWebPollersStatusResponse", "DimseListenerStatus",
    "DimseListenersStatusResponse", "DimseListenerFullStatus", "DimseListenersFullStatusResponse",
    "DimseQrSourceStatus", "DimseQrSourcesStatusResponse",
    "DirectoryUsageStats", # ADDED if it wasn't explicitly in __all__ but imported
    # DICOMweb & DIMSE Config
    "AuthType", "DicomWebSourceConfigBase", "DicomWebSourceConfigCreate",
    "DicomWebSourceConfigUpdate", "DicomWebSourceConfigRead",
    "DimseListenerConfigBase", "DimseListenerConfigCreate", "DimseListenerConfigUpdate",
    "DimseListenerConfigRead", "DimseQueryRetrieveSourceBase",
    "DimseQueryRetrieveSourceCreate", "DimseQueryRetrieveSourceUpdate",
    "DimseQueryRetrieveSourceRead", "DimseQueryRetrieveSourceCreatePayload",
    "DimseQueryRetrieveSourceUpdatePayload",
    # Storage & Processing
    "StorageBackendConfigBase", "StorageBackendConfigCreate",
    "StorageBackendConfigUpdate", "StorageBackendConfigRead",
    "JsonProcessRequest", "JsonProcessResponse",
    # Crosswalk
    "CrosswalkDbType", "CrosswalkSyncStatus", "CrosswalkDataSourceBase",
    "CrosswalkDataSourceCreate", "CrosswalkDataSourceUpdate", "CrosswalkDataSourceRead",
    "CrosswalkMapBase", "CrosswalkMapCreate", "CrosswalkMapUpdate", "CrosswalkMapRead",
    # Schedule
    "TimeRange", "ScheduleBase", "ScheduleCreate", "ScheduleUpdate", "ScheduleRead",
    # AI Assist
    "RuleGenRequest", "RuleGenSuggestion", "RuleGenResponse",
    # Google Healthcare Source
    "GoogleHealthcareSourceBase", "GoogleHealthcareSourceCreate",
    "GoogleHealthcareSourceUpdate", "GoogleHealthcareSourceRead", # ADDED
    # AI Prompt Config
    "AIPromptConfigBase",
    "AIPromptConfigCreate",
    "AIPromptConfigUpdate",
    "AIPromptConfigRead",
    "AIPromptConfigSummary",
    "AIPromptConfigInDBBase",
    "ImagingOrderBase",               # Read model
    "ImagingOrderCreate",        # Create model
    "ImagingOrderUpdate",    
    "ImagingOrderRead",    # Update model
    "ImagingOrderReadResponse",  # Assuming this is a response model for the Orders API
    "OrderStatus",
    # Order DICOM Evidence
    "OrderDicomEvidenceBase", "OrderDicomEvidenceCreate", "OrderDicomEvidenceUpdate", 
    "OrderDicomEvidenceRead", "OrderDicomEvidenceSummary", 
    # MPPS
    "MppsBase", "MppsCreate", "MppsUpdate", "MppsRead",
    # Facility and Modality
    "FacilityBase", "FacilityCreate", "FacilityUpdate", "Facility", 
    "FacilityModalityInfo",
    "ModalityBase", "ModalityCreate", "ModalityUpdate", "Modality", 
    # System Settings
    "SystemSettingCreate",
    "SystemSettingUpdate",
    "SystemSettingRead",
]
