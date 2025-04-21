# app/schemas/dicomweb.py
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

# Based on PS3.18 Table 10.6.1-3. Attributes for Referenced SOP Sequence Item Macro
class ReferencedSOP(BaseModel):
    ReferencedSOPClassUID: str = Field(..., alias="00081150")
    ReferencedSOPInstanceUID: str = Field(..., alias="00081155")
    RetrieveURL: Optional[str] = Field(None, alias="00081190") # Optional WADO-RS Retrieve URL

# Based on PS3.18 Table 10.6.1-4. Attributes for Failed SOP Sequence Item Macro
class FailedSOP(BaseModel):
    ReferencedSOPClassUID: str = Field(..., alias="00081150")
    ReferencedSOPInstanceUID: str = Field(..., alias="00081155")
    FailureReason: int = Field(..., alias="00081197") # DICOM Failure Reason code
    ReasonDetail: Optional[str] = Field(None) # Custom field for more detail

# Based on PS3.18 Table 10.6.1-2. Attributes for STOW-RS Response Payload
class STOWResponse(BaseModel):
    TransactionUID: Optional[str] = Field(None, alias="00081195") # Optional
    ReferencedSOPSequence: Optional[List[ReferencedSOP]] = Field(None, alias="00081199")
    FailedSOPSequence: Optional[List[FailedSOP]] = Field(None, alias="00081198")
    # Include other potential response fields if needed later, e.g., warnings

    # Allow 'extra' fields for potential future DICOM attributes or custom extensions
    class Config:
        extra = 'allow'
        populate_by_name = True # Allow using DICOM tag aliases
        json_encoders = {
            # Add custom encoders if needed, e.g., for specific DICOM types
        }
        # Ensure aliases are used when generating JSON response
        by_alias = True


# --- Helper codes for FailureReason (0008,1197) ---
# Simplified subset based on PS3.7 Annex C - Status Codes
class FailureReasonCode:
    # Specific STOW related failure reasons (PS3.18 Section 10.6.1.3.1)
    ProcessingFailure = 0x0110  # Processing failure
    NoSuchAttribute = 0x0105 # No Such Attribute (e.g., missing required element for matching)
    InvalidAttributeValue = 0x0106 # Invalid Attribute Value
    RefusedSOPClassNotSupported = 0x0112 # SOP Class Not Supported
    RefusedOutOfResources = 0x0122 # Out of Resources
    # General Data Set or SOP Instance Error
    ErrorDataSetDoesNotMatchSOPClass = 0xA900
    ErrorCannotUnderstand = 0xC000 # Cannot understand/parse
    # Custom Axiom Flow specific codes (use ranges outside official DICOM codes if possible)
    RuleMatchingFailed = 0xF001 # Custom: Failed during rule matching phase
    DispatchFailed = 0xF002 # Custom: Failed during dispatch to destination
    StorageBackendError = 0xF003 # Custom: Storage backend reported an error
    QueuingFailed = 0xF004 # Custom: Failed to queue instance for processing
    BadRequestMultipart = 0xF100 # Custom: Request body was not valid multipart/related
    BadRequestMissingDicomPart = 0xF101 # Custom: Multipart message missing DICOM application/dicom parts
    BadRequestParsingError = 0xF102 # Custom: Error parsing a DICOM part
