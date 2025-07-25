# app/schemas/imaging_order.py
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from datetime import datetime, date
from .enums import OrderStatus

# Shared properties
class ImagingOrderBase(BaseModel):
    patient_name: Optional[str] = Field(None, max_length=255)
    patient_id: str = Field(..., max_length=128, description="Patient's Medical Record Number (MRN).")
    patient_dob: Optional[date] = None
    patient_sex: Optional[str] = Field(None, max_length=16)
    # --- NEW PATIENT FIELDS ---
    patient_address: Optional[str] = Field(None, description="Patient's full address.")
    patient_phone_number: Optional[str] = Field(None, max_length=64, description="Patient's primary phone number.")
    patient_class: Optional[str] = Field(None, max_length=16, description="e.g., O (Outpatient), I (Inpatient).")
    visit_number: Optional[str] = Field(None, max_length=64, description="Patient's visit/encounter number.")
    # --- END NEW PATIENT FIELDS ---

    accession_number: str = Field(..., max_length=64, description="Unique identifier for the imaging study.")
    placer_order_number: Optional[str] = Field(None, max_length=128)
    filler_order_number: Optional[str] = Field(None, max_length=128)
    # --- NEW ORDER FIELDS ---
    placer_group_number: Optional[str] = Field(None, max_length=128, description="Groups related orders together.")
    # --- END NEW ORDER FIELDS ---

    requested_procedure_description: Optional[str] = Field(None, max_length=255)
    requested_procedure_code: Optional[str] = Field(None, max_length=64)

    scheduled_station_ae_title: Optional[str] = Field(None, max_length=16)
    scheduled_station_name: Optional[str] = Field(None, max_length=128)
    scheduled_procedure_step_start_datetime: Optional[datetime] = None

    requesting_physician: Optional[str] = Field(None, max_length=255)
    referring_physician: Optional[str] = Field(None, max_length=255)
    # --- NEW PHYSICIAN FIELDS ---
    attending_physician: Optional[str] = Field(None, max_length=255, description="Attending physician.")
    # --- END NEW PHYSICIAN FIELDS ---

    order_status: Optional[OrderStatus] = Field(default=OrderStatus.SCHEDULED)
    study_instance_uid: Optional[str] = Field(None, max_length=128, description="DICOM Study Instance UID.")

    source: str = Field(..., description="Source system identifier.")
    # --- NEW SOURCE FIELDS ---
    source_sending_application: Optional[str] = Field(None, max_length=255)
    source_sending_facility: Optional[str] = Field(None, max_length=255)
    source_receiving_application: Optional[str] = Field(None, max_length=255)
    source_receiving_facility: Optional[str] = Field(None, max_length=255)
    source_message_control_id: Optional[str] = Field(None, max_length=128)
    # --- END NEW SOURCE FIELDS ---
    raw_hl7_message: Optional[str] = Field(None, description="The raw HL7 message received.")


# Properties to receive on item creation
class ImagingOrderCreate(ImagingOrderBase):
    modality: str = Field(..., max_length=16)
    study_instance_uid: Optional[str] = None
    scheduled_station_name: Optional[str] = None

# Properties to receive on item update
class ImagingOrderUpdate(ImagingOrderBase):
    # Almost everything is optional on an update
    patient_name: Optional[str] = None
    patient_id: Optional[str] = Field(None, max_length=128, description="Patient's Medical Record Number (MRN).") # type: ignore # Allow updating patient ID? Risky, but possible.
    patient_dob: Optional[date] = None
    patient_sex: Optional[str] = None
    # --- NEW PATIENT FIELDS ---
    patient_address: Optional[str] = None
    patient_phone_number: Optional[str] = None
    patient_class: Optional[str] = None
    visit_number: Optional[str] = None
    # --- END NEW PATIENT FIELDS ---
    placer_order_number: Optional[str] = None
    filler_order_number: Optional[str] = None
    # --- NEW ORDER FIELDS ---
    placer_group_number: Optional[str] = None
    # --- END NEW ORDER FIELDS ---
    requested_procedure_description: Optional[str] = None
    requested_procedure_code: Optional[str] = None
    modality: Optional[str] = Field(None, max_length=16)
    scheduled_station_ae_title: Optional[str] = None
    scheduled_station_name: Optional[str] = None
    scheduled_procedure_step_start_datetime: Optional[datetime] = None
    requesting_physician: Optional[str] = None
    referring_physician: Optional[str] = None
    # --- NEW PHYSICIAN FIELDS ---
    attending_physician: Optional[str] = None
    # --- END NEW PHYSICIAN FIELDS ---
    order_status: Optional[OrderStatus] = None
    study_instance_uid: Optional[str] = None
    # --- NEW SOURCE FIELDS ---
    source_sending_application: Optional[str] = None
    source_sending_facility: Optional[str] = None
    source_receiving_application: Optional[str] = None
    source_receiving_facility: Optional[str] = None
    source_message_control_id: Optional[str] = None
    # --- END NEW SOURCE FIELDS ---
    raw_hl7_message: Optional[str] = None


# Properties to return to client
class ImagingOrderRead(ImagingOrderBase):
    id: int
    created_at: datetime
    updated_at: datetime
    order_received_at: datetime
    modality: str = Field(..., max_length=16)

    model_config = ConfigDict(from_attributes=True)

class ImagingOrderReadResponse(BaseModel):
    """Response model for paginated imaging orders."""
    items: List[ImagingOrderRead]
    total: int