# app/schemas/imaging_order.py
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime, date
from .enums import OrderStatus

# Shared properties
class ImagingOrderBase(BaseModel):
    patient_name: Optional[str] = Field(None, max_length=255)
    patient_id: str = Field(..., max_length=128, description="Patient's Medical Record Number (MRN).")
    patient_dob: Optional[date] = None
    patient_sex: Optional[str] = Field(None, max_length=16)

    accession_number: str = Field(..., max_length=64, description="Unique identifier for the imaging study.")
    placer_order_number: Optional[str] = Field(None, max_length=128)
    filler_order_number: Optional[str] = Field(None, max_length=128)

    requested_procedure_description: Optional[str] = Field(None, max_length=255)
    requested_procedure_code: Optional[str] = Field(None, max_length=64)
    modality: str = Field(..., max_length=16)

    scheduled_station_ae_title: Optional[str] = Field(None, max_length=16)
    scheduled_station_name: Optional[str] = Field(None, max_length=128)
    scheduled_procedure_step_start_datetime: Optional[datetime] = None

    requesting_physician: Optional[str] = Field(None, max_length=255)
    referring_physician: Optional[str] = Field(None, max_length=255)

    order_status: OrderStatus = Field(default=OrderStatus.SCHEDULED)
    study_instance_uid: Optional[str] = Field(None, max_length=128, description="DICOM Study Instance UID.")

    source: str = Field(..., description="Source system identifier.")
    raw_hl7_message: Optional[str] = Field(None, description="The raw HL7 message received.")

# Properties to receive on item creation
class ImagingOrderCreate(ImagingOrderBase):
    pass # All fields are required via Base

# Properties to receive on item update
class ImagingOrderUpdate(BaseModel):
    # Almost everything is optional on an update
    patient_name: Optional[str] = None
    patient_dob: Optional[date] = None
    patient_sex: Optional[str] = None
    placer_order_number: Optional[str] = None
    filler_order_number: Optional[str] = None
    requested_procedure_description: Optional[str] = None
    requested_procedure_code: Optional[str] = None
    modality: Optional[str] = None
    scheduled_station_ae_title: Optional[str] = None
    scheduled_station_name: Optional[str] = None
    scheduled_procedure_step_start_datetime: Optional[datetime] = None
    requesting_physician: Optional[str] = None
    referring_physician: Optional[str] = None
    order_status: Optional[OrderStatus] = None
    study_instance_uid: Optional[str] = None
    raw_hl7_message: Optional[str] = None

# Properties to return to client
class ImagingOrderRead(ImagingOrderBase):
    id: int
    created_at: datetime
    updated_at: datetime
    order_received_at: datetime

    model_config = ConfigDict(from_attributes=True)