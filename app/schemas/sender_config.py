# app/schemas/sender_config.py
from pydantic import BaseModel, Field, model_validator
from typing import Optional, Literal, Union, Annotated
from datetime import datetime

class SenderConfigBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique name for this sender configuration.")
    description: Optional[str] = Field(None, description="Optional description of the sender's purpose.")
    is_enabled: bool = Field(True, description="Whether this sender configuration is active and should be used.")

# --- Type-Specific Schemas ---

class PynetdicomSenderConfig(BaseModel):
    sender_type: Literal["pynetdicom"] = "pynetdicom"
    local_ae_title: str = Field("AXIOM_SCU", max_length=16, description="The AE Title our pynetdicom sender will use when associating.")
    # Add any other pynetdicom-specific fields here in the future

class Dcm4cheSenderConfig(BaseModel):
    sender_type: Literal["dcm4che"] = "dcm4che"
    local_ae_title: str = Field("AXIOM_SCU", max_length=16, description="The AE Title our dcm4che sender will use when associating.")
    # Add any other dcm4che-specific fields here in the future

# --- Create Schemas ---

class SenderConfigCreate_Pynetdicom(SenderConfigBase, PynetdicomSenderConfig):
    pass

class SenderConfigCreate_Dcm4che(SenderConfigBase, Dcm4cheSenderConfig):
    pass

SenderConfigCreate = Annotated[
    Union[
        SenderConfigCreate_Pynetdicom,
        SenderConfigCreate_Dcm4che,
    ],
    Field(discriminator="sender_type")
]

# --- Update Schema ---

class SenderConfigUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    is_enabled: Optional[bool] = None
    local_ae_title: Optional[str] = Field(None, max_length=16)

    @model_validator(mode='after')
    def check_at_least_one_value(self) -> 'SenderConfigUpdate':
        if not self.model_fields_set:
            raise ValueError("At least one field must be provided for update")
        return self

# --- Read Schemas ---

class SenderConfigRead_Pynetdicom(SenderConfigBase, PynetdicomSenderConfig):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class SenderConfigRead_Dcm4che(SenderConfigBase, Dcm4cheSenderConfig):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

SenderConfigRead = Annotated[
    Union[
        SenderConfigRead_Pynetdicom,
        SenderConfigRead_Dcm4che
    ],
    Field(discriminator="sender_type")
]
