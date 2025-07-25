from pydantic import BaseModel
from typing import Optional

# Shared properties
class SystemSettingBase(BaseModel):
    key: str
    value: str

# Properties to receive on item creation
class SystemSettingCreate(SystemSettingBase):
    pass

# Properties to receive on item update
class SystemSettingUpdate(BaseModel):
    value: str

# Properties to return to client
class SystemSettingRead(SystemSettingBase):
    class Config:
        orm_mode = True
