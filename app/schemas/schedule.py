# app/schemas/schedule.py
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import Optional, List, Dict, Any, Literal
from datetime import datetime, time
import re

# --- Helper Functions ---
TIME_REGEX = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$") # HH:MM (24-hour)
ALLOWED_DAYS = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}

def _validate_time_format(v: str) -> str:
    """Validates HH:MM format."""
    if not TIME_REGEX.match(v):
        raise ValueError("Time must be in HH:MM format (e.g., 08:00, 17:30).")
    return v

def _validate_days(days: List[str]) -> List[str]:
    """Validates the list of days."""
    if not days:
        raise ValueError("Days list cannot be empty for a time range.")
    normalized_days = set()
    for day in days:
        if not isinstance(day, str):
             raise ValueError("Each item in 'days' must be a string.")
        day_title = day.strip().title() # Normalize case (e.g., 'mon' -> 'Mon')
        if day_title not in ALLOWED_DAYS:
            raise ValueError(f"Invalid day '{day}'. Must be one of {', '.join(ALLOWED_DAYS)}.")
        normalized_days.add(day_title)
    # Return sorted list for consistency
    return sorted(list(normalized_days))

# --- Schema for a Single Time Range ---
class TimeRange(BaseModel):
    """Defines a single active time window within a schedule."""
    days: List[str] = Field(..., description=f"List of active days ({', '.join(ALLOWED_DAYS)}).")
    start_time: str = Field(..., description="Start time in HH:MM format (24-hour).")
    end_time: str = Field(..., description="End time in HH:MM format (24-hour). Can wrap overnight (e.g., 17:00-08:00).")

    # Add validators for fields within TimeRange
    _validate_days = field_validator('days')(_validate_days)
    _validate_start_time = field_validator('start_time')(_validate_time_format)
    _validate_end_time = field_validator('end_time')(_validate_time_format)

    # Optional: Add validation to ensure start != end if needed, though overnight makes this tricky
    # @model_validator(mode='after')
    # def check_start_not_equal_end(self) -> 'TimeRange':
    #     if self.start_time == self.end_time:
    #         raise ValueError("Start time and end time cannot be identical.")
    #     return self


# --- Schedule Schemas ---

class ScheduleBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="Unique name for the schedule.")
    description: Optional[str] = Field(None, description="Optional description.")
    is_enabled: bool = Field(True, description="Whether this schedule can be assigned to rules.")
    time_ranges: List[TimeRange] = Field(..., description="List of active time ranges.")


class ScheduleCreate(ScheduleBase):
    pass # Inherits fields and validation


class ScheduleUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    is_enabled: Optional[bool] = None
    # Allow replacing the entire list of time ranges
    time_ranges: Optional[List[TimeRange]] = Field(None, description="Replace existing time ranges with this list.")


class ScheduleRead(ScheduleBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
