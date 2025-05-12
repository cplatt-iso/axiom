# backend/app/schemas/ai_prompt_config.py
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
import uuid # We might use UUID for request/response IDs in future, but DB ID is int

# Re-using the tag validator from rule.py - good idea to move it to a common utils if used often
# For now, I'll assume we can define a similar one or import it.
# Let's define a simple one here for now for dicom_tag_keyword.
import re

def _validate_dicom_tag_keyword_format(v: str) -> str:
    if not isinstance(v, str):
        raise ValueError("DICOM tag keyword must be a string")
    v_stripped = v.strip()
    if not v_stripped: # Cannot be empty
        raise ValueError("DICOM tag keyword cannot be empty")
    # Basic check for Alphanumeric (DICOM Keyword)
    # Allows only letters and numbers, no spaces or special characters for keywords
    if not re.match(r"^[a-zA-Z0-9]+$", v_stripped):
        # If not a simple keyword, check for GGGG,EEEE format
        # This regex is a bit more forgiving with optional parentheses and spaces
        if not re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", v_stripped):
            raise ValueError(
                f"Tag '{v_stripped}' is not a valid DICOM keyword (e.g., 'PatientName') "
                f"or in GGGG,EEEE format (e.g., '0010,0010')."
            )
        # If it matches GGGG,EEEE, we could normalize it here if desired, e.g.:
        # match = re.match(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", v_stripped)
        # return f"{match.group(1).upper()},{match.group(2).upper()}"
    return v_stripped


class AIPromptConfigBase(BaseModel):
    name: str = Field(..., min_length=3, max_length=255,
                      description="Unique, human-readable name for this AI prompt configuration.")
    description: Optional[str] = Field(None, description="Optional detailed description.")
    dicom_tag_keyword: str = Field(
        ..., max_length=100,
        description="DICOM keyword of the tag this configuration targets (e.g., BodyPartExamined, StudyDescription)."
    )
    prompt_template: str = Field(
        ..., min_length=10,
        description="The prompt template. Must include '{value}'. Can also use '{dicom_tag_keyword}'."
    )
    model_identifier: str = Field(
        default="gemini-1.5-flash-001", # Made it a default instead of ...
        max_length=100,
        description="Identifier for the AI model to be used."
    )
    model_parameters: Optional[Dict[str, Any]] = Field(
        default=None, # Explicitly default to None
        example={"temperature": 0.2, "max_output_tokens": 50, "top_p": 0.9},
        description="JSON object for model-specific parameters."
    )

    @field_validator('dicom_tag_keyword')
    @classmethod
    def validate_tag_keyword(cls, v: str) -> str:
        return _validate_dicom_tag_keyword_format(v)

    # For Pydantic V2, when a field validator needs access to other fields' values,
    # you use `info: ValidationInfo` and access `info.data`.
    # However, for this specific validator, we only need the value of 'prompt_template' itself.
    @field_validator('prompt_template')
    @classmethod
    def validate_prompt_template_placeholders(cls, v: str) -> str:
        if "{value}" not in v:
            raise ValueError("Prompt template must include the '{value}' placeholder.")
        # '{dicom_tag_keyword}' is optional in the prompt, so no validation for it unless specified.
        return v

class AIPromptConfigCreate(AIPromptConfigBase):
    pass

class AIPromptConfigUpdate(BaseModel):
    # All fields are optional for partial updates
    name: Optional[str] = Field(default=None, min_length=3, max_length=255)
    description: Optional[str] = Field(default=None)
    dicom_tag_keyword: Optional[str] = Field(default=None, max_length=100)
    prompt_template: Optional[str] = Field(default=None, min_length=10)
    model_identifier: Optional[str] = Field(default=None, max_length=100)
    model_parameters: Optional[Dict[str, Any]] = Field(default=None) # Allows setting to null explicitly

    @field_validator('dicom_tag_keyword')
    @classmethod
    def validate_tag_keyword_update(cls, v: Optional[str]) -> Optional[str]:
        if v is not None: # Only validate if provided
            return _validate_dicom_tag_keyword_format(v)
        return v # Return None if None was passed

    @field_validator('prompt_template')
    @classmethod
    def validate_prompt_template_update(cls, v: Optional[str]) -> Optional[str]:
        if v is not None: # Only validate if provided
            if "{value}" not in v:
                raise ValueError("Prompt template must include the '{value}' placeholder.")
        return v # Return None if None was passed or return validated string

    # Pydantic V2: model_validator can be mode='before' or mode='after'
    # 'before' is like pre=True, 'after' is like pre=False (default)
    @model_validator(mode='after') # Check after individual fields are validated
    def ensure_at_least_one_field_is_not_none(self) -> 'AIPromptConfigUpdate':
        # self here is the model instance. We can iterate through its fields.
        # For Pydantic v2, self.model_fields_set gives fields explicitly set by the user
        if not self.model_fields_set:
             raise ValueError("At least one field must be provided for update.")
        # An alternative or additional check could be if all values are None,
        # but model_fields_set is more accurate for "was anything actually sent for update?"
        # all_none = all(value is None for field, value in self.__iter__())
        # if all_none and self.model_fields_set: # if fields were set but all to None
        #     pass # this might be allowed if you want to nullify fields
        return self

class AIPromptConfigInDBBase(AIPromptConfigBase): # Renamed for consistency if you have Base/InDBBase pattern
    id: int
    created_at: datetime
    updated_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

class AIPromptConfigRead(AIPromptConfigInDBBase): # This is the one you'll usually return
    pass

class AIPromptConfigSummary(BaseModel):
    id: int
    name: str
    dicom_tag_keyword: str
    model_identifier: str
    
    model_config = ConfigDict(from_attributes=True)